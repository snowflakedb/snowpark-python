#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from sklearn.cluster import KMeans
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.preprocessing import StandardScaler

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas


@pytest.fixture(scope="module")
def test_dfs():
    data = {
        "feature1": [1, 5, 3, 4, 4, 6, 7, 2, 9, 70],
        "feature2": [2, 4, 1, 3, 5, 7, 6, 3, 10, 9],
        "target": [0, 0, 1, 0, 1, 1, 1, 0, 1, 0],
    }
    return pd.DataFrame(data), native_pd.DataFrame(data)


def assert_sklearn_equal(got, expect, method):
    if method == "test_train_test_split":
        assert_snowpark_pandas_equal_to_pandas(got, expect, check_dtype=False)
    else:
        assert type(expect) == type(got)
        np.testing.assert_allclose(expect, got)


def test_train_test_split(test_dfs):
    df = test_dfs[0]
    X = df[["feature1", "feature2"]]
    y = df["target"]
    # Data Splitting
    (X_train, X_test, y_train, y_test) = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    native_df = test_dfs[1]
    native_X = native_df[["feature1", "feature2"]]
    native_y = native_df["target"]
    # Data Splitting
    (native_X_train, native_X_test, native_y_train, native_y_test) = train_test_split(
        native_X, native_y, test_size=0.2, random_state=42
    )
    assert_sklearn_equal(X_train, native_X_train, "test_train_test_split")
    assert_sklearn_equal(X_test, native_X_test, "test_train_test_split")
    assert_sklearn_equal(y_train, native_y_train, "test_train_test_split")
    assert_sklearn_equal(y_test, native_y_test, "test_train_test_split")


def test_classification(test_dfs):
    from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
    from sklearn.pipeline import make_pipeline

    df = test_dfs[0]
    lda = LinearDiscriminantAnalysis(solver="svd", store_covariance=True)
    X = df[["feature1", "feature2"]]
    y = df["target"]

    # Data Splitting
    (X_train, X_test, y_train, y_test) = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    native_X_train, native_X_test, native_y_train, native_y_test = (
        X_train._to_pandas(),
        X_test._to_pandas(),
        y_train._to_pandas(),
        y_test._to_pandas(),
    )
    clf = make_pipeline(StandardScaler(), lda)
    clf.fit(X_train, y_train)
    score = clf.score(X_test, y_test)
    native_score = clf.fit(native_X_train, native_y_train).score(
        native_X_test, native_y_test
    )
    assert_sklearn_equal(score, native_score, "scalar_function")


def test_regression(test_dfs):
    df = test_dfs[0]
    X = df[["feature1", "feature2"]]
    y = df["target"]

    # Data Splitting
    (X_train, X_test, y_train, y_test) = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    model = LogisticRegression()
    model.fit(X_train, y_train)

    # prediction phase
    y_pred = model.predict(X_test)
    accuracy = accuracy_score(y_test, y_pred)

    assert isinstance(accuracy, float)


def test_clustering():
    rng = np.random.default_rng(42)
    nsamps = 300
    X = rng.random((nsamps, 2))
    data = pd.DataFrame(X, columns=["x", "y"])
    native_data = native_pd.DataFrame(X, columns=["x", "y"])

    # Create and fit a KMeans clustering model
    kmeans = KMeans(n_clusters=3, random_state=42)
    kmeans.fit(data)

    native_kmeans_clusters = kmeans.fit(native_data).cluster_centers_
    assert_sklearn_equal(
        kmeans.cluster_centers_, native_kmeans_clusters, "scalar_function"
    )


def test_feature_selection():
    # Dimensionality Reduction
    rng = np.random.default_rng(42)
    n_samples = 100
    n_features = 10

    X = rng.random((n_samples, n_features))
    y = rng.integers(0, 2, size=n_samples)

    data = pd.DataFrame(X, columns=[f"feature{i}" for i in range(1, n_features + 1)])
    data["target"] = y

    # Select the top k features
    k_best = SelectKBest(score_func=f_classif, k=5)
    k_best.fit_transform(X, y)

    feat_inds = k_best.get_support(indices=True)
    features = data.iloc[:, feat_inds]

    return sorted(features.columns.tolist())


def test_model_selection(test_dfs):
    df = test_dfs[0]
    X = df[["feature1", "feature2"]]
    y = df["target"]

    # Data Splitting
    (X_train, X_test, y_train, y_test) = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    print(type(X_train))
    model = LogisticRegression()

    # Define hyperparameters to tune
    param_grid = {"C": [0.001, 0.01, 0.1, 1, 10, 100], "penalty": ["l1", "l2"]}
    grid_search = RandomizedSearchCV(model, param_grid, cv=5, scoring="accuracy")
    grid_search.fit(X_train, y_train)
    best_model = grid_search.best_estimator_
    y_pred = best_model.predict(X_test)
    assert isinstance(y_pred, float)


def test_data_scaling():
    data = pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    scaler = StandardScaler()

    scaled_data = scaler.fit_transform(data.values.reshape(-1, 1))
    return scaled_data
