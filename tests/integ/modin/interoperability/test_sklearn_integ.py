#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pytest
from sklearn.cluster import KMeans
from sklearn.feature_selection import SelectKBest, f_classif
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import accuracy_score
from sklearn.model_selection import RandomizedSearchCV, train_test_split
from sklearn.preprocessing import StandardScaler

import snowflake.snowpark.modin.plugin  # noqa: F401


@pytest.fixture(scope="module")
def df():
    data = {
        "feature1": [1, 5, 3, 4, 4, 6, 7, 2, 9, 70],
        "feature2": [2, 4, 1, 3, 5, 7, 6, 3, 10, 9],
        "target": [0, 0, 1, 0, 1, 1, 1, 0, 1, 0],
    }
    return pd.DataFrame(data)


def test_train_test_split(df):
    X = df[["feature1", "feature2"]]
    y = df["target"]

    # Data Splitting
    (X_train, X_test, y_train, y_test) = train_test_split(
        X, y, test_size=0.2, random_state=42
    )
    assert isinstance(X_train, pd.DataFrame)
    assert isinstance(X_test, pd.DataFrame)
    assert isinstance(y_train, pd.Series)
    assert isinstance(y_test, pd.Series)


def test_classification(df):
    from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
    from sklearn.pipeline import make_pipeline

    lda = LinearDiscriminantAnalysis(solver="svd", store_covariance=True)
    X = df[["feature1", "feature2"]]
    y = df["target"]

    # Data Splitting
    (X_train, X_test, y_train, y_test) = train_test_split(
        X, y, test_size=0.2, random_state=42
    )

    clf = make_pipeline(StandardScaler(), lda)
    clf.fit(X_train, y_train)
    score = clf.score(X_test, y_test)
    assert isinstance(score, float)


def test_regression(df):

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

    # Create and fit a KMeans clustering model
    kmeans = KMeans(n_clusters=3, random_state=42)
    kmeans.fit(data)
    return kmeans.cluster_centers_


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


def test_model_selection(df):
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
