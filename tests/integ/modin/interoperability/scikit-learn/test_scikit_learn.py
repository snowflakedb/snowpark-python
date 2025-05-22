#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from sklearn.decomposition import PCA
from sklearn.preprocessing import MaxAbsScaler

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import RandomizedSearchCV
from sklearn.cluster import KMeans
from tests.integ.modin.utils import create_test_dfs, eval_snowpark_pandas_result
from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
import numpy as np
import pytest

"""
------
README
------

This test suite tests scikit-learn's interoperability with Snowpark pandas.

Generally, scikit-learn seems to work with Snowpark pandas inputs via a
combination of the dataframe interchange protocol and converting Snowpark
pandas inputs to numpy with methods like __array__() and np.asarray(). Some
scikit-learn methods may cause Snowpark pandas to execute many Snowflake
queries or to materialize Snowpark pandas data one or more times. We don't
plan to fix the performance of scikit-learn with Snowpark pandas inputs, and
we recommend that users convert their data to native pandas before passing it
to scikit-learn if scikit-learn takes too long with Snowpark pandas inputs.

We group the tests into scenarios into the following use cases, listed under
https://scikit-learn.org/stable/index.html:

- Classification
- Regression
- Clustering
- Dimensionality reduction
- Model selection
- Preprocessing

Many scikit-learn methods produce non-deterministic results, and not all of
them provide a way to seed the results so that they are consistent for a test.
Generally, we only validate that 1) we can pass Snowpark pandas dataframe/series
into methods that accept native pandas inputs and 2) the outputs have the correct
type and, in case they are numpy arrays, they have the correct shape.

To test interoperability with a particular scikit-learn method:

1) Read about what the method does and how to use it
2) Start writing a test case under the test class for the category that the
   method belongs to (e.g. under TestClassification for
   LinearDiscriminantAnalysis)
2) Construct a use case that works with native pandas and produces a meaningful
   result (for example, train a model on pandas training data and fit it to test
   data)
3) Write a test case checking that replacing the pandas input with Snowpark
   pandas produces results of the same type and, in the case of array-like
   outputs, of the same dimensions. `assert_numpy_results_valid` can validate
   numpy results. Avoid checking that the values in the result are the same
   values we would get if we use pandas, because many scikit-learn methods
   are non-deterministic.
4) Wrap the test with an empty sql_count_checker() decorator to see how many
   queries and joins it requires. If it it requires a very large number of
   queries, see if you can simplify the test case so that it causes fewer
   queries, so that the test finishes quickly. If you can't reduce the number of
   queries to a reasonable level, you should pass the SQL count checker the
   `no_check=True` parameter because the number of queries is likely to vary
   across scikit-learn and Snowpark pandas versions, and we don't gain much
   insight by adjusting the query count every time it changes.
5) Add a row describing interoperability with the new method in the
   [documentation](docs/source/modin/interoperability.rst)
"""


def assert_numpy_results_valid(snow_result, pandas_result) -> None:
    assert isinstance(snow_result, np.ndarray)
    assert isinstance(pandas_result, np.ndarray)
    # Generally a meaningful test case should produce a non-empty result
    assert pandas_result.size > 0
    assert snow_result.shape == pandas_result.shape


@pytest.fixture()
def test_dfs():
    data = {
        "feature1": [1, 5, 3, 4, 4, 6, 7, 2, 9, 70],
        "feature2": [2, 4, 1, 3, 5, 7, 6, 3, 10, 9],
        "target": [0, 0, 1, 0, 1, 1, 1, 0, 1, 0],
    }
    return create_test_dfs(data)


class TestClassification:
    @sql_count_checker(query_count=6)
    def test_linear_discriminant_analysis(self, test_dfs):
        def get_predictions(df) -> np.ndarray:
            X = df[["feature1", "feature2"]]
            y = df["target"]
            train_size = 8
            X_train, X_test = X.iloc[:train_size], X.iloc[train_size:]
            y_train = y.iloc[:train_size]
            return LinearDiscriminantAnalysis().fit(X_train, y_train).predict(X_test)

        eval_snowpark_pandas_result(
            *test_dfs, get_predictions, comparator=assert_numpy_results_valid
        )


class TestRegression:
    @sql_count_checker(query_count=6)
    def test_logistic_regression(self, test_dfs):
        def get_predictions(df) -> np.ndarray:
            X = df[["feature1", "feature2"]]
            y = df["target"]
            train_size = 8
            X_train, X_test = X.iloc[:train_size], X.iloc[train_size:]
            y_train = y.iloc[:train_size]
            return LogisticRegression().fit(X_train, y_train).predict(X_test)

        eval_snowpark_pandas_result(
            *test_dfs, get_predictions, comparator=assert_numpy_results_valid
        )


class TestClustering:
    @sql_count_checker(query_count=2)
    def test_clustering(self, test_dfs):
        def get_cluster_centers(df) -> np.ndarray:
            return KMeans(n_clusters=3).fit(df).cluster_centers_

        eval_snowpark_pandas_result(
            *test_dfs, get_cluster_centers, comparator=assert_numpy_results_valid
        )


class TestDimensionalityReduction:
    @sql_count_checker(query_count=2)
    def test_principal_component_analysis(self, test_dfs):
        def get_principal_components(df) -> np.ndarray:
            return PCA(n_components=2).fit(df).components_

        eval_snowpark_pandas_result(
            *test_dfs, get_principal_components, comparator=assert_numpy_results_valid
        )


class TestModelSelection:
    @sql_count_checker(
        # Model search is a complex, iterative process. Pushing it down to
        # Snowflake requires many queries (approximately 31 for this case).
        # Since the number of queries and the number of joins are so large, they
        # are likely to change due to changes in both scikit-learn and Snowpark
        # pandas. We don't get much insight from the exact number of queries, so
        # we skip the query count check. The recommended solution to this query
        # explosion is for users to convert the Snowpark pandas object to pandas
        # with to_pandas() and pass the result to scikit-learn.
        no_check=True
    )
    def test_randomized_search_cv(self, test_dfs):
        def get_best_estimator(df) -> dict:
            # Initialize the hyperparameter search with parameters that will
            # reduce the search time as much as possible.
            return (
                RandomizedSearchCV(
                    LogisticRegression(),
                    param_distributions={
                        "C": [0.001],
                    },
                    # cv=2 means 2-fold validation, which requires the fewest queries.
                    cv=2,
                    # Test just one combination of parameters.
                    n_iter=1,
                    # refit=False means that the search doesn't have to actually
                    # train a model using the parameters that it chooses. Setting
                    # refit=False should further reduce the number of queries.
                    refit=False,
                )
                .fit(df[["feature1", "feature2"]], df["target"])
                .best_params_
            )

        def validate_search_results(snow_estimator, pandas_estimator):
            assert isinstance(snow_estimator, dict)
            assert isinstance(pandas_estimator, dict)

        eval_snowpark_pandas_result(
            *test_dfs, get_best_estimator, comparator=validate_search_results
        )


class TestPreprocessing:
    @sql_count_checker(query_count=4)
    def test_maxabs(self, test_dfs):
        eval_snowpark_pandas_result(
            *test_dfs,
            MaxAbsScaler().fit_transform,
            comparator=assert_numpy_results_valid
        )


"""
------
README
------

Please see the README at the top of this file for instructions on adding test
cases.
"""
