#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from unittest.mock import patch

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import udf
from tests.utils import IS_IN_STORED_PROC, TempObjectType, Utils

permanent_stage_name = "stage_for_packaging_tests_v5"


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path):
    try:
        print(
            f"Files on permanent stage: {session._list_files_in_stage(permanent_stage_name)}"
        )
    except SnowparkSQLException as sse:
        if "does not exist or not authorized" not in str(sse):
            raise sse
        Utils.create_stage(session, permanent_stage_name, is_temporary=False)


@pytest.fixture(autouse=True)
def clean_up(session):
    # Note: All tests in this module are skipped as these tests are only intended for in-depth testing of packaging.
    # Please run these tests when any change to packaging functionality is made.
    pytest.skip()
    session.clear_packages()
    session.clear_imports()
    yield


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Subprocess calls are not allowed within stored procedures",
)
def test_skfuzzy(session):
    """
    Assert that scikit-fuzzy package is usable.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(["scikit-fuzzy"], persist_path=permanent_stage_name)
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def skfuzzy_test() -> str:
            import numpy as np
            import skfuzzy as fuzz

            # Generate random data
            np.random.seed(0)
            x = np.arange(0, 10, 0.1)  # Input variable
            x_len = len(x)
            y = np.random.randn(x_len)  # Output variable

            # Apply fuzzy c-means clustering
            cntr, u, _, _, _, _, _ = fuzz.cluster.cmeans(
                data=np.vstack([x, y]),
                c=2,  # Number of clusters
                m=2,  # Fuzziness coefficient
                error=0.005,
                maxiter=1000,
                init=None,
            )

            # Get the membership values of the data points for each cluster
            cluster_membership = np.argmax(u, axis=0)
            return str(len(cluster_membership))

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("100")],
        )


@pytest.mark.skipif(
    IS_IN_STORED_PROC,
    reason="Subprocess calls are not allowed within stored procedures",
)
def test_pyod(session):
    """
    Assert that pyod package is usable.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(["pyod"], persist_path=permanent_stage_name)
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def skfuzzy_test() -> str:
            import numpy as np
            from pyod.models.knn import KNN

            # Generate random data
            np.random.seed(0)
            X_train = np.random.randn(1000, 2)  # 1000 samples with 2 features

            # Create and fit the KNN outlier detection model
            model = KNN(
                contamination=0.1
            )  # contamination represents the expected proportion of outliers
            model.fit(X_train)

            # Generate test data with outliers
            X_test = np.random.randn(200, 2)  # 200 samples with 2 features
            outlier_scores = model.decision_function(
                X_test
            )  # Obtain outlier scores for test data

            return str(len(outlier_scores))

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("200")],
        )
