#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from unittest.mock import patch

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import udf
from tests.utils import TempObjectType, Utils

permanent_stage_name = "permanent_stage_for_package_testing"
reinstall_options = [True, False]


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


@pytest.mark.parametrize("force_install", reinstall_options)
def test_sktime(session, force_install):
    """
    Assert that sktime package is usable by running an K Neighbors Time Series classifier on random data.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["sktime", "pmdarima"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def sktime_test() -> str:
            import numpy as np
            from sktime.classification.distance_based import (
                KNeighborsTimeSeriesClassifier,
            )

            # Generate random time series data
            n_samples = 100  # Number of time series samples
            n_timestamps = 50  # Number of timestamps per time series
            n_classes = 3  # Number of classes
            X = np.random.rand(n_samples, n_timestamps)
            y = np.random.randint(n_classes, size=n_samples)

            # Create a K-Nearest Neighbors Time Series Classifier
            classifier = KNeighborsTimeSeriesClassifier(n_neighbors=3)

            # Fit the classifier to the data
            classifier.fit(X, y)

            # Make predictions on new data
            X_test = np.random.rand(5, n_timestamps)
            y_pred = classifier.predict(X_test)

            return str(len(y_pred))

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("5")],
        )


@pytest.mark.parametrize("force_install", reinstall_options)
def test_skfuzzy(session, force_install):
    """
    Assert that scikit-fuzzy package is usable by running c-means clustering.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["scikit-fuzzy"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
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


@pytest.mark.parametrize("force_install", reinstall_options)
def test_pyod(session, force_install):
    """
    Assert that pyod package is usable by running a KNN classifier.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["pyod"], persist_path=permanent_stage_name, force_install=force_install
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def pyod_test() -> str:
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


@pytest.mark.parametrize("force_install", reinstall_options)
def test_parsy(session, force_install):
    """
    Assert that parsy package is usable by parsing a dd-mm-yy date.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["parsy"], persist_path=permanent_stage_name, force_install=force_install
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def parsy_test() -> str:
            from datetime import date

            from parsy import regex, string

            ddmmyy = (
                regex(r"[0-9]{2}")
                .map(int)
                .sep_by(string("-"), min=3, max=3)
                .combine(lambda d, m, y: date(2000 + y, m, d))
            )
            return str(ddmmyy.parse("06-05-14"))

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("2014-05-06")],
        )


@pytest.mark.skip("atpublic is not usable due to KeyError issues with __all__")
@pytest.mark.parametrize("force_install", reinstall_options)
def test_atpublic(session, force_install):
    """
    Assert that atpublic package is usable by making a 'public' decorated function.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["atpublic"], persist_path=permanent_stage_name, force_install=force_install
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def atpublic_test() -> str:
            from public import public

            @public
            def foo():
                pass

            return "worked"

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("worked")],
        )


@pytest.mark.parametrize(
    "force_install",
    [
        True
    ],  # No "False" here because no point using a persisted environment for a failing package.
)
def test_tiktoken(session, force_install):
    """
    Assert that tiktoken package is not usable as it contains native code.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        with pytest.raises(RuntimeError) as ex_info:
            session.add_packages(
                ["tiktoken"],
                persist_path=permanent_stage_name,
                force_install=force_install,
            )
        assert "Your code depends on native dependencies" in str(ex_info)


@pytest.mark.parametrize("force_install", reinstall_options)
def test_ibis(session, force_install):
    """
    Assert that ibis package is usable by creating a simple 'and' template.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["ibis"], persist_path=permanent_stage_name, force_install=force_install
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def ibis_test() -> str:
            import ibis

            template = ibis.Template("{{foo}} and {{bar}}")
            return template.render({"foo": "ham", "bar": "eggs"})

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("ham and eggs")],
        )


@pytest.mark.parametrize("force_install", reinstall_options)
def test_np_financial(session, force_install):
    """
    Assert that numpy-financial package is usable by calculating present discounted cash flow value.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["numpy-financial"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def numpy_financial_test() -> str:
            import numpy as np
            import numpy_financial as npf

            # Generate cash flows
            cash_flows = np.array([-100, 50, 40, 30, 20])

            # Set discount rate
            discount_rate = 0.1

            # Calculate present value
            present_value = npf.npv(discount_rate, cash_flows)
            return str(round(present_value, 2))

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("14.71")],
        )


@pytest.mark.skip("monai is not usable due to psutil.__version__ attribute missing")
@pytest.mark.parametrize("force_install", reinstall_options)
def test_monai(session, force_install):
    """
    Assert that monai package is usable by transforming images.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["monai", "pytorch", "psutil==5.9.5"],
            persist_path=permanent_stage_name,
            force_push=True,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def monai_test() -> str:
            import torch
            from monai.data import DataLoader
            from monai.transforms import Compose, ScaleIntensity

            # Create a dummy dataset
            class DummyDataset(torch.utils.data.Dataset):
                def __init__(self) -> None:
                    self.data = torch.randn((100, 1, 64, 64))
                    return

                def __getitem__(self, index):
                    return self.data[index]

                def __len__(self):
                    return len(self.data)

            # Initialize the dataset and data loader
            dataset = DummyDataset()
            dataloader = DataLoader(dataset, batch_size=8, num_workers=0)

            # Define the transform pipeline
            transform = Compose([ScaleIntensity()])

            # Apply transform to each batch in the data loader
            for batch in dataloader:
                transformed_batch = transform(batch)
                print(transformed_batch.shape)
            return "worked"

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("worked")],
        )


@pytest.mark.parametrize("force_install", reinstall_options)
def test_textdistance(session, force_install):
    """
    Assert that textdistance package is usable by calculating normalized levenshtein similarity.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["textdistance"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def textdistance_test() -> str:
            import textdistance

            return str(
                textdistance.levenshtein.normalized_similarity("text", "distance")
            )

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("0.125")],
        )
