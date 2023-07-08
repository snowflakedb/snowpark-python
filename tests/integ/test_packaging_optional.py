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
# Note: Tests will run much faster if only False is included in reinstall_options.
# Only including False will test the UDF snippets without pip installing packages.


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


@pytest.mark.xfail(reason="atpublic is not usable due to KeyError issues with __all__")
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


def test_tiktoken(session):
    """
    Assert that tiktoken package is not usable as it contains native code.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        with pytest.raises(RuntimeError) as ex_info:
            session.add_packages(
                ["tiktoken"],
                persist_path=permanent_stage_name,
                force_install=True,
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


@pytest.mark.xfail(
    reason="monai is not usable due to psutil.__version__ attribute missing"
)
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


def test_faiss(session):
    """
    Assert that faiss package is not usable because it contains native code.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        with pytest.raises(RuntimeError) as ex_info:
            session.add_packages(
                ["tiktoken"],
                persist_path=permanent_stage_name,
                force_install=True,
            )
        assert "Your code depends on native dependencies" in str(ex_info)


def test_duckdb(session):
    """
    Assert that duckdb package is not usable because it contains native code.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        with pytest.raises(RuntimeError) as ex_info:
            session.add_packages(
                ["duckdb"],
                persist_path=permanent_stage_name,
                force_install=True,
            )
        assert "Your code depends on native dependencies" in str(ex_info)


@pytest.mark.parametrize("force_install", reinstall_options)
def test_asana(session, force_install):
    """
    Assert that asana package is usable by attempting an oauth connection to Asana.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["asana"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def asana_test() -> str:
            import asana

            client = asana.Client.oauth(
                client_id="ASANA_CLIENT_ID",
                client_secret="ASANA_CLIENT_SECRET",
                redirect_uri="https://yourapp.com/auth/asana/callback",
            )
            (url, state) = client.session.authorization_url()
            return str(len(state))

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("30")],
        )


def test_whylogs(session):
    """
    Assert that whylogs package is not usable because it contains native code.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        with pytest.raises(RuntimeError) as ex_info:
            session.add_packages(
                ["whylogs"],
                persist_path=permanent_stage_name,
                force_install=True,
            )
        assert "Your code depends on native dependencies" in str(ex_info)


@pytest.mark.parametrize("force_install", reinstall_options)
def test_deepdiff(session, force_install):
    """
    Assert that deepdiff package is usable by diffing between two dictionaries.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["deepdiff"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def deepdiff_test() -> str:
            from deepdiff import DeepDiff

            # Sample dictionaries
            dict1 = {"a": 1, "b": 2, "c": 3}
            dict2 = {"a": 1, "b": 2, "c": 4}

            # Perform deep comparison
            diff = DeepDiff(dict1, dict2)

            # Print the differences
            return diff

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [
                Row(
                    "{'values_changed': {\"root['c']\": {'new_value': 4, 'old_value': 3}}}"
                )
            ],
        )


def test_quantile_forest(session):
    """
    Assert that quantile_forest package is not usable because it contains native code.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        with pytest.raises(RuntimeError) as ex_info:
            session.add_packages(
                ["quantile_forest", "setuptools"],
                persist_path=permanent_stage_name,
                force_install=True,
            )
        assert "Your code depends on native dependencies" in str(ex_info)


@pytest.mark.xfail(
    reason="pysoundfile is not usable because it needs the libsndfile C library to be present."
)
@pytest.mark.parametrize("force_install", reinstall_options)
def test_pysoundfile(session, force_install):
    """
    Assert that PySoundFile package is usable by importing it.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["PySoundFile"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def pysoundfile_test() -> str:
            import soundfile as sf

            print(sf.__version__)
            return "works"

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("works")],
        )


def test_librosa(session):
    """
    Assert that librosa package is unusable because of native dependencies.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        with pytest.raises(RuntimeError) as ex_info:
            session.add_packages(
                ["librosa"],
                persist_path=permanent_stage_name,
                force_install=True,
            )
        assert "Your code depends on native dependencies" in str(ex_info)


@pytest.mark.parametrize("force_install", reinstall_options)
def test_sqlglot(session, force_install):
    """
    Assert that sqlglot package is usable by converting duckdb SQL into a Hive query.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["sqlglot"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def sqlglot_test() -> str:
            import sqlglot

            return sqlglot.transpile(
                "SELECT EPOCH_MS(1618088028295)", read="duckdb", write="hive"
            )[0]

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("SELECT FROM_UNIXTIME(1618088028295 / 1000)")],
        )


@pytest.mark.xfail(reason="greykite is not building on my machine.")
@pytest.mark.parametrize("force_install", reinstall_options)
def test_greykite(session, force_install):
    """
    Assert that greykite package is usable by performing a time series forecasting task.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["greykite", "cmake"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def greykite_test() -> str:
            from greykite.framework.templates.forecaster import Forecaster

            return str(Forecaster)

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("TBD")],
        )


@pytest.mark.parametrize("force_install", reinstall_options)
def test_text2num(session, force_install):
    """
    Assert that text2num package is usable by converting Spanish number text to digits.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["text2num"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def text2num_test() -> str:
            from text_to_num import text2num

            return str(text2num("nueve mil novecientos noventa y nueve", "es"))

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("9999")],
        )


@pytest.mark.parametrize("force_install", reinstall_options)
def test_scrubadub(session, force_install):
    """
    Assert that scrubadub package is usable by cleaning PII information from text.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["scrubadub"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def scrubadub_test() -> str:
            import scrubadub

            return scrubadub.clean("Contact Vivek Nayak at vivek.nayak@snowflake.com")

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("Contact Vivek {{EMAIL}}@snowflake.com")],
        )


@pytest.mark.xfail(reason="Mecab does not build on my machine")
@pytest.mark.parametrize("force_install", reinstall_options)
def test_mecab(session, force_install):
    """
    Assert that mecab package is usable by parsing Japanese.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["mecab-python3"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def mecab_test() -> str:
            import MeCab

            wakati = MeCab.Tagger("-Owakati")
            return wakati.parse("pythonが大好きです").split()[2]

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("大好き")],
        )


@pytest.mark.xfail(reason="Unable to import pptx file from the zipped environment.")
@pytest.mark.parametrize("force_install", reinstall_options)
def test_python_pptx(session, force_install):
    """
    Assert that python-pptx package is usable by creating a presentation
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["python-pptx"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def pptx_test() -> str:
            from pptx import Presentation

            prs = Presentation()
            title_slide_layout = prs.slide_layouts[0]
            slide = prs.slides.add_slide(title_slide_layout)
            title = slide.shapes.title
            subtitle = slide.placeholders[1]

            title.text = "Hello, World!"
            subtitle.text = "python-pptx was here!"
            return "worked"

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("worked")],
        )


@pytest.mark.parametrize("force_install", reinstall_options)
def test_imodels(session, force_install):
    """
    Assert that imodels is usable by training a hierarchical sparse decision tree.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["imodels"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def imodels_test() -> str:
            import numpy as np
            from imodels import HSTreeClassifierCV
            from sklearn.model_selection import train_test_split

            # Generate synthetic data
            np.random.seed(42)
            n_samples = 1000
            n_features = 10
            X = np.random.randn(n_samples, n_features)
            y = np.random.randint(0, 2, size=n_samples)
            feature_names = [f"Feature_{i}" for i in range(n_features)]

            # Split the data into training and testing sets
            X_train, X_test, y_train, y_test = train_test_split(X, y, random_state=42)

            # Fit the model
            model = HSTreeClassifierCV(max_leaf_nodes=4)
            model.fit(X_train, y_train, feature_names=feature_names)

            # Make predictions
            preds_proba = model.predict_proba(X_test)

            return str(len(preds_proba))

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("250")],
        )


@pytest.mark.parametrize("force_install", reinstall_options)
def test_kedro(session, force_install):
    """
    Assert that kedro is usable by defining a square root node in a pipeline.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["kedro"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def kedro_test() -> str:
            from kedro.pipeline import Pipeline, node

            # Define a simple node
            def square(x):
                return x**2

            # Create a pipeline
            pipeline = Pipeline([node(square, "input", "output")])

            return str(pipeline)

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("Pipeline([\nNode(square, 'input', 'output', None)\n])")],
        )


@pytest.mark.parametrize("force_install", reinstall_options)
def test_polars(session, force_install):
    """
    Assert that polars package is not usable as it contains native code.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        with pytest.raises(RuntimeError) as ex_info:
            session.add_packages(
                ["polars"],
                persist_path=permanent_stage_name,
                force_install=True,
            )
        assert "Your code depends on native dependencies" in str(ex_info)


@pytest.mark.parametrize("force_install", reinstall_options)
def test_coboljsonifier(session, force_install):
    """
    Assert that coboljsonifier is usable by importing it.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["coboljsonifier"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def coboljsonifier_test() -> str:
            from coboljsonifier.config.parser_type_enum import ParseType
            from coboljsonifier.copybookextractor import CopybookExtractor
            from coboljsonifier.parser import Parser

            print(str(ParseType), str(CopybookExtractor), str(Parser))

            return "works"

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("works")],
        )


@pytest.mark.parametrize("force_install", reinstall_options)
def test_sklearn_crfsuite(session, force_install):
    """
    Assert that sklearn_crfsuite is usable by training a CRF model.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["sklearn-crfsuite", "scikit-learn"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def crfsuite_test() -> str:
            import sklearn_crfsuite

            # Generate sample data
            X_train = [
                [("feature1", "value1"), ("feature2", "value2")],
                [("feature3", "value3"), ("feature4", "value4")],
            ]
            y_train = [["label1", "label2"], ["label3", "label4"]]
            X_test = [
                [("feature5", "value5"), ("feature6", "value6")],
                [("feature7", "value7"), ("feature8", "value8")],
            ]

            # Define and train the CRF model
            crf_model = sklearn_crfsuite.CRF()
            crf_model.fit(X_train, y_train)

            # Perform prediction on test data
            y_pred = crf_model.predict(X_test)

            # Evaluate the model
            return str(len(y_pred))

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("2")],
        )


@pytest.mark.xfail(
    reason="great_expectations fails often when built from inside a conda environment"
)
@pytest.mark.parametrize("force_install", reinstall_options)
def test_great_expectations(session, force_install):
    """
    Assert that great_expectations is usable by fetching data context.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["great_expectations"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def ge_test() -> str:
            import great_expectations as gx

            data_context = gx.get_context()
            return str(data_context)

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("TBD")],
        )


@pytest.mark.parametrize("force_install", reinstall_options)
def test_simhash(session, force_install):
    """
    Assert that simhash is usable by fetching distance between two simhashes
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["simhash"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def simhash_test() -> str:
            from simhash import Simhash

            return Simhash("aa").distance(Simhash("bb"))

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("31")],
        )


@pytest.mark.parametrize("force_install", reinstall_options)
def test_eth_abi(session, force_install):
    """
    Assert that eth_abi is usable by encoding two strings to match Solidity's ABI specification.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["eth_abi", "setuptools"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def ethabi_test() -> str:
            from eth_abi import encode

            return str(encode(["bytes32", "bytes32"], [b"a", b"b"]))[:4]

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("b'a\\")],
        )


@pytest.mark.xfail(
    reason="librdkafka C file (required by dependency kafka-confluent) is needed to install hopsworks"
)
@pytest.mark.parametrize("force_install", reinstall_options)
def test_hopsworks(session, force_install):
    """
    Assert that hopsworks is usable by creating a Hopsworks project object.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        session.add_packages(
            ["hopsworks"],
            persist_path=permanent_stage_name,
            force_install=force_install,
        )
        udf_name = Utils.random_name_for_temp_object(TempObjectType.FUNCTION)

        @udf(name=udf_name, session=session)
        def hopsworks_test() -> str:
            import hopsworks

            connection = hopsworks.connection()
            project = connection.get_project("my_project")
            return str(project)

        Utils.check_answer(
            session.sql(f"select {udf_name}()").collect(),
            [Row("TBD")],
        )


@pytest.mark.parametrize("force_install", reinstall_options)
def test_openai_whisper(session, force_install):
    """
    Assert that openai_whisper is unusable due to native dependencies.
    """
    with patch.object(session, "_is_anaconda_terms_acknowledged", lambda: True):
        with pytest.raises(RuntimeError) as ex_info:
            session.add_packages(
                ["openai-whisper", "pytorch"],
                persist_path=permanent_stage_name,
                force_install=True,
            )
        assert "Your code depends on native dependencies" in str(ex_info)
