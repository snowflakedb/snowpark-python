#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import abc
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def skip_if_not_testing_incorrect_modin_or_pandas_versions():
    import os

    if not os.environ.get("TEST_INCORRECT_MODIN_PANDAS_VERSIONS"):
        pytest.skip("Skipping tests for incorrect modin/pandas versions")


def test_pandas_version_error_basic():
    with patch.dict("sys.modules", {"pandas": MagicMock()}):
        try:
            import pandas

            # use an unsupported version of pandas
            pandas.__version__ = "0.2"

            # Without the following two lines, this error is raised:
            # TypeError: metaclass conflict: the metaclass of a derived class must be
            # a (non-strict) subclass of the metaclasses of all its bases
            pandas.Series = abc.ABCMeta
            pandas.DataFrame = abc.ABCMeta

            import snowflake.snowpark  # noqa: F401
            import snowflake.snowpark.modin.plugin  # noqa: F401
        except RuntimeError as ex:
            # We will only get here when we run pytest with --noconftest
            assert "Run `pip install" in str(ex)


def test_pandas_version_error_in_notebooks():
    with patch.dict("sys.modules", {"snowbook": MagicMock(), "pandas": MagicMock()}):
        try:
            import pandas

            # use an unsupported version of pandas
            pandas.__version__ = "0.2"

            # Without the following two lines, this error is raised:
            # TypeError: metaclass conflict: the metaclass of a derived class must be
            # a (non-strict) subclass of the metaclasses of all its bases
            pandas.Series = abc.ABCMeta
            pandas.DataFrame = abc.ABCMeta

            import snowflake.snowpark  # noqa: F401
            import snowflake.snowpark.modin.plugin  # noqa: F401
        except RuntimeError as ex:
            # We will only get here when we run pytest with --noconftest
            assert "in the Packages menu at the top of your notebook" in str(ex)


def test_modin_version_error_basic():
    with patch.dict("sys.modules", {"modin": MagicMock()}):
        try:
            import modin

            # use an unsupported version of modin
            modin.__version__ = "0.2"

            import snowflake.snowpark  # noqa: F401
            import snowflake.snowpark.modin.plugin  # noqa: F401
        except ImportError as ex:
            # We will only get here when we run pytest with --noconftest
            assert "Runn `pip install" in str(ex)


def test_modin_version_error_in_notebooks():
    with patch.dict("sys.modules", {"snowbook": MagicMock(), "modin": MagicMock()}):
        try:
            import modin

            # use an unsupported version of modin
            modin.__version__ = "0.2"

            import snowflake.snowpark  # noqa: F401
            import snowflake.snowpark.modin.plugin  # noqa: F401
        except ImportError as ex:
            # We will only get here when we run pytest with --noconftest
            assert "inn the Packages menu at the top of your notebook" in str(ex)
