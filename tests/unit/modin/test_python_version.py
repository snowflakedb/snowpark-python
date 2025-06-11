#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import abc
import sys
from unittest.mock import MagicMock, patch

import pytest


def test_error_on_import():
    # On Python 3.8, we must run pytest with --noconftest, since various conftest.py files
    # will error when importing modin.pandas.
    # We therefore cannot test any Snowpark DataFrame operations, since we don't have the ability
    # to set up a session here; we can only test that imports work as expected.
    if sys.version_info.major == 3 and sys.version_info.minor == 8:
        # Importing snowpark (without pandas) should not error
        import snowflake.snowpark  # noqa: F401

        with pytest.raises(RuntimeError):
            # Importing snowpark pandas should error
            import snowflake.snowpark.modin.plugin  # noqa: F401
    else:
        # Should not error
        import snowflake.snowpark  # noqa: F401
        import snowflake.snowpark.modin.plugin  # noqa: F401


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
    with patch.dict("sys.modules", {"snowbook": MagicMock()}):
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
            assert "Run `pip install" in str(ex)


def test_modin_version_error_in_notebooks():
    with patch.dict("sys.modules", {"snowbook": MagicMock()}):
        with patch.dict("sys.modules", {"modin": MagicMock()}):
            try:
                import modin

                # use an unsupported version of modin
                modin.__version__ = "0.2"

                import snowflake.snowpark  # noqa: F401
                import snowflake.snowpark.modin.plugin  # noqa: F401
            except ImportError as ex:
                # We will only get here when we run pytest with --noconftest
                assert "in the Packages menu at the top of your notebook" in str(ex)
