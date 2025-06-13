#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import abc
import os
import re
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def skip_if_not_testing_incorrect_modin_or_pandas_versions():
    if not os.environ.get("TEST_INCORRECT_MODIN_PANDAS_VERSIONS"):
        pytest.skip("Skipping tests for incorrect modin/pandas versions")


def test_error_on_import():
    with patch.dict("sys.modules", {"snowbook": MagicMock(), "pandas": MagicMock()}):
        with pytest.raises(
            RuntimeError,
            match=re.escape("in the Packages menu at the top of your notebook"),
        ):
            import pandas

            # use an unsupported version of pandas
            pandas.__version__ = "1.0"

            # Without the following two lines, this error is raised:
            # TypeError: metaclass conflict: the metaclass of a derived class must be
            # a (non-strict) subclass of the metaclasses of all its bases
            pandas.Series = abc.ABCMeta
            pandas.DataFrame = abc.ABCMeta

            import snowflake.snowpark  # noqa: F401
            import snowflake.snowpark.modin.plugin  # noqa: F401
