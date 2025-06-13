#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import re
from unittest.mock import MagicMock, patch

import pytest


@pytest.fixture(autouse=True)
def skip_if_not_testing_incorrect_modin_or_pandas_versions():
    if not os.environ.get("TEST_INCORRECT_MODIN_PANDAS_VERSIONS"):
        pytest.skip("Skipping tests for incorrect modin/pandas versions")


def test_error_on_import():
    with patch.dict("sys.modules", {"snowbook": MagicMock(), "modin": MagicMock()}):
        with pytest.raises(
            ImportError,
            match=re.escape("in the Packages menu at the top of your notebook"),
        ):
            import modin

            # use an unsupported version of modin
            modin.__version__ = "0.2"

            import snowflake.snowpark  # noqa: F401
            import snowflake.snowpark.modin.plugin  # noqa: F401
