#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from packaging import version

# We need this import here to prevent circular dependency issues, since snowflake.snowpark.modin.pandas
# currently imports some internal utilities from snowflake.snowpark.modin.plugin. Test cases will
# import snowflake.snowpark.modin.plugin before snowflake.snowpark.modin.pandas, so in order to prevent
# circular dependencies from manifesting, apparently snowflake.snowpark.modin.pandas needs to
# be imported first.
from snowflake.snowpark.modin import pandas  # noqa: F401
from snowflake.snowpark.modin.config import DocModule
from snowflake.snowpark.modin.plugin import docstrings

DocModule.put(docstrings.__name__)

install_msg = "Run `pip install snowflake-snowpark-python[modin]` to resolve."
try:
    import modin
except ModuleNotFoundError:  # pragma: no cover
    raise ModuleNotFoundError(
        "Modin is not installed. " + install_msg
    )  # pragma: no cover

supported_modin_version = "0.28.1"
if version.parse(modin.__version__) != version.parse(supported_modin_version):
    raise ImportError(
        "Installed Modin version is not supported. " + install_msg
    )  # pragma: no cover
