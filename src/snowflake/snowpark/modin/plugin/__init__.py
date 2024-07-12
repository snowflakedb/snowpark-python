#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import sys
import warnings

from packaging import version

import snowflake.snowpark._internal.utils

if sys.version_info.major == 3 and sys.version_info.minor == 8:
    raise RuntimeError(
        "Snowpark pandas does not support Python 3.8. Please update to Python 3.9 or later."
    )  # pragma: no cover


install_msg = 'Run `pip install "snowflake-snowpark-python[modin]"` to resolve.'

# pandas import needs to come before Python version + modin checks,
# since modin may raise its own warnings/errors on the wrong pandas version
import pandas  # isort: skip  # noqa: E402

supported_pandas_version = "2.2.1"
if pandas.__version__ != supported_pandas_version:
    raise RuntimeError(
        f"The pandas version installed ({pandas.__version__}) does not match the supported pandas version in"
        + f" Snowpark pandas ({supported_pandas_version}). "
        + install_msg
    )  # pragma: no cover

try:
    import modin  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    raise ModuleNotFoundError(
        "Modin is not installed. " + install_msg
    )  # pragma: no cover

supported_modin_version = "0.28.1"
if version.parse(modin.__version__) != version.parse(supported_modin_version):
    raise ImportError(
        f"The Modin version installed ({modin.__version__}) does not match the supported Modin version in"
        + f" Snowpark pandas ({supported_modin_version}). "
        + install_msg
    )  # pragma: no cover


warnings.warn(
    "Snowpark pandas has been in Public Preview since 1.17.0."
    + " See https://docs.snowflake.com/developer-guide/snowpark/python/snowpark-pandas for details.",
    stacklevel=1,
)

# We need this import here to prevent circular dependency issues, since snowflake.snowpark.modin.pandas
# currently imports some internal utilities from snowflake.snowpark.modin.plugin. Test cases will
# import snowflake.snowpark.modin.plugin before snowflake.snowpark.modin.pandas, so in order to prevent
# circular dependencies from manifesting, apparently snowflake.snowpark.modin.pandas needs to
# be imported first.
# These imports also all need to occur after modin + pandas dependencies are validated.
from snowflake.snowpark.modin import pandas  # isort: skip  # noqa: E402,F401
from snowflake.snowpark.modin.config import DocModule  # isort: skip  # noqa: E402
from snowflake.snowpark.modin.plugin import docstrings  # isort: skip  # noqa: E402

DocModule.put(docstrings.__name__)


# We cannot call ModinDocModule.put directly because it will produce a call to `importlib.reload`
# that will overwrite our extensions. We instead directly call the _inherit_docstrings annotation
# See https://github.com/modin-project/modin/issues/7122
import modin.utils  # type: ignore[import]  # isort: skip  # noqa: E402
import modin.pandas.series_utils  # type: ignore[import]  # isort: skip  # noqa: E402

modin.utils._inherit_docstrings(
    docstrings.series_utils.StringMethods,
    overwrite_existing=True,
)(modin.pandas.series_utils.StringMethods)

modin.utils._inherit_docstrings(
    docstrings.series_utils.CombinedDatetimelikeProperties,
    overwrite_existing=True,
)(modin.pandas.series_utils.DatetimeProperties)

# Don't warn the user about our internal usage of private preview pivot
# features. The user should have already been warned that Snowpark pandas
# is in public or private preview. They likely don't know or care that we are
# using Snowpark DataFrame pivot() internally, let alone that we are using
# private preview features of Snowpark Python.

snowflake.snowpark._internal.utils.should_warn_dynamic_pivot_is_in_private_preview = (
    False
)


# TODO: SNOW-1504302: Modin upgrade - use Snowpark pandas DataFrame for isocalendar
# OSS Modin's DatetimeProperties frontend class wraps the returned query compiler with `modin.pandas.DataFrame`.
# Since we currently replace `pd.DataFrame` with our own Snowpark pandas DataFrame object, this causes errors
# since OSS Modin explicitly imports its own DataFrame class here. This override can be removed once the frontend
# DataFrame class is removed from our codebase.
def isocalendar(self):  # type: ignore
    from snowflake.snowpark.modin.pandas import DataFrame

    return DataFrame(query_compiler=self._query_compiler.dt_isocalendar())


modin.pandas.series_utils.DatetimeProperties.isocalendar = isocalendar
