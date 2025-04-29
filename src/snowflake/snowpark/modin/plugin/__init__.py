#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import inspect
import sys

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

# TODO SNOW-1758773: perform version check in modin instead
supported_pandas_major_version = 2
supported_pandas_minor_version = 2
actual_pandas_version = version.parse(pandas.__version__)
if (
    actual_pandas_version.major != supported_pandas_major_version
    and actual_pandas_version.minor != supported_pandas_minor_version
):
    raise RuntimeError(
        f"The pandas version installed ({pandas.__version__}) does not match the supported pandas version in"
        + f" Snowpark pandas ({supported_pandas_major_version}.{supported_pandas_minor_version}.x). "
        + install_msg
    )  # pragma: no cover

try:
    import modin  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    raise ModuleNotFoundError(
        "Modin is not installed. " + install_msg
    )  # pragma: no cover

supported_modin_version = "0.32.0"
if version.parse(modin.__version__) != version.parse(supported_modin_version):
    raise ImportError(
        f"The Modin version installed ({modin.__version__}) does not match the supported Modin version in"
        + f" Snowpark pandas ({supported_modin_version}). "
        + install_msg
    )  # pragma: no cover


# === INITIALIZE EXTENSION SYSTEM ===
# Initialize all extension modules.
import snowflake.snowpark.modin.plugin.extensions.pd_extensions  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.io_overrides  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.general_overrides  # isort: skip  # noqa: E402,F401

# base overrides occur before subclass overrides in case subclasses override a base method
import snowflake.snowpark.modin.plugin.extensions.base_extensions  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.base_overrides  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.dataframe_extensions  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.dataframe_overrides  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.series_extensions  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.series_overrides  # isort: skip  # noqa: E402,F401

# === INITIALIZE DOCSTRINGS ===
# These imports also all need to occur after modin + pandas dependencies are validated.
from snowflake.snowpark.modin.config import DocModule  # isort: skip  # noqa: E402
from snowflake.snowpark.modin.plugin import docstrings  # isort: skip  # noqa: E402

DocModule.put(docstrings.__name__)

# We cannot call ModinDocModule.put directly because it will produce a call to `importlib.reload`
# that will overwrite our extensions. We instead directly call the _inherit_docstrings annotation
# See https://github.com/modin-project/modin/issues/7122
import modin.utils  # type: ignore[import]  # isort: skip  # noqa: E402
import modin.pandas.series_utils  # type: ignore[import]  # isort: skip  # noqa: E402

# TODO: SNOW-1643979 pull in fixes for
# https://github.com/modin-project/modin/issues/7113 and https://github.com/modin-project/modin/issues/7134
# Upstream Modin has issues with certain docstring generation edge cases, so we should use our version instead
_inherit_docstrings = snowflake.snowpark.modin.utils._inherit_docstrings

inherit_modules = [
    (docstrings.base.BasePandasDataset, modin.pandas.base.BasePandasDataset),
    (docstrings.dataframe.DataFrame, modin.pandas.dataframe.DataFrame),
    (docstrings.series.Series, modin.pandas.series.Series),
    (docstrings.series_utils.StringMethods, modin.pandas.series_utils.StringMethods),
    (
        docstrings.series_utils.CombinedDatetimelikeProperties,
        modin.pandas.series_utils.DatetimeProperties,
    ),
]

for (doc_module, target_object) in inherit_modules:
    _inherit_docstrings(doc_module, overwrite_existing=True)(target_object)

# _inherit_docstrings needs a function or class as argument, so we must explicitly iterate over
# all members of io and general
function_inherit_modules = [
    (docstrings.io, modin.pandas.io),
    (docstrings.general, modin.pandas.general),
]

for (doc_module, target_module) in function_inherit_modules:
    for name in dir(target_module):
        doc_obj = getattr(doc_module, name, None)
        if not name.startswith("_") and doc_obj is not None:
            _inherit_docstrings(doc_obj, overwrite_existing=True)(
                getattr(target_module, name)
            )


# === SET UP I/O ===
# Configure Modin engine so it detects our Snowflake I/O classes.
# This is necessary to define the `from_pandas` method for each Modin backend, which is called in I/O methods.

from modin.config import Engine  # isort: skip  # noqa: E402

# Secretly insert our factory class into Modin so the dispatcher can find it
from modin.core.execution.dispatching.factories import (  # isort: skip  # noqa: E402
    factories as modin_factories,
)

from snowflake.snowpark.modin.plugin.io.factories import (  # isort: skip  # noqa: E402
    PandasOnSnowflakeFactory,
)

modin_factories.PandasOnSnowflakeFactory = PandasOnSnowflakeFactory

Engine.add_option("Snowflake")
Engine.put("Snowflake")


# === SET UP TELEMETRY ===
# dt and str accessors raise AttributeErrors that get caught by Modin __getitem__. Whitelist
# them in _ATTRS_NO_LOOKUP here to avoid this.
# In upstream Modin, we should change __getitem__ to perform a direct getitem call rather than
# calling self.index[].
from snowflake.snowpark.modin.plugin.utils.frontend_constants import (  # isort: skip  # noqa: E402,F401
    _ATTRS_NO_LOOKUP,
)

modin.pandas.base._ATTRS_NO_LOOKUP.add("dt")
modin.pandas.base._ATTRS_NO_LOOKUP.add("str")
modin.pandas.base._ATTRS_NO_LOOKUP.add("columns")
modin.pandas.base._ATTRS_NO_LOOKUP.update(_ATTRS_NO_LOOKUP)


# For any method defined on Series/DF, add telemetry to it if the method name does not start with an
# _, or the method is in TELEMETRY_PRIVATE_METHODS. This includes methods defined as an extension/override.
from modin.pandas import DataFrame, Series  # isort: skip  # noqa: E402,F401
from modin.pandas.api.extensions import (  # isort: skip  # noqa: E402,F401
    register_dataframe_accessor,
    register_pd_accessor,
    register_series_accessor,
)
from modin.pandas.accessor import ModinAPI  # isort: skip  # noqa: E402,F401

from snowflake.snowpark.modin.plugin._internal.telemetry import (  # isort: skip  # noqa: E402,F401
    TELEMETRY_PRIVATE_METHODS,
    snowpark_pandas_telemetry_standalone_function_decorator,
    try_add_telemetry_to_attribute,
)

# Add telemetry on the ModinAPI accessor object.
# modin 0.30.1 introduces the pd.DataFrame.modin accessor object for non-pandas methods,
# such as pd.DataFrame.modin.to_pandas and pd.DataFrame.modin.to_ray. We will automatically
# raise NotImplementedError for all methods on this accessor object except to_pandas, but
# we still want to record telemetry.
for attr_name in dir(ModinAPI):
    if not attr_name.startswith("_") or attr_name in TELEMETRY_PRIVATE_METHODS:
        setattr(
            ModinAPI,
            attr_name,
            try_add_telemetry_to_attribute(attr_name, getattr(ModinAPI, attr_name)),
        )

for attr_name in dir(Series):
    # Since Series is defined in upstream Modin, all of its members were either defined upstream
    # or overridden by extension.
    # Skip the `modin` accessor object, since we apply telemetry to all its fields.
    if attr_name != "modin" and (
        not attr_name.startswith("_") or attr_name in TELEMETRY_PRIVATE_METHODS
    ):
        register_series_accessor(attr_name)(
            try_add_telemetry_to_attribute(attr_name, getattr(Series, attr_name))
        )

for attr_name in dir(DataFrame):
    # Since DataFrame is defined in upstream Modin, all of its members were either defined upstream
    # or overridden by extension.
    # Skip the `modin` accessor object, since we apply telemetry to all its fields.
    if attr_name != "modin" and (
        not attr_name.startswith("_") or attr_name in TELEMETRY_PRIVATE_METHODS
    ):
        register_dataframe_accessor(attr_name)(
            try_add_telemetry_to_attribute(attr_name, getattr(DataFrame, attr_name))
        )

# Apply telemetry to all top-level functions in the pd namespace.

for attr_name, attr_value in modin.pandas.__dict__.items():
    # Do not add telemetry to any method that is mirrored from native pandas
    if (
        inspect.isfunction(attr_value)
        and not attr_name.startswith("_")
        and attr_value is not getattr(pandas, attr_name, None)
    ):
        register_pd_accessor(attr_name)(
            snowpark_pandas_telemetry_standalone_function_decorator(attr_value)
        )

# === SESSION INITIALIZATION ===
# Make SnowpandasSessionHolder the __class__ of modin.pandas so that we can make
# "session" a lazy property of the module.
# This implementation follows Python's suggestion here:
# https://docs.python.org/3.12/reference/datamodel.html#customizing-module-attribute-access
from snowflake.snowpark.modin.plugin._internal.session import (  # isort: skip  # noqa: E402,F401
    SnowpandasSessionHolder,
)

if "modin.pandas" in sys.modules:
    sys.modules["modin.pandas"].__class__ = SnowpandasSessionHolder


# === OTHER SETUP ===
# Upstream modin does not re-export the offsets module, so we need to do so here
register_pd_accessor("offsets")(pandas.offsets)
