#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import inspect
import sys
from typing import Any, Union

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

# Hybrid Mode Imports
from modin.core.storage_formats.pandas.query_compiler_caster import (
    _GENERAL_EXTENSIONS,
    register_function_for_post_op_switch,
    register_function_for_pre_op_switch,
)

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

for doc_module, target_object in inherit_modules:
    _inherit_docstrings(doc_module, overwrite_existing=True)(target_object)

# _inherit_docstrings needs a function or class as argument, so we must explicitly iterate over
# all members of io and general
function_inherit_modules = [
    (docstrings.io, modin.pandas.io),
    (docstrings.general, modin.pandas.general),
]

for doc_module, target_module in function_inherit_modules:
    for name in dir(target_module):
        doc_obj = getattr(doc_module, name, None)
        if not name.startswith("_") and doc_obj is not None:
            _inherit_docstrings(doc_obj, overwrite_existing=True)(
                getattr(target_module, name)
            )


# === SET UP I/O ===
# Configure Modin engine so it detects our Snowflake I/O classes.
# This is necessary to define the `from_pandas` method for each Modin backend, which is called in I/O methods.

from modin.config import Engine, Backend, Execution  # isort: skip  # noqa: E402

# Secretly insert our factory class into Modin so the dispatcher can find it
from modin.core.execution.dispatching.factories import (  # isort: skip  # noqa: E402
    factories as modin_factories,
)

from snowflake.snowpark.modin.plugin.io.factories import (  # isort: skip  # noqa: E402
    PandasOnSnowflakeFactory,
)

modin_factories.SnowflakeOnSnowflakeFactory = PandasOnSnowflakeFactory

Engine.add_option("Snowflake")

Backend.register_backend(
    "Snowflake", Execution(engine="Snowflake", storage_format="Snowflake")
)
Backend.put("snowflake")

# Hybrid Mode Registration
pre_op_switch_points = [
    {"class_name": "DataFrame", "method": "__init__"},
    {"class_name": "Series", "method": "__init__"},
    {"class_name": "DataFrame", "method": "apply"},
    {"class_name": "Series", "method": "apply"},
    {"class_name": "Series", "method": "items"},
    {"class_name": "DataFrame", "method": "__repr__"},
    {"class_name": "DataFrame", "method": "itertuples"},
    {"class_name": "DataFrame", "method": "iterrows"},
    {"class_name": "DataFrame", "method": "plot"},
    {"class_name": "DataFrame", "method": "describe"},
    {"class_name": "Series", "method": "describe"},
]

post_op_switch_points = [
    {"class_name": None, "method": "read_snowflake"},
    {"class_name": "Series", "method": "value_counts"},
    {"class_name": "DataFrame", "method": "value_counts"},
    {"class_name": "DataFrame", "method": "tail"},
    {"class_name": "DataFrame", "method": "var"},
    {"class_name": "DataFrame", "method": "sum"},
]


for point in pre_op_switch_points:
    print(f"DEBUG: Registering pre-op {point}")
    register_function_for_pre_op_switch(
        class_name=point["class_name"],
        method=point["method"],
        backend="Snowflake",
    )

for point in post_op_switch_points:
    print(f"DEBUG: Registering post-op {point}")
    register_function_for_post_op_switch(
        class_name=point["class_name"],
        method=point["method"],
        backend="Snowflake",
    )

Backend.set_active_backends(["Snowflake", "Pandas"])

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
#
# Telemetry is currently not recorded for the ModinAPI accessor object, which contains methods such as
# df.modin.to_pandas() that Snowpark pandas raises NotImplementedError for.
from modin.pandas import DataFrame, Series  # isort: skip  # noqa: E402,F401
from modin.pandas.base import BasePandasDataset  # isort: skip  # noqa: E402,F401
from modin.pandas.api.extensions import (  # isort: skip  # noqa: E402,F401
    register_pd_accessor,
    register_series_accessor,
    register_dataframe_accessor,
    register_base_accessor,
)
from modin.pandas.accessor import ModinAPI  # isort: skip  # noqa: E402,F401
from modin.core.storage_formats.pandas.query_compiler_caster import (  # isort: skip  # noqa: E402,F401
    _NON_EXTENDABLE_ATTRIBUTES,
    _GENERAL_EXTENSIONS,
)

from snowflake.snowpark.modin.plugin._internal.telemetry import (  # isort: skip  # noqa: E402,F401
    TELEMETRY_PRIVATE_METHODS,
    snowpark_pandas_telemetry_standalone_function_decorator,
    try_add_telemetry_to_attribute,
    connect_modin_telemetry,
)


def _maybe_apply_telemetry(
    cls: Union[DataFrame, Series, BasePandasDataset],
    register_method: callable,
    attr_name: str,
) -> Any:
    if (
        # Skip the `modin` accessor object.
        attr_name != "modin"
        and attr_name not in _NON_EXTENDABLE_ATTRIBUTES
        and (not attr_name.startswith("_") or attr_name in TELEMETRY_PRIVATE_METHODS)
    ):
        # If we already defined the method via the extensions system, then we need to retrieve it from
        # the extensions dictionary directly to circumvent modin's caster dispatch wrapper. If the
        # method was not defined by extension, then just use the original upstream definition.
        # Note that unlike prior versions of Snowpark pandas, we check BasePandasDataset extensions
        # separately from child Series/DataFrame extensions.
        attr_value = cls._extensions["Snowflake"].get(
            attr_name, getattr(cls, attr_name)
        )
        # Because the QueryCompilerCaster ABC automatically wraps all methods with a dispatch to the appropriate
        # backend, we must use the __wrapped__ property of the originally-defined attribute to avoid
        # infinite recursion.
        # Do not check for __wrapped__ if this was defined as a Snowflake extension, since we use
        # decorators to raise NotImplementedError and apply exceptions.
        if attr_name not in cls._extensions["Snowflake"] and hasattr(
            attr_value, "__wrapped__"
        ):
            attr_value = attr_value.__wrapped__

        register_method(attr_name, backend="Snowflake")(
            try_add_telemetry_to_attribute(attr_name, attr_value)
        )


# Iterating over __dict__ will skip over any methods defined by BasePandasDataset, while
# still picking up methods like to_Snowflake defined via the extensions system.
for attr_name in Series.__dict__:
    _maybe_apply_telemetry(Series, register_series_accessor, attr_name)

for attr_name in DataFrame.__dict__:
    _maybe_apply_telemetry(DataFrame, register_dataframe_accessor, attr_name)

for attr_name in BasePandasDataset.__dict__:
    # To prevent double-counting of APIs, only record telemetry if Series/DataFrame BOTH do not
    # define the method as extensions themselves. This will create underreporting in certain edge cases
    # where BasePandasDataset and DataFrame define both a method but Series does not, but few APIs
    # are affected by this.
    if (
        attr_name not in DataFrame._extensions["Snowflake"]
        and attr_name not in Series._extensions["Snowflake"]
    ):
        _maybe_apply_telemetry(BasePandasDataset, register_base_accessor, attr_name)


# Apply telemetry to top-level functions in the pd namespace.

for attr_name in dir(modin.pandas):
    # Upstream modin creates a dispatch wrapper for top-level methods, so we need to read
    # from the extensions dict instead of directly calling getattr to prevent recursion.
    if attr_name in _GENERAL_EXTENSIONS["Snowflake"]:
        defined_backend = "Snowflake"
    else:
        # We can't call register_pd_accessor(backend="Snowflake")(attr_value) if the object was not
        # defined on the Snowflake backend because this causes infinite mutual recursion.
        defined_backend = None
        continue
    attr_value = _GENERAL_EXTENSIONS[defined_backend].get(
        attr_name, getattr(modin.pandas, attr_name)
    )
    # Do not check for __wrapped__ if this was defined as a Snowflake extension, since we use
    # decorators to raise NotImplementedError and apply exceptions.
    if defined_backend != "Snowflake" and hasattr(attr_value, "__wrapped__"):
        attr_value = attr_value.__wrapped__
    # Do not add telemetry to any method that is mirrored from native pandas
    if (
        inspect.isfunction(attr_value)
        and not attr_name.startswith("_")
        and attr_value is not getattr(pandas, attr_name, None)
    ):
        register_pd_accessor(attr_name, backend=defined_backend)(
            snowpark_pandas_telemetry_standalone_function_decorator(attr_value)
        )


# enable Modin's metrics system to collect API data for hybrid execution in parallel with
# Snowpark-pandas specific information
connect_modin_telemetry()


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
