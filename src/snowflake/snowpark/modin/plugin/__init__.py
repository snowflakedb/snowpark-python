#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import inspect
import sys
from typing import Union, Callable, Any
import warnings

from packaging import version

if sys.version_info.major == 3 and sys.version_info.minor == 8:
    raise RuntimeError(
        "Snowpark pandas does not support Python 3.8. Please update to Python 3.9 or later."
    )  # pragma: no cover

# pandas import needs to come before Python version + modin checks,
# since modin may raise its own warnings/errors on the wrong pandas version
import pandas  # isort: skip  # noqa: E402

recommended_supported_modin_version = "0.37.0"

install_modin_msg = (
    f"Please set the modin version as {recommended_supported_modin_version} in the Packages menu at the top of your notebook."
    if "snowbook" in sys.modules  # this indicates the environment is Snowflake Notebook
    else 'Run `pip install --upgrade "snowflake-snowpark-python[modin]"` to resolve.'
)

try:
    import modin  # type: ignore
except ModuleNotFoundError:  # pragma: no cover
    raise ModuleNotFoundError(
        "Modin is not installed. " + install_modin_msg
    )  # pragma: no cover

modin_min_supported_version = version.parse("0.36.0")
modin_max_supported_version = version.parse("0.38.0")  # non-inclusive
actual_modin_version = version.parse(modin.__version__)
if not (
    modin_min_supported_version <= actual_modin_version < modin_max_supported_version
):
    raise ImportError(
        f"The Modin version installed ({modin.__version__}) does not match the currently supported Modin versions in"
        + f" Snowpark pandas (modin >= {modin_min_supported_version}, < {modin_max_supported_version})."
        + install_modin_msg
    )  # pragma: no cover


# TODO SNOW-1758773: perform pandas version check in modin instead
actual_pandas_version = version.parse(pandas.__version__)
supported_pandas_major_version = 2
recommended_pandas_minor_version = 3
pandas_version_supported = (
    actual_pandas_version.major == supported_pandas_major_version
    and actual_pandas_version.minor in (2, 3)
)

install_pandas_msg = (
    f"Please set the pandas version as {supported_pandas_major_version}.{recommended_pandas_minor_version}.x in the Packages menu at the top of your notebook."
    if "snowbook" in sys.modules  # this indicates the environment is Snowflake Notebook
    else 'Run `pip install --upgrade "snowflake-snowpark-python[modin]"` to resolve.'
)

if not pandas_version_supported:
    raise RuntimeError(
        f"The pandas version installed ({pandas.__version__}) does not match the supported pandas version in"
        + f" Snowpark pandas ({supported_pandas_major_version}.{recommended_pandas_minor_version}.x). "
        + install_pandas_msg
    )  # pragma: no cover


# === INITIALIZE EXTENSION SYSTEM ===
# Initialize all extension modules.
import snowflake.snowpark.modin.plugin.extensions.pd_extensions  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.io_overrides  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.general_overrides  # isort: skip  # noqa: E402,F401

# base overrides occur before subclass overrides in case subclasses override a base method
import snowflake.snowpark.modin.plugin.extensions.base_overrides  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.dataframe_extensions  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.dataframe_overrides  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.series_extensions  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.series_overrides  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.dataframe_groupby_overrides  # isort: skip  # noqa: E402,F401
import snowflake.snowpark.modin.plugin.extensions.series_groupby_overrides  # isort: skip  # noqa: E402,F401

# === INITIALIZE DOCSTRINGS ===
# These imports also all need to occur after modin + pandas dependencies are validated.
from snowflake.snowpark.modin.config import DocModule  # isort: skip  # noqa: E402
from snowflake.snowpark.modin.plugin import docstrings  # isort: skip  # noqa: E402

DocModule.put(docstrings.__name__)

import modin.pandas.series_utils  # type: ignore[import]  # isort: skip  # noqa: E402
import modin.pandas.groupby  # isort: skip  # noqa: E402

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
    (
        docstrings.groupby.DataFrameGroupBy,
        modin.pandas.groupby.DataFrameGroupBy,
    ),
    (
        docstrings.groupby.SeriesGroupBy,
        modin.pandas.groupby.SeriesGroupBy,
    ),
]

for (doc_module, target_object) in inherit_modules:
    _inherit_docstrings(doc_module, overwrite_existing=True)(target_object)

# _inherit_docstrings needs a function or class as argument, so we must explicitly iterate over
# all members of io and general. Their override targets must be in the top-level pandas namespace
# to resolve properly with the extensions system.
function_inherit_modules = [
    (docstrings.io, modin.pandas),
    (docstrings.general, modin.pandas),
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

from modin.config import (  # isort: skip  # noqa: E402
    Engine,
    Backend,
    Execution,
    ValueSource,
)

# Secretly insert our factory class into Modin so the dispatcher can find it
from modin.core.execution.dispatching.factories import (  # isort: skip  # noqa: E402
    factories as modin_factories,
)

from snowflake.snowpark.modin.plugin.io.factories import (  # isort: skip  # noqa: E402
    PandasOnSnowflakeFactory,
)

modin_factories.SnowflakeOnSnowflakeFactory = PandasOnSnowflakeFactory
Engine.add_option("Snowflake")
if "Snowflake" not in Backend.get_active_backends():
    Backend.register_backend(
        "Snowflake", Execution(engine="Snowflake", storage_format="Snowflake")
    )
Backend.put("snowflake")

from modin.core.storage_formats.pandas.query_compiler_caster import (  # isort: skip  # noqa: E402
    _GENERAL_EXTENSIONS,
    _NON_EXTENDABLE_ATTRIBUTES,
    register_function_for_post_op_switch,
    register_function_for_pre_op_switch,
)
from modin.config import AutoSwitchBackend  # isort: skip  # noqa: E402

HYBRID_WARNING = (
    "Snowpark pandas now runs with hybrid execution enabled by default, and will perform certain operations "
    + "on smaller data using local, in-memory pandas. To disable this behavior and force all computations to occur in "
    + "Snowflake, run this line:\nfrom modin.config import AutoSwitchBackend; AutoSwitchBackend.disable()"
)

warnings.filterwarnings("once", message=HYBRID_WARNING)

if AutoSwitchBackend.get_value_source() is ValueSource.DEFAULT:
    AutoSwitchBackend.enable()

if AutoSwitchBackend.get():
    warnings.warn(HYBRID_WARNING, stacklevel=1)

# Hybrid Mode Registration
# In hybrid execution mode, the client will automatically switch backends when a
# wholly-unimplemented method is called. Those switch points are registered separately in
# extensions files via the register_*_not_implemented family of methods.
pre_op_switch_points: list[dict[str, Union[str, None]]] = [
    {"class_name": "DataFrame", "method": "__init__"},
    {"class_name": "Series", "method": "__init__"},
    {"class_name": "DataFrame", "method": "apply"},
    {"class_name": "Series", "method": "apply"},
    {"class_name": "Series", "method": "items"},
    {"class_name": "DataFrame", "method": "itertuples"},
    {"class_name": "DataFrame", "method": "iterrows"},
    {"class_name": "DataFrame", "method": "plot"},
    {"class_name": "DataFrame", "method": "quantile"},
    {"class_name": "Series", "method": "plot"},
    {"class_name": "Series", "method": "quantile"},
    {"class_name": "DataFrame", "method": "T"},
    {"class_name": "DataFrame", "method": "transpose"},
    {"class_name": None, "method": "read_csv"},
    {"class_name": None, "method": "read_json"},
    {"class_name": None, "method": "concat"},
    {"class_name": None, "method": "merge"},
    {"class_name": "DataFrame", "method": "merge"},
    {"class_name": "DataFrame", "method": "join"},
]

# Always auto-switch for aggregations, since if they return a 1-D frame/series it will be much smaller
# than the original data.
# Not all of these are currently supported in Snowpark pandas.
# None of these need to be registered for Series because those methods always return scalars.
aggregations = [
    # note that head and tail are groupby-filters, not aggregations
    "tail",
    "var",
    "std",
    "sum",
    "sem",
    "max",
    "mean",
    "min",
    "agg",
    "aggregate",
    "count",
    "nunique",
    # TODO these cumulative functions are window functions, not aggregations, right?
    "cummax",
    "cummin",
    "cumprod",
    "cumsum",
]

post_op_switch_points: list[dict[str, Union[str, None]]] = (
    [  # type: ignore[assignment]
        {"class_name": None, "method": "read_snowflake"},
        {"class_name": "Series", "method": "value_counts"},
        {"class_name": "DataFrame", "method": "value_counts"},
        # Series.agg can return a Series if a list of aggregations is provided
        {"class_name": "Series", "method": "agg"},
        {"class_name": "Series", "method": "aggregate"},
    ]
    + [{"class_name": "DataFrame", "method": agg_method} for agg_method in aggregations]
    + [
        {"class_name": "DataFrameGroupBy", "method": agg_method}
        for agg_method in aggregations
    ]
    + [
        {"class_name": "SeriesGroupBy", "method": agg_method}
        for agg_method in aggregations
    ]
)

for point in pre_op_switch_points:
    register_function_for_pre_op_switch(
        class_name=point["class_name"],
        method=point["method"],
        backend="Snowflake",
    )

for point in post_op_switch_points:
    register_function_for_post_op_switch(
        class_name=point["class_name"],
        method=point["method"],
        backend="Snowflake",
    )

# On the pandas backend, auto-switch for apply-like methods so that those
# methods can apply Snowpark and Cortex functions.
for class_name, method in (
    ("DataFrame", "apply"),
    ("DataFrame", "applymap"),
    ("DataFrame", "map"),
    ("Series", "apply"),
    ("Series", "map"),
):
    register_function_for_pre_op_switch(
        class_name=class_name,
        method=method,
        backend="Pandas",
    )

Backend.set_active_backends(["Snowflake", "Pandas", "Ray"])


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
    connect_modin_telemetry,
    snowpark_pandas_telemetry_standalone_function_decorator,
    try_add_telemetry_to_attribute,
)

# Telemetry is currently not recorded for the ModinAPI accessor object, which contains methods such as
# df.modin.to_pandas() that Snowpark pandas raises NotImplementedError for.
from modin.pandas.base import BasePandasDataset  # isort: skip  # noqa: E402,F401
from modin.pandas.groupby import (  # isort: skip  # noqa: E402,F401
    DataFrameGroupBy,
    SeriesGroupBy,
)
from modin.pandas.api.extensions import (  # isort: skip  # noqa: E402,F401
    register_base_accessor,
    register_dataframe_groupby_accessor,
    register_series_groupby_accessor,
)


def _maybe_apply_telemetry(
    cls: Union[DataFrame, Series, BasePandasDataset],
    register_method: Callable,
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
        # backend, we must use the _wrapped_method_for_casting property of the originally-defined attribute to avoid
        # infinite recursion.
        # Do not check for _wrapped_method_for_casting if this was already defined as a Snowflake extension,
        # since we use decorators to raise NotImplementedError and apply exceptions.
        if attr_name not in cls._extensions["Snowflake"] and hasattr(
            attr_value, "_wrapped_method_for_casting"
        ):
            attr_value = attr_value._wrapped_method_for_casting

        register_method(attr_name, backend="Snowflake")(
            try_add_telemetry_to_attribute(attr_name, attr_value)
        )


# Iterating over __dict__ will skip over any methods defined by BasePandasDataset, while
# still picking up methods like to_snowflake defined via the extensions system.
for attr_name in Series.__dict__:
    _maybe_apply_telemetry(Series, register_series_accessor, attr_name)

for attr_name in DataFrame.__dict__:
    _maybe_apply_telemetry(DataFrame, register_dataframe_accessor, attr_name)

for attr_name in BasePandasDataset.__dict__:
    # To prevent double-counting of APIs, only record telemetry if Series/DataFrame BOTH do not
    # define the method as extensions themselves. This will create under-reporting in certain edge cases
    # where BasePandasDataset and DataFrame both define a method but Series does not, but few APIs
    # are affected by this.
    if (
        attr_name not in DataFrame._extensions["Snowflake"]
        and attr_name not in Series._extensions["Snowflake"]
    ):
        _maybe_apply_telemetry(BasePandasDataset, register_base_accessor, attr_name)

for attr_name in DataFrameGroupBy.__dict__:
    _maybe_apply_telemetry(
        DataFrameGroupBy, register_dataframe_groupby_accessor, attr_name
    )

for attr_name in SeriesGroupBy.__dict__:
    _maybe_apply_telemetry(SeriesGroupBy, register_series_groupby_accessor, attr_name)

# Apply telemetry to top-level functions in the pd namespace.
defined_backend = None
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
    # Do not check for _wrapped_method_for_casting if this was defined as a Snowflake extension, since we use
    # decorators to raise NotImplementedError and apply exceptions.
    if defined_backend != "Snowflake" and hasattr(
        attr_value, "_wrapped_method_for_casting"
    ):
        attr_value = attr_value._wrapped_method_for_casting
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
