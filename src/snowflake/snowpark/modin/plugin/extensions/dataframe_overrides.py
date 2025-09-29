#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
File containing DataFrame APIs defined in the Modin API layer, but with different behavior in Snowpark
pandas, such as `DataFrame.memory_usage`.
"""

from __future__ import annotations

import collections
import copy
import datetime
import functools
import itertools
import sys
import warnings
from typing import (
    IO,
    Any,
    Callable,
    Hashable,
    Iterable,
    Iterator,
    Literal,
    Mapping,
    Sequence,
)

import modin.pandas as pd
import numpy as np
import pandas as native_pd
from modin.pandas import DataFrame, Series
from pandas.core.interchange.dataframe_protocol import DataFrame as InterchangeDataframe
from modin.pandas.base import BasePandasDataset
from modin.pandas.io import from_pandas
from modin.pandas.utils import is_scalar
from modin.core.computation.eval import eval as _eval
from modin.core.storage_formats.pandas.query_compiler_caster import (
    register_function_for_pre_op_switch,
)
from pandas._libs.lib import NoDefault, no_default
from pandas._typing import (
    AggFuncType,
    AnyArrayLike,
    Axes,
    Axis,
    CompressionOptions,
    FilePath,
    FillnaOptions,
    IgnoreRaise,
    IndexLabel,
    Level,
    PythonFuncType,
    Renamer,
    Scalar,
    StorageOptions,
    Suffixes,
    WriteBuffer,
    WriteExcelBuffer,
)
from pandas.core.common import apply_if_callable, is_bool_indexer
from pandas.core.dtypes.common import (
    infer_dtype_from_object,
    is_bool_dtype,
    is_dict_like,
    is_list_like,
    is_numeric_dtype,
)
from pandas.core.dtypes.inference import is_hashable, is_integer
from pandas.core.indexes.base import ensure_index as ensure_native_index
from pandas.core.indexes.frozen import FrozenList
from pandas.io.formats.printing import pprint_thing
from pandas.util._validators import validate_bool_kwarg

from snowflake.snowpark.modin.plugin._internal.aggregation_utils import (
    is_snowflake_agg_func,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    new_snow_series,
    add_extra_columns_and_select_required_columns,
    assert_fields_are_none,
    convert_index_to_list_of_qcs,
    convert_index_to_qc,
    error_checking_for_init,
    is_repr_truncated,
)
from snowflake.snowpark.modin.plugin._typing import ListLike
from snowflake.snowpark.modin.plugin.compiler.snowflake_query_compiler import (
    SnowflakeQueryCompiler,
    HYBRID_SWITCH_FOR_UNIMPLEMENTED_METHODS,
)
from snowflake.snowpark.modin.plugin.extensions.index import Index
from snowflake.snowpark.modin.plugin.extensions.snow_partition_iterator import (
    SnowparkPandasRowPartitionIterator,
)
from snowflake.snowpark.modin.plugin.extensions.utils import (
    create_empty_native_pandas_frame,
    raise_if_native_pandas_objects,
    replace_external_data_keys_with_empty_pandas_series,
    replace_external_data_keys_with_query_compiler,
    try_convert_index_to_native,
    update_eval_and_query_engine_kwarg_and_maybe_warn,
)
from snowflake.snowpark.modin.plugin.utils.error_message import (
    ErrorMessage,
    dataframe_not_implemented,
)
from snowflake.snowpark.modin.plugin.utils.frontend_constants import (
    DF_ITERROWS_ITERTUPLES_WARNING_MESSAGE,
    DF_SETITEM_LIST_LIKE_KEY_AND_RANGE_LIKE_VALUE,
    DF_SETITEM_SLICE_AS_SCALAR_VALUE,
)
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.modin.utils import (
    _inherit_docstrings,
    hashable,
    validate_int_kwarg,
)
from snowflake.snowpark.udf import UserDefinedFunction

from modin.pandas.groupby import DataFrameGroupBy
from snowflake.snowpark.modin.plugin.extensions.dataframe_groupby_overrides import (
    validate_groupby_args,
)
from modin.pandas.api.extensions import (
    register_dataframe_accessor as _register_dataframe_accessor,
)


# eval() and query() let the user reference variables according to dynamic
# scope at `level` stack frames below the stack frame that called them. If
# a snowflake override function here wraps an eval/query implementation
# that's in another function, the implementation function frame is 4 frames
# above the override function frame:
# 1) query_compiler_caster wrapper dispatches to snowflake implementation
# 2) telemetry wrapper 1
# 3) telemetry wrapper 2 calls the snowflake implementation.
# 4) The snowflake implementation calls the implementation function.
# so we add 4 to the `level` param.
EVAL_QUERY_EXTRA_STACK_LEVELS = 4


register_dataframe_accessor = functools.partial(
    _register_dataframe_accessor, backend="Snowflake"
)


def register_dataframe_not_implemented():
    def decorator(base_method: Any):
        func = dataframe_not_implemented()(base_method)
        name = base_method.__name__
        HYBRID_SWITCH_FOR_UNIMPLEMENTED_METHODS.add(("DataFrame", name))
        register_function_for_pre_op_switch(
            class_name="DataFrame", backend="Snowflake", method=name
        )
        register_dataframe_accessor(name)(func)
        return func

    return decorator


# === UNIMPLEMENTED METHODS ===
# The following methods are not implemented in Snowpark pandas, and must be overridden on the
# frontend. These methods fall into a few categories:
# 1. Would work in Snowpark pandas, but we have not tested it.
# 2. Would work in Snowpark pandas, but requires more SQL queries than we are comfortable with.
# 3. Requires materialization (usually via a frontend _default_to_pandas call).
# 4. Performs operations on a native pandas Index object that are nontrivial for Snowpark pandas to manage.


# Avoid overwriting builtin `map` by accident
@register_dataframe_accessor("map")
def _map(self, func: PythonFuncType, na_action: str | None = None, **kwargs):
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    if not callable(func):
        raise TypeError(f"{func} is not callable")  # pragma: no cover
    return self.__constructor__(
        query_compiler=self._query_compiler.applymap(
            func, na_action=na_action, **kwargs
        )
    )


def boxplot(
    self,
    column=None,
    by=None,
    ax=None,
    fontsize=None,
    rot=0,
    grid=True,
    figsize=None,
    layout=None,
    return_type=None,
    backend=None,
    **kwargs,
):  # noqa: PR01, RT01, D200
    WarningMessage.single_warning(
        "DataFrame.boxplot materializes data to the local machine."
    )
    return self._to_pandas().boxplot(
        column=column,
        by=by,
        ax=ax,
        fontsize=fontsize,
        rot=rot,
        grid=grid,
        figsize=figsize,
        layout=layout,
        return_type=return_type,
        backend=backend,
        **kwargs,
    )


@register_dataframe_not_implemented()
def combine(
    self, other, func, fill_value=None, overwrite=True
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_dataframe_not_implemented()
def corrwith(
    self, other, axis=0, drop=False, method="pearson", numeric_only=False
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_dataframe_not_implemented()
def cov(
    self, min_periods=None, ddof: int | None = 1, numeric_only=False
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_dataframe_not_implemented()
def dot(self, other):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


# Override eval because
# 1) we treat the "engine" parameter differently
# 2) We have to update the level parameter to reflect this method's place in
#    the function stack. Modin can't do that for us since it doesn't know our
#    place in the stack.
@register_dataframe_accessor("eval")
def eval(self, expr, inplace=False, **kwargs):  # noqa: PR01, RT01, D200
    """
    Evaluate a string describing operations on ``DataFrame`` columns.
    """
    if self._query_compiler.nlevels() > 1:
        # If the rows of this dataframe have a multi-index, we store the index
        # as a native_pd.MultiIndex, and the usual method of getting index
        # resolvers with _get_index_resolvers() does not work.
        ErrorMessage.not_implemented("eval() does not support a multi-level index.")

    inplace = validate_bool_kwarg(inplace, "inplace")

    update_eval_and_query_engine_kwarg_and_maybe_warn(kwargs)

    kwargs["level"] = kwargs.get("level", 0) + EVAL_QUERY_EXTRA_STACK_LEVELS

    index_resolvers = self._get_index_resolvers()
    column_resolvers = self._get_cleaned_column_resolvers()
    kwargs["resolvers"] = (
        *kwargs.get("resolvers", ()),
        index_resolvers,
        column_resolvers,
    )

    if "target" not in kwargs:
        kwargs["target"] = self

    return _eval(expr, inplace=inplace, **kwargs)


@register_dataframe_not_implemented()
def hist(
    self,
    column=None,
    by=None,
    grid=True,
    xlabelsize=None,
    xrot=None,
    ylabelsize=None,
    yrot=None,
    ax=None,
    sharex=False,
    sharey=False,
    figsize=None,
    layout=None,
    bins=10,
    **kwds,
):
    pass  # pragma: no cover


@register_dataframe_not_implemented()
def isetitem(self, loc, value):
    pass  # pragma: no cover


@register_dataframe_not_implemented()
def prod(
    self,
    axis=None,
    skipna=True,
    numeric_only=False,
    min_count=0,
    **kwargs,
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


register_dataframe_accessor("product")(prod)


@register_dataframe_accessor("query")
def query(self, expr, inplace=False, **kwargs):
    if self._query_compiler.nlevels() > 1:
        # If the rows of this dataframe have a multi-index, we store the index
        # as a native_pd.MultiIndex, and the usual method of getting index
        # resolvers with _get_index_resolvers() does not work.
        ErrorMessage.not_implemented("query() does not support a multi-level index.")

    inplace = validate_bool_kwarg(inplace, "inplace")

    update_eval_and_query_engine_kwarg_and_maybe_warn(kwargs)

    if inplace and "target" not in kwargs:
        kwargs["target"] = self
    else:
        # have to explicitly set target=None to get correct error for
        # multi-line query.
        kwargs["target"] = None

    key = self.eval(
        expr,
        inplace=False,
        **(kwargs | {"level": kwargs.get("level", 0) + EVAL_QUERY_EXTRA_STACK_LEVELS}),
    )

    try:
        result = self.loc[key]
    except ValueError:
        # when res is multi-dimensional, loc raises an error, but that is
        # sometimes a valid query.
        result = self[key]

    return self._create_or_update_from_compiler(result._query_compiler, inplace=inplace)


@register_dataframe_not_implemented()
def reindex_like(
    self,
    other,
    method=None,
    copy: bool | None = None,
    limit=None,
    tolerance=None,
) -> DataFrame:  # pragma: no cover
    pass  # pragma: no cover


@register_dataframe_not_implemented()
def to_feather(self, path, **kwargs):  # pragma: no cover # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_dataframe_not_implemented()
def to_gbq(
    self,
    destination_table,
    project_id=None,
    chunksize=None,
    reauth=False,
    if_exists="fail",
    auth_local_webserver=True,
    table_schema=None,
    location=None,
    progress_bar=True,
    credentials=None,
):  # pragma: no cover # noqa: PR01, RT01, D200
    pass  # pragma: no cover


def to_excel(
    self,
    excel_writer: FilePath | WriteExcelBuffer | pd.ExcelWriter,
    sheet_name: str = "Sheet1",
    na_rep: str = "",
    float_format: str | None = None,
    columns: Sequence[Hashable] | None = None,
    header: Sequence[Hashable] | bool = True,
    index: bool = True,
    index_label: IndexLabel | None = None,
    startrow: int = 0,
    startcol: int = 0,
    engine: Literal["openpyxl", "xlsxwriter"] | None = None,
    merge_cells: bool = True,
    inf_rep: str = "inf",
    freeze_panes: tuple[int, int] | None = None,
    storage_options: StorageOptions | None = None,
    engine_kwargs: dict[str, Any] | None = None,
):  # noqa: PR01, RT01, D200
    WarningMessage.single_warning(
        "DataFrame.to_excel materializes data to the local machine."
    )
    return self._to_pandas().to_excel(
        excel_writer=excel_writer,
        sheet_name=sheet_name,
        na_rep=na_rep,
        float_format=float_format,
        columns=columns,
        header=header,
        index=index,
        index_label=index_label,
        startrow=startrow,
        startcol=startcol,
        engine=engine,
        merge_cells=merge_cells,
        inf_rep=inf_rep,
        freeze_panes=freeze_panes,
        storage_options=storage_options,
        engine_kwargs=engine_kwargs,
    )


@register_dataframe_not_implemented()
def to_orc(self, path=None, *, engine="pyarrow", index=None, engine_kwargs=None):
    pass  # pragma: no cover


def to_html(
    self,
    buf=None,
    columns=None,
    col_space=None,
    header=True,
    index=True,
    na_rep="NaN",
    formatters=None,
    float_format=None,
    sparsify=None,
    index_names=True,
    justify=None,
    max_rows=None,
    max_cols=None,
    show_dimensions=False,
    decimal=".",
    bold_rows=True,
    classes=None,
    escape=True,
    notebook=False,
    border=None,
    table_id=None,
    render_links=False,
    encoding=None,
):  # noqa: PR01, RT01, D200
    WarningMessage.single_warning(
        "DataFrame.to_html materializes data to the local machine."
    )
    return self._to_pandas().to_html


@register_dataframe_not_implemented()
def to_parquet(
    self,
    path=None,
    engine="auto",
    compression="snappy",
    index=None,
    partition_cols=None,
    storage_options: StorageOptions = None,
    **kwargs,
):
    pass  # pragma: no cover


@register_dataframe_not_implemented()
def to_period(
    self, freq=None, axis=0, copy=True
):  # pragma: no cover # noqa: PR01, RT01, D200
    pass  # pragma: no cover


@register_dataframe_not_implemented()
def to_records(
    self, index=True, column_dtypes=None, index_dtypes=None
):  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


def to_string(
    self,
    buf=None,
    columns=None,
    col_space=None,
    header=True,
    index=True,
    na_rep="NaN",
    formatters=None,
    float_format=None,
    sparsify=None,
    index_names=True,
    justify=None,
    max_rows=None,
    min_rows=None,
    max_cols=None,
    show_dimensions=False,
    decimal=".",
    line_width=None,
    max_colwidth=None,
    encoding=None,
):  # noqa: PR01, RT01, D200
    WarningMessage.single_warning(
        "DataFrame.to_string materializes data to the local machine."
    )
    return self._to_pandas().to_string(
        buf=buf,
        columns=columns,
        col_space=col_space,
        header=header,
        index=index,
        na_rep=na_rep,
        formatters=formatters,
        float_format=float_format,
        sparsify=sparsify,
        index_names=index_names,
        justify=justify,
        max_rows=max_rows,
        min_rows=min_rows,
        max_cols=max_cols,
        show_dimensions=show_dimensions,
        decimal=decimal,
        line_width=line_width,
        max_colwidth=max_colwidth,
        encoding=encoding,
    )


@register_dataframe_not_implemented()
def to_stata(
    self,
    path: FilePath | WriteBuffer[bytes],
    convert_dates: dict[Hashable, str] | None = None,
    write_index: bool = True,
    byteorder: str | None = None,
    time_stamp: datetime.datetime | None = None,
    data_label: str | None = None,
    variable_labels: dict[Hashable, str] | None = None,
    version: int | None = 114,
    convert_strl: Sequence[Hashable] | None = None,
    compression: CompressionOptions = "infer",
    storage_options: StorageOptions = None,
    *,
    value_labels: dict[Hashable, dict[float | int, str]] | None = None,
):
    pass  # pragma: no cover


@register_dataframe_not_implemented()
def to_xml(
    self,
    path_or_buffer=None,
    index=True,
    root_name="data",
    row_name="row",
    na_rep=None,
    attr_cols=None,
    elem_cols=None,
    namespaces=None,
    prefix=None,
    encoding="utf-8",
    xml_declaration=True,
    pretty_print=True,
    parser="lxml",
    stylesheet=None,
    compression="infer",
    storage_options=None,
):
    pass  # pragma: no cover


@register_dataframe_accessor("style")
@property
def style(self):  # noqa: RT01, D200
    return self._to_pandas().style


@register_dataframe_not_implemented()
def __reduce__(self):
    pass  # pragma: no cover


@register_dataframe_not_implemented()
def __divmod__(self, other):
    pass  # pragma: no cover


@register_dataframe_not_implemented()
def __rdivmod__(self, other):
    pass  # pragma: no cover


@register_dataframe_not_implemented()
def update(self, other) -> None:  # noqa: PR01, RT01, D200
    pass  # pragma: no cover


# The from_dict and from_records accessors are class methods and cannot be overridden via the
# extensions module, as they need to be foisted onto the namespace directly because they are not
# routed through getattr. To this end, we manually set DataFrame.from_dict to our new method.
@classmethod
def from_dict(
    cls, data, orient="columns", dtype=None, columns=None
):  # pragma: no cover # noqa: PR01, RT01, D200
    """
    Construct ``DataFrame`` from dict of array-like or dicts.
    """
    return DataFrame(
        native_pd.DataFrame.from_dict(
            data=data,
            orient=orient,
            dtype=dtype,
            columns=columns,
        )
    )


DataFrame.from_dict = from_dict


@classmethod
def from_records(
    cls,
    data,
    index=None,
    exclude=None,
    columns=None,
    coerce_float=False,
    nrows=None,
):  # pragma: no cover # noqa: PR01, RT01, D200
    """
    Convert structured or record ndarray to ``DataFrame``.
    """
    if isinstance(data, DataFrame):
        ErrorMessage.not_implemented(
            "Snowpark pandas 'DataFrame.from_records' method does not yet support 'data' parameter of type 'DataFrame'"
        )
    return DataFrame(
        native_pd.DataFrame.from_records(
            data=data,
            index=index,
            exclude=exclude,
            columns=columns,
            coerce_float=coerce_float,
            nrows=nrows,
        )
    )


DataFrame.from_records = from_records


# === OVERRIDDEN METHODS ===
# The below methods have their frontend implementations overridden compared to the version present
# in series.py. This is usually for one of the following reasons:
# 1. The underlying QC interface used differs from that of modin. Notably, this applies to aggregate
#    and binary operations; further work is needed to refactor either our implementation or upstream
#    modin's implementation.
# 2. Modin performs extra validation queries that perform extra SQL queries. Some of these are already
#    fixed on main; see https://github.com/modin-project/modin/issues/7340 for details.
# 3. Upstream Modin defaults to pandas for some edge cases. Defaulting to pandas at the query compiler
#    layer is acceptable because we can force the method to raise NotImplementedError, but if a method
#    defaults at the frontend, Modin raises a warning and performs the operation by coercing the
#    dataset to a native pandas object. Removing these is tracked by
#    https://github.com/modin-project/modin/issues/7104


# Snowpark pandas overrides the constructor for two reasons:
# 1. To support the Snowpark pandas lazy index object
# 2. To avoid raising "UserWarning: Distributing <class 'list'> object. This may take some time."
#    when a literal is passed in as data.
@register_dataframe_accessor("__init__")
def __init__(
    self,
    data=None,
    index=None,
    columns=None,
    dtype=None,
    copy=None,
    query_compiler=None,
) -> None:
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    # Siblings are other dataframes that share the same query compiler. We
    # use this list to update inplace when there is a shallow copy.

    self._siblings = []

    # Setting the query compiler
    # --------------------------
    if query_compiler is not None:
        # If a query_compiler is passed in only use the query_compiler field to create a new DataFrame.
        # Verify that the data, index, and columns parameters are None.
        assert_fields_are_none(
            class_name="DataFrame", data=data, index=index, dtype=dtype, columns=columns
        )
        self._query_compiler = query_compiler
        return

    # A DataFrame cannot be used as an index and Snowpark pandas does not support the Categorical type yet.
    # Check that index is not a DataFrame and dtype is not "category".
    error_checking_for_init(index, dtype)

    # Convert columns to a local object if it is lazy.
    if columns is not None:
        columns = (
            columns.to_pandas()
            if isinstance(columns, (Index, BasePandasDataset))
            else columns
        )
        columns = ensure_native_index(columns)

    # The logic followed here is:
    # STEP 1: Obtain the query_compiler from the provided data if the data is lazy. If data is local, keep the query
    #         compiler as None.
    # STEP 2: If columns are provided, set the columns if the data is lazy.
    # STEP 3: If both the data and index are local (or index is None), create a query compiler from it with local index.
    # STEP 4: Otherwise, for lazy index, set the index through set_index or reindex.
    # STEP 5: If a dtype is given, and it is different from the current dtype of the query compiler so far,
    #         convert the query compiler to the given dtype if the data is lazy.
    # STEP 6: The resultant query_compiler is then set as the query_compiler for the DataFrame.

    # STEP 1: Setting the data
    # ------------------------
    if isinstance(data, Index):
        # If the data is an Index object, convert it to a DataFrame to make sure that the values are in the
        # correct format: the values should be a data column, not an index column.
        # Converting the Index object to its DataFrame version sets the resultant DataFrame's column name correctly -
        # it should be 0 if the name is None.
        query_compiler = data.to_frame(index=False)._query_compiler
    elif isinstance(data, Series):
        # Rename the Series object to 0 if its name is None and grab its query compiler.
        query_compiler = data.rename(
            0 if data.name is None else data.name, inplace=False
        )._query_compiler
    elif isinstance(data, DataFrame):
        query_compiler = data._query_compiler
        if (
            copy is False
            and index is None
            and columns is None
            and (dtype is None or dtype == getattr(data, "dtype", None))
        ):
            # When copy is False and no index, columns, and dtype are provided, the DataFrame is a shallow copy of the
            # original DataFrame.
            # If a dtype is provided, and the new dtype does not match the dtype of the original query compiler,
            # self is no longer a sibling of the original DataFrame.
            self._query_compiler = query_compiler
            data._add_sibling(self)
            return

    # STEP 2: Setting the columns if data is lazy
    # -------------------------------------------
    # When data is lazy, the query compiler is not None.
    if query_compiler is not None:
        if columns is not None:
            if (
                isinstance(data, (Index, Series))
                and query_compiler.get_columns()[0] not in columns
            ):
                # If the name of the Series/Index is not in the columns, clear the DataFrame and set the columns.
                query_compiler = from_pandas(
                    native_pd.DataFrame(columns=columns)
                )._query_compiler
            else:
                # Treat any columns not in data.columns (or data.name if data is a Series/Index) as extra columns.
                # They will be appended as NaN columns. Then, select the required columns in the order provided by `columns`.
                query_compiler = add_extra_columns_and_select_required_columns(
                    query_compiler, columns
                )

    # STEP 3: Creating a query compiler from pandas
    # ---------------------------------------------
    else:  # When the data is local, the query compiler is None.
        # If the data, columns, and index are local objects, the query compiler representation is created from pandas.
        # However, when the data is a dict but the index is lazy, the index is converted to pandas and the query
        # compiler is created from pandas.
        if not isinstance(
            data, (native_pd.Series, native_pd.DataFrame, native_pd.Index)
        ) and is_list_like(data):
            # If data is a pandas object, directly handle it with the pandas constructor.
            if is_dict_like(data):
                if columns is not None:
                    # Reduce the dictionary to only the relevant columns as the keys.
                    data = {key: value for key, value in data.items() if key in columns}

                if len(data) and all(
                    isinstance(v, (Index, BasePandasDataset)) for v in data.values()
                ):
                    # Special case: data is a dict where all the values are Snowpark pandas objects.
                    self._query_compiler = (
                        _df_init_dict_data_with_snowpark_pandas_values(
                            data, index, columns, dtype
                        )
                    )
                    return

                # If only some data is a Snowpark pandas object, convert the lazy data to pandas objects.
                res = {}
                for k, v in data.items():
                    if isinstance(v, Index):
                        res[k] = v.to_pandas()
                    elif isinstance(v, BasePandasDataset):
                        # Need to perform reindex on the Series or DataFrame objects since only the data
                        # whose index matches the given index is kept.
                        res[k] = v.reindex(index=index).to_pandas()
                    else:
                        res[k] = v
                # If the index is lazy, convert it to a pandas object so that the pandas constructor can handle it.
                index = try_convert_index_to_native(index)
                data = res

            else:  # list-like but not dict-like data.
                if len(data) and all(
                    isinstance(v, (Index, BasePandasDataset)) for v in data
                ):
                    # Special case: data is a list/dict where all the values are Snowpark pandas objects.
                    self._query_compiler = (
                        _df_init_list_data_with_snowpark_pandas_values(
                            data, index, columns, dtype
                        )
                    )
                    return

                # Sometimes the ndarray representation of a list is different from a regular list.
                # For instance, [(1, 2, 3), (4, 5, 6), (7, 8, 9)], dtype=[("a", "i4"), ("b", "i4"), ("c", "i4")]
                # is different from np.array([(1, 2, 3), (4, 5, 6), (7, 8, 9)], dtype=[("a", "i4"), ("b", "i4"), ("c", "i4")]).
                # The list has the shape (3, 3) while the ndarray has the shape (3,).
                # Therefore, do not modify the ndarray data.
                if not isinstance(data, np.ndarray):
                    # If only some data is a Snowpark pandas object, convert it to pandas objects.
                    res = [
                        v.to_pandas()
                        if isinstance(v, (Index, BasePandasDataset))
                        else v
                        for v in data
                    ]
                    data = res

        query_compiler = from_pandas(
            native_pd.DataFrame(
                data=data,
                # Handle setting the index, if it is a lazy index, outside this block in STEP 4.
                index=None if isinstance(index, (Index, Series)) else index,
                columns=columns,
                dtype=dtype,
                copy=copy,
            )
        )._query_compiler

    # STEP 4: Setting the index
    # -------------------------
    # The index is already set if the data and index are non-Snowpark pandas objects.
    # If either the data or the index is a Snowpark pandas object, set the index here.
    if index is not None and (
        isinstance(index, (Index, Series))
        or isinstance(data, (Index, BasePandasDataset))
    ):
        if isinstance(data, (type(self), Series, type(None))):
            # The `index` parameter is used to select the rows from `data` that will be in the resultant DataFrame.
            # If a value in `index` is not present in `data`'s index, it will be filled with a NaN value.
            # If data is None and an index is provided, set the index.
            query_compiler = query_compiler.reindex(
                axis=0, labels=convert_index_to_qc(index)
            )
        else:
            # Performing set index to directly set the index column (joining on row-position instead of index).
            query_compiler = query_compiler.set_index(
                convert_index_to_list_of_qcs(index)
            )

    # STEP 5: Setting the dtype if data is lazy
    # -----------------------------------------
    # If data is a Snowpark pandas object and a dtype is provided, and it does not match the current dtype of the
    # query compiler, convert the query compiler's dtype to the new dtype.
    # Local data should have the dtype parameter taken care of by the pandas constructor at the end.
    if (
        dtype is not None
        and isinstance(data, (Index, BasePandasDataset))
        and dtype != getattr(data, "dtype", None)
    ):
        query_compiler = query_compiler.astype(
            {col: dtype for col in query_compiler.columns}
        )

    # STEP 6: Setting the query compiler
    # ----------------------------------
    self._query_compiler = query_compiler


def _df_init_dict_data_with_snowpark_pandas_values(
    data: AnyArrayLike | list,
    index: list | AnyArrayLike | Series | Index,
    columns: list | AnyArrayLike | Series | Index,
    dtype: str | np.dtype | native_pd.ExtensionDtype | None,
) -> SnowflakeQueryCompiler:
    """
    Helper function for initializing a DataFrame with a dictionary where all the values
    are Snowpark pandas objects.
    """
    # Special case: data is a dict where all the values are Snowpark pandas objects.
    # Concat can only be performed with BasePandasDataset objects.
    # If a value is an Index, convert it to a Series where the index is the index to be set since these values
    # are always present in the final DataFrame.
    from snowflake.snowpark.modin.plugin.extensions.general_overrides import concat

    values = [
        Series(v, index=index) if isinstance(v, Index) else v for v in data.values()
    ]
    new_qc = concat(values, axis=1, keys=data.keys())._query_compiler
    if dtype is not None:
        new_qc = new_qc.astype({col: dtype for col in new_qc.columns})
    if index is not None:
        new_qc = new_qc.reindex(axis=0, labels=convert_index_to_qc(index))
    if columns is not None:
        new_qc = new_qc.reindex(axis=1, labels=columns)
    return new_qc


def _df_init_list_data_with_snowpark_pandas_values(
    data: AnyArrayLike | list,
    index: list | AnyArrayLike | Series | Index,
    columns: list | AnyArrayLike | Series | Index,
    dtype: str | np.dtype | native_pd.ExtensionDtype | None,
):
    """
    Helper function for initializing a DataFrame with a list where all the values
    are Snowpark pandas objects.
    """
    # Special case: data is a list/dict where all the values are Snowpark pandas objects.
    # Concat can only be performed with BasePandasDataset objects.
    # If a value is an Index, convert it to a Series.
    from snowflake.snowpark.modin.plugin.extensions.general_overrides import concat

    values = [Series(v) if isinstance(v, Index) else v for v in data]
    new_qc = concat(values, axis=1).T._query_compiler
    if dtype is not None:
        new_qc = new_qc.astype({col: dtype for col in new_qc.columns})
    if index is not None:
        new_qc = new_qc.set_index([convert_index_to_qc(index)])
    if columns is not None:
        if all(isinstance(v, Index) for v in data):
            # Special case: if all the values are Index objects, they are always present in the
            # final result with the provided column names. Therefore, rename the columns.
            new_qc = new_qc.set_columns(columns)
        else:
            new_qc = new_qc.reindex(axis=1, labels=columns)
    return new_qc


@register_dataframe_accessor("__dataframe__")
def __dataframe__(
    self, nan_as_null: bool = False, allow_copy: bool = True
) -> InterchangeDataframe:
    return self._query_compiler.to_interchange_dataframe(
        nan_as_null=nan_as_null, allow_copy=allow_copy
    )


# Snowpark pandas defaults to axis=1 instead of axis=0 for these; we need to investigate if the same should
# apply to upstream Modin.
@register_dataframe_accessor("__and__")
def __and__(self, other):
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    return self._binary_op("__and__", other, axis=1)


@register_dataframe_accessor("__rand__")
def __rand__(self, other):
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    return self._binary_op("__rand__", other, axis=1)


@register_dataframe_accessor("__or__")
def __or__(self, other):
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    return self._binary_op("__or__", other, axis=1)


@register_dataframe_accessor("__ror__")
def __ror__(self, other):
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    return self._binary_op("__ror__", other, axis=1)


# Upstream Modin defaults to pandas in some cases.
@register_dataframe_accessor("apply")
def apply(
    self,
    func: AggFuncType | UserDefinedFunction,
    axis: Axis = 0,
    raw: bool = False,
    result_type: Literal["expand", "reduce", "broadcast"] | None = None,
    args=(),
    **kwargs,
):
    """
    Apply a function along an axis of the ``DataFrame``.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    axis = self._get_axis_number(axis)
    query_compiler = self._query_compiler.apply(
        func,
        axis,
        raw=raw,
        result_type=result_type,
        args=args,
        **kwargs,
    )
    if not isinstance(query_compiler, type(self._query_compiler)):
        # A scalar was returned
        return query_compiler

    # If True, it is an unamed series.
    # Theoretically, if df.apply returns a Series, it will only be an unnamed series
    # because the function is supposed to be series -> scalar.
    if query_compiler._modin_frame.is_unnamed_series():
        return Series(query_compiler=query_compiler)
    else:
        return self.__constructor__(query_compiler=query_compiler)


# Snowpark pandas uses a separate QC method, while modin directly calls map.
@register_dataframe_accessor("applymap")
def applymap(self, func: PythonFuncType, na_action: str | None = None, **kwargs):
    warnings.warn(
        "DataFrame.applymap has been deprecated. Use DataFrame.map instead.",
        FutureWarning,
        stacklevel=2,
    )
    return self.map(func, na_action=na_action, **kwargs)


# In older versions of Snowpark pandas, overrides to base methods would automatically override
# corresponding DataFrame/Series API definitions as well. For consistency between methods, this
# is no longer the case, and DataFrame/Series must separately apply this override.
def _set_attrs(self, value: dict) -> None:  # noqa: RT01, D200
    # Use a field on the query compiler instead of self to avoid any possible ambiguity with
    # a column named "_attrs"
    self._query_compiler._attrs = copy.deepcopy(value)


def _get_attrs(self) -> dict:  # noqa: RT01, D200
    return self._query_compiler._attrs


register_dataframe_accessor("attrs")(property(_get_attrs, _set_attrs))


# We need to override _get_columns to satisfy
# tests/unit/modin/test_type_annotations.py::test_properties_snow_1374293[_get_columns-type_hints1]
# since Modin doesn't provide this type hint.
def _get_columns(self) -> native_pd.Index:
    """
    Get the columns for this Snowpark pandas ``DataFrame``.

    Returns
    -------
    Index
        The all columns.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    return self._query_compiler.columns


# Snowpark pandas wraps this in an update_in_place
def _set_columns(self, new_columns: Axes) -> None:
    """
    Set the columns for this Snowpark pandas  ``DataFrame``.

    Parameters
    ----------
    new_columns :
        The new columns to set.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    self._update_inplace(
        new_query_compiler=self._query_compiler.set_columns(new_columns)
    )


register_dataframe_accessor("columns")(property(_get_columns, _set_columns))


# Snowpark pandas does preprocessing for numeric_only (should be pushed to QC).
@register_dataframe_accessor("corr")
def corr(
    self,
    method: str | Callable = "pearson",
    min_periods: int | None = None,
    numeric_only: bool = False,
):  # noqa: PR01, RT01, D200
    """
    Compute pairwise correlation of columns, excluding NA/null values.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    corr_df = self
    if numeric_only:
        corr_df = self.drop(
            columns=[
                i for i in self.dtypes.index if not is_numeric_dtype(self.dtypes[i])
            ]
        )
    return self.__constructor__(
        query_compiler=corr_df._query_compiler.corr(
            method=method,
            min_periods=min_periods,
        )
    )


# Snowpark pandas does not respect `ignore_index`, and upstream Modin does not respect `how`.
@register_dataframe_accessor("dropna")
def dropna(
    self,
    *,
    axis: Axis = 0,
    how: str | NoDefault = no_default,
    thresh: int | NoDefault = no_default,
    subset: IndexLabel = None,
    inplace: bool = False,
):  # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    return super(DataFrame, self)._dropna(
        axis=axis, how=how, thresh=thresh, subset=subset, inplace=inplace
    )


# Snowpark pandas uses `self_is_series`, while upstream Modin uses `squeeze_self` and `squeeze_value`.
@register_dataframe_accessor("fillna")
def fillna(
    self,
    value: Hashable | Mapping | Series | DataFrame = None,
    *,
    method: FillnaOptions | None = None,
    axis: Axis | None = None,
    inplace: bool = False,
    limit: int | None = None,
    downcast: dict | None = None,
) -> DataFrame | None:
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    return super(DataFrame, self).fillna(
        self_is_series=False,
        value=value,
        method=method,
        axis=axis,
        inplace=inplace,
        limit=limit,
        downcast=downcast,
    )


# Snowpark pandas does different validation and returns a custom GroupBy object.
@register_dataframe_accessor("groupby")
def groupby(
    self,
    by=None,
    axis: Axis | NoDefault = no_default,
    level: IndexLabel | None = None,
    as_index: bool = True,
    sort: bool = True,
    group_keys: bool = True,
    observed: bool | NoDefault = no_default,
    dropna: bool = True,
):
    """
    Group ``DataFrame`` using a mapper or by a ``Series`` of columns.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    if axis is not no_default:
        axis = self._get_axis_number(axis)
        if axis == 1:
            warnings.warn(
                "DataFrame.groupby with axis=1 is deprecated. Do "
                + "`frame.T.groupby(...)` without axis instead.",
                FutureWarning,
                stacklevel=1,
            )
        else:
            warnings.warn(
                "The 'axis' keyword in DataFrame.groupby is deprecated and "
                + "will be removed in a future version.",
                FutureWarning,
                stacklevel=1,
            )
    else:
        axis = 0

    validate_groupby_args(by, level, observed)

    axis = self._get_axis_number(axis)

    if axis != 0 and as_index is False:
        raise ValueError("as_index=False only valid for axis=0")

    idx_name = None

    return_tuple_when_iterating = False
    if (
        not isinstance(by, Series)
        and is_list_like(by)
        and len(by) == 1
        # if by is a list-like of (None,), we have to keep it as a list because
        # None may be referencing a column or index level whose label is
        # `None`, and by=None wold mean that there is no `by` param.
        and by[0] is not None
    ):
        return_tuple_when_iterating = True
        by = by[0]

    if hashable(by) and (
        not callable(by) and not isinstance(by, (native_pd.Grouper, FrozenList))
    ):
        idx_name = by
    elif isinstance(by, Series):
        idx_name = by.name
        if by._parent is self:
            # if the SnowSeries comes from the current dataframe,
            # convert it to labels directly for easy processing
            by = by.name
    elif is_list_like(by):
        if axis == 0 and all(
            (
                (hashable(o) and (o in self))
                or isinstance(o, Series)
                or (isinstance(o, native_pd.Grouper) and o.key in self)
                or (is_list_like(o) and len(o) == len(self.shape[axis]))
            )
            for o in by
        ):
            # OSS modin needs to determine which `by` keys come from self and which do not,
            # but we defer this decision to a lower layer to preserve lazy evaluation semantics.
            by = [
                current_by.name
                if isinstance(current_by, Series) and current_by._parent is self
                else current_by
                for current_by in by
            ]

    return DataFrameGroupBy(
        self,
        by,
        axis,
        level,
        as_index,
        sort,
        group_keys,
        idx_name,
        observed=observed,
        dropna=dropna,
        drop=False,  # TODO reconcile with OSS modin's drop flag
        return_tuple_when_iterating=return_tuple_when_iterating,
        backend_pinned=self.is_backend_pinned(),
    )


# Upstream Modin uses a proxy DataFrameInfo object
@register_dataframe_accessor("info")
def info(
    self,
    verbose: bool | None = None,
    buf: IO[str] | None = None,
    max_cols: int | None = None,
    memory_usage: bool | str | None = None,
    show_counts: bool | None = None,
    null_counts: bool | None = None,
):  # noqa: PR01, D200
    """
    Print a concise summary of the ``DataFrame``.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    def put_str(src, output_len=None, spaces=2):
        src = str(src)
        return src.ljust(output_len if output_len else len(src)) + " " * spaces

    def format_size(num):
        for x in ["bytes", "KB", "MB", "GB", "TB"]:
            if num < 1024.0:
                return f"{num:3.1f} {x}"
            num /= 1024.0
        return f"{num:3.1f} PB"

    output = []

    type_line = str(type(self))
    index_line = "SnowflakeIndex"
    columns = self.columns
    columns_len = len(columns)
    dtypes = self.dtypes
    dtypes_line = f"dtypes: {', '.join(['{}({})'.format(dtype, count) for dtype, count in dtypes.value_counts().items()])}"

    if max_cols is None:
        max_cols = 100

    exceeds_info_cols = columns_len > max_cols

    if buf is None:
        buf = sys.stdout

    if null_counts is None:
        null_counts = not exceeds_info_cols

    if verbose is None:
        verbose = not exceeds_info_cols

    if null_counts and verbose:
        # We're gonna take items from `non_null_count` in a loop, which
        # works kinda slow with `Modin.Series`, that's why we call `_to_pandas()` here
        # that will be faster.
        non_null_count = self.count()._to_pandas()

    if memory_usage is None:
        memory_usage = True

    def get_header(spaces=2):
        output = []
        head_label = " # "
        column_label = "Column"
        null_label = "Non-Null Count"
        dtype_label = "Dtype"
        non_null_label = " non-null"
        delimiter = "-"

        lengths = {}
        lengths["head"] = max(len(head_label), len(pprint_thing(len(columns))))
        lengths["column"] = max(
            len(column_label), max(len(pprint_thing(col)) for col in columns)
        )
        lengths["dtype"] = len(dtype_label)
        dtype_spaces = (
            max(lengths["dtype"], max(len(pprint_thing(dtype)) for dtype in dtypes))
            - lengths["dtype"]
        )

        header = put_str(head_label, lengths["head"]) + put_str(
            column_label, lengths["column"]
        )
        if null_counts:
            lengths["null"] = max(
                len(null_label),
                max(len(pprint_thing(x)) for x in non_null_count) + len(non_null_label),
            )
            header += put_str(null_label, lengths["null"])
        header += put_str(dtype_label, lengths["dtype"], spaces=dtype_spaces)

        output.append(header)

        delimiters = put_str(delimiter * lengths["head"]) + put_str(
            delimiter * lengths["column"]
        )
        if null_counts:
            delimiters += put_str(delimiter * lengths["null"])
        delimiters += put_str(delimiter * lengths["dtype"], spaces=dtype_spaces)
        output.append(delimiters)

        return output, lengths

    output.extend([type_line, index_line])

    def verbose_repr(output):
        columns_line = f"Data columns (total {len(columns)} columns):"
        header, lengths = get_header()
        output.extend([columns_line, *header])
        for i, col in enumerate(columns):
            i, col_s, dtype = map(pprint_thing, [i, col, dtypes[col]])

            to_append = put_str(f" {i}", lengths["head"]) + put_str(
                col_s, lengths["column"]
            )
            if null_counts:
                non_null = pprint_thing(non_null_count[col])
                to_append += put_str(f"{non_null} non-null", lengths["null"])
            to_append += put_str(dtype, lengths["dtype"], spaces=0)
            output.append(to_append)

    def non_verbose_repr(output):
        output.append(columns._summary(name="Columns"))

    if verbose:
        verbose_repr(output)
    else:
        non_verbose_repr(output)

    output.append(dtypes_line)

    if memory_usage:
        deep = memory_usage == "deep"
        mem_usage_bytes = self.memory_usage(index=True, deep=deep).sum()
        mem_line = f"memory usage: {format_size(mem_usage_bytes)}"

        output.append(mem_line)

    output.append("")
    buf.write("\n".join(output))


# Snowpark pandas does different validation.
@register_dataframe_accessor("insert")
def insert(
    self,
    loc: int,
    column: Hashable,
    value: Scalar | AnyArrayLike,
    allow_duplicates: bool | NoDefault = no_default,
) -> None:
    """
    Insert column into ``DataFrame`` at specified location.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    raise_if_native_pandas_objects(value)
    if allow_duplicates is no_default:
        allow_duplicates = False
    if not allow_duplicates and column in self.columns:
        raise ValueError(f"cannot insert {column}, already exists")

    if not isinstance(loc, int):
        raise TypeError("loc must be int")

    # If columns labels are multilevel, we implement following behavior (this is
    # name native pandas):
    # Case 1: if 'column' is tuple it's length must be same as number of levels
    #    otherwise raise error.
    # Case 2: if 'column' is not a tuple, create a tuple out of it by filling in
    #     empty strings to match the length of column levels in self frame.
    if self.columns.nlevels > 1:
        if isinstance(column, tuple) and len(column) != self.columns.nlevels:
            # same error as native pandas.
            raise ValueError("Item must have length equal to number of levels.")
        if not isinstance(column, tuple):
            # Fill empty strings to match length of levels
            suffix = [""] * (self.columns.nlevels - 1)
            column = tuple([column] + suffix)

    # Dictionary keys are treated as index column and this should be joined with
    # index of target dataframe. This behavior is similar to 'value' being DataFrame
    # or Series, so we simply create Series from dict data here.
    if isinstance(value, set):
        raise TypeError(f"'{type(value).__name__}' type is unordered")

    if isinstance(value, dict):
        value = Series(value, name=column)

    if isinstance(value, DataFrame) or (
        isinstance(value, np.ndarray) and len(value.shape) > 1
    ):
        # Supported numpy array shapes are
        # 1. (N, )  -> Ex. [1, 2, 3]
        # 2. (N, 1) -> Ex> [[1], [2], [3]]
        if value.shape[1] != 1:
            if isinstance(value, DataFrame):
                # Error message updated in pandas 2.1, needs to be upstreamed to OSS modin
                raise ValueError(
                    f"Expected a one-dimensional object, got a {type(value).__name__} with {value.shape[1]} columns instead."
                )
            else:
                raise ValueError(
                    f"Expected a 1D array, got an array with shape {value.shape}"
                )
        # Change numpy array shape from (N, 1) to (N, )
        if isinstance(value, np.ndarray):
            value = value.squeeze(axis=1)

    if (
        is_list_like(value)
        and not isinstance(value, (Series, DataFrame))
        and len(value) != self.shape[0]
        and not 0 == self.shape[0]  # dataframe holds no rows
    ):
        raise ValueError(
            "Length of values ({}) does not match length of index ({})".format(
                len(value), len(self)
            )
        )
    if not -len(self.columns) <= loc <= len(self.columns):
        raise IndexError(
            f"index {loc} is out of bounds for axis 0 with size {len(self.columns)}"
        )
    elif loc < 0:
        raise ValueError("unbounded slice")

    join_on_index = False
    if isinstance(value, (Series, DataFrame)):
        value = value._query_compiler
        join_on_index = True
    elif is_list_like(value):
        value = Series(value, name=column)._query_compiler

    new_query_compiler = self._query_compiler.insert(loc, column, value, join_on_index)
    # In pandas, 'insert' operation is always inplace.
    self._update_inplace(new_query_compiler=new_query_compiler)


# Snowpark pandas does more specialization based on the type of `values`
@register_dataframe_accessor("isin")
def isin(
    self, values: ListLike | Series | DataFrame | dict[Hashable, ListLike]
) -> DataFrame:
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    if isinstance(values, dict):
        return super(DataFrame, self).isin(values)
    elif isinstance(values, Series):
        # Note: pandas performs explicit is_unique check here, deactivated for performance reasons.
        # if not values.index.is_unique:
        #   raise ValueError("cannot compute isin with a duplicate axis.")
        return self.__constructor__(
            query_compiler=self._query_compiler.isin(values._query_compiler)
        )
    elif isinstance(values, DataFrame):
        # Note: pandas performs explicit is_unique check here, deactivated for performance reasons.
        # if not (values.columns.is_unique and values.index.is_unique):
        #    raise ValueError("cannot compute isin with a duplicate axis.")
        return self.__constructor__(
            query_compiler=self._query_compiler.isin(values._query_compiler)
        )
    else:
        if not is_list_like(values):
            # throw pandas compatible error
            raise TypeError(
                "only list-like or dict-like objects are allowed "
                f"to be passed to {self.__class__.__name__}.isin(), "
                f"you passed a '{type(values).__name__}'"
            )
        return super(DataFrame, self).isin(values)


# Upstream Modin defaults to pandas for some arguments.
@register_dataframe_accessor("join")
def join(
    self,
    other: DataFrame | Series | Iterable[DataFrame | Series],
    on: IndexLabel | None = None,
    how: str = "left",
    lsuffix: str = "",
    rsuffix: str = "",
    sort: bool = False,
    validate: str | None = None,
) -> DataFrame:
    """
    Join columns of another ``DataFrame``.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    for o in other if isinstance(other, list) else [other]:
        raise_if_native_pandas_objects(o)

    # Similar to native pandas we implement 'join' using 'pd.merge' method.
    # Following code is copied from native pandas (with few changes explained below)
    # https://github.com/pandas-dev/pandas/blob/v1.5.3/pandas/core/frame.py#L10002
    if isinstance(other, Series):
        # Same error as native pandas.
        if other.name is None:
            raise ValueError("Other Series must have a name")
        other = DataFrame(other)
    elif is_list_like(other):
        if any([isinstance(o, Series) and o.name is None for o in other]):
            raise ValueError("Other Series must have a name")

    if isinstance(other, DataFrame):
        if how == "cross":
            return pd.merge(
                self,
                other,
                how=how,
                on=on,
                suffixes=(lsuffix, rsuffix),
                sort=sort,
                validate=validate,
            )
        return pd.merge(
            self,
            other,
            left_on=on,
            how=how,
            left_index=on is None,
            right_index=True,
            suffixes=(lsuffix, rsuffix),
            sort=sort,
            validate=validate,
        )
    else:  # List of DataFrame/Series
        # Same error as native pandas.
        if on is not None:
            raise ValueError(
                "Joining multiple DataFrames only supported for joining on index"
            )

        # Same error as native pandas.
        if rsuffix or lsuffix:
            raise ValueError("Suffixes not supported when joining multiple DataFrames")

        # NOTE: These are not the differences between Snowpark pandas API and pandas behavior
        # these are differences between native pandas join behavior when join
        # frames have unique index or not.

        # In native pandas logic to join multiple DataFrames/Series is data
        # dependent. Under the hood it will either use 'concat' or 'merge' API
        # Case 1. If all objects being joined have unique index use 'concat' (axis=1)
        # Case 2. Otherwise use 'merge' API by looping through objects left to right.
        # https://github.com/pandas-dev/pandas/blob/v1.5.3/pandas/core/frame.py#L10046

        # Even though concat (axis=1) and merge are very similar APIs they have
        # some differences which leads to inconsistent behavior in native pandas.
        # 1. Treatment of un-named Series
        # Case #1: Un-named series is allowed in concat API. Objects are joined
        #   successfully by assigning a number as columns name (see 'concat' API
        #   documentation for details on treatment of un-named series).
        # Case #2: It raises 'ValueError: Other Series must have a name'

        # 2. how='right'
        # Case #1: 'concat' API doesn't support right join. It raises
        #   'ValueError: Only can inner (intersect) or outer (union) join the other axis'
        # Case #2: Merges successfully.

        # 3. Joining frames with duplicate labels but no conflict with other frames
        # Example:  self = DataFrame(... columns=["A", "B"])
        #           other = [DataFrame(... columns=["C", "C"])]
        # Case #1: 'ValueError: Indexes have overlapping values'
        # Case #2: Merged successfully.

        # In addition to this, native pandas implementation also leads to another
        # type of inconsistency where left.join(other, ...) and
        # left.join([other], ...) might behave differently for cases mentioned
        # above.
        # Example:
        # import pandas as pd
        # df = pd.DataFrame({"a": [4, 5]})
        # other = pd.Series([1, 2])
        # df.join([other])  # this is successful
        # df.join(other)  # this raises 'ValueError: Other Series must have a name'

        # In Snowpark pandas API, we provide consistent behavior by always using 'merge' API
        # to join multiple DataFrame/Series. So always follow the behavior
        # documented as Case #2 above.

        joined = self
        for frame in other:
            if isinstance(frame, DataFrame):
                overlapping_cols = set(joined.columns).intersection(set(frame.columns))
                if len(overlapping_cols) > 0:
                    # Native pandas raises: 'Indexes have overlapping values'
                    # We differ slightly from native pandas message to make it more
                    # useful to users.
                    raise ValueError(
                        f"Join dataframes have overlapping column labels: {overlapping_cols}"
                    )
            joined = pd.merge(
                joined,
                frame,
                how=how,
                left_index=True,
                right_index=True,
                validate=validate,
                sort=sort,
                suffixes=(None, None),
            )
        return joined


# Snowpark pandas does extra error checking.
@register_dataframe_accessor("mask")
def mask(
    self,
    cond: DataFrame | Series | Callable | AnyArrayLike,
    other: DataFrame | Series | Callable | Scalar | None = np.nan,
    *,
    inplace: bool = False,
    axis: Axis | None = None,
    level: Level | None = None,
):
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    if isinstance(other, Series) and axis is None:
        raise ValueError(
            "df.mask requires an axis parameter (0 or 1) when given a Series"
        )

    return super(DataFrame, self).mask(
        cond,
        other=other,
        inplace=inplace,
        axis=axis,
        level=level,
    )


# Snowpark pandas does more thorough error checking.
@register_dataframe_accessor("merge")
def merge(
    self,
    right: DataFrame | Series,
    how: str = "inner",
    on: IndexLabel | None = None,
    left_on: Hashable | AnyArrayLike | Sequence[Hashable | AnyArrayLike] | None = None,
    right_on: Hashable | AnyArrayLike | Sequence[Hashable | AnyArrayLike] | None = None,
    left_index: bool = False,
    right_index: bool = False,
    sort: bool = False,
    suffixes: Suffixes = ("_x", "_y"),
    copy: bool = True,
    indicator: bool = False,
    validate: str | None = None,
) -> DataFrame:
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    # Raise error if native pandas objects are passed.
    raise_if_native_pandas_objects(right)

    if isinstance(right, Series) and right.name is None:
        raise ValueError("Cannot merge a Series without a name")
    if not isinstance(right, (Series, DataFrame)):
        raise TypeError(
            f"Can only merge Series or DataFrame objects, a {type(right)} was passed"
        )

    if isinstance(right, Series):
        right_column_nlevels = len(right.name) if isinstance(right.name, tuple) else 1
    else:
        right_column_nlevels = right.columns.nlevels
    if self.columns.nlevels != right_column_nlevels:
        # This is deprecated in native pandas. We raise explicit error for this.
        raise ValueError(
            "Can not merge objects with different column levels."
            + f" ({self.columns.nlevels} levels on the left,"
            + f" {right_column_nlevels} on the right)"
        )

    # Merge empty native pandas dataframes for error checking. Otherwise, it will
    # require a lot of logic to be written. This takes care of raising errors for
    # following scenarios:
    # 1. Only 'left_index' is set to True.
    # 2. Only 'right_index is set to True.
    # 3. Only 'left_on' is provided.
    # 4. Only 'right_on' is provided.
    # 5. 'on' and 'left_on' both are provided
    # 6. 'on' and 'right_on' both are provided
    # 7. 'on' and 'left_index' both are provided
    # 8. 'on' and 'right_index' both are provided
    # 9. 'left_on' and 'left_index' both are provided
    # 10. 'right_on' and 'right_index' both are provided
    # 11. Length mismatch between 'left_on' and 'right_on'
    # 12. 'left_index' is not a bool
    # 13. 'right_index' is not a bool
    # 14. 'on' is not None and how='cross'
    # 15. 'left_on' is not None and how='cross'
    # 16. 'right_on' is not None and how='cross'
    # 17. 'left_index' is True and how='cross'
    # 18. 'right_index' is True and how='cross'
    # 19. Unknown label in 'on', 'left_on' or 'right_on'
    # 20. Provided 'suffixes' is not sufficient to resolve conflicts.
    # 21. Merging on column with duplicate labels.
    # 22. 'how' not in {'left', 'right', 'inner', 'outer', 'cross'}
    # 23. conflict with existing labels for array-like join key
    # 24. 'indicator' argument is not bool or str
    # 25. indicator column label conflicts with existing data labels
    create_empty_native_pandas_frame(self).merge(
        create_empty_native_pandas_frame(right),
        on=on,
        how=how,
        left_on=replace_external_data_keys_with_empty_pandas_series(left_on),
        right_on=replace_external_data_keys_with_empty_pandas_series(right_on),
        left_index=left_index,
        right_index=right_index,
        suffixes=suffixes,
        indicator=indicator,
    )

    return self.__constructor__(
        query_compiler=self._query_compiler.merge(
            right._query_compiler,
            how=how,
            on=on,
            left_on=replace_external_data_keys_with_query_compiler(self, left_on),
            right_on=replace_external_data_keys_with_query_compiler(right, right_on),
            left_index=left_index,
            right_index=right_index,
            sort=sort,
            suffixes=suffixes,
            copy=copy,
            indicator=indicator,
            validate=validate,
        )
    )


@_inherit_docstrings(native_pd.DataFrame.memory_usage, apilink="pandas.DataFrame")
@register_dataframe_accessor("memory_usage")
def memory_usage(self, index: bool = True, deep: bool = False) -> Any:
    """
    Memory Usage (Dummy Information)

    The memory usage of a snowflake dataframe is not fully implemented.
    This method returns a series like the pandas dataframe to maintain
    compatibility with code which calls this for logging purposes, but
    the values are 0.

    Args:
        index: return dummy index memory usage
        deep: ignored

    Returns:
        Series with zeros for each index and column in the dataframe

    Examples:

        >>> df = pd.DataFrame({'Animal': ['Falcon', 'Falcon',
        ...                               'Parrot', 'Parrot'],
        ...                    'Max Speed': [380., 370., 24., 26.]})
        >>> df.memory_usage()
        Index        0
        Animal       0
        Max Speed    0
        dtype: int64

        >>> df.memory_usage(index=False)
        Animal       0
        Max Speed    0
        dtype: int64
    """
    # TODO: SNOW-1264697: push implementation down to query compiler
    columns = (["Index"] if index else []) + self._get_columns().array.tolist()
    return native_pd.Series([0] * len(columns), index=columns)


# Snowpark pandas handles `inplace` differently.
@register_dataframe_accessor("replace")
def replace(
    self,
    to_replace=None,
    value=no_default,
    inplace: bool = False,
    limit=None,
    regex: bool = False,
    method: str | NoDefault = no_default,
):
    """
    Replace values given in `to_replace` with `value`.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    inplace = validate_bool_kwarg(inplace, "inplace")
    new_query_compiler = self._query_compiler.replace(
        to_replace=to_replace,
        value=value,
        limit=limit,
        regex=regex,
        method=method,
    )
    return self._create_or_update_from_compiler(new_query_compiler, inplace)


# Snowpark pandas interacts with the inplace flag differently.
@register_dataframe_accessor("rename")
def rename(
    self,
    mapper: Renamer | None = None,
    *,
    index: Renamer | None = None,
    columns: Renamer | None = None,
    axis: Axis | None = None,
    copy: bool | None = None,
    inplace: bool = False,
    level: Level | None = None,
    errors: IgnoreRaise = "ignore",
) -> DataFrame | None:
    """
    Alter axes labels.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    inplace = validate_bool_kwarg(inplace, "inplace")
    if mapper is None and index is None and columns is None:
        raise TypeError("must pass an index to rename")

    if index is not None or columns is not None:
        if axis is not None:
            raise TypeError(
                "Cannot specify both 'axis' and any of 'index' or 'columns'"
            )
        elif mapper is not None:
            raise TypeError(
                "Cannot specify both 'mapper' and any of 'index' or 'columns'"
            )
    else:
        # use the mapper argument
        if axis and self._get_axis_number(axis) == 1:
            columns = mapper
        else:
            index = mapper

    if copy is not None:
        WarningMessage.ignored_argument(
            operation="dataframe.rename",
            argument="copy",
            message="copy parameter has been ignored with Snowflake execution engine",
        )

    if isinstance(index, dict):
        index = new_snow_series(index)

    new_qc = self._query_compiler.rename(
        index_renamer=index, columns_renamer=columns, level=level, errors=errors
    )
    return self._create_or_update_from_compiler(
        new_query_compiler=new_qc, inplace=inplace
    )


# Upstream modin converts aggfunc to a cython function if it's a string.
@register_dataframe_accessor("pivot_table")
def pivot_table(
    self,
    values=None,
    index=None,
    columns=None,
    aggfunc="mean",
    fill_value=None,
    margins=False,
    dropna=True,
    margins_name="All",
    observed=False,
    sort=True,
):
    """
    Create a spreadsheet-style pivot table as a ``DataFrame``.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    result = self.__constructor__(
        query_compiler=self._query_compiler.pivot_table(
            index=index,
            values=values,
            columns=columns,
            aggfunc=aggfunc,
            fill_value=fill_value,
            margins=margins,
            dropna=dropna,
            margins_name=margins_name,
            observed=observed,
            sort=sort,
        )
    )
    return result


# Snowpark pandas produces a different warning for materialization.
@register_dataframe_accessor("plot")
@property
def plot(
    self,
    x=None,
    y=None,
    kind="line",
    ax=None,
    subplots=False,
    sharex=None,
    sharey=False,
    layout=None,
    figsize=None,
    use_index=True,
    title=None,
    grid=None,
    legend=True,
    style=None,
    logx=False,
    logy=False,
    loglog=False,
    xticks=None,
    yticks=None,
    xlim=None,
    ylim=None,
    rot=None,
    fontsize=None,
    colormap=None,
    table=False,
    yerr=None,
    xerr=None,
    secondary_y=False,
    sort_columns=False,
    **kwargs,
):  # noqa: PR01, RT01, D200
    """
    Make plots of ``DataFrame``. Materializes data into memory and uses the
    existing pandas PlotAccessor
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    WarningMessage.single_warning(
        "DataFrame.plot materializes data to the local machine for plotting."
    )
    return self._to_pandas().plot


# Upstream Modin defaults when other is a Series.
@register_dataframe_accessor("pow")
def pow(
    self, other, axis="columns", level=None, fill_value=None
):  # noqa: PR01, RT01, D200
    """
    Get exponential power of ``DataFrame`` and `other`, element-wise (binary operator `pow`).
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    return self._binary_op(
        "pow",
        other,
        axis=axis,
        level=level,
        fill_value=fill_value,
    )


@register_dataframe_accessor("rpow")
def rpow(
    self, other, axis="columns", level=None, fill_value=None
):  # noqa: PR01, RT01, D200
    """
    Get exponential power of ``DataFrame`` and `other`, element-wise (binary operator `rpow`).
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    return self._binary_op(
        "rpow",
        other,
        axis=axis,
        level=level,
        fill_value=fill_value,
    )


# Snowpark pandas does extra argument validation, and uses iloc instead of drop at the end.
@register_dataframe_accessor("select_dtypes")
def select_dtypes(
    self,
    include: ListLike | str | type | None = None,
    exclude: ListLike | str | type | None = None,
) -> DataFrame:
    """
    Return a subset of the ``DataFrame``'s columns based on the column dtypes.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    # This line defers argument validation to pandas, which will raise errors on our behalf in cases
    # like if `include` and `exclude` are None, the same type is specified in both lists, or a string
    # dtype (as opposed to object) is specified.
    native_pd.DataFrame().select_dtypes(include, exclude)

    if include and not is_list_like(include):
        include = [include]
    elif include is None:
        include = []
    if exclude and not is_list_like(exclude):
        exclude = [exclude]
    elif exclude is None:
        exclude = []

    sel = tuple(map(set, (include, exclude)))

    # The width of the np.int_/float_ alias differs between Windows and other platforms, so
    # we need to include a workaround.
    # https://github.com/numpy/numpy/issues/9464
    # https://github.com/pandas-dev/pandas/blob/f538741432edf55c6b9fb5d0d496d2dd1d7c2457/pandas/core/frame.py#L5036
    def check_sized_number_infer_dtypes(dtype):
        if (isinstance(dtype, str) and dtype == "int") or (dtype is int):
            return [np.int32, np.int64]
        elif dtype == "float" or dtype is float:
            return [np.float64, np.float32]
        else:
            return [infer_dtype_from_object(dtype)]

    include, exclude = map(
        lambda x: set(
            itertools.chain.from_iterable(map(check_sized_number_infer_dtypes, x))
        ),
        sel,
    )
    # We need to index on column position rather than label in case of duplicates
    include_these = native_pd.Series(not bool(include), index=range(len(self.columns)))
    exclude_these = native_pd.Series(not bool(exclude), index=range(len(self.columns)))

    def is_dtype_instance_mapper(dtype):
        return functools.partial(issubclass, dtype.type)

    for i, dtype in enumerate(self.dtypes):
        if include:
            include_these[i] = any(map(is_dtype_instance_mapper(dtype), include))
        if exclude:
            exclude_these[i] = not any(map(is_dtype_instance_mapper(dtype), exclude))

    dtype_indexer = include_these & exclude_these
    indicate = [i for i, should_keep in dtype_indexer.items() if should_keep]
    # We need to use iloc instead of drop in case of duplicate column names
    return self.iloc[:, indicate]


# Snowpark pandas does extra validation on the `axis` argument.
@register_dataframe_accessor("set_axis")
def set_axis(
    self,
    labels: IndexLabel,
    *,
    axis: Axis = 0,
    copy: bool | NoDefault = no_default,  # ignored
):
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    if not is_scalar(axis):
        raise TypeError(f"{type(axis).__name__} is not a valid type for axis.")
    return super(DataFrame, self).set_axis(
        labels=labels,
        # 'columns', 'rows, 'index, 0, and 1 are the only valid axis values for df.
        axis=native_pd.DataFrame._get_axis_name(axis),
        copy=copy,
    )


# Snowpark pandas needs extra logic for the lazy index class.
@register_dataframe_accessor("set_index")
def set_index(
    self,
    keys: IndexLabel
    | list[IndexLabel | pd.Index | pd.Series | list | np.ndarray | Iterable],
    drop: bool = True,
    append: bool = False,
    inplace: bool = False,
    verify_integrity: bool = False,
) -> None | DataFrame:
    """
    Set the ``DataFrame`` index using existing columns.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    inplace = validate_bool_kwarg(inplace, "inplace")
    if not isinstance(keys, list):
        keys = [keys]

    # make sure key is either hashable, index, or series
    label_or_series = []

    missing = []
    columns = self.columns.tolist()
    for key in keys:
        raise_if_native_pandas_objects(key)
        if isinstance(key, pd.Series):
            label_or_series.append(key._query_compiler)
        elif isinstance(key, (np.ndarray, list, Iterator)):
            label_or_series.append(pd.Series(key)._query_compiler)
        elif isinstance(key, (pd.Index, native_pd.MultiIndex)):
            label_or_series += [s._query_compiler for s in self._to_series_list(key)]
        else:
            if not is_hashable(key):
                raise TypeError(
                    f'The parameter "keys" may be a column key, one-dimensional array, or a list '
                    f"containing only valid column keys and one-dimensional arrays. Received column "
                    f"of type {type(key)}"
                )
            label_or_series.append(key)
            found = key in columns
            if columns.count(key) > 1:
                raise ValueError(f"The column label '{key}' is not unique")
            elif not found:
                missing.append(key)

    if missing:
        raise KeyError(f"None of {missing} are in the columns")

    new_query_compiler = self._query_compiler.set_index(
        label_or_series, drop=drop, append=append
    )

    # TODO: SNOW-782633 improve this code once duplicate is supported
    # this needs to pull all index which is inefficient
    if verify_integrity and not new_query_compiler.index.is_unique:
        duplicates = new_query_compiler.index[
            new_query_compiler.index.to_pandas().duplicated()
        ].unique()
        raise ValueError(f"Index has duplicate keys: {duplicates}")

    return self._create_or_update_from_compiler(new_query_compiler, inplace=inplace)


# Upstream Modin uses `len(self.index)` instead of `len(self)`, which gives an extra query.
@register_dataframe_accessor("shape")
@property
def shape(self) -> tuple[int, int]:
    """
    Return a tuple representing the dimensionality of the ``DataFrame``.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    return len(self), len(self.columns)


# Snowpark pands has rewrites to minimize queries from length checks.
@register_dataframe_accessor("squeeze")
def squeeze(self, axis: Axis | None = None):
    """
    Squeeze 1 dimensional axis objects into scalars.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    axis = self._get_axis_number(axis) if axis is not None else None
    len_columns = self._query_compiler.get_axis_len(1)
    if axis == 1 and len_columns == 1:
        return Series(query_compiler=self._query_compiler)
    if axis in [0, None]:
        # get_axis_len(0) results in a sql query to count number of rows in current
        # dataframe. We should only compute len_index if axis is 0 or None.
        len_index = len(self)
        if axis is None and (len_columns == 1 or len_index == 1):
            return Series(query_compiler=self._query_compiler).squeeze()
        if axis == 0 and len_index == 1:
            return Series(query_compiler=self.T._query_compiler)
    return self.copy()


# Upstream modin defines sum differently for series/DF, but we use the same implementation for both.
@register_dataframe_accessor("sum")
def sum(
    self,
    axis: Axis | None = None,
    skipna: bool = True,
    numeric_only: bool = False,
    min_count: int = 0,
    **kwargs: Any,
):
    # TODO: SNOW-1119855: Modin upgrade - modin.pandas.base.BasePandasDataset
    min_count = validate_int_kwarg(min_count, "min_count")
    kwargs.update({"min_count": min_count})
    return self._agg_helper(
        func="sum",
        axis=axis,
        skipna=skipna,
        numeric_only=numeric_only,
        **kwargs,
    )


# Snowpark pandas raises a warning where modin defaults to pandas.
@register_dataframe_accessor("stack")
def stack(
    self,
    level: int | str | list = -1,
    dropna: bool | NoDefault = no_default,
    sort: bool | NoDefault = no_default,
    future_stack: bool = False,  # ignored
):
    """
    Stack the prescribed level(s) from columns to index.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    if future_stack is not False:
        WarningMessage.ignored_argument(  # pragma: no cover
            operation="DataFrame.stack",
            argument="future_stack",
            message="future_stack parameter has been ignored with Snowflake execution engine",
        )
    if dropna is NoDefault:
        dropna = True  # pragma: no cover
    if sort is NoDefault:
        sort = True  # pragma: no cover

    # This ensures that non-pandas MultiIndex objects are caught.
    is_multiindex = len(self.columns.names) > 1
    if not is_multiindex or (
        is_multiindex and is_list_like(level) and len(level) == self.columns.nlevels
    ):
        return self._reduce_dimension(
            query_compiler=self._query_compiler.stack(level, dropna, sort)
        )
    else:
        return self.__constructor__(
            query_compiler=self._query_compiler.stack(level, dropna, sort)
        )


# Upstream modin doesn't pass `copy`, so we can't raise a warning for it.
# No need to override the `T` property since that can't take any extra arguments.
@register_dataframe_accessor("transpose")
def transpose(self, copy=False, *args):  # noqa: PR01, RT01, D200
    """
    Transpose index and columns.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    if copy:
        WarningMessage.ignored_argument(
            operation="transpose",
            argument="copy",
            message="Transpose ignore copy argument in Snowpark pandas API",
        )

    if args:
        WarningMessage.ignored_argument(
            operation="transpose",
            argument="args",
            message="Transpose ignores args in Snowpark pandas API",
        )

    return self.__constructor__(query_compiler=self._query_compiler.transpose())


# Upstream modin implements transform in base.py, but we don't yet support Series.transform.
@register_dataframe_accessor("transform")
def transform(
    self, func: PythonFuncType, axis: Axis = 0, *args: Any, **kwargs: Any
) -> DataFrame:  # noqa: PR01, RT01, D200
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    if is_list_like(func) or is_dict_like(func):
        ErrorMessage.not_implemented(
            "dict and list parameters are not supported for transform"
        )
    # throw the same error as pandas for cases where the function type is
    # invalid.
    if not isinstance(func, str) and not callable(func):
        raise TypeError(f"{type(func)} object is not callable")

    # if the function is an aggregation function, we'll produce
    # some bogus results while pandas will throw the error the
    # code below is throwing. So we do the same.
    if is_snowflake_agg_func(func):
        raise ValueError("Function did not transform")

    return self.apply(func, axis, False, args=args, **kwargs)


# Upstream modin defaults to pandas for some arguments.
@register_dataframe_accessor("unstack")
def unstack(
    self,
    level: int | str | list = -1,
    fill_value: int | str | dict = None,
    sort: bool = True,
):
    """
    Pivot a level of the (necessarily hierarchical) index labels.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    # This ensures that non-pandas MultiIndex objects are caught.
    nlevels = self._query_compiler.nlevels()
    is_multiindex = nlevels > 1

    if not is_multiindex or (
        is_multiindex and is_list_like(level) and len(level) == nlevels
    ):
        return self._reduce_dimension(
            query_compiler=self._query_compiler.unstack(
                level, fill_value, sort, is_series_input=False
            )
        )
    else:
        return self.__constructor__(
            query_compiler=self._query_compiler.unstack(
                level, fill_value, sort, is_series_input=False
            )
        )


# Upstream modin does different validation and sorting.
@register_dataframe_accessor("value_counts")
def value_counts(
    self,
    subset: Sequence[Hashable] | None = None,
    normalize: bool = False,
    sort: bool = True,
    ascending: bool = False,
    dropna: bool = True,
):
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    return Series(
        query_compiler=self._query_compiler.value_counts(
            subset=subset,
            normalize=normalize,
            sort=sort,
            ascending=ascending,
            dropna=dropna,
        ),
        name="proportion" if normalize else "count",
    )


@register_dataframe_accessor("where")
def where(
    self,
    cond: DataFrame | Series | Callable | AnyArrayLike,
    other: DataFrame | Series | Callable | Scalar | None = np.nan,
    *,
    inplace: bool = False,
    axis: Axis | None = None,
    level: Level | None = None,
):
    """
    Replace values where the condition is False.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    if isinstance(other, Series) and axis is None:
        raise ValueError(
            "df.where requires an axis parameter (0 or 1) when given a Series"
        )

    return super(DataFrame, self).where(
        cond,
        other=other,
        inplace=inplace,
        axis=axis,
        level=level,
    )


# Snowpark pandas has a custom iterator.
@register_dataframe_accessor("iterrows")
def iterrows(self) -> Iterator[tuple[Hashable, Series]]:
    """
    Iterate over ``DataFrame`` rows as (index, ``Series``) pairs.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    def iterrow_builder(s):
        """Return tuple of the given `s` parameter name and the parameter themselves."""
        return s.name, s

    # Raise warning message since iterrows is very inefficient.
    WarningMessage.single_warning(
        DF_ITERROWS_ITERTUPLES_WARNING_MESSAGE.format("DataFrame.iterrows")
    )

    partition_iterator = SnowparkPandasRowPartitionIterator(self, iterrow_builder)
    yield from partition_iterator


# Snowpark pandas has a custom iterator.
@register_dataframe_accessor("itertuples")
def itertuples(
    self, index: bool = True, name: str | None = "Pandas"
) -> Iterable[tuple[Any, ...]]:
    """
    Iterate over ``DataFrame`` rows as ``namedtuple``-s.
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions

    def itertuples_builder(s):
        """Return the next namedtuple."""
        # s is the Series of values in the current row.
        fields = []  # column names
        data = []  # values under each column

        if index:
            data.append(s.name)
            fields.append("Index")

        # Fill column names and values.
        fields.extend(list(self.columns))
        data.extend(s)

        if name is not None:
            # Creating the namedtuple.
            itertuple = collections.namedtuple(name, fields, rename=True)
            return itertuple._make(data)

        # When the name is None, return a regular tuple.
        return tuple(data)

    # Raise warning message since itertuples is very inefficient.
    WarningMessage.single_warning(
        DF_ITERROWS_ITERTUPLES_WARNING_MESSAGE.format("DataFrame.itertuples")
    )
    return SnowparkPandasRowPartitionIterator(self, itertuples_builder, True)


# Snowpark pandas truncates the repr output.
@register_dataframe_accessor("__repr__")
def __repr__(self):
    """
    Return a string representation for a particular ``DataFrame``.

    Returns
    -------
    str
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    num_rows = native_pd.get_option("display.max_rows") or len(self)
    # see _repr_html_ for comment, allow here also all column behavior
    num_cols = native_pd.get_option("display.max_columns") or len(self.columns)

    (
        row_count,
        col_count,
        repr_df,
    ) = self._query_compiler.build_repr_df(num_rows, num_cols, "x")
    result = repr(repr_df)

    # if truncated, add shape information
    if is_repr_truncated(row_count, col_count, num_rows, num_cols):
        # The split here is so that we don't repr pandas row lengths.
        return result.rsplit("\n\n", 1)[0] + "\n\n[{} rows x {} columns]".format(
            row_count, col_count
        )
    else:
        return result


# Snowpark pandas uses a different default `num_rows` value.
@register_dataframe_accessor("_repr_html_")
def _repr_html_(self):  # pragma: no cover
    """
    Return a html representation for a particular ``DataFrame``.

    Returns
    -------
    str

    Notes
    -----
    Supports pandas `display.max_rows` and `display.max_columns` options.
    """
    num_rows = native_pd.get_option("display.max_rows") or 60
    # Modin uses here 20 as default, but this does not coincide well with pandas option. Therefore allow
    # here value=0 which means display all columns.
    num_cols = native_pd.get_option("display.max_columns")

    (
        row_count,
        col_count,
        repr_df,
    ) = self._query_compiler.build_repr_df(num_rows, num_cols)
    result = repr_df._repr_html_()

    if is_repr_truncated(row_count, col_count, num_rows, num_cols):
        # We split so that we insert our correct dataframe dimensions.
        return (
            result.split("<p>")[0]
            + f"<p>{row_count} rows × {col_count} columns</p>\n</div>"
        )
    else:
        return result


# Upstream modin just uses `to_datetime` rather than `dataframe_to_datetime` on the query compiler.
@register_dataframe_accessor("_to_datetime")
def _to_datetime(self, **kwargs):
    """
    Convert `self` to datetime.

    Parameters
    ----------
    **kwargs : dict
        Optional arguments to use during query compiler's
        `to_datetime` invocation.

    Returns
    -------
    Series of datetime64 dtype
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    return self._reduce_dimension(
        query_compiler=self._query_compiler.dataframe_to_datetime(**kwargs)
    )


# Snowpark pandas has the extra `statement_params` argument.
@register_dataframe_accessor("_to_pandas")
def _to_pandas(
    self,
    *,
    statement_params: dict[str, str] | None = None,
    **kwargs: Any,
) -> native_pd.DataFrame:
    """
    Convert Snowpark pandas DataFrame to pandas DataFrame

    Args:
        statement_params: Dictionary of statement level parameters to be set while executing this action.

    Returns:
        pandas DataFrame
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    return self._query_compiler.to_pandas(statement_params=statement_params, **kwargs)


# Snowpark pandas does more validation and error checking than upstream Modin, and uses different
# helper methods for dispatch.
@register_dataframe_accessor("__setitem__")
def __setitem__(self, key: Any, value: Any):
    """
    Set attribute `value` identified by `key`.

    Args:
        key: Key to set
        value:  Value to set

    Note:
        In the case where value is any list like or array, pandas checks the array length against the number of rows
        of the input dataframe. If there is a mismatch, a ValueError is raised. Snowpark pandas indexing won't throw
        a ValueError because knowing the length of the current dataframe can trigger eager evaluations; instead if
        the array is longer than the number of rows we ignore the additional values. If the array is shorter, we use
        enlargement filling with the last value in the array.

    Returns:
        None
    """
    # TODO: SNOW-1063346: Modin upgrade - modin.pandas.DataFrame functions
    key = apply_if_callable(key, self)
    if isinstance(key, DataFrame) or (
        isinstance(key, np.ndarray) and len(key.shape) == 2
    ):
        # This case uses mask's codepath to perform the set, but
        # we need to duplicate the code here since we are passing
        # an additional kwarg `cond_fillna_with_true` to the QC here.
        # We need this additional kwarg, since if df.shape
        # and key.shape do not align (i.e. df has more rows),
        # mask's codepath would mask the additional rows in df
        # while for setitem, we need to keep the original values.
        if not isinstance(key, DataFrame):
            if key.dtype != bool:
                raise TypeError(
                    "Must pass DataFrame or 2-d ndarray with boolean values only"
                )
            key = DataFrame(key)
            key._query_compiler._shape_hint = "array"

        if value is not None:
            value = apply_if_callable(value, self)

            if isinstance(value, np.ndarray):
                value = DataFrame(value)
                value._query_compiler._shape_hint = "array"
            elif isinstance(value, pd.Series):
                # pandas raises the `mask` ValueError here: Must specify axis = 0 or 1. We raise this
                # error instead, since it is more descriptive.
                raise ValueError(
                    "setitem with a 2D key does not support Series values."
                )

            if isinstance(value, BasePandasDataset):
                value = value._query_compiler

        query_compiler = self._query_compiler.mask(
            cond=key._query_compiler,
            other=value,
            axis=None,
            level=None,
            cond_fillna_with_true=True,
        )

        return self._create_or_update_from_compiler(query_compiler, inplace=True)

    # Error Checking:
    if (isinstance(key, pd.Series) or is_list_like(key)) and (isinstance(value, range)):
        raise NotImplementedError(DF_SETITEM_LIST_LIKE_KEY_AND_RANGE_LIKE_VALUE)
    elif isinstance(value, slice):
        # Here, the whole slice is assigned as a scalar variable, i.e., a spot at an index gets a slice value.
        raise NotImplementedError(DF_SETITEM_SLICE_AS_SCALAR_VALUE)

    # Note: when key is a boolean indexer or slice the key is a row key; otherwise, the key is always a column
    # key.
    index, columns = slice(None), key
    index_is_bool_indexer = False
    if isinstance(key, slice):
        if is_integer(key.start) and is_integer(key.stop):
            # when slice are integer slice, e.g., df[1:2] = val, the behavior is the same as
            # df.iloc[1:2, :] = val
            self.iloc[key] = value
            return
        index, columns = key, slice(None)
    elif isinstance(key, pd.Series):
        if is_bool_dtype(key.dtype):
            index, columns = key, slice(None)
            index_is_bool_indexer = True
    elif is_bool_indexer(key):
        index, columns = pd.Series(key), slice(None)
        index_is_bool_indexer = True

    # The reason we do not call loc directly is that setitem has different behavior compared to loc in this case
    # we have to explicitly set matching_item_columns_by_label to False for setitem.
    index = index._query_compiler if isinstance(index, BasePandasDataset) else index
    columns = (
        columns._query_compiler if isinstance(columns, BasePandasDataset) else columns
    )
    from snowflake.snowpark.modin.plugin.extensions.indexing_overrides import (
        is_2d_array,
    )

    matching_item_rows_by_label = not is_2d_array(value)
    if is_2d_array(value):
        value = DataFrame(value)
    item = value._query_compiler if isinstance(value, BasePandasDataset) else value
    new_qc = self._query_compiler.set_2d_labels(
        index,
        columns,
        item,
        # setitem always matches item by position
        matching_item_columns_by_label=False,
        matching_item_rows_by_label=matching_item_rows_by_label,
        index_is_bool_indexer=index_is_bool_indexer,
        # setitem always deduplicates columns. E.g., if df has two columns "A" and "B", after calling
        # df[["A","A"]] = item, df still only has two columns "A" and "B", and "A"'s values are set by the
        # second "A" column from value; instead, if we call df.loc[:, ["A", "A"]] = item, then df will have
        # three columns "A", "A", "B". Similarly, if we call df[["X","X"]] = item, df will have three columns
        # "A", "B", "X", while if we call df.loc[:, ["X", "X"]] = item, then df will have four columns "A", "B",
        # "X", "X".
        deduplicate_columns=True,
    )
    return self._update_inplace(new_query_compiler=new_qc)
