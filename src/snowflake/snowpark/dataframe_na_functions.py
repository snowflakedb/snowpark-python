#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import math
import sys
from logging import getLogger
from typing import Dict, Optional, Union

import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.expression import ColumnSum
from snowflake.snowpark._internal.ast.utils import (
    build_expr_from_python_val,
    with_src_position,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import add_api_call, adjust_api_subcalls
from snowflake.snowpark._internal.type_utils import (
    VALID_PYTHON_TYPES_FOR_LITERAL_VALUE,
    LiteralType,
    python_type_to_snow_type,
)
from snowflake.snowpark._internal.utils import publicapi, quote_name
from snowflake.snowpark.column import Column
from snowflake.snowpark.functions import iff, lit, when
from snowflake.snowpark.types import (
    DataType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
)

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

_logger = getLogger(__name__)


def _is_value_type_matching_for_na_function(
    value: LiteralType,
    datatype: DataType,
    include_decimal: bool = False,
) -> bool:
    # Python `int` can match into FloatType/DoubleType,
    # but Python `float` can't match IntegerType/LongType.
    # None should be compatible with any Snowpark type.
    int_types = (IntegerType, LongType, FloatType, DoubleType)
    float_types = (FloatType, DoubleType)
    # Python `int` and `float` can also match for DecimalType,
    # for now this is protected by this argument
    if include_decimal:
        int_types = (int_types, DecimalType)
        float_types = (float_types, DecimalType)
    return (
        value is None
        or (
            isinstance(value, int)
            # bool is a subclass of int, but we don't want to consider it numeric
            and not isinstance(value, bool)
            and isinstance(datatype, int_types)
        )
        or (isinstance(value, float) and isinstance(datatype, float_types))
        or isinstance(datatype, type(python_type_to_snow_type(type(value))[0]))
    )


_SUBSET_CHECK_ERROR_MESSAGE = (
    "subset should be a single column name, list or tuple of column names"
)


def _check_subset_parameter(subset: Optional[Union[str, Iterable[str]]]) -> None:
    """Produces exception when invalid subset parameter was passed."""
    if (
        subset is not None
        and not isinstance(subset, str)
        and not isinstance(subset, (list, tuple))
    ):
        raise TypeError(_SUBSET_CHECK_ERROR_MESSAGE)


class DataFrameNaFunctions:
    """Provides functions for handling missing values in a :class:`DataFrame`."""

    def __init__(self, dataframe: "snowflake.snowpark.DataFrame") -> None:
        self._dataframe = dataframe

    @publicapi
    def drop(
        self,
        how: str = "any",
        thresh: Optional[int] = None,
        subset: Optional[Union[str, Iterable[str]]] = None,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.DataFrame":
        """
        Returns a new DataFrame that excludes all rows containing fewer than
        a specified number of non-null and non-NaN values in the specified
        columns.

        Args:
            how: An ``str`` with value either 'any' or 'all'. If 'any', drop a row if
                it contains any nulls. If 'all', drop a row only if all its values are null.
                The default value is 'any'. If ``thresh`` is provided, ``how`` will be ignored.
            thresh: The minimum number of non-null and non-NaN
                values that should be in the specified columns in order for the
                row to be included. It overwrites ``how``. In each case:

                    * If ``thresh`` is not provided or ``None``, the length of ``subset``
                      will be used when ``how`` is 'any' and 1 will be used when ``how``
                      is 'all'.

                    * If ``thresh`` is greater than the number of the specified columns,
                      the method returns an empty DataFrame.

                    * If ``thresh`` is less than 1, the method returns the original DataFrame.

            subset: A list of the names of columns to check for null and NaN values.
                In each case:

                    * If ``subset`` is not provided or ``None``, all columns will be included.

                    * If ``subset`` is empty, the method returns the original DataFrame.

        Examples::

            >>> df = session.create_dataframe([[1.0, 1], [float('nan'), 2], [None, 3], [4.0, None], [float('nan'), None]]).to_df("a", "b")
            >>> # drop a row if it contains any nulls, with checking all columns
            >>> df.na.drop().show()
            -------------
            |"A"  |"B"  |
            -------------
            |1.0  |1    |
            -------------
            <BLANKLINE>
            >>> # drop a row only if all its values are null, with checking all columns
            >>> df.na.drop(how='all').show()
            ---------------
            |"A"   |"B"   |
            ---------------
            |1.0   |1     |
            |nan   |2     |
            |NULL  |3     |
            |4.0   |NULL  |
            ---------------
            <BLANKLINE>
            >>> # drop a row if it contains at least one non-null and non-NaN values, with checking all columns
            >>> df.na.drop(thresh=1).show()
            ---------------
            |"A"   |"B"   |
            ---------------
            |1.0   |1     |
            |nan   |2     |
            |NULL  |3     |
            |4.0   |NULL  |
            ---------------
            <BLANKLINE>
            >>> # drop a row if it contains any nulls, with checking column "a"
            >>> df.na.drop(subset=["a"]).show()
            --------------
            |"A"  |"B"   |
            --------------
            |1.0  |1     |
            |4.0  |NULL  |
            --------------
            <BLANKLINE>
            >>> df.na.drop(subset="a").show()
            --------------
            |"A"  |"B"   |
            --------------
            |1.0  |1     |
            |4.0  |NULL  |
            --------------
            <BLANKLINE>

        See Also:
            :func:`DataFrame.dropna`
        """
        # translate to
        # select * from table where
        # iff(float_col = 'NaN' or float_col is null, 0, 1)
        # iff(non_float_col is null, 0, 1) >= thresh

        if how is not None and how not in ["any", "all"]:
            raise ValueError(f"how ('{how}') should be 'any' or 'all'")

        _check_subset_parameter(subset)

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_na_drop__python, stmt)
            ast.how = how
            if thresh is not None:
                ast.thresh.value = thresh
            if isinstance(subset, str):
                ast.subset.variadic = True
                build_expr_from_python_val(ast.subset.args.add(), subset)
            elif isinstance(subset, Iterable):
                ast.subset.variadic = False
                for col in subset:
                    build_expr_from_python_val(ast.subset.args.add(), col)
            self._dataframe._set_ast_ref(ast.df)

        # if subset is not provided, drop will be applied to all columns
        if subset is None:
            subset = self._dataframe.columns
        elif isinstance(subset, str):
            subset = [subset]

        # if thresh is not provided,
        # drop a row if it contains any nulls when how == 'any',
        # otherwise drop a row only if all its values are null.
        if thresh is None:
            thresh = len(subset) if how == "any" else 1

        # if thresh is less than 1, or no column is specified
        # to be dropped, return the dataframe directly
        if thresh < 1 or len(subset) == 0:
            new_df = self._dataframe._copy_without_ast()
            add_api_call(new_df, "DataFrameNaFunctions.drop")
            if _emit_ast:
                new_df._ast_id = stmt.uid
            return self._dataframe
        # if thresh is greater than the number of columns,
        # drop a row only if all its values are null
        elif thresh > len(subset):
            new_df = self._dataframe.limit(0, _ast_stmt=stmt, _emit_ast=False)
            adjust_api_subcalls(new_df, "DataFrameNaFunctions.drop", len_subcalls=1)
            if _emit_ast:
                new_df._ast_id = stmt.uid
            return new_df
        else:
            df_col_type_dict = {
                quote_name(field.name): field.datatype
                for field in self._dataframe.schema.fields
            }
            normalized_col_name_set = {quote_name(col_name) for col_name in subset}
            is_na_columns = []
            for normalized_col_name in normalized_col_name_set:
                if normalized_col_name not in df_col_type_dict:
                    raise SnowparkClientExceptionMessages.DF_CANNOT_RESOLVE_COLUMN_NAME(
                        normalized_col_name
                    )
                col = self._dataframe.col(normalized_col_name, _emit_ast=False)
                if isinstance(
                    df_col_type_dict[normalized_col_name], (FloatType, DoubleType)
                ):
                    # iff(col = 'NaN' or col is null, 0, 1)
                    is_na = iff(
                        (col == math.nan) | col.is_null(_emit_ast=False),
                        0,
                        1,
                        _emit_ast=False,
                    )
                else:
                    # iff(col is null, 0, 1)
                    is_na = iff(col.is_null(_emit_ast=False), 0, 1, _emit_ast=False)
                is_na_columns.append(is_na)
            col_counter = Column(
                ColumnSum([c._expression for c in is_na_columns]), _emit_ast=False
            )
            new_df = self._dataframe.where(col_counter >= thresh, _emit_ast=False)
            adjust_api_subcalls(new_df, "DataFrameNaFunctions.drop", len_subcalls=1)

            if _emit_ast:
                new_df._ast_id = stmt.uid

            return new_df

    @publicapi
    def fill(
        self,
        value: Union[LiteralType, Dict[str, LiteralType]],
        subset: Optional[Union[str, Iterable[str]]] = None,
        _emit_ast: bool = True,
        *,
        # keyword only arguments
        include_decimal: bool = False,
    ) -> "snowflake.snowpark.DataFrame":
        """
        Returns a new DataFrame that replaces all null and NaN values in the specified
        columns with the values provided.

        Args:
            value: A scalar value or a ``dict`` that associates the names of columns with the
                values that should be used to replace null and NaN values in those
                columns. If ``value`` is a ``dict``, ``subset`` is ignored. If ``value``
                is an empty ``dict``, the method returns the original DataFrame.
            subset: A list of the names of columns to check for null and NaN values.
                In each case:

                    * If ``subset`` is not provided or ``None``, all columns will be included.

                    * If ``subset`` is empty, the method returns the original DataFrame.
            include_decimal: Whether to allow ``Decimal`` values to fill in ``IntegerType``
                and ``FloatType`` columns.

        Examples::

            >>> df = session.create_dataframe([[1.0, 1], [float('nan'), 2], [None, 3], [4.0, None], [float('nan'), None]]).to_df("a", "b")
            >>> # fill null and NaN values in all columns
            >>> df.na.fill(3.14).show()
            ---------------
            |"A"   |"B"   |
            ---------------
            |1.0   |1     |
            |3.14  |2     |
            |3.14  |3     |
            |4.0   |NULL  |
            |3.14  |NULL  |
            ---------------
            <BLANKLINE>
            >>> # fill null and NaN values in column "a"
            >>> df.na.fill(3.14, subset="a").show()
            ---------------
            |"A"   |"B"   |
            ---------------
            |1.0   |1     |
            |3.14  |2     |
            |3.14  |3     |
            |4.0   |NULL  |
            |3.14  |NULL  |
            ---------------
            <BLANKLINE>
            >>> # fill null and NaN values in column "a"
            >>> df.na.fill({"a": 3.14}).show()
            ---------------
            |"A"   |"B"   |
            ---------------
            |1.0   |1     |
            |3.14  |2     |
            |3.14  |3     |
            |4.0   |NULL  |
            |3.14  |NULL  |
            ---------------
            <BLANKLINE>
            >>> # fill null and NaN values in column "a" and "b"
            >>> df.na.fill({"a": 3.14, "b": 15}).show()
            --------------
            |"A"   |"B"  |
            --------------
            |1.0   |1    |
            |3.14  |2    |
            |3.14  |3    |
            |4.0   |15   |
            |3.14  |15   |
            --------------
            <BLANKLINE>
            >>> df2 = session.create_dataframe([[1.0, True], [2.0, False], [3.0, False], [None, None]]).to_df("a", "b")
            >>> df2.na.fill(True).show()
            ----------------
            |"A"   |"B"    |
            ----------------
            |1.0   |True   |
            |2.0   |False  |
            |3.0   |False  |
            |NULL  |True   |
            ----------------
            <BLANKLINE>

        Note:
            If the type of a given value in ``value`` doesn't match the
            column data type (e.g. a ``float`` for :class:`~snowflake.snowpark.types.StringType`
            column), this replacement will be skipped in this column. Especially,

                * ``int`` can be filled in a column with
                  :class:`~snowflake.snowpark.types.FloatType` or
                  :class:`~snowflake.snowpark.types.DoubleType`, but ``float`` cannot
                  filled in a column with :class:`~snowflake.snowpark.types.IntegerType`
                  or :class:`~snowflake.snowpark.types.LongType`.

        See Also:
            :func:`DataFrame.fillna`
        """
        # translate to
        # select col, iff(float_col = 'NaN' or float_col is null, replacement, float_col)
        # iff(non_float_col is null, replacement, non_float_col) from table where

        _check_subset_parameter(subset)

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_na_fill, stmt)
            self._dataframe._set_ast_ref(ast.df)
            if isinstance(value, dict):
                for k, v in value.items():
                    # N.B. In Phase 1, error checking will be incorporated directly here.
                    if isinstance(k, str):
                        entry = ast.value_map.add()
                        entry._1 = k
                        build_expr_from_python_val(entry._2, v)
            else:
                build_expr_from_python_val(ast.value, value)
            if isinstance(subset, str):
                ast.subset.variadic = True
                build_expr_from_python_val(ast.subset.args.add(), subset)
            elif isinstance(subset, Iterable):
                ast.subset.variadic = False
                for col in subset:
                    build_expr_from_python_val(ast.subset.args.add(), col)
            ast.include_decimal = include_decimal

        if subset is None:
            subset = self._dataframe.columns
        elif isinstance(subset, str):
            subset = [subset]

        if isinstance(value, dict):
            if not all([isinstance(k, str) for k in value.keys()]):
                raise ValueError(
                    "All keys in value should be column names (str)"
                )  # pragma: no cover
            value_dict = value
        else:
            value_dict = {col_name: value for col_name in subset}
        if not value_dict:
            new_df = self._dataframe._copy_without_ast()
            add_api_call(new_df, "DataFrameNaFunctions.fill")
            if _emit_ast:
                new_df._ast_id = stmt.uid
            return new_df
        if not all(
            [
                isinstance(v, VALID_PYTHON_TYPES_FOR_LITERAL_VALUE)
                for v in value_dict.values()
            ]
        ):
            raise ValueError(  # pragma: no cover
                "All values in value should be in one of "
                f"{VALID_PYTHON_TYPES_FOR_LITERAL_VALUE} types"
            )

        # the dictionary is ordered after Python3.7
        df_col_type_dict = {
            quote_name(field.name): field.datatype
            for field in self._dataframe.schema.fields
        }
        normalized_value_dict = {}
        for col_name, value in value_dict.items():
            normalized_col_name = quote_name(col_name)
            if normalized_col_name not in df_col_type_dict:
                raise SnowparkClientExceptionMessages.DF_CANNOT_RESOLVE_COLUMN_NAME(
                    normalized_col_name
                )
            normalized_value_dict[normalized_col_name] = value

        res_columns = []
        for col_name, datatype in df_col_type_dict.items():
            col = self._dataframe.col(col_name)
            if col_name in normalized_value_dict:
                value = normalized_value_dict[col_name]
                if _is_value_type_matching_for_na_function(
                    value, datatype, include_decimal=include_decimal
                ):
                    if isinstance(datatype, (FloatType, DoubleType)):
                        # iff(col = 'NaN' or col is null, value, col)
                        res_columns.append(
                            iff((col == math.nan) | col.is_null(), value, col).as_(
                                col_name
                            )
                        )
                    else:
                        # iff(col is null, value, col)
                        res_columns.append(
                            iff(
                                col.is_null(_emit_ast=False),
                                value,
                                col,
                                _emit_ast=False,
                            ).as_(col_name, _emit_ast=False)
                        )
                else:
                    _logger.warning(
                        "Input value type doesn't match the target column data type, "
                        f"this replacement was skipped. Column Name: {col_name}, "
                        f"Type: {datatype}, Input Value: {value}, Type: {type(value)}"
                    )
                    res_columns.append(col)
            else:
                # it's not in the value dict, just append the original column
                res_columns.append(col)

        new_df = self._dataframe.select(res_columns, _ast_stmt=stmt)
        adjust_api_subcalls(new_df, "DataFrameNaFunctions.fill", len_subcalls=1)
        return new_df

    @publicapi
    def replace(
        self,
        to_replace: Union[
            LiteralType,
            Iterable[LiteralType],
            Dict[LiteralType, LiteralType],
        ],
        value: Optional[Union[LiteralType, Iterable[LiteralType]]] = None,
        subset: Optional[Union[str, Iterable[str]]] = None,
        _emit_ast: bool = True,
        *,
        # keyword only arguments
        include_decimal: bool = False,
    ) -> "snowflake.snowpark.DataFrame":
        """
        Returns a new DataFrame that replaces values in the specified columns.

        Args:
            to_replace: A scalar value, or a list of values or a ``dict`` that associates
                the original values with the replacement values. If ``to_replace``
                is a ``dict``, ``value`` and ``subset`` are ignored. To replace a null
                value, use ``None`` in ``to_replace``. To replace a NaN value, use
                ``float("nan")`` in ``to_replace``. If ``to_replace`` is empty,
                the method returns the original DataFrame.
            value: A scalar value, or a list of values for the replacement. If
                ``value`` is a list, ``value`` should be of the same length as
                ``to_replace``. If ``value`` is a scalar and ``to_replace`` is a list,
                then ``value`` is used as a replacement for each item in ``to_replace``.
            subset: A list of the names of columns in which the values should be
                replaced. If ``cols`` is not provided or ``None``, the replacement
                will be applied to all columns. If ``cols`` is empty, the method
                returns the original DataFrame.
            include_decimal: Whether to allow ``Decimal`` values to replace ``IntegerType``
                and ``FloatType`` values.
        Examples::

            >>> df = session.create_dataframe([[1, 1.0, "1.0"], [2, 2.0, "2.0"]], schema=["a", "b", "c"])
            >>> # replace 1 with 3 in all columns
            >>> df.na.replace(1, 3).show()
            -------------------
            |"A"  |"B"  |"C"  |
            -------------------
            |3    |3.0  |1.0  |
            |2    |2.0  |2.0  |
            -------------------
            <BLANKLINE>
            >>> # replace 1 with 3 and 2 with 4 in all columns
            >>> df.na.replace([1, 2], [3, 4]).show()
            -------------------
            |"A"  |"B"  |"C"  |
            -------------------
            |3    |3.0  |1.0  |
            |4    |4.0  |2.0  |
            -------------------
            <BLANKLINE>
            >>> # replace 1 with 3 and 2 with 3 in all columns
            >>> df.na.replace([1, 2], 3).show()
            -------------------
            |"A"  |"B"  |"C"  |
            -------------------
            |3    |3.0  |1.0  |
            |3    |3.0  |2.0  |
            -------------------
            <BLANKLINE>
            >>> # the following line intends to replaces 1 with 3 and 2 with 4 in all columns
            >>> # and will give [Row(3, 3.0, "1.0"), Row(4, 4.0, "2.0")]
            >>> df.na.replace({1: 3, 2: 4}).show()
            -------------------
            |"A"  |"B"  |"C"  |
            -------------------
            |3    |3.0  |1.0  |
            |4    |4.0  |2.0  |
            -------------------
            <BLANKLINE>
            >>> # the following line intends to replace 1 with "3" in column "a",
            >>> # but will be ignored since "3" (str) doesn't match the original data type
            >>> df.na.replace({1: "3"}, ["a"]).show()
            -------------------
            |"A"  |"B"  |"C"  |
            -------------------
            |1    |1.0  |1.0  |
            |2    |2.0  |2.0  |
            -------------------
            <BLANKLINE>

        Note:
            If the type of a given value in ``to_replace`` or ``value`` doesn't match the
            column data type (e.g. a ``float`` for :class:`~snowflake.snowpark.types.StringType`
            column), this replacement will be skipped in this column. Especially,

                * ``int`` can replace or be replaced in a column with
                  :class:`~snowflake.snowpark.types.FloatType` or
                  :class:`~snowflake.snowpark.types.DoubleType`, but ``float`` cannot
                  replace or be replaced in a column with :class:`~snowflake.snowpark.types.IntegerType`
                  or :class:`~snowflake.snowpark.types.LongType`.

                * ``None`` can replace or be replaced in a column with any data type.

        See Also:
            :func:`DataFrame.replace`
        """

        _check_subset_parameter(subset)

        # AST.
        stmt = None
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.dataframe_na_replace, stmt)
            self._dataframe._set_ast_ref(ast.df)

            if isinstance(to_replace, dict):
                for k, v in to_replace.items():
                    entry = ast.replacement_map.add()
                    build_expr_from_python_val(entry._1, k)
                    build_expr_from_python_val(entry._2, v)
            elif isinstance(to_replace, Iterable):
                for v in to_replace:
                    entry = ast.to_replace_list.add()
                    build_expr_from_python_val(entry, v)
            else:
                build_expr_from_python_val(ast.to_replace_value, to_replace)

            if isinstance(value, Iterable):
                for v in value:
                    entry = ast.values.add()
                    build_expr_from_python_val(entry, v)
            else:
                build_expr_from_python_val(ast.value, value)

            if isinstance(subset, str):
                ast.subset.variadic = True
                build_expr_from_python_val(ast.subset.args.add(), subset)
            elif isinstance(subset, Iterable):
                ast.subset.variadic = False
                for col in subset:
                    build_expr_from_python_val(ast.subset.args.add(), col)
            ast.include_decimal = include_decimal

        # Modify subset.
        if subset is None:
            subset = self._dataframe.columns
        elif isinstance(subset, str):
            subset = [subset]

        if len(subset) == 0:
            new_df = self._dataframe._copy_without_ast()
            add_api_call(new_df, "DataFrameNaFunctions.replace")
            if _emit_ast:
                new_df._ast_id = stmt.uid
            return new_df

        if isinstance(to_replace, dict):
            replacement = to_replace
        elif isinstance(to_replace, (list, tuple)):
            if isinstance(value, (list, tuple)):
                if len(to_replace) != len(value):
                    raise ValueError(
                        "to_replace and value lists should be of the same length."
                        f"Got {len(to_replace)} and {len(value)}"
                    )
                else:
                    replacement = {k: v for k, v in zip(to_replace, value)}
            else:
                replacement = {k: value for k in to_replace}
        else:
            replacement = {to_replace: value}
        if not replacement:
            new_df = self._dataframe._copy_without_ast()
            add_api_call(new_df, "DataFrameNaFunctions.replace")
            if _emit_ast:
                new_df._ast_id = stmt.uid
            return new_df
        if not all(
            [
                isinstance(k, VALID_PYTHON_TYPES_FOR_LITERAL_VALUE)
                and isinstance(v, VALID_PYTHON_TYPES_FOR_LITERAL_VALUE)
                for k, v in replacement.items()
            ]
        ):
            raise ValueError(  # pragma: no cover
                "All keys and values in value should be in one of "
                f"{VALID_PYTHON_TYPES_FOR_LITERAL_VALUE} types"
            )

        # the dictionary is ordered after Python3.7
        df_col_type_dict = {
            quote_name(field.name): field.datatype
            for field in self._dataframe.schema.fields
        }
        normalized_col_name_set = {quote_name(col_name) for col_name in subset}
        for normalized_col_name in normalized_col_name_set:
            if normalized_col_name not in df_col_type_dict:
                raise SnowparkClientExceptionMessages.DF_CANNOT_RESOLVE_COLUMN_NAME(
                    normalized_col_name
                )

        res_columns = []
        for col_name, datatype in df_col_type_dict.items():
            col = self._dataframe.col(col_name)
            if col_name in normalized_col_name_set:
                case_when = None
                for key, value in replacement.items():
                    if _is_value_type_matching_for_na_function(
                        key,
                        datatype,
                        include_decimal=include_decimal,
                    ) and _is_value_type_matching_for_na_function(
                        value,
                        datatype,
                        include_decimal=include_decimal,
                    ):
                        cond = col.is_null() if key is None else (col == lit(key))
                        replace_value = lit(None) if value is None else lit(value)
                        case_when = (
                            case_when.when(cond, replace_value)
                            if case_when is not None
                            else when(cond, replace_value)
                        )
                    else:
                        _logger.warning(
                            "Input key or value type doesn't match the target column data type, "
                            f"this replacement was skipped. Column Name: {col_name}, "
                            f"Type: {datatype}, Input Key: {key}, Type: {type(key)}, "
                            f"Input Value: {value}, Type: {type(value)}"
                        )
                if case_when is not None:
                    case_when = case_when.otherwise(col).as_(col_name)
                    res_columns.append(case_when)
                else:
                    # all replacements are skipped due to data type mismatch
                    res_columns.append(col)
            else:
                res_columns.append(col)

        new_df = self._dataframe.select(res_columns, _ast_stmt=stmt)
        adjust_api_subcalls(new_df, "DataFrameNaFunctions.replace", len_subcalls=1)
        return new_df
