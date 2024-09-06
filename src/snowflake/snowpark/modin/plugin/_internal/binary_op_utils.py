#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import functools
from collections.abc import Hashable
from dataclasses import dataclass
from types import MappingProxyType

import numpy as np
import pandas as native_pd
from pandas._typing import Callable, Scalar

from snowflake.snowpark.column import Column as SnowparkColumn
from snowflake.snowpark.functions import (
    cast,
    ceil,
    col,
    concat,
    dateadd,
    datediff,
    floor,
    iff,
    is_null,
    repeat,
    when,
)
from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.modin.plugin._internal.join_utils import (
    JoinOrAlignInternalFrameResult,
)
from snowflake.snowpark.modin.plugin._internal.snowpark_pandas_types import (
    SnowparkPandasColumn,
    TimedeltaType,
)
from snowflake.snowpark.modin.plugin._internal.type_utils import (
    DataTypeGetter,
    infer_object_type,
)
from snowflake.snowpark.modin.plugin._internal.utils import pandas_lit
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.types import (
    DataType,
    LongType,
    NullType,
    StringType,
    TimestampTimeZone,
    TimestampType,
    _FractionalType,
    _IntegralType,
    _NumericType,
)

NAN_COLUMN = pandas_lit("nan").cast("float")

# set of supported binary operations that can be mapped to Snowflake
SUPPORTED_BINARY_OPERATIONS = {
    "truediv",
    "rtruediv",
    "floordiv",
    "rfloordiv",
    "mod",
    "rmod",
    "pow",
    "rpow",
    "__or__",
    "__ror__",
    "__and__",
    "__rand__",
    "add",
    "radd",
    "sub",
    "rsub",
    "mul",
    "rmul",
    "eq",
    "ne",
    "gt",
    "lt",
    "ge",
    "le",
}


def compute_modulo_between_snowpark_columns(
    first_operand: SnowparkColumn,
    first_datatype: DataType,
    second_operand: SnowparkColumn,
    second_datatype: DataType,
) -> SnowparkColumn:
    """
    Compute modulo between two Snowpark columns ``first_operand`` and ``second_operand``.
    Supports only numeric values for operands, raises NotImplementedError otherwise.
    Module may produce results different from native pandas or Python.
    """
    # 0. if f or s is NULL, return NULL (Snowflake's rule)
    # 1. s == 0, return nan
    # 2. if s != 0, return f % s
    #
    #     Examples
    # --------
    # >>> a = pd.Series([7, 7, -7, -7])
    # >>> b = pd.Series([5, -5, 5, -5])
    # >>> a % b
    # 0    2.0
    # 1    2.0
    # 2   -2.0
    # 3   -2.0
    # dtype: float64

    # >>> a = pd.Series([8.9, -0.22, np.nan, -1.02, 3.15, 2.0])
    # >>> b = pd.Series([-2.3, -76.34, 5.3, 5.3, 8.12])
    # >>> a % b
    # 0    2.00
    # 1   -0.22
    # 2     NaN
    # 3   -1.02
    # 4    3.15
    # 5     NaN
    # dtype: float64

    # Behavior differences
    # --------------------
    # Python               pandas 1.5            Snowflake
    #  7 %  5 =  2          7 %  5 =  2           7 %  5 =  2
    #  7 % -5 = -3          7 % -5 = -3           7 % -5 =  2
    # -7 %  5 =  3         -7 %  5 =  3          -7 %  5 = -2
    # -7 % -5 = -2         -7 % -5 = -2          -7 % -5 = -2
    #
    # Snowpark pandas API differs from native pandas results whenever an operand with a negative
    # sign is used.

    is_first_operand_numeric_type = (
        isinstance(first_datatype, _IntegralType)
        or isinstance(first_datatype, _FractionalType)
        or isinstance(first_datatype, NullType)
    )

    is_second_operand_numeric_type = (
        isinstance(second_datatype, _IntegralType)
        or isinstance(second_datatype, _FractionalType)
        or isinstance(second_datatype, NullType)
    )

    if is_first_operand_numeric_type and is_second_operand_numeric_type:
        return (
            when(first_operand.is_null() | second_operand.is_null(), None)
            .when(second_operand == 0, NAN_COLUMN)
            .otherwise(first_operand % second_operand)
        )
    else:
        ErrorMessage.not_implemented(
            "Modulo does not support non-numeric types, consider using a UDF with apply instead."
        )


def compute_power_between_snowpark_columns(
    first_operand: SnowparkColumn,
    second_operand: SnowparkColumn,
) -> SnowparkColumn:
    """
    Compute power between two Snowpark columns ``first_operand`` and ``second_operand``.
    """
    # 0. if f == 1 or s == 0, return 1 or 1.0 based on f's type (pandas' behavior)
    # 1. if f or s is NULL, return NULL (Snowflake's behavior)
    # 2. if f is nan, or s is nan, or f < 0 and s can not be cast to int without loss (int(s) != s), return nan
    #    In Snowflake, if f < 0 and s is not an integer, an invalid floating point operation will be raised.
    #    E.g., pow(-7, -10.0) is valid, but pow(-7, -10.1) is invalid in snowflake.
    #    In pandas, pow(-7, -10.1) returns NaN.
    # 3. else return f ** s
    result = (
        when((first_operand == 1) | (second_operand == 0), 1)
        .when(first_operand.is_null() | second_operand.is_null(), None)
        .when(
            (first_operand == NAN_COLUMN)
            | (second_operand == NAN_COLUMN)
            | (
                (first_operand < 0)
                # it checks whether the value can be cast int without loss
                & (second_operand.cast("int") != second_operand)
            ),
            NAN_COLUMN,
        )
        .otherwise(first_operand**second_operand)
    )
    return result


def is_binary_op_supported(op: str) -> bool:
    """
    check whether binary operation is mappable to Snowflake
    Args
        op: op as string

    Returns:
        True if binary operation can be mapped to Snowflake/Snowpark, else False
    """

    return op in SUPPORTED_BINARY_OPERATIONS


def _compute_subtraction_between_snowpark_timestamp_columns(
    first_operand: SnowparkColumn,
    first_datatype: DataType,
    second_operand: SnowparkColumn,
    second_datatype: DataType,
) -> SnowparkPandasColumn:
    """
    Compute subtraction between two snowpark columns.

    Args:
        first_operand: SnowparkColumn for lhs
        first_datatype: Snowpark datatype for lhs
        second_operand: SnowparkColumn for rhs
        second_datatype: Snowpark datatype for rhs
        subtraction_type: Type of subtraction.
    """
    if (
        first_datatype.tz is TimestampTimeZone.NTZ
        and second_datatype.tz is TimestampTimeZone.TZ
    ) or (
        first_datatype.tz is TimestampTimeZone.TZ
        and second_datatype.tz is TimestampTimeZone.NTZ
    ):
        raise TypeError("Cannot subtract tz-naive and tz-aware datetime-like objects.")
    return SnowparkPandasColumn(
        iff(
            is_null(first_operand).__or__(is_null(second_operand)),
            pandas_lit(native_pd.NaT),
            datediff("ns", second_operand, first_operand),
        ),
        TimedeltaType(),
    )


# This is an immmutable map from right-sided binary operations to the
# equivalent left-sided binary operations. For example, "rsub" maps to "sub"
# because rsub(col(a), col(b)) is equivalent to sub(col(b), col(a)).
_RIGHT_BINARY_OP_TO_LEFT_BINARY_OP: MappingProxyType[str, str] = MappingProxyType(
    {
        "rtruediv": "truediv",
        "rfloordiv": "floordiv",
        "rpow": "pow",
        "radd": "add",
        "rmul": "mul",
        "rsub": "sub",
        "rmod": "mod",
        "__rand__": "__and__",
        "__ror__": "__or__",
    }
)


def _op_is_between_two_timedeltas_or_timedelta_and_null(
    first_datatype: DataType, second_datatype: DataType
) -> bool:
    """
    Whether the binary operation is between two timedeltas, or between timedelta and null.

    Args:
        first_datatype: First datatype
        second_datatype: Second datatype

    Returns:
        bool: Whether op is between two timedeltas or between timedelta and null.
    """
    return (
        isinstance(first_datatype, TimedeltaType)
        and isinstance(second_datatype, (TimedeltaType, NullType))
    ) or (
        isinstance(first_datatype, (TimedeltaType, NullType))
        and isinstance(second_datatype, TimedeltaType)
    )


def _is_numeric_non_timedelta_type(datatype: DataType) -> bool:
    """
    Whether the datatype is numeric, but not a timedelta type.

    Args:
        datatype: The datatype

    Returns:
        bool: Whether the datatype is numeric, but not a timedelta type.
    """
    return isinstance(datatype, _NumericType) and not isinstance(
        datatype, TimedeltaType
    )


def _op_is_between_timedelta_and_numeric(
    first_datatype: DataTypeGetter, second_datatype: DataTypeGetter
) -> bool:
    """
    Whether the binary operation is between a timedelta and a numeric type.

    Returns true if either operand is a timedelta and the other operand is a
    non-timedelta numeric.

    Args:
        First datatype: Getter for first datatype.
        Second datatype: Getter for second datatype.

    Returns:
        bool: Whether the binary operation is between a timedelta and a numeric type.
    """
    return (
        isinstance(first_datatype(), TimedeltaType)
        and _is_numeric_non_timedelta_type(second_datatype())
    ) or (
        _is_numeric_non_timedelta_type(first_datatype())
        and isinstance(second_datatype(), TimedeltaType)
    )


def compute_binary_op_between_snowpark_columns(
    op: str,
    first_operand: SnowparkColumn,
    first_datatype: DataTypeGetter,
    second_operand: SnowparkColumn,
    second_datatype: DataTypeGetter,
) -> SnowparkPandasColumn:
    """
    Compute pandas binary operation for two SnowparkColumns
    Args:
        op: pandas operation
        first_operand: SnowparkColumn for lhs
        first_datatype: Callable for Snowpark Datatype for lhs
        second_operand: SnowparkColumn for rhs
        second_datatype: Callable for Snowpark DateType for rhs
        it is not needed.

    Returns:
        SnowparkPandasColumn for translated pandas operation
    """
    if op in _RIGHT_BINARY_OP_TO_LEFT_BINARY_OP:
        # Normalize right-sided binary operations to the equivalent left-sided
        # operations with swapped operands. For example, rsub(col(a), col(b))
        # becomes sub(col(b), col(a))
        op, first_operand, first_datatype, second_operand, second_datatype = (
            _RIGHT_BINARY_OP_TO_LEFT_BINARY_OP[op],
            second_operand,
            second_datatype,
            first_operand,
            first_datatype,
        )

    binary_op_result_column = None
    snowpark_pandas_type = None

    # some operators and the data types have to be handled specially to align with pandas
    # However, it is difficult to fail early if the arithmetic operator is not compatible
    # with the data type, so we just let the server raise exception (e.g. a string minus a string).
    if (
        op == "add"
        and isinstance(second_datatype(), TimedeltaType)
        and isinstance(first_datatype(), TimestampType)
    ):
        binary_op_result_column = dateadd("ns", second_operand, first_operand)
    elif (
        op == "add"
        and isinstance(first_datatype(), TimedeltaType)
        and isinstance(second_datatype(), TimestampType)
    ):
        binary_op_result_column = dateadd("ns", first_operand, second_operand)
    elif op in (
        "add",
        "sub",
        "eq",
        "ne",
        "gt",
        "ge",
        "lt",
        "le",
        "floordiv",
        "truediv",
    ) and (
        (
            isinstance(first_datatype(), TimedeltaType)
            and isinstance(second_datatype(), NullType)
        )
        or (
            isinstance(second_datatype(), TimedeltaType)
            and isinstance(first_datatype(), NullType)
        )
    ):
        return SnowparkPandasColumn(pandas_lit(None), TimedeltaType())
    elif (
        op == "sub"
        and isinstance(second_datatype(), TimedeltaType)
        and isinstance(first_datatype(), TimestampType)
    ):
        binary_op_result_column = dateadd("ns", -1 * second_operand, first_operand)
    elif (
        op == "sub"
        and isinstance(first_datatype(), TimedeltaType)
        and isinstance(second_datatype(), TimestampType)
    ):
        # Timedelta - Timestamp doesn't make sense. Raise the same error
        # message as pandas.
        raise TypeError("bad operand type for unary -: 'DatetimeArray'")
    elif op == "mod" and _op_is_between_two_timedeltas_or_timedelta_and_null(
        first_datatype(), second_datatype()
    ):
        binary_op_result_column = compute_modulo_between_snowpark_columns(
            first_operand, first_datatype(), second_operand, second_datatype()
        )
        snowpark_pandas_type = TimedeltaType()
    elif op == "pow" and _op_is_between_two_timedeltas_or_timedelta_and_null(
        first_datatype(), second_datatype()
    ):
        raise TypeError("unsupported operand type for **: Timedelta")
    elif op == "__or__" and _op_is_between_two_timedeltas_or_timedelta_and_null(
        first_datatype(), second_datatype()
    ):
        raise TypeError("unsupported operand type for |: Timedelta")
    elif op == "__and__" and _op_is_between_two_timedeltas_or_timedelta_and_null(
        first_datatype(), second_datatype()
    ):
        raise TypeError("unsupported operand type for &: Timedelta")
    elif (
        op in ("add", "sub")
        and isinstance(first_datatype(), TimedeltaType)
        and isinstance(second_datatype(), TimedeltaType)
    ):
        snowpark_pandas_type = TimedeltaType()
    elif op == "mul" and _op_is_between_two_timedeltas_or_timedelta_and_null(
        first_datatype(), second_datatype()
    ):
        raise np.core._exceptions._UFuncBinaryResolutionError(  # type: ignore[attr-defined]
            np.multiply, (np.dtype("timedelta64[ns]"), np.dtype("timedelta64[ns]"))
        )
    elif op in (
        "eq",
        "ne",
        "gt",
        "ge",
        "lt",
        "le",
    ) and _op_is_between_two_timedeltas_or_timedelta_and_null(
        first_datatype(), second_datatype()
    ):
        # These operations, when done between timedeltas, work without any
        # extra handling in `snowpark_pandas_type` or `binary_op_result_column`.
        pass
    elif op == "mul" and (
        _op_is_between_timedelta_and_numeric(first_datatype, second_datatype)
    ):
        binary_op_result_column = cast(
            floor(first_operand * second_operand), LongType()
        )
        snowpark_pandas_type = TimedeltaType()
    # For `eq` and `ne`, note that Snowflake will consider 1 equal to
    # Timedelta(1) because those two have the same representation in Snowflake,
    # so we have to compare types in the client.
    elif op == "eq" and (
        _op_is_between_timedelta_and_numeric(first_datatype, second_datatype)
    ):
        binary_op_result_column = pandas_lit(False)
    elif op == "ne" and _op_is_between_timedelta_and_numeric(
        first_datatype, second_datatype
    ):
        binary_op_result_column = pandas_lit(True)
    elif (
        op in ("truediv", "floordiv")
        and isinstance(first_datatype(), TimedeltaType)
        and _is_numeric_non_timedelta_type(second_datatype())
    ):
        binary_op_result_column = cast(
            floor(first_operand / second_operand), LongType()
        )
        snowpark_pandas_type = TimedeltaType()
    elif (
        op == "mod"
        and isinstance(first_datatype(), TimedeltaType)
        and _is_numeric_non_timedelta_type(second_datatype())
    ):
        binary_op_result_column = ceil(
            compute_modulo_between_snowpark_columns(
                first_operand, first_datatype(), second_operand, second_datatype()
            )
        )
        snowpark_pandas_type = TimedeltaType()
    elif op in ("add", "sub") and (
        (
            isinstance(first_datatype(), TimedeltaType)
            and _is_numeric_non_timedelta_type(second_datatype())
        )
        or (
            _is_numeric_non_timedelta_type(first_datatype())
            and isinstance(second_datatype(), TimedeltaType)
        )
    ):
        raise TypeError(
            "Snowpark pandas does not support addition or subtraction between timedelta values and numeric values."
        )
    elif op in ("truediv", "floordiv", "mod") and (
        _is_numeric_non_timedelta_type(first_datatype())
        and isinstance(second_datatype(), TimedeltaType)
    ):
        raise TypeError(
            "Snowpark pandas does not support dividing numeric values by timedelta values with div (/), mod (%), or floordiv (//)."
        )
    elif op in (
        "add",
        "sub",
        "truediv",
        "floordiv",
        "mod",
        "gt",
        "ge",
        "lt",
        "le",
        "ne",
        "eq",
    ) and (
        (
            isinstance(first_datatype(), TimedeltaType)
            and isinstance(second_datatype(), StringType)
        )
        or (
            isinstance(second_datatype(), TimedeltaType)
            and isinstance(first_datatype(), StringType)
        )
    ):
        # TODO(SNOW-1646604): Support these cases.
        ErrorMessage.not_implemented(
            f"Snowpark pandas does not yet support the operation {op} between timedelta and string"
        )
    elif op in ("gt", "ge", "lt", "le", "pow", "__or__", "__and__") and (
        _op_is_between_timedelta_and_numeric(first_datatype, second_datatype)
    ):
        raise TypeError(
            f"Snowpark pandas does not support binary operation {op} between timedelta and a non-timedelta type."
        )
    elif op == "floordiv":
        binary_op_result_column = floor(first_operand / second_operand)
    elif op == "mod":
        binary_op_result_column = compute_modulo_between_snowpark_columns(
            first_operand, first_datatype(), second_operand, second_datatype()
        )
    elif op == "pow":
        binary_op_result_column = compute_power_between_snowpark_columns(
            first_operand, second_operand
        )
    elif op == "__or__":
        binary_op_result_column = first_operand | second_operand
    elif op == "__and__":
        binary_op_result_column = first_operand & second_operand
    elif (
        op == "add"
        and isinstance(second_datatype(), StringType)
        and isinstance(first_datatype(), StringType)
    ):
        # string/string case (only for add)
        binary_op_result_column = concat(first_operand, second_operand)
    elif op == "mul" and (
        (
            isinstance(second_datatype(), _IntegralType)
            and isinstance(first_datatype(), StringType)
        )
        or (
            isinstance(second_datatype(), StringType)
            and isinstance(first_datatype(), _IntegralType)
        )
    ):
        # string/integer case (only for mul/rmul).
        # swap first_operand with second_operand because
        # REPEAT(<input>, <n>) expects <input> to be string
        if isinstance(first_datatype(), _IntegralType):
            first_operand, second_operand = second_operand, first_operand

        binary_op_result_column = iff(
            second_operand > pandas_lit(0),
            repeat(first_operand, second_operand),
            # Snowflake's repeat doesn't support negative number,
            # but pandas will return an empty string
            pandas_lit(""),
        )
    elif op == "equal_null":
        # TODO(SNOW-1641716): In Snowpark pandas, generally use this equal_null
        # with type checking intead of snowflake.snowpark.functions.equal_null.
        if not are_equal_types(first_datatype(), second_datatype()):
            binary_op_result_column = pandas_lit(False)
        else:
            binary_op_result_column = first_operand.equal_null(second_operand)
    elif (
        op == "sub"
        and isinstance(first_datatype(), TimestampType)
        and isinstance(second_datatype(), NullType)
    ):
        # Timestamp - NULL or NULL - Timestamp raises SQL compilation error,
        # but it's valid in pandas and returns NULL.
        binary_op_result_column = pandas_lit(None)
    elif (
        op == "sub"
        and isinstance(first_datatype(), NullType)
        and isinstance(second_datatype(), TimestampType)
    ):
        # Timestamp - NULL or NULL - Timestamp raises SQL compilation error,
        # but it's valid in pandas and returns NULL.
        binary_op_result_column = pandas_lit(None)
    elif (
        op == "sub"
        and isinstance(first_datatype(), TimestampType)
        and isinstance(second_datatype(), TimestampType)
    ):
        return _compute_subtraction_between_snowpark_timestamp_columns(
            first_operand=first_operand,
            first_datatype=first_datatype(),
            second_operand=second_operand,
            second_datatype=second_datatype(),
        )
    # If there is no special binary_op_result_column result, it means the operator and
    # the data type of the column don't need special handling. Then we get the overloaded
    # operator from Snowpark Column class, e.g., __add__ to perform binary operations.
    if binary_op_result_column is None:
        binary_op_result_column = getattr(first_operand, f"__{op}__")(second_operand)

    return SnowparkPandasColumn(
        snowpark_column=binary_op_result_column,
        snowpark_pandas_type=snowpark_pandas_type,
    )


def are_equal_types(type1: DataType, type2: DataType) -> bool:
    """
    Check if given types are considered equal in context of df.equals(other) or
    series.equals(other) methods.
    Args:
        type1: First type to compare.
        type2: Second type to compare.
    Returns:
        True if given types are equal, False otherwise.
    """
    if isinstance(type1, TimedeltaType) or isinstance(type2, TimedeltaType):
        return type1 == type2
    if isinstance(type1, _IntegralType) and isinstance(type2, _IntegralType):
        return True
    if isinstance(type1, _FractionalType) and isinstance(type2, _FractionalType):
        return True
    if isinstance(type1, StringType) and isinstance(type2, StringType):
        return True

    return type1 == type2


def compute_binary_op_between_snowpark_column_and_scalar(
    op: str,
    first_operand: SnowparkColumn,
    datatype: DataTypeGetter,
    second_operand: Scalar,
) -> SnowparkPandasColumn:
    """
    Compute the binary operation between a Snowpark column and a scalar.
    Args:
        op: the name of binary operation
        first_operand: The SnowparkColumn for lhs
        datatype: Callable for Snowpark data type
        second_operand: Scalar value

    Returns:
        SnowparkPandasColumn for translated pandas operation
    """

    def second_datatype() -> DataType:
        return infer_object_type(second_operand)

    return compute_binary_op_between_snowpark_columns(
        op, first_operand, datatype, pandas_lit(second_operand), second_datatype
    )


def compute_binary_op_between_scalar_and_snowpark_column(
    op: str,
    first_operand: Scalar,
    second_operand: SnowparkColumn,
    datatype: DataTypeGetter,
) -> SnowparkPandasColumn:
    """
    Compute the binary operation between a scalar and a Snowpark column.
    Args:
        op: the name of binary operation
        first_operand: Scalar value
        second_operand: The SnowparkColumn for rhs
        datatype: Callable for Snowpark data type
        it is not needed.

    Returns:
        SnowparkPandasColumn for translated pandas operation
    """

    def first_datatype() -> DataType:
        return infer_object_type(first_operand)

    return compute_binary_op_between_snowpark_columns(
        op, pandas_lit(first_operand), first_datatype, second_operand, datatype
    )


def compute_binary_op_with_fill_value(
    op: str,
    lhs: SnowparkColumn,
    lhs_datatype: DataTypeGetter,
    rhs: SnowparkColumn,
    rhs_datatype: DataTypeGetter,
    fill_value: Scalar,
) -> SnowparkPandasColumn:
    """
    Helper method for performing binary operations.
    1. Fills NaN/None values in the lhs and rhs with the given fill_value.
    2. Computes the binary operation expression for lhs <op> rhs.

    fill_value replaces NaN/None values when only either lhs or rhs is NaN/None, not both lhs and rhs.
    For instance, with fill_value = 100,
    1. Given lhs = None and rhs = 10, lhs is replaced with fill_value.
           result = lhs + rhs => None + 10 => 100 (replaced) + 10 = 110
    2. Given lhs = 3 and rhs = None, rhs is replaced with fill_value.
           result = lhs + rhs => 3 + None => 3 + 100 (replaced) = 103
    3. Given lhs = None and rhs = None, neither lhs nor rhs is replaced since they both are None.
           result = lhs + rhs => None + None => None.

    Args:
        op: pandas operation to perform between lhs and rhs
        lhs: the lhs SnowparkColumn
        lhs_datatype: Callable for Snowpark Datatype for lhs
        rhs: the rhs SnowparkColumn
        rhs_datatype: Callable for Snowpark Datatype for rhs
        fill_value: Fill existing missing (NaN) values, and any new element needed for
            successful DataFrame alignment, with this value before computation.

    Returns:
        SnowparkPandasColumn for translated pandas operation
    """
    lhs_cond, rhs_cond = lhs, rhs
    if fill_value is not None:
        fill_value_lit = pandas_lit(fill_value)
        lhs_cond = iff(lhs.is_null() & ~rhs.is_null(), fill_value_lit, lhs)
        rhs_cond = iff(rhs.is_null() & ~lhs.is_null(), fill_value_lit, rhs)

    return compute_binary_op_between_snowpark_columns(
        op, lhs_cond, lhs_datatype, rhs_cond, rhs_datatype
    )


def merge_label_and_identifier_pairs(
    sorted_column_labels: list[str],
    q_frame_sorted: list[tuple[str, str]],
    q_missing_sorted: list[tuple[str, str]],
) -> list[tuple[str, str]]:
    """
    Helper function to create a merged list of column label/snowflake quoted identifiers. Assume q_frame_sorted and q_missing_sorted are disjoint wrt to labels.

    Example:
         Given sorted_column_labels = [1, 2, 3]
         and q_frame_sorted =  [(1, "A"), (3, "C")]    q_missing_sorted =  [(2, "B")]
         this function will produce as output [(1, "A"), (2, "B"), (3, "C")].
         Each q_frame_sorted and q_missing_sorted are lists of label/identifier pairs.
         I.e., [(1, "A"), (3, "C")] should be understood as 1 -> "A", 3 -> "B".
         They're each assumed to be sorted with respect to their labels, and all labels must be contained within
         the sorted_column_labels variable.
         The result is a combined, sorted representation 1 -> "A", 2 -> "B", 3 -> "C" which resembles the merge-step
         of a classical mergesort algorithm.
    Args:
        sorted_column_labels: The labels to merge for
        q_frame_sorted: sorted list of label/identifier pairs. All labels must be contained within sorted_column_labels.
        q_missing_sorted: sorted list of label/identifier pairs. All labels must be contained within sorted_column_labels.

    Returns:
        List of label/identifier pairs. If the labels were projected out, they would form sorted_column_labels.
    """
    if len(q_frame_sorted) > 0 and len(q_missing_sorted) > 0:
        # merge labels/identifiers

        i_frame = 0
        i_missing = 0

        pairs = []
        for label in sorted_column_labels:
            # Leave merge iff either queue is exhausted.
            if i_frame >= len(q_frame_sorted) or i_missing >= len(q_missing_sorted):
                break

            if label == q_frame_sorted[i_frame][0]:
                pairs.append(q_frame_sorted[i_frame])
                i_frame += 1
            elif label == q_missing_sorted[i_missing][0]:
                pairs.append(q_missing_sorted[i_missing])
                i_missing += 1
            # else case not relevant here, because labels of q_frame_sorted and q_missing_sorted must be disjoint.

        if i_frame < len(q_frame_sorted):
            pairs += q_frame_sorted[i_frame:]
        elif i_missing < len(q_missing_sorted):
            pairs += q_missing_sorted[i_missing:]

        return pairs
    elif len(q_missing_sorted) == 0:
        return q_frame_sorted
    else:
        return q_missing_sorted


@dataclass
class BinaryOperationPair:
    # For detailed description of the members, cf. `prepare_binop_pairs_between_dataframe_and_dataframe`.
    # This is a helper class to hold the results of this function.
    identifier: str
    lhs: SnowparkColumn
    lhs_datatype: Callable
    rhs: SnowparkColumn
    rhs_datatype: Callable


def prepare_binop_pairs_between_dataframe_and_dataframe(
    aligned_rhs_and_lhs: JoinOrAlignInternalFrameResult,
    combined_data_labels: list[Hashable],
    lhs_frame: InternalFrame,
    rhs_frame: InternalFrame,
) -> list[BinaryOperationPair]:
    """
    Returns a list of BinaryOperationPair which can be used to carry out a binary operation between two dataframes.
    Each BinaryOperationPair consists of the following:
    - identifier: an identifier that can be used within align_result to hold the result of a binary operation between two columns
    - lhs: a SnowparkColumn expression for the left operand
    - lhs_typer: a function to lazily determine the Snowpark datatype of `lhs`
    - rhs: a SnowparkColumn expression for the right operand
    - rhs_typer: a function to lazily determine the Snowpark datatype of `rhs`

    BinaryOperationPair will be returned in the order of `combined_data_labels`

    Args:
        aligned_rhs_and_lhs: the align result between other_frame and self_frame
        combined_data_labels: the combined data labels to be used for align result.
        rhs_frame: a frame representing the right side.
        lhs_frame: a frame representing the left side.

    Returns:
        List of BinaryOperationPair.
    """
    # construct list of pairs which label belongs to which quoted identifier
    type_map = aligned_rhs_and_lhs.result_frame.get_snowflake_type
    left_right_pairs = []
    for label in combined_data_labels:
        left_identifier, right_identifier = None, None

        try:
            left_idx = lhs_frame.data_column_pandas_labels.index(label)
            left_quoted_identifier = lhs_frame.data_column_snowflake_quoted_identifiers[
                left_idx
            ]
            left_identifier = (
                aligned_rhs_and_lhs.result_column_mapper.left_quoted_identifiers_map[
                    left_quoted_identifier
                ]
            )
            left = col(left_identifier)
            # To avoid referencing always the last right_identifier in the loop, use functools.partial
            left_typer = functools.partial(
                lambda identifier: type_map(identifier), left_identifier
            )  # noqa: E731
        except ValueError:
            # lhs label not in list.
            left = pandas_lit(None)
            left_typer = lambda: infer_object_type(  # type: ignore[assignment] # noqa: E731
                None
            )

        try:
            right_idx = rhs_frame.data_column_pandas_labels.index(label)
            right_quoted_identifier = (
                rhs_frame.data_column_snowflake_quoted_identifiers[right_idx]
            )
            right_identifier = (
                aligned_rhs_and_lhs.result_column_mapper.right_quoted_identifiers_map[
                    right_quoted_identifier
                ]
            )
            right = col(right_identifier)
            # To avoid referencing always the last right_identifier in the loop, use functools.partial
            right_typer = functools.partial(
                lambda identifier: type_map(identifier), right_identifier
            )  # noqa: E731
        except ValueError:
            # rhs label not in list
            right = pandas_lit(None)
            right_typer = lambda: infer_object_type(None)  # type: ignore[assignment] # noqa: E731

        identifier_to_replace = left_identifier or right_identifier
        assert identifier_to_replace, "either identifier must be valid"

        # We return a lambda to determine the datatype of each operand lazily as this allows to defer
        # invoking a DESCRIBE query as much as possible.
        left_right_pairs.append(
            BinaryOperationPair(
                identifier=identifier_to_replace,
                lhs=left,
                lhs_datatype=left_typer,
                rhs=right,
                rhs_datatype=right_typer,
            )
        )
    return left_right_pairs
