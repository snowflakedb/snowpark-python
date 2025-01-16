#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from functools import lru_cache, reduce
from typing import Any, Callable, Union

import numpy as np
import pandas as native_pd
from pandas import DatetimeTZDtype
from pandas.api.extensions import ExtensionDtype
from pandas.api.types import (
    is_datetime64_any_dtype,
    is_object_dtype,
    is_scalar,
    is_string_dtype,
)
from pandas.core.arrays.boolean import BooleanDtype
from pandas.core.arrays.floating import Float32Dtype, Float64Dtype
from pandas.core.arrays.integer import (
    Int8Dtype,
    Int16Dtype,
    Int32Dtype,
    Int64Dtype,
    UInt8Dtype,
    UInt16Dtype,
    UInt32Dtype,
    UInt64Dtype,
)
from pandas.core.arrays.string_ import StringDtype
from pandas.core.dtypes.common import is_bool_dtype, is_float_dtype, is_integer_dtype

from snowflake.snowpark import Column
from snowflake.snowpark._internal.type_utils import infer_type, merge_type
from snowflake.snowpark.dataframe import DataFrame as SnowparkDataFrame
from snowflake.snowpark.functions import (
    builtin,
    cast,
    col,
    date_part,
    floor,
    iff,
    length,
    to_char,
    to_varchar,
    to_variant,
)
from snowflake.snowpark.modin.plugin._internal.snowpark_pandas_types import (
    SnowparkPandasType,
    TimedeltaType,
)
from snowflake.snowpark.modin.plugin._internal.timestamp_utils import (
    generate_timestamp_col,
)
from snowflake.snowpark.modin.plugin._internal.utils import pandas_lit
from snowflake.snowpark.modin.plugin.utils.warning_message import WarningMessage
from snowflake.snowpark.types import (
    ArrayType,
    BinaryType,
    BooleanType,
    ByteType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    GeographyType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    ShortType,
    StringType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VariantType,
    _FractionalType,
    _IntegralType,
    _NumericType,
)

# This type is for a function that returns a DataType. By using it to lazily
# get a DataType, we can sometimes defer metadata queries until we need to
# check a type.
DataTypeGetter = Callable[[], DataType]

# The order of this mapping is important because the first match in either
# direction is used by TypeMapper.to_pandas() and TypeMapper.to_snowflake()
NUMPY_SNOWFLAKE_TYPE_PAIRS: list[tuple[Union[type, str], DataType]] = [
    (np.int64, LongType()),
    (np.uint64, LongType()),
    (np.int32, IntegerType()),
    (np.uint32, IntegerType()),
    (np.int16, ShortType()),
    (np.uint16, ShortType()),
    (np.int8, ByteType()),
    (np.uint8, ByteType()),
    (np.float32, FloatType()),
    (np.half, FloatType()),
    (np.float16, FloatType()),
    (np.float64, DoubleType()),
    (np.object_, VariantType()),
    (np.bool_, BooleanType()),
    ("datetime64[ns]", TimestampType()),
]

# Note strictly speaking these are only used to map FROM pandas TO snowflake
PANDAS_EXT_SNOWFLAKE_TYPE_PAIRS: list[tuple[ExtensionDtype, DataType]] = [
    (BooleanDtype(), BooleanType()),
    (Float32Dtype(), FloatType()),
    (Float64Dtype(), DoubleType()),
    (Int64Dtype(), LongType()),
    (UInt64Dtype(), LongType()),
    (Int32Dtype(), IntegerType()),
    (UInt32Dtype(), IntegerType()),
    (Int16Dtype(), ShortType()),
    (UInt16Dtype(), ShortType()),
    (Int8Dtype(), ByteType()),
    (UInt8Dtype(), ByteType()),
    (StringDtype(), StringType()),
]


# List of snowflake types that are treated as numeric data types
NUMERIC_SNOWFLAKE_TYPES: list[DataType] = [
    LongType,
    IntegerType,
    ShortType,
    ByteType,
    FloatType,
    DoubleType,
    DecimalType,
    # note that in snowflake boolean type is not treated as numeric, but
    # in pandas it is. Here we treat it as numeric dtype to stay consistent
    # with pandas behavior.
    BooleanType,
]
NUMERIC_SNOWFLAKE_TYPES_TUPLE = tuple(NUMERIC_SNOWFLAKE_TYPES)
TIME_SNOWFLAKE_TYPES: list[DataType] = [DateType, TimeType, TimestampType]
STRING_SNOWFLAKE_TYPES: list[DataType] = [StringType, BinaryType]
# List of snowflake types that are non-numeric.
NON_NUMERIC_SNOWFLAKE_TYPES: list[DataType] = (
    TIME_SNOWFLAKE_TYPES
    + STRING_SNOWFLAKE_TYPES
    + [
        GeographyType,
        MapType,
        ArrayType,
        VariantType,
    ]
)


def generate_pandas_to_snowflake_map() -> dict[
    Union[np.dtype, ExtensionDtype], DataType
]:
    d = {}
    # Create a mapping from pandas to snowflake types
    # the type pair mapping has duplicates so add only the first one.
    for (nptype, s) in NUMPY_SNOWFLAKE_TYPE_PAIRS:
        p: np.dtype = np.dtype(nptype)
        if p not in d:
            d[p] = s

    for (p, s) in PANDAS_EXT_SNOWFLAKE_TYPE_PAIRS:
        if p not in d:
            d[p] = s
    return d


# Note that we are DELIBERATELY leaving out SNOWFLAKE_TYPE_PAIRS.
# By default we return numpy types. We can change this decision by
# including the inverse of the PANDAS_EXT_SNOWFLAKE_TYPE_PAIRS here.
def generate_snowflake_to_pandas_map() -> dict[DataType, np.dtype]:
    d: dict[DataType, np.dtype] = {}
    for (p, s) in NUMPY_SNOWFLAKE_TYPE_PAIRS:
        if s not in d:
            d[s] = np.dtype(p)
    return d


PANDAS_TO_SNOWFLAKE_MAP = generate_pandas_to_snowflake_map()
SNOWFLAKE_TO_PANDAS_MAP = generate_snowflake_to_pandas_map()


def infer_series_type(series: native_pd.Series) -> DataType:
    """Infer the snowpark DataType for the given native pandas series"""

    data_type = series.dtype
    if data_type == np.object_:
        # if the series type is object type, try to derive the snowpark type based on
        # the type of each data element of the series. If failed to derive the type
        # information from the data or no data is available, we map it to VariantType()
        # to indicate this column may have mixed data types or data type is unknown.
        if series.size > 0:
            try:
                snowflake_type = reduce(
                    merge_type, (infer_object_type(o) for o in series)
                )
            except (TypeError, NotImplementedError):
                # if failed to infer type for object column, we treat it as VariantType
                snowflake_type = VariantType()
        else:
            snowflake_type = VariantType()
    else:
        snowflake_type = TypeMapper.to_snowflake(data_type)

    return snowflake_type


def infer_object_type(obj: Any) -> DataType:
    """Infer the snowpark DataType from obj"""

    # For scalar obj, we do a check to see if it is a missing value
    # in pandas first, if it is missing value, it will be mapped to
    # None in snowpark, and mapped to NULL in snowflake. Therefore, the
    # Type for the object will be NullType. pandas missing value includes
    # np.nan, None, pd.NaT and pd.NA.
    if is_scalar(obj) and native_pd.isna(obj):
        return NullType()

    try:
        # try to derive the regular python type by calling snowpark infer_type
        datatype = infer_type(obj)
    except TypeError:
        datatype = TypeMapper.to_snowflake(type(obj))
        if datatype == TimestampType() and getattr(obj, "tzinfo", None):
            datatype = TimestampType(TimestampTimeZone.TZ)
    return datatype


class TypeMapper:
    @classmethod
    def to_snowflake(
        cls, p: Union[np.dtype, ExtensionDtype, native_pd.Timestamp]
    ) -> DataType:
        """
        map a pandas or numpy type to snowpark data type.
        """
        snowpark_pandas_type = (
            SnowparkPandasType.get_snowpark_pandas_type_for_pandas_type(p)
        )
        if snowpark_pandas_type is not None:
            return snowpark_pandas_type

        if isinstance(p, DatetimeTZDtype):
            return TimestampType(TimestampTimeZone.TZ)
        if p is native_pd.Timestamp or is_datetime64_any_dtype(p):
            return TimestampType()
        if is_object_dtype(p):
            return VariantType()
        if is_string_dtype(p):
            return StringType()

        if is_bool_dtype(p):
            return BooleanType()
        if is_integer_dtype(p):
            return LongType()
        if is_float_dtype(p):
            return DoubleType()

        try:
            return PANDAS_TO_SNOWFLAKE_MAP[p]
        except KeyError:
            raise NotImplementedError(f"pandas type {p} is not implemented")

    @classmethod
    def to_pandas(cls, s: DataType) -> Union[np.dtype, ExtensionDtype]:
        """
        map a snowpark type to numpy type or pandas extended dtype.
        """
        # Treat decimal as a special case
        if isinstance(s, DecimalType):
            return np.dtype("int64") if s.scale == 0 else np.dtype("float64")
        if isinstance(s, TimestampType):
            return np.dtype("datetime64[ns]")
        if isinstance(s, TimedeltaType):
            return np.dtype("timedelta64[ns]")
        # We also need to treat parameterized types correctly
        if isinstance(s, (StringType, ArrayType, MapType, GeographyType)):
            return np.dtype(np.object_)
        if isinstance(s, SnowparkPandasType):
            return type(s).pandas_type
        return SNOWFLAKE_TO_PANDAS_MAP.get(s, np.dtype(np.object_))


def column_astype(
    id: str,
    from_sf_type: DataType,
    to_dtype: Union[np.dtype, ExtensionDtype],
    to_sf_type: DataType,
) -> Column:
    """
    Generate new column after calling astype on that column.
    Args:
        id: the quoted identifier
        from_sf_type: from Snowflake type
        to_dtype: to pandas dtype
        to_sf_type: to Snowflake type

    Returns:
        The new column after calling astype
    """
    curr_col = col(id)

    if to_dtype == np.object_:
        return to_variant(curr_col)
    if from_sf_type == to_sf_type:
        if isinstance(to_sf_type, BooleanType):
            new_col = to_variant(curr_col)
            # treat NULL values in boolean columns as False to match pandas behavior
            return iff(curr_col.is_null(), False, curr_col)
        return curr_col

    if isinstance(to_sf_type, _IntegralType) and "int64" not in str(to_dtype).lower():
        WarningMessage.single_warning(
            "Snowpark pandas API auto cast all integers to int64"
        )

    if (
        isinstance(to_sf_type, (FloatType, DoubleType))
        and "float64" not in str(to_dtype).lower()
    ):
        WarningMessage.single_warning(
            "Snowpark pandas API auto cast all floating points to float64"
        )

    if (
        isinstance(from_sf_type, TimestampType)
        and from_sf_type.tz == TimestampTimeZone.LTZ
    ):
        # treat TIMESTAMP_LTZ columns as same as TIMESTAMP_TZ
        curr_col = builtin("to_timestamp_tz")(curr_col)

    if isinstance(to_sf_type, TimestampType):
        assert to_sf_type.tz != TimestampTimeZone.LTZ, (
            "Cast to TIMESTAMP_LTZ is not supported in astype since "
            "Snowpark pandas API maps tz aware datetime to TIMESTAMP_TZ"
        )
        # convert to timestamp
        new_col = generate_timestamp_col(
            curr_col,
            from_sf_type,
            target_tz=str(to_dtype.tz)
            if isinstance(to_dtype, DatetimeTZDtype)
            else None,
            unit="ns",
        )
    elif isinstance(from_sf_type, StringType) and isinstance(to_sf_type, BooleanType):
        new_col = iff(length(curr_col) > 0, True, False)
    elif isinstance(from_sf_type, BooleanType) and isinstance(to_sf_type, StringType):
        new_col = iff(curr_col, "True", "False")
    elif isinstance(from_sf_type, TimestampType) and isinstance(
        to_sf_type, tuple(NUMERIC_SNOWFLAKE_TYPES)
    ):
        # pandas datetime unit is always ns from epoch, so we have to make this conversion too
        new_col = cast(date_part("epoch_nanosecond", curr_col), to_sf_type)
    elif isinstance(from_sf_type, TimestampType) and isinstance(to_sf_type, StringType):
        if from_sf_type.tz == TimestampTimeZone.NTZ:
            # e.g., "1970-01-01 00:00:00.000000001"
            new_col = to_varchar(curr_col, "YYYY-MM-DD HH24:MI:SS.FF")
        else:
            # e.g., "1970-01-01 00:00:00.000000001+09:00". See format details in
            # https://docs.snowflake.com/en/user-guide/date-time-input-output#about-the-elements-used-in-input-and-output-formats
            new_col = to_varchar(curr_col, "YYYY-MM-DD HH24:MI:SS.FFTZH:TZM")
    elif isinstance(from_sf_type, BooleanType) and isinstance(
        to_sf_type, _FractionalType
    ):
        # Snowflake does not allow casting boolean to float directly
        # make sure the column is cast to numeric first
        new_col = cast(cast(curr_col, LongType()), to_sf_type)
    elif isinstance(from_sf_type, _FractionalType) and isinstance(
        to_sf_type, BooleanType
    ):
        # Snowflake does not allow casting float to boolean directly
        # make sure the column is cast to numeric first
        new_col = cast(cast(curr_col, LongType()), to_sf_type)
    elif isinstance(from_sf_type, (TimeType, DateType)) and isinstance(
        to_sf_type, BooleanType
    ):
        # e.g., pd.Series([date(year=1, month=1, day=1)]*3).astype(bool) returns all true values
        new_col = cast(pandas_lit(True), to_sf_type)
    elif isinstance(to_sf_type, TimedeltaType):
        if isinstance(from_sf_type, _NumericType):
            # pandas always rounds down for Fractional type conversion to timedelta
            new_col = cast(floor(curr_col), LongType())
        else:
            new_col = cast(curr_col, LongType())
    else:
        new_col = cast(curr_col, to_sf_type)
    # astype should not have any effect on NULL values except when casting to boolean
    if isinstance(to_sf_type, BooleanType):
        # treat NULL values in boolean columns as False to match pandas behavior
        return iff(curr_col.is_null(), False, new_col)
    else:
        return iff(curr_col.is_null(), None, new_col)


def is_astype_type_error(
    from_sf_type: DataType,
    to_sf_type: DataType,
) -> bool:
    """
    Check whether astype will raise TypeError
    Args:
        from_sf_type: from mapped Snowflake type
        to_sf_type: to mapped Snowflake type

    Returns:
        True if it is one of the following pandas TypeError:
        - convert from any datetime to float
        - convert from boolean to DatetimeTZDtype
        - convert from time to any numeric or datetime
        - convert from date to any numeric
    """
    if isinstance(from_sf_type, TimestampType) and isinstance(
        to_sf_type, (FloatType, DoubleType)
    ):
        return True
    elif (
        isinstance(from_sf_type, BooleanType)
        and isinstance(to_sf_type, TimestampType)
        and to_sf_type.tz == TimestampTimeZone.TZ
    ):
        return True
    elif isinstance(from_sf_type, TimeType) and isinstance(
        to_sf_type, (_NumericType, TimestampType)
    ):
        return True
    elif isinstance(from_sf_type, DateType) and isinstance(to_sf_type, _NumericType):
        return True
    elif isinstance(from_sf_type, TimestampType) and isinstance(
        to_sf_type, TimedeltaType
    ):
        return True
    else:
        return False


def is_numeric_snowpark_type(snowpark_type: DataType) -> bool:
    return isinstance(snowpark_type, tuple(NUMERIC_SNOWFLAKE_TYPES))


def is_compatible_snowpark_types(sp_type_1: DataType, sp_type_2: DataType) -> bool:
    """
    Check whether two Snowpark types are compatible. Two Snowpark types are compatible if
    they are the same type or both are Snowpark numeric type.
    """
    if sp_type_1 == sp_type_2:
        return True

    if isinstance(sp_type_1, _NumericType) and isinstance(sp_type_2, _NumericType):
        return True

    # StringType of different length are compatible types.
    if isinstance(sp_type_1, StringType) and isinstance(sp_type_2, StringType):
        return True
    return False


@lru_cache
def _get_timezone_from_timestamp_tz(
    snowpark_dataframe: SnowparkDataFrame, snowflake_quoted_identifier: str
) -> Union[str, DatetimeTZDtype]:
    tz_df = (
        snowpark_dataframe.filter(col(snowflake_quoted_identifier).is_not_null())
        .select(to_char(col(snowflake_quoted_identifier), format="TZHTZM").as_("tz"))
        .group_by(["tz"])
        .agg()
        .limit(2)  # only need 2 to check whether it contains multiple timezones
        .to_pandas()
    )
    assert (
        len(tz_df) > 0
    ), f"col {snowflake_quoted_identifier} does not contain valid timezone offset"
    if len(tz_df) == 2:  # multi timezone cases
        return "object"
    return DatetimeTZDtype(tz="UTC" + tz_df.iloc[0, 0].replace("Z", ""))
