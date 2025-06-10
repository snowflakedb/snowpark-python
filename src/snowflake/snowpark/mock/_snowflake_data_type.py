#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import Any, Dict, Iterable, NamedTuple, Optional, Union

from snowflake.snowpark.mock._options import installed_pandas, pandas as pd
from snowflake.snowpark.mock._telemetry import LocalTestOOBTelemetryService
from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException
from snowflake.snowpark.types import (
    ArrayType,
    BooleanType,
    DataType,
    DateType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    LongType,
    MapType,
    NullType,
    StringType,
    TimestampTimeZone,
    TimestampType,
    TimeType,
    VariantType,
    _FractionalType,
    _IntegralType,
    _NumericType,
)

# pandas is an optional requirement for local test, so make snowpark compatible with env where pandas
# not installed, here we redefine the base class to avoid ImportError
PandasDataframeType = object if not installed_pandas else pd.DataFrame
PandasSeriesType = object if not installed_pandas else pd.Series

# https://docs.snowflake.com/en/sql-reference/parameters#label-timestamp-type-mapping
# SNOW-1630258 for local testing session parameters support
_TIMESTAMP_TYPE_MAPPING = "TIMESTAMP_NTZ"


_TIMESTAMP_TYPE_TIMEZONE_MAPPING = {
    "TIMESTAMP_NTZ": TimestampTimeZone.NTZ,
    "TIMESTAMP_LTZ": TimestampTimeZone.LTZ,
    "TIMESTAMP_TZ": TimestampTimeZone.TZ,
}


def infer_sp_type_from_python_type(p: Any) -> DataType:
    """helper function to map python types (using pandas) to Snowpark types."""
    from pandas.core.dtypes.common import (
        is_bool_dtype,
        is_float_dtype,
        is_integer_dtype,
        is_object_dtype,
        is_string_dtype,
    )

    # TODO SNOW-1826001: refactor this with Snowpark pandas to avoid redundancy.

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
    return VariantType()


class Operator:
    def op(self, *operands):
        pass


class Add(Operator):
    def op(self, *operands):
        if len(operands) == 1:
            return type(operands[0])


class Minus(Operator):
    ...


class Multiply(Operator):
    ...


class FunctionCall(Operator):
    ...


"""
https://docs.snowflake.com/en/sql-reference/data-type-conversion
"""

"""
@dataclasses.dataclass
class SnowDataTypeConversion:
    from_type: Type[DataType]
    to_type: Type[DataType]
    castable: bool
    coercible: bool


SNOW_DATA_TYPE_CONVERSION_LIST = [
    SnowDataTypeConversion(ArrayType, StringType, True, False),
    SnowDataTypeConversion(ArrayType, VariantType, True, True),
    SnowDataTypeConversion(BinaryType, StringType, True, False),
    SnowDataTypeConversion(BinaryType, VariantType, True, False),
    SnowDataTypeConversion(BooleanType, DecimalType, True, False),
    SnowDataTypeConversion(BooleanType, StringType, True, True),
    SnowDataTypeConversion(BooleanType, VariantType, True, True),
    SnowDataTypeConversion(DateType, TimestampType, True, False),
    SnowDataTypeConversion(DateType, StringType, True, True),
    SnowDataTypeConversion(DateType, VariantType, True, False),
    SnowDataTypeConversion(FloatType, BooleanType, True, True),
    SnowDataTypeConversion(FloatType, DecimalType, True, True),
    SnowDataTypeConversion(FloatType, StringType, True, True),
    SnowDataTypeConversion(FloatType, VariantType, True, True),
    SnowDataTypeConversion(GeographyType, VariantType, True, False),
    # SnowDataTypeConversion(GeometryType, VariantType, True, False),  # GeometryType isn't available yet.
    SnowDataTypeConversion(DecimalType, BooleanType, True, True),
    SnowDataTypeConversion(DecimalType, FloatType, True, True),
    SnowDataTypeConversion(DecimalType, TimestampType, True, True),
    SnowDataTypeConversion(DecimalType, StringType, True, True),
    SnowDataTypeConversion(DecimalType, VariantType, True, True),
    SnowDataTypeConversion(MapType, ArrayType, True, False),
    SnowDataTypeConversion(MapType, StringType, True, False),
    SnowDataTypeConversion(MapType, VariantType, True, True),
    SnowDataTypeConversion(TimeType, StringType, True, True),
    SnowDataTypeConversion(TimeType, VariantType, True, False),
    SnowDataTypeConversion(TimestampType, DateType, True, True),
    SnowDataTypeConversion(TimestampType, TimeType, True, True),
    SnowDataTypeConversion(TimestampType, StringType, True, True),
    SnowDataTypeConversion(TimestampType, VariantType, True, False),
    SnowDataTypeConversion(StringType, BooleanType, True, True),
    SnowDataTypeConversion(StringType, DateType, True, True),
    SnowDataTypeConversion(StringType, FloatType, True, True),
    SnowDataTypeConversion(StringType, DecimalType, True, True),
    SnowDataTypeConversion(StringType, TimeType, True, True),
    SnowDataTypeConversion(StringType, TimestampType, True, True),
    SnowDataTypeConversion(StringType, VariantType, True, False),
    SnowDataTypeConversion(VariantType, DateType, True, True),
    SnowDataTypeConversion(VariantType, FloatType, True, True),
    SnowDataTypeConversion(VariantType, GeographyType, True, False),
    SnowDataTypeConversion(VariantType, DecimalType, True, True),
    SnowDataTypeConversion(VariantType, MapType, True, True),
    SnowDataTypeConversion(VariantType, TimeType, True, True),
    SnowDataTypeConversion(VariantType, TimestampType, True, True),
    SnowDataTypeConversion(VariantType, StringType, True, True),
]


SNOW_DATA_TYPE_CONVERSION_DICT = {
    (x.from_type, x.to_type): x for x in SNOW_DATA_TYPE_CONVERSION_LIST
}
"""


def isna_helper(obj: Any) -> bool:
    """Small helper function to detect whether object is considered NULL. Needed because for
    lists, tuples, ... pandas isna() does not handle correctly."""
    if isinstance(obj, Iterable):
        return False
    return pd.isna(obj)


class ColumnType(NamedTuple):
    datatype: DataType
    nullable: bool


def normalize_decimal(d: DecimalType):
    if d.scale > d.precision or d.scale > 38 or d.scale < 0 or d.precision < 0:
        SnowparkLocalTestingException.raise_from_error(
            ValueError(
                f"Inferred data type DecimalType({d.precision}, {d.scale}) is invalid."
            )
        )
    d.precision = min(38, d.precision)


def normalize_output_sf_type(t: DataType) -> DataType:
    if t == DecimalType(38, 0):
        return LongType()
    return t


def reset_nan_to_none_if_necessary(col_a, col_b, res_col):
    # in pandas arithmetic operation, None are automatically convert to nan
    if isinstance(res_col.sf_type.datatype, _NumericType):
        for idx, (x, y) in enumerate(zip(col_a, col_b)):
            if x is None or y is None:
                res_col[idx] = None
    return res_col


def infer_column_type_from_python_object(obj: Any) -> ColumnType:
    """Helper to map the type of an underlying python object to a ColumnType to be used in the ColumnEmulator."""

    if isinstance(obj, ColumnEmulator):
        return obj.sf_type

    if obj is None:
        ColumnType(VariantType(), True)

    return ColumnType(infer_sp_type_from_python_type(type(obj)), False)


def calculate_type(c1: ColumnType, c2: ColumnType, op: Union[str]):
    """op, left, right decide what's next."""
    t1, t2 = c1.datatype, c2.datatype
    nullable = c1.nullable or c2.nullable
    decimal_types = (_IntegralType, DecimalType)
    if isinstance(t1, decimal_types) and isinstance(t2, decimal_types):
        p1, s1 = get_number_precision_scale(t1)
        p2, s2 = get_number_precision_scale(t2)
        if op == "/":
            division_min_scale = 6
            division_max_scale = 12
            l1 = p1 - s1
            res_scale = max(min(s1 + division_min_scale, division_max_scale), s1)
            res_lead = l1 + s2
            res_precision = min(38, res_scale + res_lead)
            result_type = normalize_output_sf_type(
                DecimalType(res_precision, res_scale)
            )
            return ColumnType(result_type, nullable)
        elif op == "*":
            multiplication_max_scale = 12
            l1 = p1 - s1
            l2 = p2 - s2
            result_scale = min(s1 + s2, max(multiplication_max_scale, max(s1, s2)))
            result_precision = min(38, result_scale + l1 + l2)
            result_type = DecimalType(result_precision, result_scale)
            normalize_decimal(result_type)
            result_type = normalize_output_sf_type(result_type)
            return ColumnType(result_type, nullable)
        elif op in ("+", "-"):
            # widen the number with smaller scale
            if s1 > s2:
                gap = s1 - s2
                if p2 - s2 == 1:  # special logic in Snowflake
                    gap = gap + 1
                p2 += gap
                s2 += gap
            elif s1 < s2:
                gap = s2 - s1
                if p1 - s1 == 1:
                    gap = gap + 1
                p1 += gap
                s1 += gap
            result_type = normalize_output_sf_type(
                DecimalType(min(38, max(p1, p2) + 1), max(s1, s2))
            )
            return ColumnType(result_type, nullable)
        elif op == "%":
            new_scale = max(s1, s2)
            new_decimal = max(p1 - s1, p2 - s2)
            new_decimal = new_decimal + new_scale
            result_type = normalize_output_sf_type(DecimalType(new_decimal, new_scale))
            return ColumnType(result_type, nullable)
        else:
            LocalTestOOBTelemetryService.get_instance().log_not_supported_error(
                external_feature_name=f"Type inference for operator {op} is implemented.",
                internal_feature_name="_snowflake_data_type.calculate_type",
                parameters_info={"op": op},
                raise_error=NotImplementedError,
            )
    elif isinstance(t1, (FloatType, DoubleType)) or isinstance(
        t2, (FloatType, DoubleType)
    ):
        return ColumnType(DoubleType(), nullable)
    elif isinstance(t1, DateType) or isinstance(t2, DateType):
        if isinstance(t2, DateType):
            t1, t2 = t2, t1
        if t2 not in (
            IntegerType,
            LongType,
            DecimalType,
            FloatType,
            DoubleType,
        ) or op not in ("+", "-"):
            SnowparkLocalTestingException.raise_from_error(
                ValueError(
                    f"Result data type can't be calculated: (type1: {t1}, op: '{op}', type2: {t2})."
                )
            )
        return ColumnType(DateType(), nullable)

    SnowparkLocalTestingException.raise_from_error(
        TypeError(
            f"Result data type can't be calculated: (type1: {t1}, op: '{op}', type2: {t2})."
        )
    )


def coerce_t1_into_t2(t1: DataType, t2: DataType) -> Optional[DataType]:
    """Based on result of SELECT system$typeof("RES") FROM (SELECT CASE WHEN (<pred>) THEN <t1> ELSE <t2> END AS "RES")"""
    if t1 == t2:
        return t2
    elif isinstance(t1, NullType):
        return t2
    if isinstance(t1, StringType):
        if isinstance(t2, StringType):
            if t1.length is None or t2.length is None:
                return StringType()
            return StringType(max(t1.length, t2.length))
        elif isinstance(
            t2,
            (
                _FractionalType,
                _IntegralType,
                DateType,
                TimeType,
                TimestampType,
                VariantType,
            ),
        ):
            return t2
    elif isinstance(t1, _IntegralType):
        if isinstance(t2, _IntegralType):
            res = calculate_type(ColumnType(t1, True), ColumnType(t2, True), "+")
            return res.datatype
        elif isinstance(t2, (_FractionalType, VariantType, BooleanType)):
            return t2
    elif isinstance(t1, _FractionalType):
        if isinstance(t2, _FractionalType):
            res = calculate_type(ColumnType(t1, True), ColumnType(t2, True), "+")
            return res.datatype
        elif isinstance(t2, (BooleanType, VariantType)):
            return t2
    elif isinstance(t1, BooleanType):
        if isinstance(t2, (StringType, VariantType)):
            return t2
    elif isinstance(t1, DateType):
        if isinstance(t2, (TimestampType, VariantType)):
            return t2
    elif isinstance(t1, ArrayType):
        if isinstance(t2, ArrayType):
            if t1.element_type == t2.element_type:
                return t2
            else:
                return ArrayType(VariantType())
        elif isinstance(t2, VariantType):
            return t2
    elif isinstance(t1, MapType):
        if isinstance(t2, MapType):
            if t1.key_type == t2.key_type and t2.value_type == t1.value_type:
                return t2
            else:
                return MapType(key_type=VariantType(), value_type=VariantType())
        elif isinstance(t2, VariantType):
            return t2
    elif isinstance(t1, (TimeType, TimestampType, MapType, ArrayType)):
        if isinstance(t2, VariantType):
            return t2
        if isinstance(t1, TimestampType) and isinstance(t2, TimestampType):
            if (
                t1.tz is TimestampTimeZone.DEFAULT
                and t2.tz is TimestampTimeZone.NTZ
                and _TIMESTAMP_TYPE_MAPPING == "TIMESTAMP_NTZ"
            ):
                return t2
    return None


def get_coerce_result_type(c1: ColumnType, c2: ColumnType):
    nullability = c1.nullable or c2.nullable
    if sf_datatype := coerce_t1_into_t2(c1.datatype, c2.datatype):
        return ColumnType(sf_datatype, nullability)
    if sf_datatype := coerce_t1_into_t2(c2.datatype, c1.datatype):
        return ColumnType(sf_datatype, nullability)
    return None


class TableEmulator(PandasDataframeType):
    _metadata = [
        "sf_types",
        "sf_types_by_col_index",
        "_null_rows_idxs_map",
        "sorted_by",
    ]

    @property
    def _constructor(self):
        return TableEmulator

    @property
    def _constructor_sliced(self):
        return ColumnEmulator

    def __init__(
        self,
        *args,
        sf_types: Optional[Dict[str, ColumnType]] = None,
        sf_types_by_col_index: Optional[Dict[int, ColumnType]] = None,
        **kwargs,
    ) -> None:
        if TableEmulator.__base__ == object:
            raise RuntimeError(
                "Local Testing requires pandas as dependency, "
                "please make sure pandas is installed in the environment.\n"
            )
        super().__init__(*args, **kwargs)
        self.sf_types = {} if not sf_types else sf_types
        # TODO: SNOW-976145, move to index based approach to store col type mapping
        self.sf_types_by_col_index = (
            {} if not sf_types_by_col_index else sf_types_by_col_index
        )
        self._null_rows_idxs_map = {}
        self.sorted_by = []

    def __getitem__(self, item):
        result = super().__getitem__(item)
        if isinstance(result, ColumnEmulator):  # pandas.Series
            result._sf_type = self.sf_types.get(item)
        elif isinstance(result, TableEmulator):  # pandas.DataFrame
            result.sf_types = self.sf_types
        else:
            # TODO: figure out what cases, it may can be removed
            # list of columns
            for ce in result:
                ce._sf_type = self.sf_types.get(ce.name)
        return result

    def __setitem__(self, key, value):
        super().__setitem__(key, value)
        if isinstance(value, ColumnEmulator):
            self.sf_types[key] = value.sf_type
            self._null_rows_idxs_map[key] = value._null_rows_idxs

    def sort_values(self, by, **kwargs):
        result = super().sort_values(by, **kwargs)
        result.sf_types = self.sf_types
        return result

    def to_pandas(self):
        return super().copy()

    def copy(self, deep=True):
        ans = super().copy(deep)
        # Without the hasattr this fails.
        if hasattr(self, "sf_types"):
            ans.sf_types = self.sf_types.copy()
        if hasattr(self, "_null_rows_idxs_map"):
            ans._null_rows_idxs_map = self._null_rows_idxs_map.copy()
        if hasattr(self, "sf_types_by_col_index"):
            ans.sf_types_by_col_index = self.sf_types_by_col_index.copy()
        if hasattr(self, "sorted_by"):
            ans.sorted_by = self.sorted_by.copy()
        return ans


def get_number_precision_scale(t: DataType):
    if isinstance(t, _IntegralType):
        return 38, 0
    if isinstance(t, DecimalType):
        return t.precision, t.scale
    return None, None


def add_date_and_number(
    col1: "ColumnEmulator", col2: "ColumnEmulator"
) -> Optional["ColumnEmulator"]:
    """If one column is DateType and another column is numeric, round and add the numeric to days"""
    if isinstance(col2.sf_type.datatype, DateType):
        col1, col2 = col2, col1
    if isinstance(col1.sf_type.datatype, DateType) and isinstance(
        col2.sf_type.datatype, _NumericType
    ):
        result = pd.to_datetime(col1) + pd.to_timedelta(round(col2), unit="d")
        result.sf_type = ColumnType(
            DateType(), col1.sf_type.nullable or col2.sf_type.nullable
        )
        return result
    SnowparkLocalTestingException.raise_from_error(
        ValueError(f"Can't add {col1.sf_type.datatype} and {col2.sf_type.datatype}")
    )


def broadcast_value(value: Any, len: int) -> "ColumnEmulator":
    """Helper function to create a ColumnEmulator out of a single scalar object of length len."""

    if isinstance(value, ColumnEmulator):
        return value

    # Create Series with length len.
    return ColumnEmulator([value] * len)


class ColumnEmulator(PandasSeriesType):
    _metadata = ["sf_type", "_null_rows_idxs"]

    @property
    def _constructor(self):
        return ColumnEmulator

    @property
    def _constructor_expanddim(self):
        return TableEmulator

    def __init__(self, *args, **kwargs) -> None:
        if ColumnEmulator.__base__ == object:
            raise RuntimeError(
                "Local Testing requires pandas as dependency, "
                "please make sure pandas is installed in the environment.\n"
            )
        sf_type = kwargs.pop("sf_type", None)
        super().__init__(*args, **kwargs)
        self._sf_type: ColumnType = sf_type
        # record which rows should be marked as null instead of None
        # snowflake SubfieldString has this behavior
        # suppose there are two Variant objects in table "v": 1. { "a": None } 2. None
        # if we do sub-field v["a"], snowpark python return ['null', None] instead of [None, None]
        # however during the calculation we want to keep using None, so we need extra data structure to store
        # the information of null vs None
        # check SNOW-960190 for more context
        self._null_rows_idxs = []

    @property
    def sf_type(self) -> ColumnType:
        if self._sf_type is not None:
            return self._sf_type

        # TODO SNOW-1826001: Else branch is taken when using UDTFs. (Remove comment if not applicable anymore)
        # If a snowflake type has not been explicitly set before, infer one from the underlying pandas Series.
        else:
            # Can not use short cut self.isna().any() as this leads to endless recursion
            # due to ColumnEmulator inheriting from a pandas Series.
            nullable = any([isna_helper(obj) for obj in self.values])

            from pandas.core.dtypes.common import is_object_dtype

            if is_object_dtype(self.dtype) and len(self) != 0:
                # Infer from data when object type for the type to become more specific.
                return ColumnType(
                    infer_sp_type_from_python_type(type(self.iloc[0])), nullable
                )
            else:
                return ColumnType(infer_sp_type_from_python_type(self.dtype), nullable)

    @sf_type.setter
    def sf_type(self, value: ColumnType):
        self._sf_type = value

    def __add__(self, other):
        """TODO: needs to calculate date +"""
        other = broadcast_value(other, len(self))
        if isinstance(self.sf_type.datatype, DateType) or isinstance(
            other.sf_type.datatype, DateType
        ):
            return add_date_and_number(self, other)
        result = super().__add__(other)
        if self.sf_type:
            result.sf_type = calculate_type(self.sf_type, other.sf_type, op="+")
        result = reset_nan_to_none_if_necessary(self, other, result)
        return result

    def __radd__(self, other):
        if isinstance(self.sf_type.datatype, DateType) or isinstance(
            other.sf_type.datatype, DateType
        ):
            return add_date_and_number(self, other)
        other = broadcast_value(other, len(self))
        result = super().__radd__(other)
        result.sf_type = calculate_type(other.sf_type, self.sf_type, op="+")
        result = reset_nan_to_none_if_necessary(self, other, result)
        return result

    def __sub__(self, other):
        if isinstance(self.sf_type.datatype, DateType) and isinstance(
            other.sf_type.datatype, _NumericType
        ):
            return add_date_and_number(self, -other)
        other = broadcast_value(other, len(self))
        result = super().__sub__(other)
        result.sf_type = calculate_type(self.sf_type, other.sf_type, op="-")
        result = reset_nan_to_none_if_necessary(self, other, result)
        return result

    def __rsub__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__rsub__(other)
        result.sf_type = calculate_type(other.sf_type, self.sf_type, op="-")
        result = reset_nan_to_none_if_necessary(self, other, result)
        return result

    def __mul__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__mul__(other)
        result.sf_type = calculate_type(self.sf_type, other.sf_type, op="*")
        result = reset_nan_to_none_if_necessary(self, other, result)
        return result

    def __rmul__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__rmul__(other)
        result.sf_type = calculate_type(other.sf_type, self.sf_type, op="*")
        result = reset_nan_to_none_if_necessary(self, other, result)
        return result

    def __bool__(self):
        result = super().__bool__()
        result.sf_type = ColumnType(BooleanType(), self.sf_type.nullable)
        return result

    def __and__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__and__(other)
        result.sf_type = ColumnType(BooleanType(), True)
        return result

    def __or__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__or__(other)
        result.sf_type = ColumnType(BooleanType(), True)
        return result

    def __ne__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__ne__(other)
        result.sf_type = ColumnType(BooleanType(), True)
        return result

    def __xor__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__xor__(other)
        result.sf_type = ColumnType(BooleanType(), True)
        return result

    def __pow__(self, power):
        result = super().__pow__(power)
        result.sf_type = ColumnType(
            DoubleType(), self.sf_type.nullable or power.sf_type.nullable
        )
        return result

    def __ge__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__ge__(other)
        result.sf_type = ColumnType(BooleanType(), True)
        return result

    def __gt__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__gt__(other)
        result.sf_type = ColumnType(BooleanType(), True)
        return result

    def __invert__(self):
        result = super().__invert__()
        result.sf_type = ColumnType(BooleanType(), True)
        return result

    def __le__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__le__(other)
        result.sf_type = ColumnType(BooleanType(), True)
        return result

    def __lt__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__lt__(other)
        result.sf_type = ColumnType(BooleanType(), True)
        return result

    def __eq__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__eq__(other)
        result.sf_type = ColumnType(BooleanType(), True)
        return result

    def __neg__(self):
        result = super().__neg__()
        result.sf_type = self.sf_type
        return result

    def __rand__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__rand__(other)
        result.sf_type = ColumnType(BooleanType(), True)
        return result

    def __mod__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__mod__(other)
        result.sf_type = calculate_type(self.sf_type, other.sf_type, op="%")
        return result

    def __rmod__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__mod__(other)
        result.sf_type = calculate_type(other.sf_type, self.sf_type, op="%")
        return result

    def __ror__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__ror__(other)
        result.sf_type = ColumnType(BooleanType(), True)
        return result

    def __round__(self, n=None):
        result = super().__round__(n)
        if isinstance(self.sf_type.datatype, (FloatType, DoubleType, _IntegralType)):
            result.sf_type = self.sf_type
        elif isinstance(self.sf_type.datatype, DecimalType):
            scale = self.sf_type.datatype.scale
            if scale <= n:
                result._sf_type = self.sf_type
            else:
                result_scale = 0 if n <= 0 else n
                result_precision = min(self.sf_type.datatype.precision + 1, 38)
                result._sf_type = ColumnType(
                    DecimalType(result_precision, result_scale), self.sf_type.nullable
                )
        return result

    def __rpow__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__rpow__(other)
        result._sf_type = ColumnType(DoubleType(), True)
        return result

    def __rtruediv__(self, other):
        other = broadcast_value(other, len(self))
        return other.__truediv__(self)

    def __truediv__(self, other):
        other = broadcast_value(other, len(self))
        result = super().__truediv__(other)
        sf_type = calculate_type(self.sf_type, other.sf_type, op="/")
        if isinstance(sf_type.datatype, DecimalType):
            result = result.astype("double").round(sf_type.datatype.scale)
        elif isinstance(sf_type.datatype, (FloatType, DoubleType)):
            result = result.astype("double").round(16)
        result._sf_type = sf_type
        result = reset_nan_to_none_if_necessary(self, other, result)
        return result

    def isna(self):
        result = super().isna()
        result._sf_type = ColumnType(BooleanType(), True)
        return result

    def isnull(self):
        result = super().isnull()
        result._sf_type = ColumnType(BooleanType(), True)
        return result
