from typing import (
    ClassVar as _ClassVar,
    Iterable as _Iterable,
    Mapping as _Mapping,
    Optional as _Optional,
    Union as _Union,
)

from google.protobuf import (
    descriptor as _descriptor,
    message as _message,
    wrappers_pb2 as _wrappers_pb2,
)
from google.protobuf.internal import (
    containers as _containers,
    enum_type_wrapper as _enum_type_wrapper,
)

DESCRIPTOR: _descriptor.FileDescriptor

class __Version__(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PROTO3_REQUIRES_THIS: _ClassVar[__Version__]
    MAX_VERSION: _ClassVar[__Version__]

PROTO3_REQUIRES_THIS: __Version__
MAX_VERSION: __Version__

class List_Expr(_message.Message):
    __slots__ = ("list",)
    LIST_FIELD_NUMBER: _ClassVar[int]
    list: _containers.RepeatedCompositeFieldContainer[Expr]
    def __init__(
        self, list: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...
    ) -> None: ...

class List_SpDataType(_message.Message):
    __slots__ = ("list",)
    LIST_FIELD_NUMBER: _ClassVar[int]
    list: _containers.RepeatedCompositeFieldContainer[SpDataType]
    def __init__(
        self, list: _Optional[_Iterable[_Union[SpDataType, _Mapping]]] = ...
    ) -> None: ...

class List_String(_message.Message):
    __slots__ = ("list",)
    LIST_FIELD_NUMBER: _ClassVar[int]
    list: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, list: _Optional[_Iterable[str]] = ...) -> None: ...

class Map_Expr_Expr(_message.Message):
    __slots__ = ("list",)
    LIST_FIELD_NUMBER: _ClassVar[int]
    list: _containers.RepeatedCompositeFieldContainer[Tuple_Expr_Expr]
    def __init__(
        self, list: _Optional[_Iterable[_Union[Tuple_Expr_Expr, _Mapping]]] = ...
    ) -> None: ...

class Map_String_Expr(_message.Message):
    __slots__ = ("list",)
    LIST_FIELD_NUMBER: _ClassVar[int]
    list: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    def __init__(
        self, list: _Optional[_Iterable[_Union[Tuple_String_Expr, _Mapping]]] = ...
    ) -> None: ...

class Map_String_String(_message.Message):
    __slots__ = ("list",)
    LIST_FIELD_NUMBER: _ClassVar[int]
    list: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self, list: _Optional[_Iterable[_Union[Tuple_String_String, _Mapping]]] = ...
    ) -> None: ...

class Tuple_Expr_Expr(_message.Message):
    __slots__ = ("_1", "_2")
    _1_FIELD_NUMBER: _ClassVar[int]
    _2_FIELD_NUMBER: _ClassVar[int]
    _1: Expr
    _2: Expr
    def __init__(
        self,
        _1: _Optional[_Union[Expr, _Mapping]] = ...,
        _2: _Optional[_Union[Expr, _Mapping]] = ...,
    ) -> None: ...

class Tuple_Expr_Float(_message.Message):
    __slots__ = ("_1", "_2")
    _1_FIELD_NUMBER: _ClassVar[int]
    _2_FIELD_NUMBER: _ClassVar[int]
    _1: Expr
    _2: float
    def __init__(
        self, _1: _Optional[_Union[Expr, _Mapping]] = ..., _2: _Optional[float] = ...
    ) -> None: ...

class Tuple_String_Expr(_message.Message):
    __slots__ = ("_1", "_2")
    _1_FIELD_NUMBER: _ClassVar[int]
    _2_FIELD_NUMBER: _ClassVar[int]
    _1: str
    _2: Expr
    def __init__(
        self, _1: _Optional[str] = ..., _2: _Optional[_Union[Expr, _Mapping]] = ...
    ) -> None: ...

class Tuple_String_List_String(_message.Message):
    __slots__ = ("_1", "_2")
    _1_FIELD_NUMBER: _ClassVar[int]
    _2_FIELD_NUMBER: _ClassVar[int]
    _1: str
    _2: _containers.RepeatedScalarFieldContainer[str]
    def __init__(
        self, _1: _Optional[str] = ..., _2: _Optional[_Iterable[str]] = ...
    ) -> None: ...

class Tuple_String_SpVariant(_message.Message):
    __slots__ = ("_1", "_2")
    _1_FIELD_NUMBER: _ClassVar[int]
    _2_FIELD_NUMBER: _ClassVar[int]
    _1: str
    _2: SpVariant
    def __init__(
        self, _1: _Optional[str] = ..., _2: _Optional[_Union[SpVariant, _Mapping]] = ...
    ) -> None: ...

class Tuple_String_String(_message.Message):
    __slots__ = ("_1", "_2")
    _1_FIELD_NUMBER: _ClassVar[int]
    _2_FIELD_NUMBER: _ClassVar[int]
    _1: str
    _2: str
    def __init__(self, _1: _Optional[str] = ..., _2: _Optional[str] = ...) -> None: ...

class Language(_message.Message):
    __slots__ = ("java_language", "python_language", "scala_language")
    JAVA_LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    PYTHON_LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    SCALA_LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    java_language: JavaLanguage
    python_language: PythonLanguage
    scala_language: ScalaLanguage
    def __init__(
        self,
        java_language: _Optional[_Union[JavaLanguage, _Mapping]] = ...,
        python_language: _Optional[_Union[PythonLanguage, _Mapping]] = ...,
        scala_language: _Optional[_Union[ScalaLanguage, _Mapping]] = ...,
    ) -> None: ...

class PythonLanguage(_message.Message):
    __slots__ = ("version",)
    VERSION_FIELD_NUMBER: _ClassVar[int]
    version: Version
    def __init__(self, version: _Optional[_Union[Version, _Mapping]] = ...) -> None: ...

class ScalaLanguage(_message.Message):
    __slots__ = ("version",)
    VERSION_FIELD_NUMBER: _ClassVar[int]
    version: Version
    def __init__(self, version: _Optional[_Union[Version, _Mapping]] = ...) -> None: ...

class JavaLanguage(_message.Message):
    __slots__ = ("version",)
    VERSION_FIELD_NUMBER: _ClassVar[int]
    version: Version
    def __init__(self, version: _Optional[_Union[Version, _Mapping]] = ...) -> None: ...

class Version(_message.Message):
    __slots__ = ("label", "major", "minor", "patch")
    LABEL_FIELD_NUMBER: _ClassVar[int]
    MAJOR_FIELD_NUMBER: _ClassVar[int]
    MINOR_FIELD_NUMBER: _ClassVar[int]
    PATCH_FIELD_NUMBER: _ClassVar[int]
    label: str
    major: int
    minor: int
    patch: int
    def __init__(
        self,
        label: _Optional[str] = ...,
        major: _Optional[int] = ...,
        minor: _Optional[int] = ...,
        patch: _Optional[int] = ...,
    ) -> None: ...

class PythonTimeZone(_message.Message):
    __slots__ = ("name", "offset_seconds")
    NAME_FIELD_NUMBER: _ClassVar[int]
    OFFSET_SECONDS_FIELD_NUMBER: _ClassVar[int]
    name: _wrappers_pb2.StringValue
    offset_seconds: int
    def __init__(
        self,
        name: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        offset_seconds: _Optional[int] = ...,
    ) -> None: ...

class FnName(_message.Message):
    __slots__ = ("fn_name_flat", "fn_name_structured")
    FN_NAME_FLAT_FIELD_NUMBER: _ClassVar[int]
    FN_NAME_STRUCTURED_FIELD_NUMBER: _ClassVar[int]
    fn_name_flat: FnNameFlat
    fn_name_structured: FnNameStructured
    def __init__(
        self,
        fn_name_flat: _Optional[_Union[FnNameFlat, _Mapping]] = ...,
        fn_name_structured: _Optional[_Union[FnNameStructured, _Mapping]] = ...,
    ) -> None: ...

class FnNameFlat(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class FnNameStructured(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, name: _Optional[_Iterable[str]] = ...) -> None: ...

class UdtfSchema(_message.Message):
    __slots__ = ("udtf_schema__names", "udtf_schema__type")
    UDTF_SCHEMA__NAMES_FIELD_NUMBER: _ClassVar[int]
    UDTF_SCHEMA__TYPE_FIELD_NUMBER: _ClassVar[int]
    udtf_schema__names: UdtfSchema_Names
    udtf_schema__type: UdtfSchema_Type
    def __init__(
        self,
        udtf_schema__names: _Optional[_Union[UdtfSchema_Names, _Mapping]] = ...,
        udtf_schema__type: _Optional[_Union[UdtfSchema_Type, _Mapping]] = ...,
    ) -> None: ...

class UdtfSchema_Type(_message.Message):
    __slots__ = ("return_type",)
    RETURN_TYPE_FIELD_NUMBER: _ClassVar[int]
    return_type: SpDataType
    def __init__(
        self, return_type: _Optional[_Union[SpDataType, _Mapping]] = ...
    ) -> None: ...

class UdtfSchema_Names(_message.Message):
    __slots__ = ("schema",)
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    schema: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, schema: _Optional[_Iterable[str]] = ...) -> None: ...

class SpWindowRelativePosition(_message.Message):
    __slots__ = (
        "sp_window_relative_position__current_row",
        "sp_window_relative_position__position",
        "sp_window_relative_position__unbounded_following",
        "sp_window_relative_position__unbounded_preceding",
    )
    SP_WINDOW_RELATIVE_POSITION__CURRENT_ROW_FIELD_NUMBER: _ClassVar[int]
    SP_WINDOW_RELATIVE_POSITION__POSITION_FIELD_NUMBER: _ClassVar[int]
    SP_WINDOW_RELATIVE_POSITION__UNBOUNDED_FOLLOWING_FIELD_NUMBER: _ClassVar[int]
    SP_WINDOW_RELATIVE_POSITION__UNBOUNDED_PRECEDING_FIELD_NUMBER: _ClassVar[int]
    sp_window_relative_position__current_row: bool
    sp_window_relative_position__position: SpWindowRelativePosition_Position
    sp_window_relative_position__unbounded_following: bool
    sp_window_relative_position__unbounded_preceding: bool
    def __init__(
        self,
        sp_window_relative_position__current_row: bool = ...,
        sp_window_relative_position__position: _Optional[
            _Union[SpWindowRelativePosition_Position, _Mapping]
        ] = ...,
        sp_window_relative_position__unbounded_following: bool = ...,
        sp_window_relative_position__unbounded_preceding: bool = ...,
    ) -> None: ...

class SpWindowRelativePosition_Position(_message.Message):
    __slots__ = ("n",)
    N_FIELD_NUMBER: _ClassVar[int]
    n: int
    def __init__(self, n: _Optional[int] = ...) -> None: ...

class PdIndexExpr(_message.Message):
    __slots__ = ("flex_ord", "key", "ord")
    FLEX_ORD_FIELD_NUMBER: _ClassVar[int]
    KEY_FIELD_NUMBER: _ClassVar[int]
    ORD_FIELD_NUMBER: _ClassVar[int]
    flex_ord: FlexOrd
    key: Key
    ord: Ord
    def __init__(
        self,
        flex_ord: _Optional[_Union[FlexOrd, _Mapping]] = ...,
        key: _Optional[_Union[Key, _Mapping]] = ...,
        ord: _Optional[_Union[Ord, _Mapping]] = ...,
    ) -> None: ...

class Ord(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: Expr
    def __init__(self, v: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...

class FlexOrd(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: Expr
    def __init__(self, v: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...

class Key(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: Expr
    def __init__(self, v: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...

class PdProjectIndexExpr(_message.Message):
    __slots__ = (
        "bool_filter_list",
        "flex_ord_list",
        "flex_ord_range",
        "key_list",
        "key_range",
        "ord_list",
        "ord_range",
    )
    BOOL_FILTER_LIST_FIELD_NUMBER: _ClassVar[int]
    FLEX_ORD_LIST_FIELD_NUMBER: _ClassVar[int]
    FLEX_ORD_RANGE_FIELD_NUMBER: _ClassVar[int]
    KEY_LIST_FIELD_NUMBER: _ClassVar[int]
    KEY_RANGE_FIELD_NUMBER: _ClassVar[int]
    ORD_LIST_FIELD_NUMBER: _ClassVar[int]
    ORD_RANGE_FIELD_NUMBER: _ClassVar[int]
    bool_filter_list: BoolFilterList
    flex_ord_list: FlexOrdList
    flex_ord_range: FlexOrdRange
    key_list: KeyList
    key_range: KeyRange
    ord_list: OrdList
    ord_range: OrdRange
    def __init__(
        self,
        bool_filter_list: _Optional[_Union[BoolFilterList, _Mapping]] = ...,
        flex_ord_list: _Optional[_Union[FlexOrdList, _Mapping]] = ...,
        flex_ord_range: _Optional[_Union[FlexOrdRange, _Mapping]] = ...,
        key_list: _Optional[_Union[KeyList, _Mapping]] = ...,
        key_range: _Optional[_Union[KeyRange, _Mapping]] = ...,
        ord_list: _Optional[_Union[OrdList, _Mapping]] = ...,
        ord_range: _Optional[_Union[OrdRange, _Mapping]] = ...,
    ) -> None: ...

class OrdRange(_message.Message):
    __slots__ = ("start", "step", "stop")
    START_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    STOP_FIELD_NUMBER: _ClassVar[int]
    start: Expr
    step: Expr
    stop: Expr
    def __init__(
        self,
        start: _Optional[_Union[Expr, _Mapping]] = ...,
        step: _Optional[_Union[Expr, _Mapping]] = ...,
        stop: _Optional[_Union[Expr, _Mapping]] = ...,
    ) -> None: ...

class FlexOrdRange(_message.Message):
    __slots__ = ("start", "step", "stop")
    START_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    STOP_FIELD_NUMBER: _ClassVar[int]
    start: Expr
    step: Expr
    stop: Expr
    def __init__(
        self,
        start: _Optional[_Union[Expr, _Mapping]] = ...,
        step: _Optional[_Union[Expr, _Mapping]] = ...,
        stop: _Optional[_Union[Expr, _Mapping]] = ...,
    ) -> None: ...

class KeyRange(_message.Message):
    __slots__ = ("start", "stop")
    START_FIELD_NUMBER: _ClassVar[int]
    STOP_FIELD_NUMBER: _ClassVar[int]
    start: Expr
    stop: Expr
    def __init__(
        self,
        start: _Optional[_Union[Expr, _Mapping]] = ...,
        stop: _Optional[_Union[Expr, _Mapping]] = ...,
    ) -> None: ...

class OrdList(_message.Message):
    __slots__ = ("ords",)
    ORDS_FIELD_NUMBER: _ClassVar[int]
    ords: Expr
    def __init__(self, ords: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...

class FlexOrdList(_message.Message):
    __slots__ = ("ords",)
    ORDS_FIELD_NUMBER: _ClassVar[int]
    ords: Expr
    def __init__(self, ords: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...

class KeyList(_message.Message):
    __slots__ = ("keys",)
    KEYS_FIELD_NUMBER: _ClassVar[int]
    keys: Expr
    def __init__(self, keys: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...

class BoolFilterList(_message.Message):
    __slots__ = ("filter",)
    FILTER_FIELD_NUMBER: _ClassVar[int]
    filter: Expr
    def __init__(self, filter: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...

class SpFlattenMode(_message.Message):
    __slots__ = (
        "sp_flatten_mode_array",
        "sp_flatten_mode_both",
        "sp_flatten_mode_object",
    )
    SP_FLATTEN_MODE_ARRAY_FIELD_NUMBER: _ClassVar[int]
    SP_FLATTEN_MODE_BOTH_FIELD_NUMBER: _ClassVar[int]
    SP_FLATTEN_MODE_OBJECT_FIELD_NUMBER: _ClassVar[int]
    sp_flatten_mode_array: bool
    sp_flatten_mode_both: bool
    sp_flatten_mode_object: bool
    def __init__(
        self,
        sp_flatten_mode_array: bool = ...,
        sp_flatten_mode_both: bool = ...,
        sp_flatten_mode_object: bool = ...,
    ) -> None: ...

class SpTableVariant(_message.Message):
    __slots__ = ("sp_session_table", "sp_table_init")
    SP_SESSION_TABLE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_INIT_FIELD_NUMBER: _ClassVar[int]
    sp_session_table: bool
    sp_table_init: bool
    def __init__(
        self, sp_session_table: bool = ..., sp_table_init: bool = ...
    ) -> None: ...

class SpSaveMode(_message.Message):
    __slots__ = (
        "sp_save_mode_append",
        "sp_save_mode_error_if_exists",
        "sp_save_mode_ignore",
        "sp_save_mode_overwrite",
        "sp_save_mode_truncate",
    )
    SP_SAVE_MODE_APPEND_FIELD_NUMBER: _ClassVar[int]
    SP_SAVE_MODE_ERROR_IF_EXISTS_FIELD_NUMBER: _ClassVar[int]
    SP_SAVE_MODE_IGNORE_FIELD_NUMBER: _ClassVar[int]
    SP_SAVE_MODE_OVERWRITE_FIELD_NUMBER: _ClassVar[int]
    SP_SAVE_MODE_TRUNCATE_FIELD_NUMBER: _ClassVar[int]
    sp_save_mode_append: bool
    sp_save_mode_error_if_exists: bool
    sp_save_mode_ignore: bool
    sp_save_mode_overwrite: bool
    sp_save_mode_truncate: bool
    def __init__(
        self,
        sp_save_mode_append: bool = ...,
        sp_save_mode_error_if_exists: bool = ...,
        sp_save_mode_ignore: bool = ...,
        sp_save_mode_overwrite: bool = ...,
        sp_save_mode_truncate: bool = ...,
    ) -> None: ...

class SpJoinType(_message.Message):
    __slots__ = (
        "sp_join_type__asof",
        "sp_join_type__cross",
        "sp_join_type__full_outer",
        "sp_join_type__inner",
        "sp_join_type__left_anti",
        "sp_join_type__left_outer",
        "sp_join_type__left_semi",
        "sp_join_type__right_outer",
    )
    SP_JOIN_TYPE__ASOF_FIELD_NUMBER: _ClassVar[int]
    SP_JOIN_TYPE__CROSS_FIELD_NUMBER: _ClassVar[int]
    SP_JOIN_TYPE__FULL_OUTER_FIELD_NUMBER: _ClassVar[int]
    SP_JOIN_TYPE__INNER_FIELD_NUMBER: _ClassVar[int]
    SP_JOIN_TYPE__LEFT_ANTI_FIELD_NUMBER: _ClassVar[int]
    SP_JOIN_TYPE__LEFT_OUTER_FIELD_NUMBER: _ClassVar[int]
    SP_JOIN_TYPE__LEFT_SEMI_FIELD_NUMBER: _ClassVar[int]
    SP_JOIN_TYPE__RIGHT_OUTER_FIELD_NUMBER: _ClassVar[int]
    sp_join_type__asof: bool
    sp_join_type__cross: bool
    sp_join_type__full_outer: bool
    sp_join_type__inner: bool
    sp_join_type__left_anti: bool
    sp_join_type__left_outer: bool
    sp_join_type__left_semi: bool
    sp_join_type__right_outer: bool
    def __init__(
        self,
        sp_join_type__asof: bool = ...,
        sp_join_type__cross: bool = ...,
        sp_join_type__full_outer: bool = ...,
        sp_join_type__inner: bool = ...,
        sp_join_type__left_anti: bool = ...,
        sp_join_type__left_outer: bool = ...,
        sp_join_type__left_semi: bool = ...,
        sp_join_type__right_outer: bool = ...,
    ) -> None: ...

class SpTimestampTimeZone(_message.Message):
    __slots__ = (
        "sp_timestamp_time_zone_default",
        "sp_timestamp_time_zone_ltz",
        "sp_timestamp_time_zone_ntz",
        "sp_timestamp_time_zone_tz",
    )
    SP_TIMESTAMP_TIME_ZONE_DEFAULT_FIELD_NUMBER: _ClassVar[int]
    SP_TIMESTAMP_TIME_ZONE_LTZ_FIELD_NUMBER: _ClassVar[int]
    SP_TIMESTAMP_TIME_ZONE_NTZ_FIELD_NUMBER: _ClassVar[int]
    SP_TIMESTAMP_TIME_ZONE_TZ_FIELD_NUMBER: _ClassVar[int]
    sp_timestamp_time_zone_default: bool
    sp_timestamp_time_zone_ltz: bool
    sp_timestamp_time_zone_ntz: bool
    sp_timestamp_time_zone_tz: bool
    def __init__(
        self,
        sp_timestamp_time_zone_default: bool = ...,
        sp_timestamp_time_zone_ltz: bool = ...,
        sp_timestamp_time_zone_ntz: bool = ...,
        sp_timestamp_time_zone_tz: bool = ...,
    ) -> None: ...

class SpDataType(_message.Message):
    __slots__ = (
        "sp_array_type",
        "sp_binary_type",
        "sp_boolean_type",
        "sp_byte_type",
        "sp_column_identifier",
        "sp_date_type",
        "sp_decimal_type",
        "sp_double_type",
        "sp_float_type",
        "sp_geography_type",
        "sp_geometry_type",
        "sp_integer_type",
        "sp_long_type",
        "sp_map_type",
        "sp_null_type",
        "sp_pandas_data_frame_type",
        "sp_pandas_series_type",
        "sp_short_type",
        "sp_string_type",
        "sp_struct_field",
        "sp_struct_type",
        "sp_time_type",
        "sp_timestamp_type",
        "sp_variant_type",
        "sp_vector_type",
    )
    SP_ARRAY_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_BINARY_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_BOOLEAN_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_BYTE_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_IDENTIFIER_FIELD_NUMBER: _ClassVar[int]
    SP_DATE_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_DECIMAL_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_DOUBLE_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_FLOAT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_GEOGRAPHY_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_GEOMETRY_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_INTEGER_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_LONG_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_MAP_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_NULL_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_PANDAS_DATA_FRAME_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_PANDAS_SERIES_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_SHORT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_STRING_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_STRUCT_FIELD_FIELD_NUMBER: _ClassVar[int]
    SP_STRUCT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_TIME_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_TIMESTAMP_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_VARIANT_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_VECTOR_TYPE_FIELD_NUMBER: _ClassVar[int]
    sp_array_type: SpArrayType
    sp_binary_type: bool
    sp_boolean_type: bool
    sp_byte_type: bool
    sp_column_identifier: SpColumnIdentifier
    sp_date_type: bool
    sp_decimal_type: SpDecimalType
    sp_double_type: bool
    sp_float_type: bool
    sp_geography_type: bool
    sp_geometry_type: bool
    sp_integer_type: bool
    sp_long_type: bool
    sp_map_type: SpMapType
    sp_null_type: bool
    sp_pandas_data_frame_type: SpPandasDataFrameType
    sp_pandas_series_type: SpPandasSeriesType
    sp_short_type: bool
    sp_string_type: SpStringType
    sp_struct_field: SpStructField
    sp_struct_type: SpStructType
    sp_time_type: bool
    sp_timestamp_type: SpTimestampType
    sp_variant_type: bool
    sp_vector_type: SpVectorType
    def __init__(
        self,
        sp_array_type: _Optional[_Union[SpArrayType, _Mapping]] = ...,
        sp_binary_type: bool = ...,
        sp_boolean_type: bool = ...,
        sp_byte_type: bool = ...,
        sp_column_identifier: _Optional[_Union[SpColumnIdentifier, _Mapping]] = ...,
        sp_date_type: bool = ...,
        sp_decimal_type: _Optional[_Union[SpDecimalType, _Mapping]] = ...,
        sp_double_type: bool = ...,
        sp_float_type: bool = ...,
        sp_geography_type: bool = ...,
        sp_geometry_type: bool = ...,
        sp_integer_type: bool = ...,
        sp_long_type: bool = ...,
        sp_map_type: _Optional[_Union[SpMapType, _Mapping]] = ...,
        sp_null_type: bool = ...,
        sp_pandas_data_frame_type: _Optional[
            _Union[SpPandasDataFrameType, _Mapping]
        ] = ...,
        sp_pandas_series_type: _Optional[_Union[SpPandasSeriesType, _Mapping]] = ...,
        sp_short_type: bool = ...,
        sp_string_type: _Optional[_Union[SpStringType, _Mapping]] = ...,
        sp_struct_field: _Optional[_Union[SpStructField, _Mapping]] = ...,
        sp_struct_type: _Optional[_Union[SpStructType, _Mapping]] = ...,
        sp_time_type: bool = ...,
        sp_timestamp_type: _Optional[_Union[SpTimestampType, _Mapping]] = ...,
        sp_variant_type: bool = ...,
        sp_vector_type: _Optional[_Union[SpVectorType, _Mapping]] = ...,
    ) -> None: ...

class SpArrayType(_message.Message):
    __slots__ = ("structured", "ty")
    STRUCTURED_FIELD_NUMBER: _ClassVar[int]
    TY_FIELD_NUMBER: _ClassVar[int]
    structured: bool
    ty: SpDataType
    def __init__(
        self, structured: bool = ..., ty: _Optional[_Union[SpDataType, _Mapping]] = ...
    ) -> None: ...

class SpColumnIdentifier(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class SpDecimalType(_message.Message):
    __slots__ = ("precision", "scale")
    PRECISION_FIELD_NUMBER: _ClassVar[int]
    SCALE_FIELD_NUMBER: _ClassVar[int]
    precision: int
    scale: int
    def __init__(
        self, precision: _Optional[int] = ..., scale: _Optional[int] = ...
    ) -> None: ...

class SpMapType(_message.Message):
    __slots__ = ("key_ty", "structured", "value_ty")
    KEY_TY_FIELD_NUMBER: _ClassVar[int]
    STRUCTURED_FIELD_NUMBER: _ClassVar[int]
    VALUE_TY_FIELD_NUMBER: _ClassVar[int]
    key_ty: SpDataType
    structured: bool
    value_ty: SpDataType
    def __init__(
        self,
        key_ty: _Optional[_Union[SpDataType, _Mapping]] = ...,
        structured: bool = ...,
        value_ty: _Optional[_Union[SpDataType, _Mapping]] = ...,
    ) -> None: ...

class SpStringType(_message.Message):
    __slots__ = ("length",)
    LENGTH_FIELD_NUMBER: _ClassVar[int]
    length: _wrappers_pb2.Int64Value
    def __init__(
        self, length: _Optional[_Union[_wrappers_pb2.Int64Value, _Mapping]] = ...
    ) -> None: ...

class SpStructField(_message.Message):
    __slots__ = ("column_identifier", "data_type", "nullable")
    COLUMN_IDENTIFIER_FIELD_NUMBER: _ClassVar[int]
    DATA_TYPE_FIELD_NUMBER: _ClassVar[int]
    NULLABLE_FIELD_NUMBER: _ClassVar[int]
    column_identifier: SpColumnIdentifier
    data_type: SpDataType
    nullable: bool
    def __init__(
        self,
        column_identifier: _Optional[_Union[SpColumnIdentifier, _Mapping]] = ...,
        data_type: _Optional[_Union[SpDataType, _Mapping]] = ...,
        nullable: bool = ...,
    ) -> None: ...

class SpStructType(_message.Message):
    __slots__ = ("fields", "structured")
    FIELDS_FIELD_NUMBER: _ClassVar[int]
    STRUCTURED_FIELD_NUMBER: _ClassVar[int]
    fields: _containers.RepeatedCompositeFieldContainer[SpStructField]
    structured: bool
    def __init__(
        self,
        fields: _Optional[_Iterable[_Union[SpStructField, _Mapping]]] = ...,
        structured: bool = ...,
    ) -> None: ...

class SpTimestampType(_message.Message):
    __slots__ = ("time_zone",)
    TIME_ZONE_FIELD_NUMBER: _ClassVar[int]
    time_zone: SpTimestampTimeZone
    def __init__(
        self, time_zone: _Optional[_Union[SpTimestampTimeZone, _Mapping]] = ...
    ) -> None: ...

class SpVectorType(_message.Message):
    __slots__ = ("dimension", "ty")
    DIMENSION_FIELD_NUMBER: _ClassVar[int]
    TY_FIELD_NUMBER: _ClassVar[int]
    dimension: int
    ty: SpDataType
    def __init__(
        self,
        dimension: _Optional[int] = ...,
        ty: _Optional[_Union[SpDataType, _Mapping]] = ...,
    ) -> None: ...

class SpPandasSeriesType(_message.Message):
    __slots__ = ("el_ty",)
    EL_TY_FIELD_NUMBER: _ClassVar[int]
    el_ty: SpDataType
    def __init__(
        self, el_ty: _Optional[_Union[SpDataType, _Mapping]] = ...
    ) -> None: ...

class SpPandasDataFrameType(_message.Message):
    __slots__ = ("col_names", "col_types")
    COL_NAMES_FIELD_NUMBER: _ClassVar[int]
    COL_TYPES_FIELD_NUMBER: _ClassVar[int]
    col_names: _containers.RepeatedScalarFieldContainer[str]
    col_types: _containers.RepeatedCompositeFieldContainer[SpDataType]
    def __init__(
        self,
        col_names: _Optional[_Iterable[str]] = ...,
        col_types: _Optional[_Iterable[_Union[SpDataType, _Mapping]]] = ...,
    ) -> None: ...

class SpVariant(_message.Message):
    __slots__ = (
        "sp_variant__big_decimal",
        "sp_variant__big_int",
        "sp_variant__bool",
        "sp_variant__bytes",
        "sp_variant__date",
        "sp_variant__double",
        "sp_variant__float",
        "sp_variant__int",
        "sp_variant__list",
        "sp_variant__object",
        "sp_variant__string",
        "sp_variant__time",
        "sp_variant__timestamp",
    )
    SP_VARIANT__BIG_DECIMAL_FIELD_NUMBER: _ClassVar[int]
    SP_VARIANT__BIG_INT_FIELD_NUMBER: _ClassVar[int]
    SP_VARIANT__BOOL_FIELD_NUMBER: _ClassVar[int]
    SP_VARIANT__BYTES_FIELD_NUMBER: _ClassVar[int]
    SP_VARIANT__DATE_FIELD_NUMBER: _ClassVar[int]
    SP_VARIANT__DOUBLE_FIELD_NUMBER: _ClassVar[int]
    SP_VARIANT__FLOAT_FIELD_NUMBER: _ClassVar[int]
    SP_VARIANT__INT_FIELD_NUMBER: _ClassVar[int]
    SP_VARIANT__LIST_FIELD_NUMBER: _ClassVar[int]
    SP_VARIANT__OBJECT_FIELD_NUMBER: _ClassVar[int]
    SP_VARIANT__STRING_FIELD_NUMBER: _ClassVar[int]
    SP_VARIANT__TIME_FIELD_NUMBER: _ClassVar[int]
    SP_VARIANT__TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    sp_variant__big_decimal: SpVariant_BigDecimal
    sp_variant__big_int: SpVariant_BigInt
    sp_variant__bool: SpVariant_Bool
    sp_variant__bytes: SpVariant_Bytes
    sp_variant__date: SpVariant_Date
    sp_variant__double: SpVariant_Double
    sp_variant__float: SpVariant_Float
    sp_variant__int: SpVariant_Int
    sp_variant__list: SpVariant_List
    sp_variant__object: SpVariant_Object
    sp_variant__string: SpVariant_String
    sp_variant__time: SpVariant_Time
    sp_variant__timestamp: SpVariant_Timestamp
    def __init__(
        self,
        sp_variant__big_decimal: _Optional[
            _Union[SpVariant_BigDecimal, _Mapping]
        ] = ...,
        sp_variant__big_int: _Optional[_Union[SpVariant_BigInt, _Mapping]] = ...,
        sp_variant__bool: _Optional[_Union[SpVariant_Bool, _Mapping]] = ...,
        sp_variant__bytes: _Optional[_Union[SpVariant_Bytes, _Mapping]] = ...,
        sp_variant__date: _Optional[_Union[SpVariant_Date, _Mapping]] = ...,
        sp_variant__double: _Optional[_Union[SpVariant_Double, _Mapping]] = ...,
        sp_variant__float: _Optional[_Union[SpVariant_Float, _Mapping]] = ...,
        sp_variant__int: _Optional[_Union[SpVariant_Int, _Mapping]] = ...,
        sp_variant__list: _Optional[_Union[SpVariant_List, _Mapping]] = ...,
        sp_variant__object: _Optional[_Union[SpVariant_Object, _Mapping]] = ...,
        sp_variant__string: _Optional[_Union[SpVariant_String, _Mapping]] = ...,
        sp_variant__time: _Optional[_Union[SpVariant_Time, _Mapping]] = ...,
        sp_variant__timestamp: _Optional[_Union[SpVariant_Timestamp, _Mapping]] = ...,
    ) -> None: ...

class SpVariant_Object(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self, v: _Optional[_Iterable[_Union[Tuple_String_String, _Mapping]]] = ...
    ) -> None: ...

class SpVariant_List(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: _containers.RepeatedCompositeFieldContainer[Map_String_String]
    def __init__(
        self, v: _Optional[_Iterable[_Union[Map_String_String, _Mapping]]] = ...
    ) -> None: ...

class SpVariant_Timestamp(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: int
    def __init__(self, v: _Optional[int] = ...) -> None: ...

class SpVariant_Date(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: int
    def __init__(self, v: _Optional[int] = ...) -> None: ...

class SpVariant_Time(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: int
    def __init__(self, v: _Optional[int] = ...) -> None: ...

class SpVariant_Bytes(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: bytes
    def __init__(self, v: _Optional[bytes] = ...) -> None: ...

class SpVariant_String(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: str
    def __init__(self, v: _Optional[str] = ...) -> None: ...

class SpVariant_Bool(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: bool
    def __init__(self, v: bool = ...) -> None: ...

class SpVariant_BigInt(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: bytes
    def __init__(self, v: _Optional[bytes] = ...) -> None: ...

class SpVariant_BigDecimal(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: bytes
    def __init__(self, v: _Optional[bytes] = ...) -> None: ...

class SpVariant_Int(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: int
    def __init__(self, v: _Optional[int] = ...) -> None: ...

class SpVariant_Float(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: int
    def __init__(self, v: _Optional[int] = ...) -> None: ...

class SpVariant_Double(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: int
    def __init__(self, v: _Optional[int] = ...) -> None: ...

class SpTableName(_message.Message):
    __slots__ = ("sp_table_name_flat", "sp_table_name_structured")
    SP_TABLE_NAME_FLAT_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_NAME_STRUCTURED_FIELD_NUMBER: _ClassVar[int]
    sp_table_name_flat: SpTableNameFlat
    sp_table_name_structured: SpTableNameStructured
    def __init__(
        self,
        sp_table_name_flat: _Optional[_Union[SpTableNameFlat, _Mapping]] = ...,
        sp_table_name_structured: _Optional[
            _Union[SpTableNameStructured, _Mapping]
        ] = ...,
    ) -> None: ...

class SpTableNameFlat(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: str
    def __init__(self, name: _Optional[str] = ...) -> None: ...

class SpTableNameStructured(_message.Message):
    __slots__ = ("name",)
    NAME_FIELD_NUMBER: _ClassVar[int]
    name: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, name: _Optional[_Iterable[str]] = ...) -> None: ...

class StagedPandasDataframe(_message.Message):
    __slots__ = ("temp_table",)
    TEMP_TABLE_FIELD_NUMBER: _ClassVar[int]
    temp_table: SpTableName
    def __init__(
        self, temp_table: _Optional[_Union[SpTableName, _Mapping]] = ...
    ) -> None: ...

class SpDataframeData(_message.Message):
    __slots__ = (
        "sp_dataframe_data__list",
        "sp_dataframe_data__pandas",
        "sp_dataframe_data__tuple",
    )
    SP_DATAFRAME_DATA__LIST_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_DATA__PANDAS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_DATA__TUPLE_FIELD_NUMBER: _ClassVar[int]
    sp_dataframe_data__list: SpDataframeData_List
    sp_dataframe_data__pandas: SpDataframeData_Pandas
    sp_dataframe_data__tuple: SpDataframeData_Tuple
    def __init__(
        self,
        sp_dataframe_data__list: _Optional[
            _Union[SpDataframeData_List, _Mapping]
        ] = ...,
        sp_dataframe_data__pandas: _Optional[
            _Union[SpDataframeData_Pandas, _Mapping]
        ] = ...,
        sp_dataframe_data__tuple: _Optional[
            _Union[SpDataframeData_Tuple, _Mapping]
        ] = ...,
    ) -> None: ...

class SpDataframeData_List(_message.Message):
    __slots__ = ("vs",)
    VS_FIELD_NUMBER: _ClassVar[int]
    vs: _containers.RepeatedCompositeFieldContainer[Expr]
    def __init__(
        self, vs: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...
    ) -> None: ...

class SpDataframeData_Tuple(_message.Message):
    __slots__ = ("vs",)
    VS_FIELD_NUMBER: _ClassVar[int]
    vs: _containers.RepeatedCompositeFieldContainer[Expr]
    def __init__(
        self, vs: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...
    ) -> None: ...

class SpDataframeData_Pandas(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: StagedPandasDataframe
    def __init__(
        self, v: _Optional[_Union[StagedPandasDataframe, _Mapping]] = ...
    ) -> None: ...

class SpDataframeSchema(_message.Message):
    __slots__ = ("sp_dataframe_schema__list", "sp_dataframe_schema__struct")
    SP_DATAFRAME_SCHEMA__LIST_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SCHEMA__STRUCT_FIELD_NUMBER: _ClassVar[int]
    sp_dataframe_schema__list: SpDataframeSchema_List
    sp_dataframe_schema__struct: SpDataframeSchema_Struct
    def __init__(
        self,
        sp_dataframe_schema__list: _Optional[
            _Union[SpDataframeSchema_List, _Mapping]
        ] = ...,
        sp_dataframe_schema__struct: _Optional[
            _Union[SpDataframeSchema_Struct, _Mapping]
        ] = ...,
    ) -> None: ...

class SpDataframeSchema_List(_message.Message):
    __slots__ = ("vs",)
    VS_FIELD_NUMBER: _ClassVar[int]
    vs: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, vs: _Optional[_Iterable[str]] = ...) -> None: ...

class SpDataframeSchema_Struct(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: SpStructType
    def __init__(self, v: _Optional[_Union[SpStructType, _Mapping]] = ...) -> None: ...

class SpCallable(_message.Message):
    __slots__ = ("id", "name")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    def __init__(
        self, id: _Optional[int] = ..., name: _Optional[str] = ...
    ) -> None: ...

class SpPivotValue(_message.Message):
    __slots__ = ("sp_pivot_value__dataframe", "sp_pivot_value__expr")
    SP_PIVOT_VALUE__DATAFRAME_FIELD_NUMBER: _ClassVar[int]
    SP_PIVOT_VALUE__EXPR_FIELD_NUMBER: _ClassVar[int]
    sp_pivot_value__dataframe: SpPivotValue_Dataframe
    sp_pivot_value__expr: SpPivotValue_Expr
    def __init__(
        self,
        sp_pivot_value__dataframe: _Optional[
            _Union[SpPivotValue_Dataframe, _Mapping]
        ] = ...,
        sp_pivot_value__expr: _Optional[_Union[SpPivotValue_Expr, _Mapping]] = ...,
    ) -> None: ...

class SpPivotValue_Expr(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: Expr
    def __init__(self, v: _Optional[_Union[Expr, _Mapping]] = ...) -> None: ...

class SpPivotValue_Dataframe(_message.Message):
    __slots__ = ("v",)
    V_FIELD_NUMBER: _ClassVar[int]
    v: SpDataframeRef
    def __init__(
        self, v: _Optional[_Union[SpDataframeRef, _Mapping]] = ...
    ) -> None: ...

class SrcPosition(_message.Message):
    __slots__ = ("end_column", "end_line", "file", "start_column", "start_line")
    END_COLUMN_FIELD_NUMBER: _ClassVar[int]
    END_LINE_FIELD_NUMBER: _ClassVar[int]
    FILE_FIELD_NUMBER: _ClassVar[int]
    START_COLUMN_FIELD_NUMBER: _ClassVar[int]
    START_LINE_FIELD_NUMBER: _ClassVar[int]
    end_column: int
    end_line: int
    file: str
    start_column: int
    start_line: int
    def __init__(
        self,
        end_column: _Optional[int] = ...,
        end_line: _Optional[int] = ...,
        file: _Optional[str] = ...,
        start_column: _Optional[int] = ...,
        start_line: _Optional[int] = ...,
    ) -> None: ...

class VarId(_message.Message):
    __slots__ = ("bitfield1",)
    BITFIELD1_FIELD_NUMBER: _ClassVar[int]
    bitfield1: int
    def __init__(self, bitfield1: _Optional[int] = ...) -> None: ...

class Request(_message.Message):
    __slots__ = ("body", "client_ast_version", "client_language", "client_version")
    BODY_FIELD_NUMBER: _ClassVar[int]
    CLIENT_AST_VERSION_FIELD_NUMBER: _ClassVar[int]
    CLIENT_LANGUAGE_FIELD_NUMBER: _ClassVar[int]
    CLIENT_VERSION_FIELD_NUMBER: _ClassVar[int]
    body: _containers.RepeatedCompositeFieldContainer[Stmt]
    client_ast_version: int
    client_language: Language
    client_version: Version
    def __init__(
        self,
        body: _Optional[_Iterable[_Union[Stmt, _Mapping]]] = ...,
        client_ast_version: _Optional[int] = ...,
        client_language: _Optional[_Union[Language, _Mapping]] = ...,
        client_version: _Optional[_Union[Version, _Mapping]] = ...,
    ) -> None: ...

class Response(_message.Message):
    __slots__ = ("body",)
    BODY_FIELD_NUMBER: _ClassVar[int]
    body: _containers.RepeatedCompositeFieldContainer[Result]
    def __init__(
        self, body: _Optional[_Iterable[_Union[Result, _Mapping]]] = ...
    ) -> None: ...

class Const(_message.Message):
    __slots__ = (
        "big_decimal_val",
        "big_int_val",
        "binary_val",
        "bool_val",
        "date_val",
        "float64_val",
        "fn_val",
        "int32_val",
        "int64_val",
        "none_val",
        "null_val",
        "python_date_val",
        "python_time_val",
        "python_timestamp_val",
        "sp_datatype_val",
        "string_val",
        "time_val",
        "timestamp_val",
    )
    BIG_DECIMAL_VAL_FIELD_NUMBER: _ClassVar[int]
    BIG_INT_VAL_FIELD_NUMBER: _ClassVar[int]
    BINARY_VAL_FIELD_NUMBER: _ClassVar[int]
    BOOL_VAL_FIELD_NUMBER: _ClassVar[int]
    DATE_VAL_FIELD_NUMBER: _ClassVar[int]
    FLOAT64_VAL_FIELD_NUMBER: _ClassVar[int]
    FN_VAL_FIELD_NUMBER: _ClassVar[int]
    INT32_VAL_FIELD_NUMBER: _ClassVar[int]
    INT64_VAL_FIELD_NUMBER: _ClassVar[int]
    NONE_VAL_FIELD_NUMBER: _ClassVar[int]
    NULL_VAL_FIELD_NUMBER: _ClassVar[int]
    PYTHON_DATE_VAL_FIELD_NUMBER: _ClassVar[int]
    PYTHON_TIME_VAL_FIELD_NUMBER: _ClassVar[int]
    PYTHON_TIMESTAMP_VAL_FIELD_NUMBER: _ClassVar[int]
    SP_DATATYPE_VAL_FIELD_NUMBER: _ClassVar[int]
    STRING_VAL_FIELD_NUMBER: _ClassVar[int]
    TIME_VAL_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_VAL_FIELD_NUMBER: _ClassVar[int]
    big_decimal_val: BigDecimalVal
    big_int_val: BigIntVal
    binary_val: BinaryVal
    bool_val: BoolVal
    date_val: DateVal
    float64_val: Float64Val
    fn_val: FnVal
    int32_val: Int32Val
    int64_val: Int64Val
    none_val: NoneVal
    null_val: NullVal
    python_date_val: PythonDateVal
    python_time_val: PythonTimeVal
    python_timestamp_val: PythonTimestampVal
    sp_datatype_val: SpDatatypeVal
    string_val: StringVal
    time_val: TimeVal
    timestamp_val: TimestampVal
    def __init__(
        self,
        big_decimal_val: _Optional[_Union[BigDecimalVal, _Mapping]] = ...,
        big_int_val: _Optional[_Union[BigIntVal, _Mapping]] = ...,
        binary_val: _Optional[_Union[BinaryVal, _Mapping]] = ...,
        bool_val: _Optional[_Union[BoolVal, _Mapping]] = ...,
        date_val: _Optional[_Union[DateVal, _Mapping]] = ...,
        float64_val: _Optional[_Union[Float64Val, _Mapping]] = ...,
        fn_val: _Optional[_Union[FnVal, _Mapping]] = ...,
        int32_val: _Optional[_Union[Int32Val, _Mapping]] = ...,
        int64_val: _Optional[_Union[Int64Val, _Mapping]] = ...,
        none_val: _Optional[_Union[NoneVal, _Mapping]] = ...,
        null_val: _Optional[_Union[NullVal, _Mapping]] = ...,
        python_date_val: _Optional[_Union[PythonDateVal, _Mapping]] = ...,
        python_time_val: _Optional[_Union[PythonTimeVal, _Mapping]] = ...,
        python_timestamp_val: _Optional[_Union[PythonTimestampVal, _Mapping]] = ...,
        sp_datatype_val: _Optional[_Union[SpDatatypeVal, _Mapping]] = ...,
        string_val: _Optional[_Union[StringVal, _Mapping]] = ...,
        time_val: _Optional[_Union[TimeVal, _Mapping]] = ...,
        timestamp_val: _Optional[_Union[TimestampVal, _Mapping]] = ...,
    ) -> None: ...

class NoneVal(_message.Message):
    __slots__ = ("src",)
    SRC_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    def __init__(self, src: _Optional[_Union[SrcPosition, _Mapping]] = ...) -> None: ...

class NullVal(_message.Message):
    __slots__ = ("src",)
    SRC_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    def __init__(self, src: _Optional[_Union[SrcPosition, _Mapping]] = ...) -> None: ...

class BoolVal(_message.Message):
    __slots__ = ("src", "v")
    SRC_FIELD_NUMBER: _ClassVar[int]
    V_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    v: bool
    def __init__(
        self, src: _Optional[_Union[SrcPosition, _Mapping]] = ..., v: bool = ...
    ) -> None: ...

class Int32Val(_message.Message):
    __slots__ = ("src", "v")
    SRC_FIELD_NUMBER: _ClassVar[int]
    V_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    v: int
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        v: _Optional[int] = ...,
    ) -> None: ...

class Int64Val(_message.Message):
    __slots__ = ("src", "v")
    SRC_FIELD_NUMBER: _ClassVar[int]
    V_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    v: int
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        v: _Optional[int] = ...,
    ) -> None: ...

class Float64Val(_message.Message):
    __slots__ = ("src", "v")
    SRC_FIELD_NUMBER: _ClassVar[int]
    V_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    v: float
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        v: _Optional[float] = ...,
    ) -> None: ...

class BigIntVal(_message.Message):
    __slots__ = ("src", "v")
    SRC_FIELD_NUMBER: _ClassVar[int]
    V_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    v: bytes
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        v: _Optional[bytes] = ...,
    ) -> None: ...

class BigDecimalVal(_message.Message):
    __slots__ = ("scale", "src", "unscaled_value")
    SCALE_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    UNSCALED_VALUE_FIELD_NUMBER: _ClassVar[int]
    scale: int
    src: SrcPosition
    unscaled_value: bytes
    def __init__(
        self,
        scale: _Optional[int] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        unscaled_value: _Optional[bytes] = ...,
    ) -> None: ...

class StringVal(_message.Message):
    __slots__ = ("src", "v")
    SRC_FIELD_NUMBER: _ClassVar[int]
    V_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    v: str
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        v: _Optional[str] = ...,
    ) -> None: ...

class BinaryVal(_message.Message):
    __slots__ = ("src", "v")
    SRC_FIELD_NUMBER: _ClassVar[int]
    V_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    v: bytes
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        v: _Optional[bytes] = ...,
    ) -> None: ...

class TimestampVal(_message.Message):
    __slots__ = ("src", "v")
    SRC_FIELD_NUMBER: _ClassVar[int]
    V_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    v: int
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        v: _Optional[int] = ...,
    ) -> None: ...

class DateVal(_message.Message):
    __slots__ = ("src", "v")
    SRC_FIELD_NUMBER: _ClassVar[int]
    V_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    v: int
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        v: _Optional[int] = ...,
    ) -> None: ...

class TimeVal(_message.Message):
    __slots__ = ("src", "v")
    SRC_FIELD_NUMBER: _ClassVar[int]
    V_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    v: int
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        v: _Optional[int] = ...,
    ) -> None: ...

class PythonTimestampVal(_message.Message):
    __slots__ = (
        "day",
        "hour",
        "microsecond",
        "minute",
        "month",
        "second",
        "src",
        "tz",
        "year",
    )
    DAY_FIELD_NUMBER: _ClassVar[int]
    HOUR_FIELD_NUMBER: _ClassVar[int]
    MICROSECOND_FIELD_NUMBER: _ClassVar[int]
    MINUTE_FIELD_NUMBER: _ClassVar[int]
    MONTH_FIELD_NUMBER: _ClassVar[int]
    SECOND_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    TZ_FIELD_NUMBER: _ClassVar[int]
    YEAR_FIELD_NUMBER: _ClassVar[int]
    day: int
    hour: int
    microsecond: int
    minute: int
    month: int
    second: int
    src: SrcPosition
    tz: PythonTimeZone
    year: int
    def __init__(
        self,
        day: _Optional[int] = ...,
        hour: _Optional[int] = ...,
        microsecond: _Optional[int] = ...,
        minute: _Optional[int] = ...,
        month: _Optional[int] = ...,
        second: _Optional[int] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        tz: _Optional[_Union[PythonTimeZone, _Mapping]] = ...,
        year: _Optional[int] = ...,
    ) -> None: ...

class PythonDateVal(_message.Message):
    __slots__ = ("day", "month", "src", "year")
    DAY_FIELD_NUMBER: _ClassVar[int]
    MONTH_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    YEAR_FIELD_NUMBER: _ClassVar[int]
    day: int
    month: int
    src: SrcPosition
    year: int
    def __init__(
        self,
        day: _Optional[int] = ...,
        month: _Optional[int] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        year: _Optional[int] = ...,
    ) -> None: ...

class PythonTimeVal(_message.Message):
    __slots__ = ("hour", "microsecond", "minute", "second", "src", "tz")
    HOUR_FIELD_NUMBER: _ClassVar[int]
    MICROSECOND_FIELD_NUMBER: _ClassVar[int]
    MINUTE_FIELD_NUMBER: _ClassVar[int]
    SECOND_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    TZ_FIELD_NUMBER: _ClassVar[int]
    hour: int
    microsecond: int
    minute: int
    second: int
    src: SrcPosition
    tz: PythonTimeZone
    def __init__(
        self,
        hour: _Optional[int] = ...,
        microsecond: _Optional[int] = ...,
        minute: _Optional[int] = ...,
        second: _Optional[int] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        tz: _Optional[_Union[PythonTimeZone, _Mapping]] = ...,
    ) -> None: ...

class FnVal(_message.Message):
    __slots__ = ("body", "params", "src")
    BODY_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    body: Expr
    params: _containers.RepeatedScalarFieldContainer[str]
    src: SrcPosition
    def __init__(
        self,
        body: _Optional[_Union[Expr, _Mapping]] = ...,
        params: _Optional[_Iterable[str]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDatatypeVal(_message.Message):
    __slots__ = ("datatype", "src")
    DATATYPE_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    datatype: SpDataType
    src: SrcPosition
    def __init__(
        self,
        datatype: _Optional[_Union[SpDataType, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class IfExpr(_message.Message):
    __slots__ = ("cond", "if_false", "if_true", "src")
    COND_FIELD_NUMBER: _ClassVar[int]
    IF_FALSE_FIELD_NUMBER: _ClassVar[int]
    IF_TRUE_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    cond: Expr
    if_false: Expr
    if_true: Expr
    src: SrcPosition
    def __init__(
        self,
        cond: _Optional[_Union[Expr, _Mapping]] = ...,
        if_false: _Optional[_Union[Expr, _Mapping]] = ...,
        if_true: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SomeVal(_message.Message):
    __slots__ = ("src", "v")
    SRC_FIELD_NUMBER: _ClassVar[int]
    V_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    v: Expr
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        v: _Optional[_Union[Expr, _Mapping]] = ...,
    ) -> None: ...

class TupleVal(_message.Message):
    __slots__ = ("src", "vs")
    SRC_FIELD_NUMBER: _ClassVar[int]
    VS_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    vs: _containers.RepeatedCompositeFieldContainer[Expr]
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        vs: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
    ) -> None: ...

class ListVal(_message.Message):
    __slots__ = ("src", "vs")
    SRC_FIELD_NUMBER: _ClassVar[int]
    VS_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    vs: _containers.RepeatedCompositeFieldContainer[Expr]
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        vs: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
    ) -> None: ...

class SeqMapVal(_message.Message):
    __slots__ = ("kvs", "src")
    KVS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    kvs: _containers.RepeatedCompositeFieldContainer[TupleVal]
    src: SrcPosition
    def __init__(
        self,
        kvs: _Optional[_Iterable[_Union[TupleVal, _Mapping]]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class ApplyExpr(_message.Message):
    __slots__ = ("fn", "named_args", "pos_args", "src")
    FN_FIELD_NUMBER: _ClassVar[int]
    NAMED_ARGS_FIELD_NUMBER: _ClassVar[int]
    POS_ARGS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    fn: FnRefExpr
    named_args: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    pos_args: _containers.RepeatedCompositeFieldContainer[Expr]
    src: SrcPosition
    def __init__(
        self,
        fn: _Optional[_Union[FnRefExpr, _Mapping]] = ...,
        named_args: _Optional[_Iterable[_Union[Tuple_String_Expr, _Mapping]]] = ...,
        pos_args: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class FnRefExpr(_message.Message):
    __slots__ = (
        "trait_fn_id_ref_expr",
        "trait_fn_name_ref_expr",
        "builtin_fn",
        "call_table_function_expr",
        "indirect_table_fn_id_ref",
        "indirect_table_fn_name_ref",
        "sp_fn_ref",
        "stored_procedure",
        "udaf",
        "udf",
        "udtf",
    )
    TRAIT_FN_ID_REF_EXPR_FIELD_NUMBER: _ClassVar[int]
    TRAIT_FN_NAME_REF_EXPR_FIELD_NUMBER: _ClassVar[int]
    BUILTIN_FN_FIELD_NUMBER: _ClassVar[int]
    CALL_TABLE_FUNCTION_EXPR_FIELD_NUMBER: _ClassVar[int]
    INDIRECT_TABLE_FN_ID_REF_FIELD_NUMBER: _ClassVar[int]
    INDIRECT_TABLE_FN_NAME_REF_FIELD_NUMBER: _ClassVar[int]
    SP_FN_REF_FIELD_NUMBER: _ClassVar[int]
    STORED_PROCEDURE_FIELD_NUMBER: _ClassVar[int]
    UDAF_FIELD_NUMBER: _ClassVar[int]
    UDF_FIELD_NUMBER: _ClassVar[int]
    UDTF_FIELD_NUMBER: _ClassVar[int]
    trait_fn_id_ref_expr: FnIdRefExpr
    trait_fn_name_ref_expr: FnNameRefExpr
    builtin_fn: BuiltinFn
    call_table_function_expr: CallTableFunctionExpr
    indirect_table_fn_id_ref: IndirectTableFnIdRef
    indirect_table_fn_name_ref: IndirectTableFnNameRef
    sp_fn_ref: SpFnRef
    stored_procedure: StoredProcedure
    udaf: Udaf
    udf: Udf
    udtf: Udtf
    def __init__(
        self,
        trait_fn_id_ref_expr: _Optional[_Union[FnIdRefExpr, _Mapping]] = ...,
        trait_fn_name_ref_expr: _Optional[_Union[FnNameRefExpr, _Mapping]] = ...,
        builtin_fn: _Optional[_Union[BuiltinFn, _Mapping]] = ...,
        call_table_function_expr: _Optional[
            _Union[CallTableFunctionExpr, _Mapping]
        ] = ...,
        indirect_table_fn_id_ref: _Optional[
            _Union[IndirectTableFnIdRef, _Mapping]
        ] = ...,
        indirect_table_fn_name_ref: _Optional[
            _Union[IndirectTableFnNameRef, _Mapping]
        ] = ...,
        sp_fn_ref: _Optional[_Union[SpFnRef, _Mapping]] = ...,
        stored_procedure: _Optional[_Union[StoredProcedure, _Mapping]] = ...,
        udaf: _Optional[_Union[Udaf, _Mapping]] = ...,
        udf: _Optional[_Union[Udf, _Mapping]] = ...,
        udtf: _Optional[_Union[Udtf, _Mapping]] = ...,
    ) -> None: ...

class FnNameRefExpr(_message.Message):
    __slots__ = (
        "builtin_fn",
        "call_table_function_expr",
        "indirect_table_fn_name_ref",
        "stored_procedure",
        "udaf",
        "udf",
        "udtf",
    )
    BUILTIN_FN_FIELD_NUMBER: _ClassVar[int]
    CALL_TABLE_FUNCTION_EXPR_FIELD_NUMBER: _ClassVar[int]
    INDIRECT_TABLE_FN_NAME_REF_FIELD_NUMBER: _ClassVar[int]
    STORED_PROCEDURE_FIELD_NUMBER: _ClassVar[int]
    UDAF_FIELD_NUMBER: _ClassVar[int]
    UDF_FIELD_NUMBER: _ClassVar[int]
    UDTF_FIELD_NUMBER: _ClassVar[int]
    builtin_fn: BuiltinFn
    call_table_function_expr: CallTableFunctionExpr
    indirect_table_fn_name_ref: IndirectTableFnNameRef
    stored_procedure: StoredProcedure
    udaf: Udaf
    udf: Udf
    udtf: Udtf
    def __init__(
        self,
        builtin_fn: _Optional[_Union[BuiltinFn, _Mapping]] = ...,
        call_table_function_expr: _Optional[
            _Union[CallTableFunctionExpr, _Mapping]
        ] = ...,
        indirect_table_fn_name_ref: _Optional[
            _Union[IndirectTableFnNameRef, _Mapping]
        ] = ...,
        stored_procedure: _Optional[_Union[StoredProcedure, _Mapping]] = ...,
        udaf: _Optional[_Union[Udaf, _Mapping]] = ...,
        udf: _Optional[_Union[Udf, _Mapping]] = ...,
        udtf: _Optional[_Union[Udtf, _Mapping]] = ...,
    ) -> None: ...

class FnIdRefExpr(_message.Message):
    __slots__ = ("indirect_table_fn_id_ref", "sp_fn_ref")
    INDIRECT_TABLE_FN_ID_REF_FIELD_NUMBER: _ClassVar[int]
    SP_FN_REF_FIELD_NUMBER: _ClassVar[int]
    indirect_table_fn_id_ref: IndirectTableFnIdRef
    sp_fn_ref: SpFnRef
    def __init__(
        self,
        indirect_table_fn_id_ref: _Optional[
            _Union[IndirectTableFnIdRef, _Mapping]
        ] = ...,
        sp_fn_ref: _Optional[_Union[SpFnRef, _Mapping]] = ...,
    ) -> None: ...

class SpFnRef(_message.Message):
    __slots__ = ("id", "src")
    ID_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    id: VarId
    src: SrcPosition
    def __init__(
        self,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class BuiltinFn(_message.Message):
    __slots__ = ("name", "src")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    name: FnName
    src: SrcPosition
    def __init__(
        self,
        name: _Optional[_Union[FnName, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class StoredProcedure(_message.Message):
    __slots__ = (
        "comment",
        "execute_as",
        "external_access_integrations",
        "func",
        "if_not_exists",
        "imports",
        "input_types",
        "is_permanent",
        "kwargs",
        "log_on_exception",
        "name",
        "packages",
        "parallel",
        "replace",
        "return_type",
        "secrets",
        "source_code_display",
        "src",
        "stage_location",
        "statement_params",
        "strict",
    )
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    EXECUTE_AS_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_ACCESS_INTEGRATIONS_FIELD_NUMBER: _ClassVar[int]
    FUNC_FIELD_NUMBER: _ClassVar[int]
    IF_NOT_EXISTS_FIELD_NUMBER: _ClassVar[int]
    IMPORTS_FIELD_NUMBER: _ClassVar[int]
    INPUT_TYPES_FIELD_NUMBER: _ClassVar[int]
    IS_PERMANENT_FIELD_NUMBER: _ClassVar[int]
    KWARGS_FIELD_NUMBER: _ClassVar[int]
    LOG_ON_EXCEPTION_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PACKAGES_FIELD_NUMBER: _ClassVar[int]
    PARALLEL_FIELD_NUMBER: _ClassVar[int]
    REPLACE_FIELD_NUMBER: _ClassVar[int]
    RETURN_TYPE_FIELD_NUMBER: _ClassVar[int]
    SECRETS_FIELD_NUMBER: _ClassVar[int]
    SOURCE_CODE_DISPLAY_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STAGE_LOCATION_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    STRICT_FIELD_NUMBER: _ClassVar[int]
    comment: _wrappers_pb2.StringValue
    execute_as: str
    external_access_integrations: _containers.RepeatedScalarFieldContainer[str]
    func: SpCallable
    if_not_exists: bool
    imports: _containers.RepeatedCompositeFieldContainer[SpTableName]
    input_types: List_SpDataType
    is_permanent: bool
    kwargs: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    log_on_exception: _wrappers_pb2.BoolValue
    name: FnName
    packages: _containers.RepeatedScalarFieldContainer[str]
    parallel: int
    replace: bool
    return_type: SpDataType
    secrets: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    source_code_display: bool
    src: SrcPosition
    stage_location: str
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    strict: bool
    def __init__(
        self,
        comment: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        execute_as: _Optional[str] = ...,
        external_access_integrations: _Optional[_Iterable[str]] = ...,
        func: _Optional[_Union[SpCallable, _Mapping]] = ...,
        if_not_exists: bool = ...,
        imports: _Optional[_Iterable[_Union[SpTableName, _Mapping]]] = ...,
        input_types: _Optional[_Union[List_SpDataType, _Mapping]] = ...,
        is_permanent: bool = ...,
        kwargs: _Optional[_Iterable[_Union[Tuple_String_Expr, _Mapping]]] = ...,
        log_on_exception: _Optional[_Union[_wrappers_pb2.BoolValue, _Mapping]] = ...,
        name: _Optional[_Union[FnName, _Mapping]] = ...,
        packages: _Optional[_Iterable[str]] = ...,
        parallel: _Optional[int] = ...,
        replace: bool = ...,
        return_type: _Optional[_Union[SpDataType, _Mapping]] = ...,
        secrets: _Optional[_Iterable[_Union[Tuple_String_String, _Mapping]]] = ...,
        source_code_display: bool = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        stage_location: _Optional[str] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
        strict: bool = ...,
    ) -> None: ...

class Udf(_message.Message):
    __slots__ = (
        "comment",
        "external_access_integrations",
        "func",
        "if_not_exists",
        "immutable",
        "imports",
        "input_types",
        "is_permanent",
        "kwargs",
        "max_batch_size",
        "name",
        "packages",
        "parallel",
        "replace",
        "return_type",
        "secrets",
        "secure",
        "source_code_display",
        "src",
        "stage_location",
        "statement_params",
        "strict",
    )
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_ACCESS_INTEGRATIONS_FIELD_NUMBER: _ClassVar[int]
    FUNC_FIELD_NUMBER: _ClassVar[int]
    IF_NOT_EXISTS_FIELD_NUMBER: _ClassVar[int]
    IMMUTABLE_FIELD_NUMBER: _ClassVar[int]
    IMPORTS_FIELD_NUMBER: _ClassVar[int]
    INPUT_TYPES_FIELD_NUMBER: _ClassVar[int]
    IS_PERMANENT_FIELD_NUMBER: _ClassVar[int]
    KWARGS_FIELD_NUMBER: _ClassVar[int]
    MAX_BATCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PACKAGES_FIELD_NUMBER: _ClassVar[int]
    PARALLEL_FIELD_NUMBER: _ClassVar[int]
    REPLACE_FIELD_NUMBER: _ClassVar[int]
    RETURN_TYPE_FIELD_NUMBER: _ClassVar[int]
    SECRETS_FIELD_NUMBER: _ClassVar[int]
    SECURE_FIELD_NUMBER: _ClassVar[int]
    SOURCE_CODE_DISPLAY_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STAGE_LOCATION_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    STRICT_FIELD_NUMBER: _ClassVar[int]
    comment: _wrappers_pb2.StringValue
    external_access_integrations: _containers.RepeatedScalarFieldContainer[str]
    func: SpCallable
    if_not_exists: bool
    immutable: bool
    imports: _containers.RepeatedCompositeFieldContainer[SpTableName]
    input_types: List_SpDataType
    is_permanent: bool
    kwargs: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    max_batch_size: _wrappers_pb2.Int64Value
    name: FnName
    packages: _containers.RepeatedScalarFieldContainer[str]
    parallel: int
    replace: bool
    return_type: SpDataType
    secrets: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    secure: bool
    source_code_display: bool
    src: SrcPosition
    stage_location: str
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    strict: bool
    def __init__(
        self,
        comment: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        external_access_integrations: _Optional[_Iterable[str]] = ...,
        func: _Optional[_Union[SpCallable, _Mapping]] = ...,
        if_not_exists: bool = ...,
        immutable: bool = ...,
        imports: _Optional[_Iterable[_Union[SpTableName, _Mapping]]] = ...,
        input_types: _Optional[_Union[List_SpDataType, _Mapping]] = ...,
        is_permanent: bool = ...,
        kwargs: _Optional[_Iterable[_Union[Tuple_String_Expr, _Mapping]]] = ...,
        max_batch_size: _Optional[_Union[_wrappers_pb2.Int64Value, _Mapping]] = ...,
        name: _Optional[_Union[FnName, _Mapping]] = ...,
        packages: _Optional[_Iterable[str]] = ...,
        parallel: _Optional[int] = ...,
        replace: bool = ...,
        return_type: _Optional[_Union[SpDataType, _Mapping]] = ...,
        secrets: _Optional[_Iterable[_Union[Tuple_String_String, _Mapping]]] = ...,
        secure: bool = ...,
        source_code_display: bool = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        stage_location: _Optional[str] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
        strict: bool = ...,
    ) -> None: ...

class Udtf(_message.Message):
    __slots__ = (
        "comment",
        "external_access_integrations",
        "handler",
        "if_not_exists",
        "immutable",
        "imports",
        "input_types",
        "is_permanent",
        "kwargs",
        "name",
        "output_schema",
        "packages",
        "parallel",
        "replace",
        "secrets",
        "secure",
        "src",
        "stage_location",
        "statement_params",
        "strict",
    )
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_ACCESS_INTEGRATIONS_FIELD_NUMBER: _ClassVar[int]
    HANDLER_FIELD_NUMBER: _ClassVar[int]
    IF_NOT_EXISTS_FIELD_NUMBER: _ClassVar[int]
    IMMUTABLE_FIELD_NUMBER: _ClassVar[int]
    IMPORTS_FIELD_NUMBER: _ClassVar[int]
    INPUT_TYPES_FIELD_NUMBER: _ClassVar[int]
    IS_PERMANENT_FIELD_NUMBER: _ClassVar[int]
    KWARGS_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    PACKAGES_FIELD_NUMBER: _ClassVar[int]
    PARALLEL_FIELD_NUMBER: _ClassVar[int]
    REPLACE_FIELD_NUMBER: _ClassVar[int]
    SECRETS_FIELD_NUMBER: _ClassVar[int]
    SECURE_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STAGE_LOCATION_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    STRICT_FIELD_NUMBER: _ClassVar[int]
    comment: _wrappers_pb2.StringValue
    external_access_integrations: _containers.RepeatedScalarFieldContainer[str]
    handler: SpCallable
    if_not_exists: bool
    immutable: bool
    imports: _containers.RepeatedCompositeFieldContainer[SpTableName]
    input_types: List_SpDataType
    is_permanent: bool
    kwargs: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    name: FnName
    output_schema: UdtfSchema
    packages: _containers.RepeatedScalarFieldContainer[str]
    parallel: int
    replace: bool
    secrets: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    secure: bool
    src: SrcPosition
    stage_location: str
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    strict: bool
    def __init__(
        self,
        comment: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        external_access_integrations: _Optional[_Iterable[str]] = ...,
        handler: _Optional[_Union[SpCallable, _Mapping]] = ...,
        if_not_exists: bool = ...,
        immutable: bool = ...,
        imports: _Optional[_Iterable[_Union[SpTableName, _Mapping]]] = ...,
        input_types: _Optional[_Union[List_SpDataType, _Mapping]] = ...,
        is_permanent: bool = ...,
        kwargs: _Optional[_Iterable[_Union[Tuple_String_Expr, _Mapping]]] = ...,
        name: _Optional[_Union[FnName, _Mapping]] = ...,
        output_schema: _Optional[_Union[UdtfSchema, _Mapping]] = ...,
        packages: _Optional[_Iterable[str]] = ...,
        parallel: _Optional[int] = ...,
        replace: bool = ...,
        secrets: _Optional[_Iterable[_Union[Tuple_String_String, _Mapping]]] = ...,
        secure: bool = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        stage_location: _Optional[str] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
        strict: bool = ...,
    ) -> None: ...

class Udaf(_message.Message):
    __slots__ = (
        "comment",
        "external_access_integrations",
        "handler",
        "if_not_exists",
        "immutable",
        "imports",
        "input_types",
        "is_permanent",
        "kwargs",
        "name",
        "packages",
        "parallel",
        "replace",
        "return_type",
        "secrets",
        "src",
        "stage_location",
        "statement_params",
    )
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    EXTERNAL_ACCESS_INTEGRATIONS_FIELD_NUMBER: _ClassVar[int]
    HANDLER_FIELD_NUMBER: _ClassVar[int]
    IF_NOT_EXISTS_FIELD_NUMBER: _ClassVar[int]
    IMMUTABLE_FIELD_NUMBER: _ClassVar[int]
    IMPORTS_FIELD_NUMBER: _ClassVar[int]
    INPUT_TYPES_FIELD_NUMBER: _ClassVar[int]
    IS_PERMANENT_FIELD_NUMBER: _ClassVar[int]
    KWARGS_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    PACKAGES_FIELD_NUMBER: _ClassVar[int]
    PARALLEL_FIELD_NUMBER: _ClassVar[int]
    REPLACE_FIELD_NUMBER: _ClassVar[int]
    RETURN_TYPE_FIELD_NUMBER: _ClassVar[int]
    SECRETS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STAGE_LOCATION_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    comment: _wrappers_pb2.StringValue
    external_access_integrations: _containers.RepeatedScalarFieldContainer[str]
    handler: SpCallable
    if_not_exists: bool
    immutable: bool
    imports: _containers.RepeatedCompositeFieldContainer[SpTableName]
    input_types: List_SpDataType
    is_permanent: bool
    kwargs: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    name: FnName
    packages: _containers.RepeatedScalarFieldContainer[str]
    parallel: int
    replace: bool
    return_type: SpDataType
    secrets: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    src: SrcPosition
    stage_location: _wrappers_pb2.StringValue
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        comment: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        external_access_integrations: _Optional[_Iterable[str]] = ...,
        handler: _Optional[_Union[SpCallable, _Mapping]] = ...,
        if_not_exists: bool = ...,
        immutable: bool = ...,
        imports: _Optional[_Iterable[_Union[SpTableName, _Mapping]]] = ...,
        input_types: _Optional[_Union[List_SpDataType, _Mapping]] = ...,
        is_permanent: bool = ...,
        kwargs: _Optional[_Iterable[_Union[Tuple_String_Expr, _Mapping]]] = ...,
        name: _Optional[_Union[FnName, _Mapping]] = ...,
        packages: _Optional[_Iterable[str]] = ...,
        parallel: _Optional[int] = ...,
        replace: bool = ...,
        return_type: _Optional[_Union[SpDataType, _Mapping]] = ...,
        secrets: _Optional[_Iterable[_Union[Tuple_String_String, _Mapping]]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        stage_location: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class IndirectTableFnNameRef(_message.Message):
    __slots__ = ("name", "src")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    name: FnName
    src: SrcPosition
    def __init__(
        self,
        name: _Optional[_Union[FnName, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class IndirectTableFnIdRef(_message.Message):
    __slots__ = ("id", "src")
    ID_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    id: VarId
    src: SrcPosition
    def __init__(
        self,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class CallTableFunctionExpr(_message.Message):
    __slots__ = ("name", "src")
    NAME_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    name: FnName
    src: SrcPosition
    def __init__(
        self,
        name: _Optional[_Union[FnName, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class UnaryOp(_message.Message):
    __slots__ = ("neg",)
    NEG_FIELD_NUMBER: _ClassVar[int]
    NOT_FIELD_NUMBER: _ClassVar[int]
    neg: Neg
    def __init__(
        self, neg: _Optional[_Union[Neg, _Mapping]] = ..., **kwargs
    ) -> None: ...

class BinOp(_message.Message):
    __slots__ = (
        "add",
        "bit_and",
        "bit_or",
        "bit_xor",
        "div",
        "eq",
        "geq",
        "gt",
        "leq",
        "lt",
        "mod",
        "mul",
        "neq",
        "pow",
        "sub",
    )
    ADD_FIELD_NUMBER: _ClassVar[int]
    AND_FIELD_NUMBER: _ClassVar[int]
    BIT_AND_FIELD_NUMBER: _ClassVar[int]
    BIT_OR_FIELD_NUMBER: _ClassVar[int]
    BIT_XOR_FIELD_NUMBER: _ClassVar[int]
    DIV_FIELD_NUMBER: _ClassVar[int]
    EQ_FIELD_NUMBER: _ClassVar[int]
    GEQ_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    LEQ_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    MOD_FIELD_NUMBER: _ClassVar[int]
    MUL_FIELD_NUMBER: _ClassVar[int]
    NEQ_FIELD_NUMBER: _ClassVar[int]
    OR_FIELD_NUMBER: _ClassVar[int]
    POW_FIELD_NUMBER: _ClassVar[int]
    SUB_FIELD_NUMBER: _ClassVar[int]
    add: Add
    bit_and: BitAnd
    bit_or: BitOr
    bit_xor: BitXor
    div: Div
    eq: Eq
    geq: Geq
    gt: Gt
    leq: Leq
    lt: Lt
    mod: Mod
    mul: Mul
    neq: Neq
    pow: Pow
    sub: Sub
    def __init__(
        self,
        add: _Optional[_Union[Add, _Mapping]] = ...,
        bit_and: _Optional[_Union[BitAnd, _Mapping]] = ...,
        bit_or: _Optional[_Union[BitOr, _Mapping]] = ...,
        bit_xor: _Optional[_Union[BitXor, _Mapping]] = ...,
        div: _Optional[_Union[Div, _Mapping]] = ...,
        eq: _Optional[_Union[Eq, _Mapping]] = ...,
        geq: _Optional[_Union[Geq, _Mapping]] = ...,
        gt: _Optional[_Union[Gt, _Mapping]] = ...,
        leq: _Optional[_Union[Leq, _Mapping]] = ...,
        lt: _Optional[_Union[Lt, _Mapping]] = ...,
        mod: _Optional[_Union[Mod, _Mapping]] = ...,
        mul: _Optional[_Union[Mul, _Mapping]] = ...,
        neq: _Optional[_Union[Neq, _Mapping]] = ...,
        pow: _Optional[_Union[Pow, _Mapping]] = ...,
        sub: _Optional[_Union[Sub, _Mapping]] = ...,
        **kwargs
    ) -> None: ...

class Not(_message.Message):
    __slots__ = ("operand", "src")
    OPERAND_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    operand: Expr
    src: SrcPosition
    def __init__(
        self,
        operand: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class And(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class Or(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class Eq(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class Neq(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class Lt(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class Leq(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class Gt(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class Geq(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class Neg(_message.Message):
    __slots__ = ("operand", "src")
    OPERAND_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    operand: Expr
    src: SrcPosition
    def __init__(
        self,
        operand: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class Add(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class Sub(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class Mul(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class Div(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class Mod(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class Pow(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class BitAnd(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class BitOr(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class BitXor(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class RangeVal(_message.Message):
    __slots__ = ("src", "start", "step", "stop")
    SRC_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    STOP_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    start: Expr
    step: Expr
    stop: Expr
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        start: _Optional[_Union[Expr, _Mapping]] = ...,
        step: _Optional[_Union[Expr, _Mapping]] = ...,
        stop: _Optional[_Union[Expr, _Mapping]] = ...,
    ) -> None: ...

class SpWindowSpecExpr(_message.Message):
    __slots__ = (
        "sp_window_spec_empty",
        "sp_window_spec_order_by",
        "sp_window_spec_partition_by",
        "sp_window_spec_range_between",
        "sp_window_spec_rows_between",
    )
    SP_WINDOW_SPEC_EMPTY_FIELD_NUMBER: _ClassVar[int]
    SP_WINDOW_SPEC_ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    SP_WINDOW_SPEC_PARTITION_BY_FIELD_NUMBER: _ClassVar[int]
    SP_WINDOW_SPEC_RANGE_BETWEEN_FIELD_NUMBER: _ClassVar[int]
    SP_WINDOW_SPEC_ROWS_BETWEEN_FIELD_NUMBER: _ClassVar[int]
    sp_window_spec_empty: SpWindowSpecEmpty
    sp_window_spec_order_by: SpWindowSpecOrderBy
    sp_window_spec_partition_by: SpWindowSpecPartitionBy
    sp_window_spec_range_between: SpWindowSpecRangeBetween
    sp_window_spec_rows_between: SpWindowSpecRowsBetween
    def __init__(
        self,
        sp_window_spec_empty: _Optional[_Union[SpWindowSpecEmpty, _Mapping]] = ...,
        sp_window_spec_order_by: _Optional[_Union[SpWindowSpecOrderBy, _Mapping]] = ...,
        sp_window_spec_partition_by: _Optional[
            _Union[SpWindowSpecPartitionBy, _Mapping]
        ] = ...,
        sp_window_spec_range_between: _Optional[
            _Union[SpWindowSpecRangeBetween, _Mapping]
        ] = ...,
        sp_window_spec_rows_between: _Optional[
            _Union[SpWindowSpecRowsBetween, _Mapping]
        ] = ...,
    ) -> None: ...

class SpWindowSpecEmpty(_message.Message):
    __slots__ = ("src", "wnd")
    SRC_FIELD_NUMBER: _ClassVar[int]
    WND_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    wnd: SpWindowSpecExpr
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        wnd: _Optional[_Union[SpWindowSpecExpr, _Mapping]] = ...,
    ) -> None: ...

class SpWindowSpecOrderBy(_message.Message):
    __slots__ = ("cols", "src", "wnd")
    COLS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    WND_FIELD_NUMBER: _ClassVar[int]
    cols: _containers.RepeatedCompositeFieldContainer[Expr]
    src: SrcPosition
    wnd: SpWindowSpecExpr
    def __init__(
        self,
        cols: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        wnd: _Optional[_Union[SpWindowSpecExpr, _Mapping]] = ...,
    ) -> None: ...

class SpWindowSpecPartitionBy(_message.Message):
    __slots__ = ("cols", "src", "wnd")
    COLS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    WND_FIELD_NUMBER: _ClassVar[int]
    cols: _containers.RepeatedCompositeFieldContainer[Expr]
    src: SrcPosition
    wnd: SpWindowSpecExpr
    def __init__(
        self,
        cols: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        wnd: _Optional[_Union[SpWindowSpecExpr, _Mapping]] = ...,
    ) -> None: ...

class SpWindowSpecRangeBetween(_message.Message):
    __slots__ = ("end", "src", "start", "wnd")
    END_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    WND_FIELD_NUMBER: _ClassVar[int]
    end: SpWindowRelativePosition
    src: SrcPosition
    start: SpWindowRelativePosition
    wnd: SpWindowSpecExpr
    def __init__(
        self,
        end: _Optional[_Union[SpWindowRelativePosition, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        start: _Optional[_Union[SpWindowRelativePosition, _Mapping]] = ...,
        wnd: _Optional[_Union[SpWindowSpecExpr, _Mapping]] = ...,
    ) -> None: ...

class SpWindowSpecRowsBetween(_message.Message):
    __slots__ = ("end", "src", "start", "wnd")
    END_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    WND_FIELD_NUMBER: _ClassVar[int]
    end: SpWindowRelativePosition
    src: SrcPosition
    start: SpWindowRelativePosition
    wnd: SpWindowSpecExpr
    def __init__(
        self,
        end: _Optional[_Union[SpWindowRelativePosition, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        start: _Optional[_Union[SpWindowRelativePosition, _Mapping]] = ...,
        wnd: _Optional[_Union[SpWindowSpecExpr, _Mapping]] = ...,
    ) -> None: ...

class Expr(_message.Message):
    __slots__ = (
        "trait_bin_op",
        "trait_const",
        "trait_fn_id_ref_expr",
        "trait_fn_name_ref_expr",
        "trait_fn_ref_expr",
        "trait_sp_column_expr",
        "trait_sp_column_fn",
        "trait_sp_dataframe_expr",
        "trait_sp_dataframe_writer",
        "trait_sp_dataframe_writer_options",
        "trait_sp_dataframe_writer_save_mode",
        "trait_sp_matched_clause",
        "trait_sp_relational_grouped_dataframe_expr",
        "trait_sp_write_file",
        "trait_unary_op",
        "add",
        "apply_expr",
        "big_decimal_val",
        "big_int_val",
        "binary_val",
        "bit_and",
        "bit_or",
        "bit_xor",
        "bool_val",
        "builtin_fn",
        "call_table_function_expr",
        "cast_expr",
        "date_val",
        "div",
        "eq",
        "float64_val",
        "fn_val",
        "geq",
        "gt",
        "if_expr",
        "indirect_table_fn_id_ref",
        "indirect_table_fn_name_ref",
        "int32_val",
        "int64_val",
        "leq",
        "list_val",
        "lt",
        "mod",
        "mul",
        "neg",
        "neq",
        "none_val",
        "null_val",
        "object_get_item",
        "pd_dataframe",
        "pd_dataframe_get_item",
        "pd_dataframe_i_loc",
        "pd_dataframe_loc",
        "pd_dataframe_set_item",
        "pd_drop_na",
        "pd_repr",
        "pow",
        "python_date_val",
        "python_time_val",
        "python_timestamp_val",
        "range_val",
        "ref",
        "seq_map_val",
        "some_val",
        "sp_column_alias",
        "sp_column_apply__int",
        "sp_column_apply__string",
        "sp_column_asc",
        "sp_column_between",
        "sp_column_case_when",
        "sp_column_cast",
        "sp_column_desc",
        "sp_column_equal_nan",
        "sp_column_equal_null",
        "sp_column_in__dataframe",
        "sp_column_in__seq",
        "sp_column_is_not_null",
        "sp_column_is_null",
        "sp_column_name",
        "sp_column_over",
        "sp_column_ref",
        "sp_column_sql_expr",
        "sp_column_string_collate",
        "sp_column_string_contains",
        "sp_column_string_ends_with",
        "sp_column_string_like",
        "sp_column_string_regexp",
        "sp_column_string_starts_with",
        "sp_column_string_substr",
        "sp_column_try_cast",
        "sp_column_within_group",
        "sp_create_dataframe",
        "sp_dataframe_agg",
        "sp_dataframe_alias",
        "sp_dataframe_analytics_compute_lag",
        "sp_dataframe_analytics_compute_lead",
        "sp_dataframe_analytics_cumulative_agg",
        "sp_dataframe_analytics_moving_agg",
        "sp_dataframe_analytics_time_series_agg",
        "sp_dataframe_apply",
        "sp_dataframe_cache_result",
        "sp_dataframe_col",
        "sp_dataframe_collect",
        "sp_dataframe_copy_into_table",
        "sp_dataframe_count",
        "sp_dataframe_create_or_replace_dynamic_table",
        "sp_dataframe_create_or_replace_view",
        "sp_dataframe_cross_join",
        "sp_dataframe_cube",
        "sp_dataframe_describe",
        "sp_dataframe_distinct",
        "sp_dataframe_drop",
        "sp_dataframe_drop_duplicates",
        "sp_dataframe_except",
        "sp_dataframe_filter",
        "sp_dataframe_first",
        "sp_dataframe_flatten",
        "sp_dataframe_group_by",
        "sp_dataframe_group_by_grouping_sets",
        "sp_dataframe_group_by__columns",
        "sp_dataframe_group_by__strings",
        "sp_dataframe_intersect",
        "sp_dataframe_join",
        "sp_dataframe_join_table_function",
        "sp_dataframe_join__dataframe__join_exprs",
        "sp_dataframe_join__dataframe__using_columns",
        "sp_dataframe_limit",
        "sp_dataframe_na_drop__python",
        "sp_dataframe_na_drop__scala",
        "sp_dataframe_na_fill",
        "sp_dataframe_na_replace",
        "sp_dataframe_natural_join",
        "sp_dataframe_pivot",
        "sp_dataframe_random_split",
        "sp_dataframe_ref",
        "sp_dataframe_rename",
        "sp_dataframe_rollup",
        "sp_dataframe_rollup__columns",
        "sp_dataframe_rollup__strings",
        "sp_dataframe_sample",
        "sp_dataframe_select__columns",
        "sp_dataframe_select__exprs",
        "sp_dataframe_show",
        "sp_dataframe_sort",
        "sp_dataframe_stat_approx_quantile",
        "sp_dataframe_stat_corr",
        "sp_dataframe_stat_cov",
        "sp_dataframe_stat_cross_tab",
        "sp_dataframe_stat_sample_by",
        "sp_dataframe_to_df",
        "sp_dataframe_to_local_iterator",
        "sp_dataframe_to_pandas",
        "sp_dataframe_to_pandas_batches",
        "sp_dataframe_union",
        "sp_dataframe_union_all",
        "sp_dataframe_union_all_by_name",
        "sp_dataframe_union_by_name",
        "sp_dataframe_unpivot",
        "sp_dataframe_where",
        "sp_dataframe_with_column",
        "sp_dataframe_with_column_renamed",
        "sp_dataframe_with_columns",
        "sp_dataframe_write",
        "sp_datatype_val",
        "sp_flatten",
        "sp_fn_ref",
        "sp_generator",
        "sp_grouping_sets",
        "sp_merge_delete_when_matched_clause",
        "sp_merge_insert_when_not_matched_clause",
        "sp_merge_update_when_matched_clause",
        "sp_range",
        "sp_read_avro",
        "sp_read_csv",
        "sp_read_json",
        "sp_read_orc",
        "sp_read_parquet",
        "sp_read_table",
        "sp_read_xml",
        "sp_relational_grouped_dataframe_agg",
        "sp_relational_grouped_dataframe_apply_in_pandas",
        "sp_relational_grouped_dataframe_builtin",
        "sp_relational_grouped_dataframe_pivot",
        "sp_relational_grouped_dataframe_ref",
        "sp_row",
        "sp_session_table_function",
        "sp_sql",
        "sp_stored_procedure",
        "sp_table",
        "sp_table_delete",
        "sp_table_drop_table",
        "sp_table_fn_call_alias",
        "sp_table_fn_call_over",
        "sp_table_merge",
        "sp_table_sample",
        "sp_table_update",
        "sp_write_copy_into_location",
        "sp_write_csv",
        "sp_write_json",
        "sp_write_pandas",
        "sp_write_parquet",
        "sp_write_table",
        "stored_procedure",
        "string_val",
        "sub",
        "time_val",
        "timestamp_val",
        "tuple_val",
        "udaf",
        "udf",
        "udtf",
    )
    TRAIT_BIN_OP_FIELD_NUMBER: _ClassVar[int]
    TRAIT_CONST_FIELD_NUMBER: _ClassVar[int]
    TRAIT_FN_ID_REF_EXPR_FIELD_NUMBER: _ClassVar[int]
    TRAIT_FN_NAME_REF_EXPR_FIELD_NUMBER: _ClassVar[int]
    TRAIT_FN_REF_EXPR_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_COLUMN_EXPR_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_COLUMN_FN_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_DATAFRAME_EXPR_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_DATAFRAME_WRITER_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_DATAFRAME_WRITER_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_DATAFRAME_WRITER_SAVE_MODE_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_MATCHED_CLAUSE_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_RELATIONAL_GROUPED_DATAFRAME_EXPR_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_WRITE_FILE_FIELD_NUMBER: _ClassVar[int]
    TRAIT_UNARY_OP_FIELD_NUMBER: _ClassVar[int]
    ADD_FIELD_NUMBER: _ClassVar[int]
    AND_FIELD_NUMBER: _ClassVar[int]
    APPLY_EXPR_FIELD_NUMBER: _ClassVar[int]
    BIG_DECIMAL_VAL_FIELD_NUMBER: _ClassVar[int]
    BIG_INT_VAL_FIELD_NUMBER: _ClassVar[int]
    BINARY_VAL_FIELD_NUMBER: _ClassVar[int]
    BIT_AND_FIELD_NUMBER: _ClassVar[int]
    BIT_OR_FIELD_NUMBER: _ClassVar[int]
    BIT_XOR_FIELD_NUMBER: _ClassVar[int]
    BOOL_VAL_FIELD_NUMBER: _ClassVar[int]
    BUILTIN_FN_FIELD_NUMBER: _ClassVar[int]
    CALL_TABLE_FUNCTION_EXPR_FIELD_NUMBER: _ClassVar[int]
    CAST_EXPR_FIELD_NUMBER: _ClassVar[int]
    DATE_VAL_FIELD_NUMBER: _ClassVar[int]
    DIV_FIELD_NUMBER: _ClassVar[int]
    EQ_FIELD_NUMBER: _ClassVar[int]
    FLOAT64_VAL_FIELD_NUMBER: _ClassVar[int]
    FN_VAL_FIELD_NUMBER: _ClassVar[int]
    GEQ_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    IF_EXPR_FIELD_NUMBER: _ClassVar[int]
    INDIRECT_TABLE_FN_ID_REF_FIELD_NUMBER: _ClassVar[int]
    INDIRECT_TABLE_FN_NAME_REF_FIELD_NUMBER: _ClassVar[int]
    INT32_VAL_FIELD_NUMBER: _ClassVar[int]
    INT64_VAL_FIELD_NUMBER: _ClassVar[int]
    LEQ_FIELD_NUMBER: _ClassVar[int]
    LIST_VAL_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    MOD_FIELD_NUMBER: _ClassVar[int]
    MUL_FIELD_NUMBER: _ClassVar[int]
    NEG_FIELD_NUMBER: _ClassVar[int]
    NEQ_FIELD_NUMBER: _ClassVar[int]
    NONE_VAL_FIELD_NUMBER: _ClassVar[int]
    NOT_FIELD_NUMBER: _ClassVar[int]
    NULL_VAL_FIELD_NUMBER: _ClassVar[int]
    OBJECT_GET_ITEM_FIELD_NUMBER: _ClassVar[int]
    OR_FIELD_NUMBER: _ClassVar[int]
    PD_DATAFRAME_FIELD_NUMBER: _ClassVar[int]
    PD_DATAFRAME_GET_ITEM_FIELD_NUMBER: _ClassVar[int]
    PD_DATAFRAME_I_LOC_FIELD_NUMBER: _ClassVar[int]
    PD_DATAFRAME_LOC_FIELD_NUMBER: _ClassVar[int]
    PD_DATAFRAME_SET_ITEM_FIELD_NUMBER: _ClassVar[int]
    PD_DROP_NA_FIELD_NUMBER: _ClassVar[int]
    PD_REPR_FIELD_NUMBER: _ClassVar[int]
    POW_FIELD_NUMBER: _ClassVar[int]
    PYTHON_DATE_VAL_FIELD_NUMBER: _ClassVar[int]
    PYTHON_TIME_VAL_FIELD_NUMBER: _ClassVar[int]
    PYTHON_TIMESTAMP_VAL_FIELD_NUMBER: _ClassVar[int]
    RANGE_VAL_FIELD_NUMBER: _ClassVar[int]
    REF_FIELD_NUMBER: _ClassVar[int]
    SEQ_MAP_VAL_FIELD_NUMBER: _ClassVar[int]
    SOME_VAL_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_ALIAS_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_APPLY__INT_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_APPLY__STRING_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_ASC_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_BETWEEN_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_CASE_WHEN_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_CAST_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_DESC_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_EQUAL_NAN_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_EQUAL_NULL_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_IN__DATAFRAME_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_IN__SEQ_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_IS_NOT_NULL_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_IS_NULL_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_NAME_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_OVER_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_REF_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_SQL_EXPR_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_COLLATE_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_CONTAINS_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_ENDS_WITH_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_LIKE_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_REGEXP_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_STARTS_WITH_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_SUBSTR_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_TRY_CAST_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_WITHIN_GROUP_FIELD_NUMBER: _ClassVar[int]
    SP_CREATE_DATAFRAME_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_AGG_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ALIAS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ANALYTICS_COMPUTE_LAG_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ANALYTICS_COMPUTE_LEAD_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ANALYTICS_CUMULATIVE_AGG_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ANALYTICS_MOVING_AGG_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ANALYTICS_TIME_SERIES_AGG_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_APPLY_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_CACHE_RESULT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_COL_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_COLLECT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_COPY_INTO_TABLE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_COUNT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_CREATE_OR_REPLACE_DYNAMIC_TABLE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_CREATE_OR_REPLACE_VIEW_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_CROSS_JOIN_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_CUBE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_DESCRIBE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_DISTINCT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_DROP_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_DROP_DUPLICATES_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_EXCEPT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_FILTER_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_FIRST_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_FLATTEN_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_GROUP_BY_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_GROUP_BY_GROUPING_SETS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_GROUP_BY__COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_GROUP_BY__STRINGS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_INTERSECT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_JOIN_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_JOIN_TABLE_FUNCTION_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_JOIN__DATAFRAME__JOIN_EXPRS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_JOIN__DATAFRAME__USING_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_LIMIT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_NA_DROP__PYTHON_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_NA_DROP__SCALA_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_NA_FILL_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_NA_REPLACE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_NATURAL_JOIN_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_PIVOT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_RANDOM_SPLIT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_REF_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_RENAME_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ROLLUP_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ROLLUP__COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ROLLUP__STRINGS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SAMPLE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SELECT__COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SELECT__EXPRS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SHOW_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SORT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_STAT_APPROX_QUANTILE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_STAT_CORR_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_STAT_COV_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_STAT_CROSS_TAB_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_STAT_SAMPLE_BY_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_TO_DF_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_TO_LOCAL_ITERATOR_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_TO_PANDAS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_TO_PANDAS_BATCHES_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_UNION_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_UNION_ALL_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_UNION_ALL_BY_NAME_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_UNION_BY_NAME_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_UNPIVOT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_WHERE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_WITH_COLUMN_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_WITH_COLUMN_RENAMED_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_WITH_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_WRITE_FIELD_NUMBER: _ClassVar[int]
    SP_DATATYPE_VAL_FIELD_NUMBER: _ClassVar[int]
    SP_FLATTEN_FIELD_NUMBER: _ClassVar[int]
    SP_FN_REF_FIELD_NUMBER: _ClassVar[int]
    SP_GENERATOR_FIELD_NUMBER: _ClassVar[int]
    SP_GROUPING_SETS_FIELD_NUMBER: _ClassVar[int]
    SP_MERGE_DELETE_WHEN_MATCHED_CLAUSE_FIELD_NUMBER: _ClassVar[int]
    SP_MERGE_INSERT_WHEN_NOT_MATCHED_CLAUSE_FIELD_NUMBER: _ClassVar[int]
    SP_MERGE_UPDATE_WHEN_MATCHED_CLAUSE_FIELD_NUMBER: _ClassVar[int]
    SP_RANGE_FIELD_NUMBER: _ClassVar[int]
    SP_READ_AVRO_FIELD_NUMBER: _ClassVar[int]
    SP_READ_CSV_FIELD_NUMBER: _ClassVar[int]
    SP_READ_JSON_FIELD_NUMBER: _ClassVar[int]
    SP_READ_ORC_FIELD_NUMBER: _ClassVar[int]
    SP_READ_PARQUET_FIELD_NUMBER: _ClassVar[int]
    SP_READ_TABLE_FIELD_NUMBER: _ClassVar[int]
    SP_READ_XML_FIELD_NUMBER: _ClassVar[int]
    SP_RELATIONAL_GROUPED_DATAFRAME_AGG_FIELD_NUMBER: _ClassVar[int]
    SP_RELATIONAL_GROUPED_DATAFRAME_APPLY_IN_PANDAS_FIELD_NUMBER: _ClassVar[int]
    SP_RELATIONAL_GROUPED_DATAFRAME_BUILTIN_FIELD_NUMBER: _ClassVar[int]
    SP_RELATIONAL_GROUPED_DATAFRAME_PIVOT_FIELD_NUMBER: _ClassVar[int]
    SP_RELATIONAL_GROUPED_DATAFRAME_REF_FIELD_NUMBER: _ClassVar[int]
    SP_ROW_FIELD_NUMBER: _ClassVar[int]
    SP_SESSION_TABLE_FUNCTION_FIELD_NUMBER: _ClassVar[int]
    SP_SQL_FIELD_NUMBER: _ClassVar[int]
    SP_STORED_PROCEDURE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_DELETE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_DROP_TABLE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_FN_CALL_ALIAS_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_FN_CALL_OVER_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_MERGE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_SAMPLE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_UPDATE_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_COPY_INTO_LOCATION_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_CSV_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_JSON_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_PANDAS_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_PARQUET_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_TABLE_FIELD_NUMBER: _ClassVar[int]
    STORED_PROCEDURE_FIELD_NUMBER: _ClassVar[int]
    STRING_VAL_FIELD_NUMBER: _ClassVar[int]
    SUB_FIELD_NUMBER: _ClassVar[int]
    TIME_VAL_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_VAL_FIELD_NUMBER: _ClassVar[int]
    TUPLE_VAL_FIELD_NUMBER: _ClassVar[int]
    UDAF_FIELD_NUMBER: _ClassVar[int]
    UDF_FIELD_NUMBER: _ClassVar[int]
    UDTF_FIELD_NUMBER: _ClassVar[int]
    trait_bin_op: BinOp
    trait_const: Const
    trait_fn_id_ref_expr: FnIdRefExpr
    trait_fn_name_ref_expr: FnNameRefExpr
    trait_fn_ref_expr: FnRefExpr
    trait_sp_column_expr: SpColumnExpr
    trait_sp_column_fn: SpColumnFn
    trait_sp_dataframe_expr: SpDataframeExpr
    trait_sp_dataframe_writer: SpDataframeWriter
    trait_sp_dataframe_writer_options: SpDataframeWriterOptions
    trait_sp_dataframe_writer_save_mode: SpDataframeWriterSaveMode
    trait_sp_matched_clause: SpMatchedClause
    trait_sp_relational_grouped_dataframe_expr: SpRelationalGroupedDataframeExpr
    trait_sp_write_file: SpWriteFile
    trait_unary_op: UnaryOp
    add: Add
    apply_expr: ApplyExpr
    big_decimal_val: BigDecimalVal
    big_int_val: BigIntVal
    binary_val: BinaryVal
    bit_and: BitAnd
    bit_or: BitOr
    bit_xor: BitXor
    bool_val: BoolVal
    builtin_fn: BuiltinFn
    call_table_function_expr: CallTableFunctionExpr
    cast_expr: CastExpr
    date_val: DateVal
    div: Div
    eq: Eq
    float64_val: Float64Val
    fn_val: FnVal
    geq: Geq
    gt: Gt
    if_expr: IfExpr
    indirect_table_fn_id_ref: IndirectTableFnIdRef
    indirect_table_fn_name_ref: IndirectTableFnNameRef
    int32_val: Int32Val
    int64_val: Int64Val
    leq: Leq
    list_val: ListVal
    lt: Lt
    mod: Mod
    mul: Mul
    neg: Neg
    neq: Neq
    none_val: NoneVal
    null_val: NullVal
    object_get_item: ObjectGetItem
    pd_dataframe: PdDataframe
    pd_dataframe_get_item: PdDataframeGetItem
    pd_dataframe_i_loc: PdDataframeILoc
    pd_dataframe_loc: PdDataframeLoc
    pd_dataframe_set_item: PdDataframeSetItem
    pd_drop_na: PdDropNa
    pd_repr: PdRepr
    pow: Pow
    python_date_val: PythonDateVal
    python_time_val: PythonTimeVal
    python_timestamp_val: PythonTimestampVal
    range_val: RangeVal
    ref: Ref
    seq_map_val: SeqMapVal
    some_val: SomeVal
    sp_column_alias: SpColumnAlias
    sp_column_apply__int: SpColumnApply_Int
    sp_column_apply__string: SpColumnApply_String
    sp_column_asc: SpColumnAsc
    sp_column_between: SpColumnBetween
    sp_column_case_when: SpColumnCaseWhen
    sp_column_cast: SpColumnCast
    sp_column_desc: SpColumnDesc
    sp_column_equal_nan: SpColumnEqualNan
    sp_column_equal_null: SpColumnEqualNull
    sp_column_in__dataframe: SpColumnIn_Dataframe
    sp_column_in__seq: SpColumnIn_Seq
    sp_column_is_not_null: SpColumnIsNotNull
    sp_column_is_null: SpColumnIsNull
    sp_column_name: SpColumnName
    sp_column_over: SpColumnOver
    sp_column_ref: SpColumnRef
    sp_column_sql_expr: SpColumnSqlExpr
    sp_column_string_collate: SpColumnStringCollate
    sp_column_string_contains: SpColumnStringContains
    sp_column_string_ends_with: SpColumnStringEndsWith
    sp_column_string_like: SpColumnStringLike
    sp_column_string_regexp: SpColumnStringRegexp
    sp_column_string_starts_with: SpColumnStringStartsWith
    sp_column_string_substr: SpColumnStringSubstr
    sp_column_try_cast: SpColumnTryCast
    sp_column_within_group: SpColumnWithinGroup
    sp_create_dataframe: SpCreateDataframe
    sp_dataframe_agg: SpDataframeAgg
    sp_dataframe_alias: SpDataframeAlias
    sp_dataframe_analytics_compute_lag: SpDataframeAnalyticsComputeLag
    sp_dataframe_analytics_compute_lead: SpDataframeAnalyticsComputeLead
    sp_dataframe_analytics_cumulative_agg: SpDataframeAnalyticsCumulativeAgg
    sp_dataframe_analytics_moving_agg: SpDataframeAnalyticsMovingAgg
    sp_dataframe_analytics_time_series_agg: SpDataframeAnalyticsTimeSeriesAgg
    sp_dataframe_apply: SpDataframeApply
    sp_dataframe_cache_result: SpDataframeCacheResult
    sp_dataframe_col: SpDataframeCol
    sp_dataframe_collect: SpDataframeCollect
    sp_dataframe_copy_into_table: SpDataframeCopyIntoTable
    sp_dataframe_count: SpDataframeCount
    sp_dataframe_create_or_replace_dynamic_table: SpDataframeCreateOrReplaceDynamicTable
    sp_dataframe_create_or_replace_view: SpDataframeCreateOrReplaceView
    sp_dataframe_cross_join: SpDataframeCrossJoin
    sp_dataframe_cube: SpDataframeCube
    sp_dataframe_describe: SpDataframeDescribe
    sp_dataframe_distinct: SpDataframeDistinct
    sp_dataframe_drop: SpDataframeDrop
    sp_dataframe_drop_duplicates: SpDataframeDropDuplicates
    sp_dataframe_except: SpDataframeExcept
    sp_dataframe_filter: SpDataframeFilter
    sp_dataframe_first: SpDataframeFirst
    sp_dataframe_flatten: SpDataframeFlatten
    sp_dataframe_group_by: SpDataframeGroupBy
    sp_dataframe_group_by_grouping_sets: SpDataframeGroupByGroupingSets
    sp_dataframe_group_by__columns: SpDataframeGroupBy_Columns
    sp_dataframe_group_by__strings: SpDataframeGroupBy_Strings
    sp_dataframe_intersect: SpDataframeIntersect
    sp_dataframe_join: SpDataframeJoin
    sp_dataframe_join_table_function: SpDataframeJoinTableFunction
    sp_dataframe_join__dataframe__join_exprs: SpDataframeJoin_Dataframe_JoinExprs
    sp_dataframe_join__dataframe__using_columns: SpDataframeJoin_Dataframe_UsingColumns
    sp_dataframe_limit: SpDataframeLimit
    sp_dataframe_na_drop__python: SpDataframeNaDrop_Python
    sp_dataframe_na_drop__scala: SpDataframeNaDrop_Scala
    sp_dataframe_na_fill: SpDataframeNaFill
    sp_dataframe_na_replace: SpDataframeNaReplace
    sp_dataframe_natural_join: SpDataframeNaturalJoin
    sp_dataframe_pivot: SpDataframePivot
    sp_dataframe_random_split: SpDataframeRandomSplit
    sp_dataframe_ref: SpDataframeRef
    sp_dataframe_rename: SpDataframeRename
    sp_dataframe_rollup: SpDataframeRollup
    sp_dataframe_rollup__columns: SpDataframeRollup_Columns
    sp_dataframe_rollup__strings: SpDataframeRollup_Strings
    sp_dataframe_sample: SpDataframeSample
    sp_dataframe_select__columns: SpDataframeSelect_Columns
    sp_dataframe_select__exprs: SpDataframeSelect_Exprs
    sp_dataframe_show: SpDataframeShow
    sp_dataframe_sort: SpDataframeSort
    sp_dataframe_stat_approx_quantile: SpDataframeStatApproxQuantile
    sp_dataframe_stat_corr: SpDataframeStatCorr
    sp_dataframe_stat_cov: SpDataframeStatCov
    sp_dataframe_stat_cross_tab: SpDataframeStatCrossTab
    sp_dataframe_stat_sample_by: SpDataframeStatSampleBy
    sp_dataframe_to_df: SpDataframeToDf
    sp_dataframe_to_local_iterator: SpDataframeToLocalIterator
    sp_dataframe_to_pandas: SpDataframeToPandas
    sp_dataframe_to_pandas_batches: SpDataframeToPandasBatches
    sp_dataframe_union: SpDataframeUnion
    sp_dataframe_union_all: SpDataframeUnionAll
    sp_dataframe_union_all_by_name: SpDataframeUnionAllByName
    sp_dataframe_union_by_name: SpDataframeUnionByName
    sp_dataframe_unpivot: SpDataframeUnpivot
    sp_dataframe_where: SpDataframeWhere
    sp_dataframe_with_column: SpDataframeWithColumn
    sp_dataframe_with_column_renamed: SpDataframeWithColumnRenamed
    sp_dataframe_with_columns: SpDataframeWithColumns
    sp_dataframe_write: SpDataframeWrite
    sp_datatype_val: SpDatatypeVal
    sp_flatten: SpFlatten
    sp_fn_ref: SpFnRef
    sp_generator: SpGenerator
    sp_grouping_sets: SpGroupingSets
    sp_merge_delete_when_matched_clause: SpMergeDeleteWhenMatchedClause
    sp_merge_insert_when_not_matched_clause: SpMergeInsertWhenNotMatchedClause
    sp_merge_update_when_matched_clause: SpMergeUpdateWhenMatchedClause
    sp_range: SpRange
    sp_read_avro: SpReadAvro
    sp_read_csv: SpReadCsv
    sp_read_json: SpReadJson
    sp_read_orc: SpReadOrc
    sp_read_parquet: SpReadParquet
    sp_read_table: SpReadTable
    sp_read_xml: SpReadXml
    sp_relational_grouped_dataframe_agg: SpRelationalGroupedDataframeAgg
    sp_relational_grouped_dataframe_apply_in_pandas: SpRelationalGroupedDataframeApplyInPandas
    sp_relational_grouped_dataframe_builtin: SpRelationalGroupedDataframeBuiltin
    sp_relational_grouped_dataframe_pivot: SpRelationalGroupedDataframePivot
    sp_relational_grouped_dataframe_ref: SpRelationalGroupedDataframeRef
    sp_row: SpRow
    sp_session_table_function: SpSessionTableFunction
    sp_sql: SpSql
    sp_stored_procedure: SpStoredProcedure
    sp_table: SpTable
    sp_table_delete: SpTableDelete
    sp_table_drop_table: SpTableDropTable
    sp_table_fn_call_alias: SpTableFnCallAlias
    sp_table_fn_call_over: SpTableFnCallOver
    sp_table_merge: SpTableMerge
    sp_table_sample: SpTableSample
    sp_table_update: SpTableUpdate
    sp_write_copy_into_location: SpWriteCopyIntoLocation
    sp_write_csv: SpWriteCsv
    sp_write_json: SpWriteJson
    sp_write_pandas: SpWritePandas
    sp_write_parquet: SpWriteParquet
    sp_write_table: SpWriteTable
    stored_procedure: StoredProcedure
    string_val: StringVal
    sub: Sub
    time_val: TimeVal
    timestamp_val: TimestampVal
    tuple_val: TupleVal
    udaf: Udaf
    udf: Udf
    udtf: Udtf
    def __init__(
        self,
        trait_bin_op: _Optional[_Union[BinOp, _Mapping]] = ...,
        trait_const: _Optional[_Union[Const, _Mapping]] = ...,
        trait_fn_id_ref_expr: _Optional[_Union[FnIdRefExpr, _Mapping]] = ...,
        trait_fn_name_ref_expr: _Optional[_Union[FnNameRefExpr, _Mapping]] = ...,
        trait_fn_ref_expr: _Optional[_Union[FnRefExpr, _Mapping]] = ...,
        trait_sp_column_expr: _Optional[_Union[SpColumnExpr, _Mapping]] = ...,
        trait_sp_column_fn: _Optional[_Union[SpColumnFn, _Mapping]] = ...,
        trait_sp_dataframe_expr: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        trait_sp_dataframe_writer: _Optional[_Union[SpDataframeWriter, _Mapping]] = ...,
        trait_sp_dataframe_writer_options: _Optional[
            _Union[SpDataframeWriterOptions, _Mapping]
        ] = ...,
        trait_sp_dataframe_writer_save_mode: _Optional[
            _Union[SpDataframeWriterSaveMode, _Mapping]
        ] = ...,
        trait_sp_matched_clause: _Optional[_Union[SpMatchedClause, _Mapping]] = ...,
        trait_sp_relational_grouped_dataframe_expr: _Optional[
            _Union[SpRelationalGroupedDataframeExpr, _Mapping]
        ] = ...,
        trait_sp_write_file: _Optional[_Union[SpWriteFile, _Mapping]] = ...,
        trait_unary_op: _Optional[_Union[UnaryOp, _Mapping]] = ...,
        add: _Optional[_Union[Add, _Mapping]] = ...,
        apply_expr: _Optional[_Union[ApplyExpr, _Mapping]] = ...,
        big_decimal_val: _Optional[_Union[BigDecimalVal, _Mapping]] = ...,
        big_int_val: _Optional[_Union[BigIntVal, _Mapping]] = ...,
        binary_val: _Optional[_Union[BinaryVal, _Mapping]] = ...,
        bit_and: _Optional[_Union[BitAnd, _Mapping]] = ...,
        bit_or: _Optional[_Union[BitOr, _Mapping]] = ...,
        bit_xor: _Optional[_Union[BitXor, _Mapping]] = ...,
        bool_val: _Optional[_Union[BoolVal, _Mapping]] = ...,
        builtin_fn: _Optional[_Union[BuiltinFn, _Mapping]] = ...,
        call_table_function_expr: _Optional[
            _Union[CallTableFunctionExpr, _Mapping]
        ] = ...,
        cast_expr: _Optional[_Union[CastExpr, _Mapping]] = ...,
        date_val: _Optional[_Union[DateVal, _Mapping]] = ...,
        div: _Optional[_Union[Div, _Mapping]] = ...,
        eq: _Optional[_Union[Eq, _Mapping]] = ...,
        float64_val: _Optional[_Union[Float64Val, _Mapping]] = ...,
        fn_val: _Optional[_Union[FnVal, _Mapping]] = ...,
        geq: _Optional[_Union[Geq, _Mapping]] = ...,
        gt: _Optional[_Union[Gt, _Mapping]] = ...,
        if_expr: _Optional[_Union[IfExpr, _Mapping]] = ...,
        indirect_table_fn_id_ref: _Optional[
            _Union[IndirectTableFnIdRef, _Mapping]
        ] = ...,
        indirect_table_fn_name_ref: _Optional[
            _Union[IndirectTableFnNameRef, _Mapping]
        ] = ...,
        int32_val: _Optional[_Union[Int32Val, _Mapping]] = ...,
        int64_val: _Optional[_Union[Int64Val, _Mapping]] = ...,
        leq: _Optional[_Union[Leq, _Mapping]] = ...,
        list_val: _Optional[_Union[ListVal, _Mapping]] = ...,
        lt: _Optional[_Union[Lt, _Mapping]] = ...,
        mod: _Optional[_Union[Mod, _Mapping]] = ...,
        mul: _Optional[_Union[Mul, _Mapping]] = ...,
        neg: _Optional[_Union[Neg, _Mapping]] = ...,
        neq: _Optional[_Union[Neq, _Mapping]] = ...,
        none_val: _Optional[_Union[NoneVal, _Mapping]] = ...,
        null_val: _Optional[_Union[NullVal, _Mapping]] = ...,
        object_get_item: _Optional[_Union[ObjectGetItem, _Mapping]] = ...,
        pd_dataframe: _Optional[_Union[PdDataframe, _Mapping]] = ...,
        pd_dataframe_get_item: _Optional[_Union[PdDataframeGetItem, _Mapping]] = ...,
        pd_dataframe_i_loc: _Optional[_Union[PdDataframeILoc, _Mapping]] = ...,
        pd_dataframe_loc: _Optional[_Union[PdDataframeLoc, _Mapping]] = ...,
        pd_dataframe_set_item: _Optional[_Union[PdDataframeSetItem, _Mapping]] = ...,
        pd_drop_na: _Optional[_Union[PdDropNa, _Mapping]] = ...,
        pd_repr: _Optional[_Union[PdRepr, _Mapping]] = ...,
        pow: _Optional[_Union[Pow, _Mapping]] = ...,
        python_date_val: _Optional[_Union[PythonDateVal, _Mapping]] = ...,
        python_time_val: _Optional[_Union[PythonTimeVal, _Mapping]] = ...,
        python_timestamp_val: _Optional[_Union[PythonTimestampVal, _Mapping]] = ...,
        range_val: _Optional[_Union[RangeVal, _Mapping]] = ...,
        ref: _Optional[_Union[Ref, _Mapping]] = ...,
        seq_map_val: _Optional[_Union[SeqMapVal, _Mapping]] = ...,
        some_val: _Optional[_Union[SomeVal, _Mapping]] = ...,
        sp_column_alias: _Optional[_Union[SpColumnAlias, _Mapping]] = ...,
        sp_column_apply__int: _Optional[_Union[SpColumnApply_Int, _Mapping]] = ...,
        sp_column_apply__string: _Optional[
            _Union[SpColumnApply_String, _Mapping]
        ] = ...,
        sp_column_asc: _Optional[_Union[SpColumnAsc, _Mapping]] = ...,
        sp_column_between: _Optional[_Union[SpColumnBetween, _Mapping]] = ...,
        sp_column_case_when: _Optional[_Union[SpColumnCaseWhen, _Mapping]] = ...,
        sp_column_cast: _Optional[_Union[SpColumnCast, _Mapping]] = ...,
        sp_column_desc: _Optional[_Union[SpColumnDesc, _Mapping]] = ...,
        sp_column_equal_nan: _Optional[_Union[SpColumnEqualNan, _Mapping]] = ...,
        sp_column_equal_null: _Optional[_Union[SpColumnEqualNull, _Mapping]] = ...,
        sp_column_in__dataframe: _Optional[
            _Union[SpColumnIn_Dataframe, _Mapping]
        ] = ...,
        sp_column_in__seq: _Optional[_Union[SpColumnIn_Seq, _Mapping]] = ...,
        sp_column_is_not_null: _Optional[_Union[SpColumnIsNotNull, _Mapping]] = ...,
        sp_column_is_null: _Optional[_Union[SpColumnIsNull, _Mapping]] = ...,
        sp_column_name: _Optional[_Union[SpColumnName, _Mapping]] = ...,
        sp_column_over: _Optional[_Union[SpColumnOver, _Mapping]] = ...,
        sp_column_ref: _Optional[_Union[SpColumnRef, _Mapping]] = ...,
        sp_column_sql_expr: _Optional[_Union[SpColumnSqlExpr, _Mapping]] = ...,
        sp_column_string_collate: _Optional[
            _Union[SpColumnStringCollate, _Mapping]
        ] = ...,
        sp_column_string_contains: _Optional[
            _Union[SpColumnStringContains, _Mapping]
        ] = ...,
        sp_column_string_ends_with: _Optional[
            _Union[SpColumnStringEndsWith, _Mapping]
        ] = ...,
        sp_column_string_like: _Optional[_Union[SpColumnStringLike, _Mapping]] = ...,
        sp_column_string_regexp: _Optional[
            _Union[SpColumnStringRegexp, _Mapping]
        ] = ...,
        sp_column_string_starts_with: _Optional[
            _Union[SpColumnStringStartsWith, _Mapping]
        ] = ...,
        sp_column_string_substr: _Optional[
            _Union[SpColumnStringSubstr, _Mapping]
        ] = ...,
        sp_column_try_cast: _Optional[_Union[SpColumnTryCast, _Mapping]] = ...,
        sp_column_within_group: _Optional[_Union[SpColumnWithinGroup, _Mapping]] = ...,
        sp_create_dataframe: _Optional[_Union[SpCreateDataframe, _Mapping]] = ...,
        sp_dataframe_agg: _Optional[_Union[SpDataframeAgg, _Mapping]] = ...,
        sp_dataframe_alias: _Optional[_Union[SpDataframeAlias, _Mapping]] = ...,
        sp_dataframe_analytics_compute_lag: _Optional[
            _Union[SpDataframeAnalyticsComputeLag, _Mapping]
        ] = ...,
        sp_dataframe_analytics_compute_lead: _Optional[
            _Union[SpDataframeAnalyticsComputeLead, _Mapping]
        ] = ...,
        sp_dataframe_analytics_cumulative_agg: _Optional[
            _Union[SpDataframeAnalyticsCumulativeAgg, _Mapping]
        ] = ...,
        sp_dataframe_analytics_moving_agg: _Optional[
            _Union[SpDataframeAnalyticsMovingAgg, _Mapping]
        ] = ...,
        sp_dataframe_analytics_time_series_agg: _Optional[
            _Union[SpDataframeAnalyticsTimeSeriesAgg, _Mapping]
        ] = ...,
        sp_dataframe_apply: _Optional[_Union[SpDataframeApply, _Mapping]] = ...,
        sp_dataframe_cache_result: _Optional[
            _Union[SpDataframeCacheResult, _Mapping]
        ] = ...,
        sp_dataframe_col: _Optional[_Union[SpDataframeCol, _Mapping]] = ...,
        sp_dataframe_collect: _Optional[_Union[SpDataframeCollect, _Mapping]] = ...,
        sp_dataframe_copy_into_table: _Optional[
            _Union[SpDataframeCopyIntoTable, _Mapping]
        ] = ...,
        sp_dataframe_count: _Optional[_Union[SpDataframeCount, _Mapping]] = ...,
        sp_dataframe_create_or_replace_dynamic_table: _Optional[
            _Union[SpDataframeCreateOrReplaceDynamicTable, _Mapping]
        ] = ...,
        sp_dataframe_create_or_replace_view: _Optional[
            _Union[SpDataframeCreateOrReplaceView, _Mapping]
        ] = ...,
        sp_dataframe_cross_join: _Optional[
            _Union[SpDataframeCrossJoin, _Mapping]
        ] = ...,
        sp_dataframe_cube: _Optional[_Union[SpDataframeCube, _Mapping]] = ...,
        sp_dataframe_describe: _Optional[_Union[SpDataframeDescribe, _Mapping]] = ...,
        sp_dataframe_distinct: _Optional[_Union[SpDataframeDistinct, _Mapping]] = ...,
        sp_dataframe_drop: _Optional[_Union[SpDataframeDrop, _Mapping]] = ...,
        sp_dataframe_drop_duplicates: _Optional[
            _Union[SpDataframeDropDuplicates, _Mapping]
        ] = ...,
        sp_dataframe_except: _Optional[_Union[SpDataframeExcept, _Mapping]] = ...,
        sp_dataframe_filter: _Optional[_Union[SpDataframeFilter, _Mapping]] = ...,
        sp_dataframe_first: _Optional[_Union[SpDataframeFirst, _Mapping]] = ...,
        sp_dataframe_flatten: _Optional[_Union[SpDataframeFlatten, _Mapping]] = ...,
        sp_dataframe_group_by: _Optional[_Union[SpDataframeGroupBy, _Mapping]] = ...,
        sp_dataframe_group_by_grouping_sets: _Optional[
            _Union[SpDataframeGroupByGroupingSets, _Mapping]
        ] = ...,
        sp_dataframe_group_by__columns: _Optional[
            _Union[SpDataframeGroupBy_Columns, _Mapping]
        ] = ...,
        sp_dataframe_group_by__strings: _Optional[
            _Union[SpDataframeGroupBy_Strings, _Mapping]
        ] = ...,
        sp_dataframe_intersect: _Optional[_Union[SpDataframeIntersect, _Mapping]] = ...,
        sp_dataframe_join: _Optional[_Union[SpDataframeJoin, _Mapping]] = ...,
        sp_dataframe_join_table_function: _Optional[
            _Union[SpDataframeJoinTableFunction, _Mapping]
        ] = ...,
        sp_dataframe_join__dataframe__join_exprs: _Optional[
            _Union[SpDataframeJoin_Dataframe_JoinExprs, _Mapping]
        ] = ...,
        sp_dataframe_join__dataframe__using_columns: _Optional[
            _Union[SpDataframeJoin_Dataframe_UsingColumns, _Mapping]
        ] = ...,
        sp_dataframe_limit: _Optional[_Union[SpDataframeLimit, _Mapping]] = ...,
        sp_dataframe_na_drop__python: _Optional[
            _Union[SpDataframeNaDrop_Python, _Mapping]
        ] = ...,
        sp_dataframe_na_drop__scala: _Optional[
            _Union[SpDataframeNaDrop_Scala, _Mapping]
        ] = ...,
        sp_dataframe_na_fill: _Optional[_Union[SpDataframeNaFill, _Mapping]] = ...,
        sp_dataframe_na_replace: _Optional[
            _Union[SpDataframeNaReplace, _Mapping]
        ] = ...,
        sp_dataframe_natural_join: _Optional[
            _Union[SpDataframeNaturalJoin, _Mapping]
        ] = ...,
        sp_dataframe_pivot: _Optional[_Union[SpDataframePivot, _Mapping]] = ...,
        sp_dataframe_random_split: _Optional[
            _Union[SpDataframeRandomSplit, _Mapping]
        ] = ...,
        sp_dataframe_ref: _Optional[_Union[SpDataframeRef, _Mapping]] = ...,
        sp_dataframe_rename: _Optional[_Union[SpDataframeRename, _Mapping]] = ...,
        sp_dataframe_rollup: _Optional[_Union[SpDataframeRollup, _Mapping]] = ...,
        sp_dataframe_rollup__columns: _Optional[
            _Union[SpDataframeRollup_Columns, _Mapping]
        ] = ...,
        sp_dataframe_rollup__strings: _Optional[
            _Union[SpDataframeRollup_Strings, _Mapping]
        ] = ...,
        sp_dataframe_sample: _Optional[_Union[SpDataframeSample, _Mapping]] = ...,
        sp_dataframe_select__columns: _Optional[
            _Union[SpDataframeSelect_Columns, _Mapping]
        ] = ...,
        sp_dataframe_select__exprs: _Optional[
            _Union[SpDataframeSelect_Exprs, _Mapping]
        ] = ...,
        sp_dataframe_show: _Optional[_Union[SpDataframeShow, _Mapping]] = ...,
        sp_dataframe_sort: _Optional[_Union[SpDataframeSort, _Mapping]] = ...,
        sp_dataframe_stat_approx_quantile: _Optional[
            _Union[SpDataframeStatApproxQuantile, _Mapping]
        ] = ...,
        sp_dataframe_stat_corr: _Optional[_Union[SpDataframeStatCorr, _Mapping]] = ...,
        sp_dataframe_stat_cov: _Optional[_Union[SpDataframeStatCov, _Mapping]] = ...,
        sp_dataframe_stat_cross_tab: _Optional[
            _Union[SpDataframeStatCrossTab, _Mapping]
        ] = ...,
        sp_dataframe_stat_sample_by: _Optional[
            _Union[SpDataframeStatSampleBy, _Mapping]
        ] = ...,
        sp_dataframe_to_df: _Optional[_Union[SpDataframeToDf, _Mapping]] = ...,
        sp_dataframe_to_local_iterator: _Optional[
            _Union[SpDataframeToLocalIterator, _Mapping]
        ] = ...,
        sp_dataframe_to_pandas: _Optional[_Union[SpDataframeToPandas, _Mapping]] = ...,
        sp_dataframe_to_pandas_batches: _Optional[
            _Union[SpDataframeToPandasBatches, _Mapping]
        ] = ...,
        sp_dataframe_union: _Optional[_Union[SpDataframeUnion, _Mapping]] = ...,
        sp_dataframe_union_all: _Optional[_Union[SpDataframeUnionAll, _Mapping]] = ...,
        sp_dataframe_union_all_by_name: _Optional[
            _Union[SpDataframeUnionAllByName, _Mapping]
        ] = ...,
        sp_dataframe_union_by_name: _Optional[
            _Union[SpDataframeUnionByName, _Mapping]
        ] = ...,
        sp_dataframe_unpivot: _Optional[_Union[SpDataframeUnpivot, _Mapping]] = ...,
        sp_dataframe_where: _Optional[_Union[SpDataframeWhere, _Mapping]] = ...,
        sp_dataframe_with_column: _Optional[
            _Union[SpDataframeWithColumn, _Mapping]
        ] = ...,
        sp_dataframe_with_column_renamed: _Optional[
            _Union[SpDataframeWithColumnRenamed, _Mapping]
        ] = ...,
        sp_dataframe_with_columns: _Optional[
            _Union[SpDataframeWithColumns, _Mapping]
        ] = ...,
        sp_dataframe_write: _Optional[_Union[SpDataframeWrite, _Mapping]] = ...,
        sp_datatype_val: _Optional[_Union[SpDatatypeVal, _Mapping]] = ...,
        sp_flatten: _Optional[_Union[SpFlatten, _Mapping]] = ...,
        sp_fn_ref: _Optional[_Union[SpFnRef, _Mapping]] = ...,
        sp_generator: _Optional[_Union[SpGenerator, _Mapping]] = ...,
        sp_grouping_sets: _Optional[_Union[SpGroupingSets, _Mapping]] = ...,
        sp_merge_delete_when_matched_clause: _Optional[
            _Union[SpMergeDeleteWhenMatchedClause, _Mapping]
        ] = ...,
        sp_merge_insert_when_not_matched_clause: _Optional[
            _Union[SpMergeInsertWhenNotMatchedClause, _Mapping]
        ] = ...,
        sp_merge_update_when_matched_clause: _Optional[
            _Union[SpMergeUpdateWhenMatchedClause, _Mapping]
        ] = ...,
        sp_range: _Optional[_Union[SpRange, _Mapping]] = ...,
        sp_read_avro: _Optional[_Union[SpReadAvro, _Mapping]] = ...,
        sp_read_csv: _Optional[_Union[SpReadCsv, _Mapping]] = ...,
        sp_read_json: _Optional[_Union[SpReadJson, _Mapping]] = ...,
        sp_read_orc: _Optional[_Union[SpReadOrc, _Mapping]] = ...,
        sp_read_parquet: _Optional[_Union[SpReadParquet, _Mapping]] = ...,
        sp_read_table: _Optional[_Union[SpReadTable, _Mapping]] = ...,
        sp_read_xml: _Optional[_Union[SpReadXml, _Mapping]] = ...,
        sp_relational_grouped_dataframe_agg: _Optional[
            _Union[SpRelationalGroupedDataframeAgg, _Mapping]
        ] = ...,
        sp_relational_grouped_dataframe_apply_in_pandas: _Optional[
            _Union[SpRelationalGroupedDataframeApplyInPandas, _Mapping]
        ] = ...,
        sp_relational_grouped_dataframe_builtin: _Optional[
            _Union[SpRelationalGroupedDataframeBuiltin, _Mapping]
        ] = ...,
        sp_relational_grouped_dataframe_pivot: _Optional[
            _Union[SpRelationalGroupedDataframePivot, _Mapping]
        ] = ...,
        sp_relational_grouped_dataframe_ref: _Optional[
            _Union[SpRelationalGroupedDataframeRef, _Mapping]
        ] = ...,
        sp_row: _Optional[_Union[SpRow, _Mapping]] = ...,
        sp_session_table_function: _Optional[
            _Union[SpSessionTableFunction, _Mapping]
        ] = ...,
        sp_sql: _Optional[_Union[SpSql, _Mapping]] = ...,
        sp_stored_procedure: _Optional[_Union[SpStoredProcedure, _Mapping]] = ...,
        sp_table: _Optional[_Union[SpTable, _Mapping]] = ...,
        sp_table_delete: _Optional[_Union[SpTableDelete, _Mapping]] = ...,
        sp_table_drop_table: _Optional[_Union[SpTableDropTable, _Mapping]] = ...,
        sp_table_fn_call_alias: _Optional[_Union[SpTableFnCallAlias, _Mapping]] = ...,
        sp_table_fn_call_over: _Optional[_Union[SpTableFnCallOver, _Mapping]] = ...,
        sp_table_merge: _Optional[_Union[SpTableMerge, _Mapping]] = ...,
        sp_table_sample: _Optional[_Union[SpTableSample, _Mapping]] = ...,
        sp_table_update: _Optional[_Union[SpTableUpdate, _Mapping]] = ...,
        sp_write_copy_into_location: _Optional[
            _Union[SpWriteCopyIntoLocation, _Mapping]
        ] = ...,
        sp_write_csv: _Optional[_Union[SpWriteCsv, _Mapping]] = ...,
        sp_write_json: _Optional[_Union[SpWriteJson, _Mapping]] = ...,
        sp_write_pandas: _Optional[_Union[SpWritePandas, _Mapping]] = ...,
        sp_write_parquet: _Optional[_Union[SpWriteParquet, _Mapping]] = ...,
        sp_write_table: _Optional[_Union[SpWriteTable, _Mapping]] = ...,
        stored_procedure: _Optional[_Union[StoredProcedure, _Mapping]] = ...,
        string_val: _Optional[_Union[StringVal, _Mapping]] = ...,
        sub: _Optional[_Union[Sub, _Mapping]] = ...,
        time_val: _Optional[_Union[TimeVal, _Mapping]] = ...,
        timestamp_val: _Optional[_Union[TimestampVal, _Mapping]] = ...,
        tuple_val: _Optional[_Union[TupleVal, _Mapping]] = ...,
        udaf: _Optional[_Union[Udaf, _Mapping]] = ...,
        udf: _Optional[_Union[Udf, _Mapping]] = ...,
        udtf: _Optional[_Union[Udtf, _Mapping]] = ...,
        **kwargs
    ) -> None: ...

class Ref(_message.Message):
    __slots__ = ("src", "var_id")
    SRC_FIELD_NUMBER: _ClassVar[int]
    VAR_ID_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    var_id: VarId
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        var_id: _Optional[_Union[VarId, _Mapping]] = ...,
    ) -> None: ...

class CastExpr(_message.Message):
    __slots__ = ("src", "typ", "v")
    SRC_FIELD_NUMBER: _ClassVar[int]
    TYP_FIELD_NUMBER: _ClassVar[int]
    V_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    typ: Type
    v: Expr
    def __init__(
        self,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        typ: _Optional[_Union[Type, _Mapping]] = ...,
        v: _Optional[_Union[Expr, _Mapping]] = ...,
    ) -> None: ...

class ObjectGetItem(_message.Message):
    __slots__ = ("args", "obj", "src")
    ARGS_FIELD_NUMBER: _ClassVar[int]
    OBJ_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    args: _containers.RepeatedCompositeFieldContainer[Expr]
    obj: VarId
    src: SrcPosition
    def __init__(
        self,
        args: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        obj: _Optional[_Union[VarId, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class PdDataframe(_message.Message):
    __slots__ = ("columns", "data", "dtype", "index", "src")
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    DATA_FIELD_NUMBER: _ClassVar[int]
    DTYPE_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    columns: Expr
    data: Expr
    dtype: Type
    index: Expr
    src: SrcPosition
    def __init__(
        self,
        columns: _Optional[_Union[Expr, _Mapping]] = ...,
        data: _Optional[_Union[Expr, _Mapping]] = ...,
        dtype: _Optional[_Union[Type, _Mapping]] = ...,
        index: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class PdDropNa(_message.Message):
    __slots__ = ("axis", "df", "src", "subset", "thresh")
    AXIS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    SUBSET_FIELD_NUMBER: _ClassVar[int]
    THRESH_FIELD_NUMBER: _ClassVar[int]
    axis: Expr
    df: Ref
    src: SrcPosition
    subset: Expr
    thresh: Expr
    def __init__(
        self,
        axis: _Optional[_Union[Expr, _Mapping]] = ...,
        df: _Optional[_Union[Ref, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        subset: _Optional[_Union[Expr, _Mapping]] = ...,
        thresh: _Optional[_Union[Expr, _Mapping]] = ...,
    ) -> None: ...

class PdDataframeGetItem(_message.Message):
    __slots__ = ("df", "key_arg", "src")
    DF_FIELD_NUMBER: _ClassVar[int]
    KEY_ARG_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df: Ref
    key_arg: Expr
    src: SrcPosition
    def __init__(
        self,
        df: _Optional[_Union[Ref, _Mapping]] = ...,
        key_arg: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class PdDataframeSetItem(_message.Message):
    __slots__ = ("df", "key_arg", "src", "v")
    DF_FIELD_NUMBER: _ClassVar[int]
    KEY_ARG_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    V_FIELD_NUMBER: _ClassVar[int]
    df: Ref
    key_arg: Expr
    src: SrcPosition
    v: Expr
    def __init__(
        self,
        df: _Optional[_Union[Ref, _Mapping]] = ...,
        key_arg: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        v: _Optional[_Union[Expr, _Mapping]] = ...,
    ) -> None: ...

class PdDataframeLoc(_message.Message):
    __slots__ = ("columns", "df", "rows", "src")
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    ROWS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    columns: Expr
    df: Ref
    rows: Expr
    src: SrcPosition
    def __init__(
        self,
        columns: _Optional[_Union[Expr, _Mapping]] = ...,
        df: _Optional[_Union[Ref, _Mapping]] = ...,
        rows: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class PdDataframeILoc(_message.Message):
    __slots__ = ("columns", "df", "rows", "src")
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    ROWS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    columns: Expr
    df: Ref
    rows: Expr
    src: SrcPosition
    def __init__(
        self,
        columns: _Optional[_Union[Expr, _Mapping]] = ...,
        df: _Optional[_Union[Ref, _Mapping]] = ...,
        rows: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class PdRepr(_message.Message):
    __slots__ = ("max_columns", "max_rows", "src", "v")
    ASYNC_FIELD_NUMBER: _ClassVar[int]
    MAX_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    MAX_ROWS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    V_FIELD_NUMBER: _ClassVar[int]
    max_columns: int
    max_rows: int
    src: SrcPosition
    v: Ref
    def __init__(
        self,
        max_columns: _Optional[int] = ...,
        max_rows: _Optional[int] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        v: _Optional[_Union[Ref, _Mapping]] = ...,
        **kwargs
    ) -> None: ...

class PdReprResult(_message.Message):
    __slots__ = (
        "num_columns",
        "num_head_columns",
        "num_head_rows",
        "num_rows",
        "value",
    )
    NUM_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    NUM_HEAD_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    NUM_HEAD_ROWS_FIELD_NUMBER: _ClassVar[int]
    NUM_ROWS_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    num_columns: int
    num_head_columns: int
    num_head_rows: int
    num_rows: int
    value: bytes
    def __init__(
        self,
        num_columns: _Optional[int] = ...,
        num_head_columns: _Optional[int] = ...,
        num_head_rows: _Optional[int] = ...,
        num_rows: _Optional[int] = ...,
        value: _Optional[bytes] = ...,
    ) -> None: ...

class Result(_message.Message):
    __slots__ = ("trait_error", "eval_ok", "session_reset_required_error")
    TRAIT_ERROR_FIELD_NUMBER: _ClassVar[int]
    EVAL_OK_FIELD_NUMBER: _ClassVar[int]
    SESSION_RESET_REQUIRED_ERROR_FIELD_NUMBER: _ClassVar[int]
    trait_error: Error
    eval_ok: EvalOk
    session_reset_required_error: SessionResetRequiredError
    def __init__(
        self,
        trait_error: _Optional[_Union[Error, _Mapping]] = ...,
        eval_ok: _Optional[_Union[EvalOk, _Mapping]] = ...,
        session_reset_required_error: _Optional[
            _Union[SessionResetRequiredError, _Mapping]
        ] = ...,
    ) -> None: ...

class EvalResult(_message.Message):
    __slots__ = (
        "trait_const",
        "big_decimal_val",
        "big_int_val",
        "binary_val",
        "bool_val",
        "date_val",
        "float64_val",
        "fn_val",
        "int32_val",
        "int64_val",
        "none_val",
        "null_val",
        "pd_repr_result",
        "python_date_val",
        "python_time_val",
        "python_timestamp_val",
        "sp_datatype_val",
        "string_val",
        "time_val",
        "timestamp_val",
    )
    TRAIT_CONST_FIELD_NUMBER: _ClassVar[int]
    BIG_DECIMAL_VAL_FIELD_NUMBER: _ClassVar[int]
    BIG_INT_VAL_FIELD_NUMBER: _ClassVar[int]
    BINARY_VAL_FIELD_NUMBER: _ClassVar[int]
    BOOL_VAL_FIELD_NUMBER: _ClassVar[int]
    DATE_VAL_FIELD_NUMBER: _ClassVar[int]
    FLOAT64_VAL_FIELD_NUMBER: _ClassVar[int]
    FN_VAL_FIELD_NUMBER: _ClassVar[int]
    INT32_VAL_FIELD_NUMBER: _ClassVar[int]
    INT64_VAL_FIELD_NUMBER: _ClassVar[int]
    NONE_VAL_FIELD_NUMBER: _ClassVar[int]
    NULL_VAL_FIELD_NUMBER: _ClassVar[int]
    PD_REPR_RESULT_FIELD_NUMBER: _ClassVar[int]
    PYTHON_DATE_VAL_FIELD_NUMBER: _ClassVar[int]
    PYTHON_TIME_VAL_FIELD_NUMBER: _ClassVar[int]
    PYTHON_TIMESTAMP_VAL_FIELD_NUMBER: _ClassVar[int]
    SP_DATATYPE_VAL_FIELD_NUMBER: _ClassVar[int]
    STRING_VAL_FIELD_NUMBER: _ClassVar[int]
    TIME_VAL_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_VAL_FIELD_NUMBER: _ClassVar[int]
    trait_const: Const
    big_decimal_val: BigDecimalVal
    big_int_val: BigIntVal
    binary_val: BinaryVal
    bool_val: BoolVal
    date_val: DateVal
    float64_val: Float64Val
    fn_val: FnVal
    int32_val: Int32Val
    int64_val: Int64Val
    none_val: NoneVal
    null_val: NullVal
    pd_repr_result: PdReprResult
    python_date_val: PythonDateVal
    python_time_val: PythonTimeVal
    python_timestamp_val: PythonTimestampVal
    sp_datatype_val: SpDatatypeVal
    string_val: StringVal
    time_val: TimeVal
    timestamp_val: TimestampVal
    def __init__(
        self,
        trait_const: _Optional[_Union[Const, _Mapping]] = ...,
        big_decimal_val: _Optional[_Union[BigDecimalVal, _Mapping]] = ...,
        big_int_val: _Optional[_Union[BigIntVal, _Mapping]] = ...,
        binary_val: _Optional[_Union[BinaryVal, _Mapping]] = ...,
        bool_val: _Optional[_Union[BoolVal, _Mapping]] = ...,
        date_val: _Optional[_Union[DateVal, _Mapping]] = ...,
        float64_val: _Optional[_Union[Float64Val, _Mapping]] = ...,
        fn_val: _Optional[_Union[FnVal, _Mapping]] = ...,
        int32_val: _Optional[_Union[Int32Val, _Mapping]] = ...,
        int64_val: _Optional[_Union[Int64Val, _Mapping]] = ...,
        none_val: _Optional[_Union[NoneVal, _Mapping]] = ...,
        null_val: _Optional[_Union[NullVal, _Mapping]] = ...,
        pd_repr_result: _Optional[_Union[PdReprResult, _Mapping]] = ...,
        python_date_val: _Optional[_Union[PythonDateVal, _Mapping]] = ...,
        python_time_val: _Optional[_Union[PythonTimeVal, _Mapping]] = ...,
        python_timestamp_val: _Optional[_Union[PythonTimestampVal, _Mapping]] = ...,
        sp_datatype_val: _Optional[_Union[SpDatatypeVal, _Mapping]] = ...,
        string_val: _Optional[_Union[StringVal, _Mapping]] = ...,
        time_val: _Optional[_Union[TimeVal, _Mapping]] = ...,
        timestamp_val: _Optional[_Union[TimestampVal, _Mapping]] = ...,
    ) -> None: ...

class EvalOk(_message.Message):
    __slots__ = ("data", "uid", "var_id")
    DATA_FIELD_NUMBER: _ClassVar[int]
    UID_FIELD_NUMBER: _ClassVar[int]
    VAR_ID_FIELD_NUMBER: _ClassVar[int]
    data: EvalResult
    uid: int
    var_id: VarId
    def __init__(
        self,
        data: _Optional[_Union[EvalResult, _Mapping]] = ...,
        uid: _Optional[int] = ...,
        var_id: _Optional[_Union[VarId, _Mapping]] = ...,
    ) -> None: ...

class Error(_message.Message):
    __slots__ = ("session_reset_required_error",)
    SESSION_RESET_REQUIRED_ERROR_FIELD_NUMBER: _ClassVar[int]
    session_reset_required_error: SessionResetRequiredError
    def __init__(
        self,
        session_reset_required_error: _Optional[
            _Union[SessionResetRequiredError, _Mapping]
        ] = ...,
    ) -> None: ...

class SessionResetRequiredError(_message.Message):
    __slots__ = ("uid", "var_id")
    UID_FIELD_NUMBER: _ClassVar[int]
    VAR_ID_FIELD_NUMBER: _ClassVar[int]
    uid: int
    var_id: VarId
    def __init__(
        self,
        uid: _Optional[int] = ...,
        var_id: _Optional[_Union[VarId, _Mapping]] = ...,
    ) -> None: ...

class SpColumnExpr(_message.Message):
    __slots__ = (
        "sp_column_case_when",
        "sp_column_equal_null",
        "sp_column_ref",
        "sp_column_sql_expr",
        "sp_dataframe_apply",
        "sp_dataframe_col",
    )
    SP_COLUMN_CASE_WHEN_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_EQUAL_NULL_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_REF_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_SQL_EXPR_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_APPLY_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_COL_FIELD_NUMBER: _ClassVar[int]
    sp_column_case_when: SpColumnCaseWhen
    sp_column_equal_null: SpColumnEqualNull
    sp_column_ref: SpColumnRef
    sp_column_sql_expr: SpColumnSqlExpr
    sp_dataframe_apply: SpDataframeApply
    sp_dataframe_col: SpDataframeCol
    def __init__(
        self,
        sp_column_case_when: _Optional[_Union[SpColumnCaseWhen, _Mapping]] = ...,
        sp_column_equal_null: _Optional[_Union[SpColumnEqualNull, _Mapping]] = ...,
        sp_column_ref: _Optional[_Union[SpColumnRef, _Mapping]] = ...,
        sp_column_sql_expr: _Optional[_Union[SpColumnSqlExpr, _Mapping]] = ...,
        sp_dataframe_apply: _Optional[_Union[SpDataframeApply, _Mapping]] = ...,
        sp_dataframe_col: _Optional[_Union[SpDataframeCol, _Mapping]] = ...,
    ) -> None: ...

class SpColumnRef(_message.Message):
    __slots__ = ("id", "src")
    ID_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    id: str
    src: SrcPosition
    def __init__(
        self,
        id: _Optional[str] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnSqlExpr(_message.Message):
    __slots__ = ("df_alias", "sql", "src")
    DF_ALIAS_FIELD_NUMBER: _ClassVar[int]
    SQL_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df_alias: _wrappers_pb2.StringValue
    sql: str
    src: SrcPosition
    def __init__(
        self,
        df_alias: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        sql: _Optional[str] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpCaseExpr(_message.Message):
    __slots__ = ("condition", "src", "value")
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    condition: Expr
    src: SrcPosition
    value: Expr
    def __init__(
        self,
        condition: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        value: _Optional[_Union[Expr, _Mapping]] = ...,
    ) -> None: ...

class SpColumnCaseWhen(_message.Message):
    __slots__ = ("cases", "src")
    CASES_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    cases: _containers.RepeatedCompositeFieldContainer[SpCaseExpr]
    src: SrcPosition
    def __init__(
        self,
        cases: _Optional[_Iterable[_Union[SpCaseExpr, _Mapping]]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnFn(_message.Message):
    __slots__ = (
        "sp_column_alias",
        "sp_column_apply__int",
        "sp_column_apply__string",
        "sp_column_asc",
        "sp_column_between",
        "sp_column_cast",
        "sp_column_desc",
        "sp_column_equal_nan",
        "sp_column_in__dataframe",
        "sp_column_in__seq",
        "sp_column_is_not_null",
        "sp_column_is_null",
        "sp_column_name",
        "sp_column_over",
        "sp_column_string_collate",
        "sp_column_string_contains",
        "sp_column_string_ends_with",
        "sp_column_string_like",
        "sp_column_string_regexp",
        "sp_column_string_starts_with",
        "sp_column_string_substr",
        "sp_column_try_cast",
        "sp_column_within_group",
    )
    SP_COLUMN_ALIAS_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_APPLY__INT_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_APPLY__STRING_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_ASC_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_BETWEEN_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_CAST_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_DESC_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_EQUAL_NAN_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_IN__DATAFRAME_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_IN__SEQ_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_IS_NOT_NULL_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_IS_NULL_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_NAME_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_OVER_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_COLLATE_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_CONTAINS_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_ENDS_WITH_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_LIKE_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_REGEXP_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_STARTS_WITH_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_SUBSTR_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_TRY_CAST_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_WITHIN_GROUP_FIELD_NUMBER: _ClassVar[int]
    sp_column_alias: SpColumnAlias
    sp_column_apply__int: SpColumnApply_Int
    sp_column_apply__string: SpColumnApply_String
    sp_column_asc: SpColumnAsc
    sp_column_between: SpColumnBetween
    sp_column_cast: SpColumnCast
    sp_column_desc: SpColumnDesc
    sp_column_equal_nan: SpColumnEqualNan
    sp_column_in__dataframe: SpColumnIn_Dataframe
    sp_column_in__seq: SpColumnIn_Seq
    sp_column_is_not_null: SpColumnIsNotNull
    sp_column_is_null: SpColumnIsNull
    sp_column_name: SpColumnName
    sp_column_over: SpColumnOver
    sp_column_string_collate: SpColumnStringCollate
    sp_column_string_contains: SpColumnStringContains
    sp_column_string_ends_with: SpColumnStringEndsWith
    sp_column_string_like: SpColumnStringLike
    sp_column_string_regexp: SpColumnStringRegexp
    sp_column_string_starts_with: SpColumnStringStartsWith
    sp_column_string_substr: SpColumnStringSubstr
    sp_column_try_cast: SpColumnTryCast
    sp_column_within_group: SpColumnWithinGroup
    def __init__(
        self,
        sp_column_alias: _Optional[_Union[SpColumnAlias, _Mapping]] = ...,
        sp_column_apply__int: _Optional[_Union[SpColumnApply_Int, _Mapping]] = ...,
        sp_column_apply__string: _Optional[
            _Union[SpColumnApply_String, _Mapping]
        ] = ...,
        sp_column_asc: _Optional[_Union[SpColumnAsc, _Mapping]] = ...,
        sp_column_between: _Optional[_Union[SpColumnBetween, _Mapping]] = ...,
        sp_column_cast: _Optional[_Union[SpColumnCast, _Mapping]] = ...,
        sp_column_desc: _Optional[_Union[SpColumnDesc, _Mapping]] = ...,
        sp_column_equal_nan: _Optional[_Union[SpColumnEqualNan, _Mapping]] = ...,
        sp_column_in__dataframe: _Optional[
            _Union[SpColumnIn_Dataframe, _Mapping]
        ] = ...,
        sp_column_in__seq: _Optional[_Union[SpColumnIn_Seq, _Mapping]] = ...,
        sp_column_is_not_null: _Optional[_Union[SpColumnIsNotNull, _Mapping]] = ...,
        sp_column_is_null: _Optional[_Union[SpColumnIsNull, _Mapping]] = ...,
        sp_column_name: _Optional[_Union[SpColumnName, _Mapping]] = ...,
        sp_column_over: _Optional[_Union[SpColumnOver, _Mapping]] = ...,
        sp_column_string_collate: _Optional[
            _Union[SpColumnStringCollate, _Mapping]
        ] = ...,
        sp_column_string_contains: _Optional[
            _Union[SpColumnStringContains, _Mapping]
        ] = ...,
        sp_column_string_ends_with: _Optional[
            _Union[SpColumnStringEndsWith, _Mapping]
        ] = ...,
        sp_column_string_like: _Optional[_Union[SpColumnStringLike, _Mapping]] = ...,
        sp_column_string_regexp: _Optional[
            _Union[SpColumnStringRegexp, _Mapping]
        ] = ...,
        sp_column_string_starts_with: _Optional[
            _Union[SpColumnStringStartsWith, _Mapping]
        ] = ...,
        sp_column_string_substr: _Optional[
            _Union[SpColumnStringSubstr, _Mapping]
        ] = ...,
        sp_column_try_cast: _Optional[_Union[SpColumnTryCast, _Mapping]] = ...,
        sp_column_within_group: _Optional[_Union[SpColumnWithinGroup, _Mapping]] = ...,
    ) -> None: ...

class SpColumnAlias(_message.Message):
    __slots__ = ("col", "name", "src", "variant_is_as")
    COL_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VARIANT_IS_AS_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    name: str
    src: SrcPosition
    variant_is_as: _wrappers_pb2.BoolValue
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        name: _Optional[str] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        variant_is_as: _Optional[_Union[_wrappers_pb2.BoolValue, _Mapping]] = ...,
    ) -> None: ...

class SpColumnApply_Int(_message.Message):
    __slots__ = ("col", "idx", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    IDX_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    idx: int
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        idx: _Optional[int] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnApply_String(_message.Message):
    __slots__ = ("col", "field", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    FIELD_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    field: str
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        field: _Optional[str] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnAsc(_message.Message):
    __slots__ = ("col", "nulls_first", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    NULLS_FIRST_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    nulls_first: _wrappers_pb2.BoolValue
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        nulls_first: _Optional[_Union[_wrappers_pb2.BoolValue, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnBetween(_message.Message):
    __slots__ = ("col", "lower_bound", "src", "upper_bound")
    COL_FIELD_NUMBER: _ClassVar[int]
    LOWER_BOUND_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    UPPER_BOUND_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    lower_bound: Expr
    src: SrcPosition
    upper_bound: Expr
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        lower_bound: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        upper_bound: _Optional[_Union[Expr, _Mapping]] = ...,
    ) -> None: ...

class SpColumnCast(_message.Message):
    __slots__ = ("col", "src", "to")
    COL_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    src: SrcPosition
    to: SpDataType
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        to: _Optional[_Union[SpDataType, _Mapping]] = ...,
    ) -> None: ...

class SpColumnTryCast(_message.Message):
    __slots__ = ("col", "src", "to")
    COL_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    TO_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    src: SrcPosition
    to: SpDataType
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        to: _Optional[_Union[SpDataType, _Mapping]] = ...,
    ) -> None: ...

class SpColumnDesc(_message.Message):
    __slots__ = ("col", "nulls_first", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    NULLS_FIRST_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    nulls_first: _wrappers_pb2.BoolValue
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        nulls_first: _Optional[_Union[_wrappers_pb2.BoolValue, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnEqualNan(_message.Message):
    __slots__ = ("col", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnEqualNull(_message.Message):
    __slots__ = ("lhs", "rhs", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    rhs: Expr
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnIn_Dataframe(_message.Message):
    __slots__ = ("col", "df", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    df: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnIn_Seq(_message.Message):
    __slots__ = ("col", "src", "values")
    COL_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    src: SrcPosition
    values: _containers.RepeatedCompositeFieldContainer[Expr]
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        values: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
    ) -> None: ...

class SpColumnIsNotNull(_message.Message):
    __slots__ = ("col", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnIsNull(_message.Message):
    __slots__ = ("col", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnName(_message.Message):
    __slots__ = ("alias", "col", "src")
    ALIAS_FIELD_NUMBER: _ClassVar[int]
    COL_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    alias: str
    col: Expr
    src: SrcPosition
    def __init__(
        self,
        alias: _Optional[str] = ...,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnOver(_message.Message):
    __slots__ = ("col", "src", "window_spec")
    COL_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    WINDOW_SPEC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    src: SrcPosition
    window_spec: SpWindowSpecExpr
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        window_spec: _Optional[_Union[SpWindowSpecExpr, _Mapping]] = ...,
    ) -> None: ...

class SpColumnWithinGroup(_message.Message):
    __slots__ = ("col", "cols", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    COLS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    cols: ExprArgList
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        cols: _Optional[_Union[ExprArgList, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnStringLike(_message.Message):
    __slots__ = ("col", "pattern", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    pattern: Expr
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        pattern: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnStringRegexp(_message.Message):
    __slots__ = ("col", "parameters", "pattern", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    PARAMETERS_FIELD_NUMBER: _ClassVar[int]
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    parameters: Expr
    pattern: Expr
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        parameters: _Optional[_Union[Expr, _Mapping]] = ...,
        pattern: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnStringStartsWith(_message.Message):
    __slots__ = ("col", "prefix", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    PREFIX_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    prefix: Expr
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        prefix: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnStringEndsWith(_message.Message):
    __slots__ = ("col", "src", "suffix")
    COL_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    SUFFIX_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    src: SrcPosition
    suffix: Expr
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        suffix: _Optional[_Union[Expr, _Mapping]] = ...,
    ) -> None: ...

class SpColumnStringSubstr(_message.Message):
    __slots__ = ("col", "len", "pos", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    LEN_FIELD_NUMBER: _ClassVar[int]
    POS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    len: Expr
    pos: Expr
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        len: _Optional[_Union[Expr, _Mapping]] = ...,
        pos: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnStringCollate(_message.Message):
    __slots__ = ("col", "collation_spec", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    COLLATION_SPEC_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    collation_spec: Expr
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        collation_spec: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpColumnStringContains(_message.Message):
    __slots__ = ("col", "pattern", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    pattern: Expr
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        pattern: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeExpr(_message.Message):
    __slots__ = (
        "sp_create_dataframe",
        "sp_dataframe_agg",
        "sp_dataframe_alias",
        "sp_dataframe_analytics_compute_lag",
        "sp_dataframe_analytics_compute_lead",
        "sp_dataframe_analytics_cumulative_agg",
        "sp_dataframe_analytics_moving_agg",
        "sp_dataframe_analytics_time_series_agg",
        "sp_dataframe_collect",
        "sp_dataframe_count",
        "sp_dataframe_cross_join",
        "sp_dataframe_describe",
        "sp_dataframe_distinct",
        "sp_dataframe_drop",
        "sp_dataframe_drop_duplicates",
        "sp_dataframe_except",
        "sp_dataframe_filter",
        "sp_dataframe_first",
        "sp_dataframe_flatten",
        "sp_dataframe_intersect",
        "sp_dataframe_join",
        "sp_dataframe_join_table_function",
        "sp_dataframe_join__dataframe__join_exprs",
        "sp_dataframe_join__dataframe__using_columns",
        "sp_dataframe_limit",
        "sp_dataframe_na_drop__python",
        "sp_dataframe_na_drop__scala",
        "sp_dataframe_na_fill",
        "sp_dataframe_na_replace",
        "sp_dataframe_natural_join",
        "sp_dataframe_random_split",
        "sp_dataframe_ref",
        "sp_dataframe_rename",
        "sp_dataframe_sample",
        "sp_dataframe_select__columns",
        "sp_dataframe_select__exprs",
        "sp_dataframe_show",
        "sp_dataframe_sort",
        "sp_dataframe_stat_approx_quantile",
        "sp_dataframe_stat_corr",
        "sp_dataframe_stat_cov",
        "sp_dataframe_stat_cross_tab",
        "sp_dataframe_stat_sample_by",
        "sp_dataframe_to_df",
        "sp_dataframe_to_local_iterator",
        "sp_dataframe_to_pandas",
        "sp_dataframe_to_pandas_batches",
        "sp_dataframe_union",
        "sp_dataframe_union_all",
        "sp_dataframe_union_all_by_name",
        "sp_dataframe_union_by_name",
        "sp_dataframe_unpivot",
        "sp_dataframe_where",
        "sp_dataframe_with_column",
        "sp_dataframe_with_column_renamed",
        "sp_dataframe_with_columns",
        "sp_dataframe_write",
        "sp_flatten",
        "sp_generator",
        "sp_range",
        "sp_read_avro",
        "sp_read_csv",
        "sp_read_json",
        "sp_read_orc",
        "sp_read_parquet",
        "sp_read_table",
        "sp_read_xml",
        "sp_relational_grouped_dataframe_agg",
        "sp_relational_grouped_dataframe_apply_in_pandas",
        "sp_relational_grouped_dataframe_builtin",
        "sp_relational_grouped_dataframe_pivot",
        "sp_session_table_function",
        "sp_sql",
        "sp_stored_procedure",
        "sp_table",
        "sp_table_delete",
        "sp_table_drop_table",
        "sp_table_merge",
        "sp_table_sample",
        "sp_table_update",
        "sp_write_pandas",
    )
    SP_CREATE_DATAFRAME_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_AGG_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ALIAS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ANALYTICS_COMPUTE_LAG_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ANALYTICS_COMPUTE_LEAD_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ANALYTICS_CUMULATIVE_AGG_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ANALYTICS_MOVING_AGG_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ANALYTICS_TIME_SERIES_AGG_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_COLLECT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_COUNT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_CROSS_JOIN_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_DESCRIBE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_DISTINCT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_DROP_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_DROP_DUPLICATES_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_EXCEPT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_FILTER_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_FIRST_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_FLATTEN_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_INTERSECT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_JOIN_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_JOIN_TABLE_FUNCTION_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_JOIN__DATAFRAME__JOIN_EXPRS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_JOIN__DATAFRAME__USING_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_LIMIT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_NA_DROP__PYTHON_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_NA_DROP__SCALA_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_NA_FILL_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_NA_REPLACE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_NATURAL_JOIN_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_RANDOM_SPLIT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_REF_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_RENAME_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SAMPLE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SELECT__COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SELECT__EXPRS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SHOW_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SORT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_STAT_APPROX_QUANTILE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_STAT_CORR_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_STAT_COV_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_STAT_CROSS_TAB_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_STAT_SAMPLE_BY_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_TO_DF_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_TO_LOCAL_ITERATOR_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_TO_PANDAS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_TO_PANDAS_BATCHES_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_UNION_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_UNION_ALL_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_UNION_ALL_BY_NAME_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_UNION_BY_NAME_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_UNPIVOT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_WHERE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_WITH_COLUMN_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_WITH_COLUMN_RENAMED_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_WITH_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_WRITE_FIELD_NUMBER: _ClassVar[int]
    SP_FLATTEN_FIELD_NUMBER: _ClassVar[int]
    SP_GENERATOR_FIELD_NUMBER: _ClassVar[int]
    SP_RANGE_FIELD_NUMBER: _ClassVar[int]
    SP_READ_AVRO_FIELD_NUMBER: _ClassVar[int]
    SP_READ_CSV_FIELD_NUMBER: _ClassVar[int]
    SP_READ_JSON_FIELD_NUMBER: _ClassVar[int]
    SP_READ_ORC_FIELD_NUMBER: _ClassVar[int]
    SP_READ_PARQUET_FIELD_NUMBER: _ClassVar[int]
    SP_READ_TABLE_FIELD_NUMBER: _ClassVar[int]
    SP_READ_XML_FIELD_NUMBER: _ClassVar[int]
    SP_RELATIONAL_GROUPED_DATAFRAME_AGG_FIELD_NUMBER: _ClassVar[int]
    SP_RELATIONAL_GROUPED_DATAFRAME_APPLY_IN_PANDAS_FIELD_NUMBER: _ClassVar[int]
    SP_RELATIONAL_GROUPED_DATAFRAME_BUILTIN_FIELD_NUMBER: _ClassVar[int]
    SP_RELATIONAL_GROUPED_DATAFRAME_PIVOT_FIELD_NUMBER: _ClassVar[int]
    SP_SESSION_TABLE_FUNCTION_FIELD_NUMBER: _ClassVar[int]
    SP_SQL_FIELD_NUMBER: _ClassVar[int]
    SP_STORED_PROCEDURE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_DELETE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_DROP_TABLE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_MERGE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_SAMPLE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_UPDATE_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_PANDAS_FIELD_NUMBER: _ClassVar[int]
    sp_create_dataframe: SpCreateDataframe
    sp_dataframe_agg: SpDataframeAgg
    sp_dataframe_alias: SpDataframeAlias
    sp_dataframe_analytics_compute_lag: SpDataframeAnalyticsComputeLag
    sp_dataframe_analytics_compute_lead: SpDataframeAnalyticsComputeLead
    sp_dataframe_analytics_cumulative_agg: SpDataframeAnalyticsCumulativeAgg
    sp_dataframe_analytics_moving_agg: SpDataframeAnalyticsMovingAgg
    sp_dataframe_analytics_time_series_agg: SpDataframeAnalyticsTimeSeriesAgg
    sp_dataframe_collect: SpDataframeCollect
    sp_dataframe_count: SpDataframeCount
    sp_dataframe_cross_join: SpDataframeCrossJoin
    sp_dataframe_describe: SpDataframeDescribe
    sp_dataframe_distinct: SpDataframeDistinct
    sp_dataframe_drop: SpDataframeDrop
    sp_dataframe_drop_duplicates: SpDataframeDropDuplicates
    sp_dataframe_except: SpDataframeExcept
    sp_dataframe_filter: SpDataframeFilter
    sp_dataframe_first: SpDataframeFirst
    sp_dataframe_flatten: SpDataframeFlatten
    sp_dataframe_intersect: SpDataframeIntersect
    sp_dataframe_join: SpDataframeJoin
    sp_dataframe_join_table_function: SpDataframeJoinTableFunction
    sp_dataframe_join__dataframe__join_exprs: SpDataframeJoin_Dataframe_JoinExprs
    sp_dataframe_join__dataframe__using_columns: SpDataframeJoin_Dataframe_UsingColumns
    sp_dataframe_limit: SpDataframeLimit
    sp_dataframe_na_drop__python: SpDataframeNaDrop_Python
    sp_dataframe_na_drop__scala: SpDataframeNaDrop_Scala
    sp_dataframe_na_fill: SpDataframeNaFill
    sp_dataframe_na_replace: SpDataframeNaReplace
    sp_dataframe_natural_join: SpDataframeNaturalJoin
    sp_dataframe_random_split: SpDataframeRandomSplit
    sp_dataframe_ref: SpDataframeRef
    sp_dataframe_rename: SpDataframeRename
    sp_dataframe_sample: SpDataframeSample
    sp_dataframe_select__columns: SpDataframeSelect_Columns
    sp_dataframe_select__exprs: SpDataframeSelect_Exprs
    sp_dataframe_show: SpDataframeShow
    sp_dataframe_sort: SpDataframeSort
    sp_dataframe_stat_approx_quantile: SpDataframeStatApproxQuantile
    sp_dataframe_stat_corr: SpDataframeStatCorr
    sp_dataframe_stat_cov: SpDataframeStatCov
    sp_dataframe_stat_cross_tab: SpDataframeStatCrossTab
    sp_dataframe_stat_sample_by: SpDataframeStatSampleBy
    sp_dataframe_to_df: SpDataframeToDf
    sp_dataframe_to_local_iterator: SpDataframeToLocalIterator
    sp_dataframe_to_pandas: SpDataframeToPandas
    sp_dataframe_to_pandas_batches: SpDataframeToPandasBatches
    sp_dataframe_union: SpDataframeUnion
    sp_dataframe_union_all: SpDataframeUnionAll
    sp_dataframe_union_all_by_name: SpDataframeUnionAllByName
    sp_dataframe_union_by_name: SpDataframeUnionByName
    sp_dataframe_unpivot: SpDataframeUnpivot
    sp_dataframe_where: SpDataframeWhere
    sp_dataframe_with_column: SpDataframeWithColumn
    sp_dataframe_with_column_renamed: SpDataframeWithColumnRenamed
    sp_dataframe_with_columns: SpDataframeWithColumns
    sp_dataframe_write: SpDataframeWrite
    sp_flatten: SpFlatten
    sp_generator: SpGenerator
    sp_range: SpRange
    sp_read_avro: SpReadAvro
    sp_read_csv: SpReadCsv
    sp_read_json: SpReadJson
    sp_read_orc: SpReadOrc
    sp_read_parquet: SpReadParquet
    sp_read_table: SpReadTable
    sp_read_xml: SpReadXml
    sp_relational_grouped_dataframe_agg: SpRelationalGroupedDataframeAgg
    sp_relational_grouped_dataframe_apply_in_pandas: SpRelationalGroupedDataframeApplyInPandas
    sp_relational_grouped_dataframe_builtin: SpRelationalGroupedDataframeBuiltin
    sp_relational_grouped_dataframe_pivot: SpRelationalGroupedDataframePivot
    sp_session_table_function: SpSessionTableFunction
    sp_sql: SpSql
    sp_stored_procedure: SpStoredProcedure
    sp_table: SpTable
    sp_table_delete: SpTableDelete
    sp_table_drop_table: SpTableDropTable
    sp_table_merge: SpTableMerge
    sp_table_sample: SpTableSample
    sp_table_update: SpTableUpdate
    sp_write_pandas: SpWritePandas
    def __init__(
        self,
        sp_create_dataframe: _Optional[_Union[SpCreateDataframe, _Mapping]] = ...,
        sp_dataframe_agg: _Optional[_Union[SpDataframeAgg, _Mapping]] = ...,
        sp_dataframe_alias: _Optional[_Union[SpDataframeAlias, _Mapping]] = ...,
        sp_dataframe_analytics_compute_lag: _Optional[
            _Union[SpDataframeAnalyticsComputeLag, _Mapping]
        ] = ...,
        sp_dataframe_analytics_compute_lead: _Optional[
            _Union[SpDataframeAnalyticsComputeLead, _Mapping]
        ] = ...,
        sp_dataframe_analytics_cumulative_agg: _Optional[
            _Union[SpDataframeAnalyticsCumulativeAgg, _Mapping]
        ] = ...,
        sp_dataframe_analytics_moving_agg: _Optional[
            _Union[SpDataframeAnalyticsMovingAgg, _Mapping]
        ] = ...,
        sp_dataframe_analytics_time_series_agg: _Optional[
            _Union[SpDataframeAnalyticsTimeSeriesAgg, _Mapping]
        ] = ...,
        sp_dataframe_collect: _Optional[_Union[SpDataframeCollect, _Mapping]] = ...,
        sp_dataframe_count: _Optional[_Union[SpDataframeCount, _Mapping]] = ...,
        sp_dataframe_cross_join: _Optional[
            _Union[SpDataframeCrossJoin, _Mapping]
        ] = ...,
        sp_dataframe_describe: _Optional[_Union[SpDataframeDescribe, _Mapping]] = ...,
        sp_dataframe_distinct: _Optional[_Union[SpDataframeDistinct, _Mapping]] = ...,
        sp_dataframe_drop: _Optional[_Union[SpDataframeDrop, _Mapping]] = ...,
        sp_dataframe_drop_duplicates: _Optional[
            _Union[SpDataframeDropDuplicates, _Mapping]
        ] = ...,
        sp_dataframe_except: _Optional[_Union[SpDataframeExcept, _Mapping]] = ...,
        sp_dataframe_filter: _Optional[_Union[SpDataframeFilter, _Mapping]] = ...,
        sp_dataframe_first: _Optional[_Union[SpDataframeFirst, _Mapping]] = ...,
        sp_dataframe_flatten: _Optional[_Union[SpDataframeFlatten, _Mapping]] = ...,
        sp_dataframe_intersect: _Optional[_Union[SpDataframeIntersect, _Mapping]] = ...,
        sp_dataframe_join: _Optional[_Union[SpDataframeJoin, _Mapping]] = ...,
        sp_dataframe_join_table_function: _Optional[
            _Union[SpDataframeJoinTableFunction, _Mapping]
        ] = ...,
        sp_dataframe_join__dataframe__join_exprs: _Optional[
            _Union[SpDataframeJoin_Dataframe_JoinExprs, _Mapping]
        ] = ...,
        sp_dataframe_join__dataframe__using_columns: _Optional[
            _Union[SpDataframeJoin_Dataframe_UsingColumns, _Mapping]
        ] = ...,
        sp_dataframe_limit: _Optional[_Union[SpDataframeLimit, _Mapping]] = ...,
        sp_dataframe_na_drop__python: _Optional[
            _Union[SpDataframeNaDrop_Python, _Mapping]
        ] = ...,
        sp_dataframe_na_drop__scala: _Optional[
            _Union[SpDataframeNaDrop_Scala, _Mapping]
        ] = ...,
        sp_dataframe_na_fill: _Optional[_Union[SpDataframeNaFill, _Mapping]] = ...,
        sp_dataframe_na_replace: _Optional[
            _Union[SpDataframeNaReplace, _Mapping]
        ] = ...,
        sp_dataframe_natural_join: _Optional[
            _Union[SpDataframeNaturalJoin, _Mapping]
        ] = ...,
        sp_dataframe_random_split: _Optional[
            _Union[SpDataframeRandomSplit, _Mapping]
        ] = ...,
        sp_dataframe_ref: _Optional[_Union[SpDataframeRef, _Mapping]] = ...,
        sp_dataframe_rename: _Optional[_Union[SpDataframeRename, _Mapping]] = ...,
        sp_dataframe_sample: _Optional[_Union[SpDataframeSample, _Mapping]] = ...,
        sp_dataframe_select__columns: _Optional[
            _Union[SpDataframeSelect_Columns, _Mapping]
        ] = ...,
        sp_dataframe_select__exprs: _Optional[
            _Union[SpDataframeSelect_Exprs, _Mapping]
        ] = ...,
        sp_dataframe_show: _Optional[_Union[SpDataframeShow, _Mapping]] = ...,
        sp_dataframe_sort: _Optional[_Union[SpDataframeSort, _Mapping]] = ...,
        sp_dataframe_stat_approx_quantile: _Optional[
            _Union[SpDataframeStatApproxQuantile, _Mapping]
        ] = ...,
        sp_dataframe_stat_corr: _Optional[_Union[SpDataframeStatCorr, _Mapping]] = ...,
        sp_dataframe_stat_cov: _Optional[_Union[SpDataframeStatCov, _Mapping]] = ...,
        sp_dataframe_stat_cross_tab: _Optional[
            _Union[SpDataframeStatCrossTab, _Mapping]
        ] = ...,
        sp_dataframe_stat_sample_by: _Optional[
            _Union[SpDataframeStatSampleBy, _Mapping]
        ] = ...,
        sp_dataframe_to_df: _Optional[_Union[SpDataframeToDf, _Mapping]] = ...,
        sp_dataframe_to_local_iterator: _Optional[
            _Union[SpDataframeToLocalIterator, _Mapping]
        ] = ...,
        sp_dataframe_to_pandas: _Optional[_Union[SpDataframeToPandas, _Mapping]] = ...,
        sp_dataframe_to_pandas_batches: _Optional[
            _Union[SpDataframeToPandasBatches, _Mapping]
        ] = ...,
        sp_dataframe_union: _Optional[_Union[SpDataframeUnion, _Mapping]] = ...,
        sp_dataframe_union_all: _Optional[_Union[SpDataframeUnionAll, _Mapping]] = ...,
        sp_dataframe_union_all_by_name: _Optional[
            _Union[SpDataframeUnionAllByName, _Mapping]
        ] = ...,
        sp_dataframe_union_by_name: _Optional[
            _Union[SpDataframeUnionByName, _Mapping]
        ] = ...,
        sp_dataframe_unpivot: _Optional[_Union[SpDataframeUnpivot, _Mapping]] = ...,
        sp_dataframe_where: _Optional[_Union[SpDataframeWhere, _Mapping]] = ...,
        sp_dataframe_with_column: _Optional[
            _Union[SpDataframeWithColumn, _Mapping]
        ] = ...,
        sp_dataframe_with_column_renamed: _Optional[
            _Union[SpDataframeWithColumnRenamed, _Mapping]
        ] = ...,
        sp_dataframe_with_columns: _Optional[
            _Union[SpDataframeWithColumns, _Mapping]
        ] = ...,
        sp_dataframe_write: _Optional[_Union[SpDataframeWrite, _Mapping]] = ...,
        sp_flatten: _Optional[_Union[SpFlatten, _Mapping]] = ...,
        sp_generator: _Optional[_Union[SpGenerator, _Mapping]] = ...,
        sp_range: _Optional[_Union[SpRange, _Mapping]] = ...,
        sp_read_avro: _Optional[_Union[SpReadAvro, _Mapping]] = ...,
        sp_read_csv: _Optional[_Union[SpReadCsv, _Mapping]] = ...,
        sp_read_json: _Optional[_Union[SpReadJson, _Mapping]] = ...,
        sp_read_orc: _Optional[_Union[SpReadOrc, _Mapping]] = ...,
        sp_read_parquet: _Optional[_Union[SpReadParquet, _Mapping]] = ...,
        sp_read_table: _Optional[_Union[SpReadTable, _Mapping]] = ...,
        sp_read_xml: _Optional[_Union[SpReadXml, _Mapping]] = ...,
        sp_relational_grouped_dataframe_agg: _Optional[
            _Union[SpRelationalGroupedDataframeAgg, _Mapping]
        ] = ...,
        sp_relational_grouped_dataframe_apply_in_pandas: _Optional[
            _Union[SpRelationalGroupedDataframeApplyInPandas, _Mapping]
        ] = ...,
        sp_relational_grouped_dataframe_builtin: _Optional[
            _Union[SpRelationalGroupedDataframeBuiltin, _Mapping]
        ] = ...,
        sp_relational_grouped_dataframe_pivot: _Optional[
            _Union[SpRelationalGroupedDataframePivot, _Mapping]
        ] = ...,
        sp_session_table_function: _Optional[
            _Union[SpSessionTableFunction, _Mapping]
        ] = ...,
        sp_sql: _Optional[_Union[SpSql, _Mapping]] = ...,
        sp_stored_procedure: _Optional[_Union[SpStoredProcedure, _Mapping]] = ...,
        sp_table: _Optional[_Union[SpTable, _Mapping]] = ...,
        sp_table_delete: _Optional[_Union[SpTableDelete, _Mapping]] = ...,
        sp_table_drop_table: _Optional[_Union[SpTableDropTable, _Mapping]] = ...,
        sp_table_merge: _Optional[_Union[SpTableMerge, _Mapping]] = ...,
        sp_table_sample: _Optional[_Union[SpTableSample, _Mapping]] = ...,
        sp_table_update: _Optional[_Union[SpTableUpdate, _Mapping]] = ...,
        sp_write_pandas: _Optional[_Union[SpWritePandas, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeRef(_message.Message):
    __slots__ = ("id", "src")
    ID_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    id: VarId
    src: SrcPosition
    def __init__(
        self,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeShow(_message.Message):
    __slots__ = ("id", "src")
    ID_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    id: VarId
    src: SrcPosition
    def __init__(
        self,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeCount(_message.Message):
    __slots__ = ("block", "id", "src", "statement_params")
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    block: bool
    id: VarId
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        block: bool = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpDataframeCollect(_message.Message):
    __slots__ = (
        "block",
        "case_sensitive",
        "id",
        "log_on_exception",
        "no_wait",
        "src",
        "statement_params",
    )
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    CASE_SENSITIVE_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    LOG_ON_EXCEPTION_FIELD_NUMBER: _ClassVar[int]
    NO_WAIT_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    block: bool
    case_sensitive: bool
    id: VarId
    log_on_exception: bool
    no_wait: bool
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        block: bool = ...,
        case_sensitive: bool = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        log_on_exception: bool = ...,
        no_wait: bool = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpDataframeToLocalIterator(_message.Message):
    __slots__ = ("block", "case_sensitive", "id", "src", "statement_params")
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    CASE_SENSITIVE_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    block: bool
    case_sensitive: bool
    id: VarId
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        block: bool = ...,
        case_sensitive: bool = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpCreateDataframe(_message.Message):
    __slots__ = ("data", "schema", "src")
    DATA_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    data: SpDataframeData
    schema: SpDataframeSchema
    src: SrcPosition
    def __init__(
        self,
        data: _Optional[_Union[SpDataframeData, _Mapping]] = ...,
        schema: _Optional[_Union[SpDataframeSchema, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpWritePandas(_message.Message):
    __slots__ = (
        "auto_create_table",
        "chunk_size",
        "compression",
        "create_temp_table",
        "df",
        "kwargs",
        "on_error",
        "overwrite",
        "parallel",
        "quote_identifiers",
        "src",
        "table_name",
        "table_type",
    )
    AUTO_CREATE_TABLE_FIELD_NUMBER: _ClassVar[int]
    CHUNK_SIZE_FIELD_NUMBER: _ClassVar[int]
    COMPRESSION_FIELD_NUMBER: _ClassVar[int]
    CREATE_TEMP_TABLE_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    KWARGS_FIELD_NUMBER: _ClassVar[int]
    ON_ERROR_FIELD_NUMBER: _ClassVar[int]
    OVERWRITE_FIELD_NUMBER: _ClassVar[int]
    PARALLEL_FIELD_NUMBER: _ClassVar[int]
    QUOTE_IDENTIFIERS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_TYPE_FIELD_NUMBER: _ClassVar[int]
    auto_create_table: bool
    chunk_size: _wrappers_pb2.Int64Value
    compression: str
    create_temp_table: bool
    df: SpDataframeData
    kwargs: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    on_error: str
    overwrite: bool
    parallel: int
    quote_identifiers: bool
    src: SrcPosition
    table_name: SpTableName
    table_type: str
    def __init__(
        self,
        auto_create_table: bool = ...,
        chunk_size: _Optional[_Union[_wrappers_pb2.Int64Value, _Mapping]] = ...,
        compression: _Optional[str] = ...,
        create_temp_table: bool = ...,
        df: _Optional[_Union[SpDataframeData, _Mapping]] = ...,
        kwargs: _Optional[_Iterable[_Union[Tuple_String_Expr, _Mapping]]] = ...,
        on_error: _Optional[str] = ...,
        overwrite: bool = ...,
        parallel: _Optional[int] = ...,
        quote_identifiers: bool = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        table_name: _Optional[_Union[SpTableName, _Mapping]] = ...,
        table_type: _Optional[str] = ...,
    ) -> None: ...

class SpFlatten(_message.Message):
    __slots__ = ("input", "mode", "outer", "path", "recursive", "src")
    INPUT_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    OUTER_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    RECURSIVE_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    input: Expr
    mode: SpFlattenMode
    outer: bool
    path: _wrappers_pb2.StringValue
    recursive: bool
    src: SrcPosition
    def __init__(
        self,
        input: _Optional[_Union[Expr, _Mapping]] = ...,
        mode: _Optional[_Union[SpFlattenMode, _Mapping]] = ...,
        outer: bool = ...,
        path: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        recursive: bool = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpGenerator(_message.Message):
    __slots__ = ("columns", "row_count", "src", "time_limit_seconds", "variadic")
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    ROW_COUNT_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    TIME_LIMIT_SECONDS_FIELD_NUMBER: _ClassVar[int]
    VARIADIC_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedCompositeFieldContainer[Expr]
    row_count: int
    src: SrcPosition
    time_limit_seconds: int
    variadic: bool
    def __init__(
        self,
        columns: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        row_count: _Optional[int] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        time_limit_seconds: _Optional[int] = ...,
        variadic: bool = ...,
    ) -> None: ...

class SpRange(_message.Message):
    __slots__ = ("end", "src", "start", "step")
    END_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    START_FIELD_NUMBER: _ClassVar[int]
    STEP_FIELD_NUMBER: _ClassVar[int]
    end: _wrappers_pb2.Int64Value
    src: SrcPosition
    start: int
    step: _wrappers_pb2.Int64Value
    def __init__(
        self,
        end: _Optional[_Union[_wrappers_pb2.Int64Value, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        start: _Optional[int] = ...,
        step: _Optional[_Union[_wrappers_pb2.Int64Value, _Mapping]] = ...,
    ) -> None: ...

class SpSql(_message.Message):
    __slots__ = ("params", "query", "src")
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    QUERY_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    params: _containers.RepeatedCompositeFieldContainer[Expr]
    query: str
    src: SrcPosition
    def __init__(
        self,
        params: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        query: _Optional[str] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpStoredProcedure(_message.Message):
    __slots__ = ("args", "sp_name", "src", "variadic")
    ARGS_FIELD_NUMBER: _ClassVar[int]
    SP_NAME_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VARIADIC_FIELD_NUMBER: _ClassVar[int]
    args: _containers.RepeatedCompositeFieldContainer[SpVariant]
    sp_name: str
    src: SrcPosition
    variadic: bool
    def __init__(
        self,
        args: _Optional[_Iterable[_Union[SpVariant, _Mapping]]] = ...,
        sp_name: _Optional[str] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        variadic: bool = ...,
    ) -> None: ...

class SpTable(_message.Message):
    __slots__ = ("is_temp_table_for_cleanup", "name", "src", "variant")
    IS_TEMP_TABLE_FOR_CLEANUP_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VARIANT_FIELD_NUMBER: _ClassVar[int]
    is_temp_table_for_cleanup: bool
    name: SpTableName
    src: SrcPosition
    variant: SpTableVariant
    def __init__(
        self,
        is_temp_table_for_cleanup: bool = ...,
        name: _Optional[_Union[SpTableName, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        variant: _Optional[_Union[SpTableVariant, _Mapping]] = ...,
    ) -> None: ...

class SpSessionTableFunction(_message.Message):
    __slots__ = ("fn", "src")
    FN_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    fn: Expr
    src: SrcPosition
    def __init__(
        self,
        fn: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeToPandas(_message.Message):
    __slots__ = ("block", "id", "src", "statement_params")
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    block: bool
    id: VarId
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        block: bool = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpDataframeToPandasBatches(_message.Message):
    __slots__ = ("block", "id", "src", "statement_params")
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    block: bool
    id: VarId
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        block: bool = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpDataframeToDf(_message.Message):
    __slots__ = ("col_names", "df", "src", "variadic")
    COL_NAMES_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VARIADIC_FIELD_NUMBER: _ClassVar[int]
    col_names: _containers.RepeatedScalarFieldContainer[str]
    df: SpDataframeExpr
    src: SrcPosition
    variadic: bool
    def __init__(
        self,
        col_names: _Optional[_Iterable[str]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        variadic: bool = ...,
    ) -> None: ...

class SpDataframeNaDrop_Scala(_message.Message):
    __slots__ = ("cols", "df", "min_non_nulls_per_row", "src")
    COLS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    MIN_NON_NULLS_PER_ROW_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    cols: _containers.RepeatedScalarFieldContainer[str]
    df: SpDataframeExpr
    min_non_nulls_per_row: int
    src: SrcPosition
    def __init__(
        self,
        cols: _Optional[_Iterable[str]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        min_non_nulls_per_row: _Optional[int] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeNaDrop_Python(_message.Message):
    __slots__ = ("df", "how", "src", "subset", "thresh")
    DF_FIELD_NUMBER: _ClassVar[int]
    HOW_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    SUBSET_FIELD_NUMBER: _ClassVar[int]
    THRESH_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    how: str
    src: SrcPosition
    subset: List_String
    thresh: _wrappers_pb2.Int64Value
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        how: _Optional[str] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        subset: _Optional[_Union[List_String, _Mapping]] = ...,
        thresh: _Optional[_Union[_wrappers_pb2.Int64Value, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeNaFill(_message.Message):
    __slots__ = ("df", "src", "subset", "value", "value_map")
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    SUBSET_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    VALUE_MAP_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    src: SrcPosition
    subset: List_String
    value: Expr
    value_map: Map_String_Expr
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        subset: _Optional[_Union[List_String, _Mapping]] = ...,
        value: _Optional[_Union[Expr, _Mapping]] = ...,
        value_map: _Optional[_Union[Map_String_Expr, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeNaReplace(_message.Message):
    __slots__ = (
        "df",
        "replacement_map",
        "src",
        "subset",
        "to_replace_list",
        "to_replace_value",
        "value",
        "values",
    )
    DF_FIELD_NUMBER: _ClassVar[int]
    REPLACEMENT_MAP_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    SUBSET_FIELD_NUMBER: _ClassVar[int]
    TO_REPLACE_LIST_FIELD_NUMBER: _ClassVar[int]
    TO_REPLACE_VALUE_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    replacement_map: Map_Expr_Expr
    src: SrcPosition
    subset: List_String
    to_replace_list: List_Expr
    to_replace_value: Expr
    value: Expr
    values: List_Expr
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        replacement_map: _Optional[_Union[Map_Expr_Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        subset: _Optional[_Union[List_String, _Mapping]] = ...,
        to_replace_list: _Optional[_Union[List_Expr, _Mapping]] = ...,
        to_replace_value: _Optional[_Union[Expr, _Mapping]] = ...,
        value: _Optional[_Union[Expr, _Mapping]] = ...,
        values: _Optional[_Union[List_Expr, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeAgg(_message.Message):
    __slots__ = ("df", "exprs", "src")
    DF_FIELD_NUMBER: _ClassVar[int]
    EXPRS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    exprs: ExprArgList
    src: SrcPosition
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        exprs: _Optional[_Union[ExprArgList, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeAlias(_message.Message):
    __slots__ = ("df", "name", "src")
    DF_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    name: str
    src: SrcPosition
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        name: _Optional[str] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeApply(_message.Message):
    __slots__ = ("col_name", "df", "src")
    COL_NAME_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col_name: str
    df: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        col_name: _Optional[str] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeCol(_message.Message):
    __slots__ = ("col_name", "df", "src")
    COL_NAME_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col_name: str
    df: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        col_name: _Optional[str] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeCrossJoin(_message.Message):
    __slots__ = ("lhs", "lsuffix", "rhs", "rsuffix", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    LSUFFIX_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    RSUFFIX_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: SpDataframeExpr
    lsuffix: _wrappers_pb2.StringValue
    rhs: SpDataframeExpr
    rsuffix: _wrappers_pb2.StringValue
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        lsuffix: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        rhs: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        rsuffix: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeCube(_message.Message):
    __slots__ = ("cols", "df", "src")
    COLS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    cols: ExprArgList
    df: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        cols: _Optional[_Union[ExprArgList, _Mapping]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeDescribe(_message.Message):
    __slots__ = ("cols", "df", "src")
    COLS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    cols: ExprArgList
    df: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        cols: _Optional[_Union[ExprArgList, _Mapping]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeDistinct(_message.Message):
    __slots__ = ("df", "src")
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeDrop(_message.Message):
    __slots__ = ("cols", "df", "src")
    COLS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    cols: ExprArgList
    df: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        cols: _Optional[_Union[ExprArgList, _Mapping]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeDropDuplicates(_message.Message):
    __slots__ = ("cols", "df", "src", "variadic")
    COLS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VARIADIC_FIELD_NUMBER: _ClassVar[int]
    cols: _containers.RepeatedScalarFieldContainer[str]
    df: SpDataframeExpr
    src: SrcPosition
    variadic: bool
    def __init__(
        self,
        cols: _Optional[_Iterable[str]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        variadic: bool = ...,
    ) -> None: ...

class SpDataframeExcept(_message.Message):
    __slots__ = ("df", "other", "src")
    DF_FIELD_NUMBER: _ClassVar[int]
    OTHER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    other: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        other: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeFilter(_message.Message):
    __slots__ = ("condition", "df", "src")
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    condition: Expr
    df: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        condition: _Optional[_Union[Expr, _Mapping]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeFlatten(_message.Message):
    __slots__ = ("df", "input", "mode", "outer", "path", "recursive", "src")
    DF_FIELD_NUMBER: _ClassVar[int]
    INPUT_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    OUTER_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    RECURSIVE_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    input: Expr
    mode: SpFlattenMode
    outer: bool
    path: _wrappers_pb2.StringValue
    recursive: bool
    src: SrcPosition
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        input: _Optional[_Union[Expr, _Mapping]] = ...,
        mode: _Optional[_Union[SpFlattenMode, _Mapping]] = ...,
        outer: bool = ...,
        path: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        recursive: bool = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeFirst(_message.Message):
    __slots__ = ("block", "df", "num", "src", "statement_params")
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    NUM_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    block: bool
    df: SpDataframeExpr
    num: int
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        block: bool = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        num: _Optional[int] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpDataframeGroupBy_Columns(_message.Message):
    __slots__ = ("cols", "df", "src", "variadic")
    COLS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VARIADIC_FIELD_NUMBER: _ClassVar[int]
    cols: _containers.RepeatedCompositeFieldContainer[SpColumnExpr]
    df: SpDataframeExpr
    src: SrcPosition
    variadic: bool
    def __init__(
        self,
        cols: _Optional[_Iterable[_Union[SpColumnExpr, _Mapping]]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        variadic: bool = ...,
    ) -> None: ...

class SpDataframeGroupBy_Strings(_message.Message):
    __slots__ = ("cols", "df", "src", "variadic")
    COLS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VARIADIC_FIELD_NUMBER: _ClassVar[int]
    cols: _containers.RepeatedScalarFieldContainer[str]
    df: SpDataframeExpr
    src: SrcPosition
    variadic: bool
    def __init__(
        self,
        cols: _Optional[_Iterable[str]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        variadic: bool = ...,
    ) -> None: ...

class SpDataframeGroupBy(_message.Message):
    __slots__ = ("cols", "df", "src")
    COLS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    cols: ExprArgList
    df: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        cols: _Optional[_Union[ExprArgList, _Mapping]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeIntersect(_message.Message):
    __slots__ = ("df", "other", "src")
    DF_FIELD_NUMBER: _ClassVar[int]
    OTHER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    other: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        other: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeJoin(_message.Message):
    __slots__ = (
        "join_expr",
        "join_type",
        "lhs",
        "lsuffix",
        "match_condition",
        "rhs",
        "rsuffix",
        "src",
    )
    JOIN_EXPR_FIELD_NUMBER: _ClassVar[int]
    JOIN_TYPE_FIELD_NUMBER: _ClassVar[int]
    LHS_FIELD_NUMBER: _ClassVar[int]
    LSUFFIX_FIELD_NUMBER: _ClassVar[int]
    MATCH_CONDITION_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    RSUFFIX_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    join_expr: Expr
    join_type: SpJoinType
    lhs: SpDataframeExpr
    lsuffix: _wrappers_pb2.StringValue
    match_condition: Expr
    rhs: SpDataframeExpr
    rsuffix: _wrappers_pb2.StringValue
    src: SrcPosition
    def __init__(
        self,
        join_expr: _Optional[_Union[Expr, _Mapping]] = ...,
        join_type: _Optional[_Union[SpJoinType, _Mapping]] = ...,
        lhs: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        lsuffix: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        match_condition: _Optional[_Union[Expr, _Mapping]] = ...,
        rhs: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        rsuffix: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeJoinTableFunction(_message.Message):
    __slots__ = ("fn", "lhs", "src")
    FN_FIELD_NUMBER: _ClassVar[int]
    LHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    fn: Expr
    lhs: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        fn: _Optional[_Union[Expr, _Mapping]] = ...,
        lhs: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeJoin_Dataframe_JoinExprs(_message.Message):
    __slots__ = ("join_exprs", "join_type", "lhs", "rhs", "src")
    JOIN_EXPRS_FIELD_NUMBER: _ClassVar[int]
    JOIN_TYPE_FIELD_NUMBER: _ClassVar[int]
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    join_exprs: SpDataframeExpr
    join_type: SpJoinType
    lhs: SpDataframeExpr
    rhs: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        join_exprs: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        join_type: _Optional[_Union[SpJoinType, _Mapping]] = ...,
        lhs: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        rhs: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeJoin_Dataframe_UsingColumns(_message.Message):
    __slots__ = ("join_type", "lhs", "rhs", "src", "using_columns", "variadic")
    JOIN_TYPE_FIELD_NUMBER: _ClassVar[int]
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    USING_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    VARIADIC_FIELD_NUMBER: _ClassVar[int]
    join_type: SpJoinType
    lhs: SpDataframeExpr
    rhs: SpDataframeExpr
    src: SrcPosition
    using_columns: List_String
    variadic: bool
    def __init__(
        self,
        join_type: _Optional[_Union[SpJoinType, _Mapping]] = ...,
        lhs: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        rhs: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        using_columns: _Optional[_Union[List_String, _Mapping]] = ...,
        variadic: bool = ...,
    ) -> None: ...

class SpDataframeLimit(_message.Message):
    __slots__ = ("df", "n", "offset", "src")
    DF_FIELD_NUMBER: _ClassVar[int]
    N_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    n: int
    offset: int
    src: SrcPosition
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        n: _Optional[int] = ...,
        offset: _Optional[int] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeNaturalJoin(_message.Message):
    __slots__ = ("join_type", "lhs", "rhs", "src")
    JOIN_TYPE_FIELD_NUMBER: _ClassVar[int]
    LHS_FIELD_NUMBER: _ClassVar[int]
    RHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    join_type: SpJoinType
    lhs: SpDataframeExpr
    rhs: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        join_type: _Optional[_Union[SpJoinType, _Mapping]] = ...,
        lhs: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        rhs: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframePivot(_message.Message):
    __slots__ = ("default_on_null", "df", "pivot_col", "src", "values")
    DEFAULT_ON_NULL_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    PIVOT_COL_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    default_on_null: Expr
    df: SpDataframeExpr
    pivot_col: Expr
    src: SrcPosition
    values: SpPivotValue
    def __init__(
        self,
        default_on_null: _Optional[_Union[Expr, _Mapping]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        pivot_col: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        values: _Optional[_Union[SpPivotValue, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeUnpivot(_message.Message):
    __slots__ = ("column_list", "df", "name_column", "src", "value_column")
    COLUMN_LIST_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    NAME_COLUMN_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VALUE_COLUMN_FIELD_NUMBER: _ClassVar[int]
    column_list: _containers.RepeatedCompositeFieldContainer[Expr]
    df: SpDataframeExpr
    name_column: str
    src: SrcPosition
    value_column: str
    def __init__(
        self,
        column_list: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        name_column: _Optional[str] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        value_column: _Optional[str] = ...,
    ) -> None: ...

class SpDataframeRandomSplit(_message.Message):
    __slots__ = ("df", "seed", "src", "statement_params", "weights")
    DF_FIELD_NUMBER: _ClassVar[int]
    SEED_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    WEIGHTS_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    seed: _wrappers_pb2.Int64Value
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    weights: _containers.RepeatedScalarFieldContainer[float]
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        seed: _Optional[_Union[_wrappers_pb2.Int64Value, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
        weights: _Optional[_Iterable[float]] = ...,
    ) -> None: ...

class SpDataframeRename(_message.Message):
    __slots__ = ("col_or_mapper", "df", "new_column", "src")
    COL_OR_MAPPER_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    NEW_COLUMN_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col_or_mapper: Expr
    df: SpDataframeExpr
    new_column: _wrappers_pb2.StringValue
    src: SrcPosition
    def __init__(
        self,
        col_or_mapper: _Optional[_Union[Expr, _Mapping]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        new_column: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeRollup_Columns(_message.Message):
    __slots__ = ("cols", "df", "src", "variadic")
    COLS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VARIADIC_FIELD_NUMBER: _ClassVar[int]
    cols: _containers.RepeatedCompositeFieldContainer[SpColumnExpr]
    df: SpDataframeExpr
    src: SrcPosition
    variadic: bool
    def __init__(
        self,
        cols: _Optional[_Iterable[_Union[SpColumnExpr, _Mapping]]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        variadic: bool = ...,
    ) -> None: ...

class SpDataframeRollup_Strings(_message.Message):
    __slots__ = ("cols", "df", "src", "variadic")
    COLS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VARIADIC_FIELD_NUMBER: _ClassVar[int]
    cols: _containers.RepeatedScalarFieldContainer[str]
    df: SpDataframeExpr
    src: SrcPosition
    variadic: bool
    def __init__(
        self,
        cols: _Optional[_Iterable[str]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        variadic: bool = ...,
    ) -> None: ...

class SpDataframeRollup(_message.Message):
    __slots__ = ("cols", "df", "src")
    COLS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    cols: ExprArgList
    df: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        cols: _Optional[_Union[ExprArgList, _Mapping]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeSample(_message.Message):
    __slots__ = ("df", "num", "probability_fraction", "src")
    DF_FIELD_NUMBER: _ClassVar[int]
    NUM_FIELD_NUMBER: _ClassVar[int]
    PROBABILITY_FRACTION_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    num: _wrappers_pb2.Int64Value
    probability_fraction: _wrappers_pb2.DoubleValue
    src: SrcPosition
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        num: _Optional[_Union[_wrappers_pb2.Int64Value, _Mapping]] = ...,
        probability_fraction: _Optional[
            _Union[_wrappers_pb2.DoubleValue, _Mapping]
        ] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeSelect_Columns(_message.Message):
    __slots__ = ("cols", "df", "src", "variadic")
    COLS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VARIADIC_FIELD_NUMBER: _ClassVar[int]
    cols: _containers.RepeatedCompositeFieldContainer[Expr]
    df: SpDataframeExpr
    src: SrcPosition
    variadic: bool
    def __init__(
        self,
        cols: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        variadic: bool = ...,
    ) -> None: ...

class SpDataframeSelect_Exprs(_message.Message):
    __slots__ = ("df", "exprs", "src", "variadic")
    DF_FIELD_NUMBER: _ClassVar[int]
    EXPRS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VARIADIC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    exprs: _containers.RepeatedScalarFieldContainer[str]
    src: SrcPosition
    variadic: bool
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        exprs: _Optional[_Iterable[str]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        variadic: bool = ...,
    ) -> None: ...

class SpDataframeSort(_message.Message):
    __slots__ = ("ascending", "cols", "cols_variadic", "df", "src")
    ASCENDING_FIELD_NUMBER: _ClassVar[int]
    COLS_FIELD_NUMBER: _ClassVar[int]
    COLS_VARIADIC_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    ascending: Expr
    cols: _containers.RepeatedCompositeFieldContainer[Expr]
    cols_variadic: bool
    df: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        ascending: _Optional[_Union[Expr, _Mapping]] = ...,
        cols: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        cols_variadic: bool = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeUnion(_message.Message):
    __slots__ = ("df", "other", "src")
    DF_FIELD_NUMBER: _ClassVar[int]
    OTHER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    other: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        other: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeUnionAll(_message.Message):
    __slots__ = ("df", "other", "src")
    DF_FIELD_NUMBER: _ClassVar[int]
    OTHER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    other: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        other: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeUnionAllByName(_message.Message):
    __slots__ = ("df", "other", "src")
    DF_FIELD_NUMBER: _ClassVar[int]
    OTHER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    other: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        other: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeUnionByName(_message.Message):
    __slots__ = ("df", "other", "src")
    DF_FIELD_NUMBER: _ClassVar[int]
    OTHER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    other: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        other: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeWhere(_message.Message):
    __slots__ = ("condition", "df", "src")
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    condition: SpColumnExpr
    df: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        condition: _Optional[_Union[SpColumnExpr, _Mapping]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeWithColumn(_message.Message):
    __slots__ = ("col", "col_name", "df", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    COL_NAME_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    col_name: str
    df: SpDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        col_name: _Optional[str] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeWithColumnRenamed(_message.Message):
    __slots__ = ("col", "df", "new_name", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    NEW_NAME_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    df: SpDataframeExpr
    new_name: str
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        new_name: _Optional[str] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeWithColumns(_message.Message):
    __slots__ = ("col_names", "df", "src", "values")
    COL_NAMES_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    col_names: _containers.RepeatedScalarFieldContainer[str]
    df: SpDataframeExpr
    src: SrcPosition
    values: _containers.RepeatedCompositeFieldContainer[Expr]
    def __init__(
        self,
        col_names: _Optional[_Iterable[str]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        values: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
    ) -> None: ...

class SpDataframeGroupByGroupingSets(_message.Message):
    __slots__ = ("df", "grouping_sets", "src", "variadic")
    DF_FIELD_NUMBER: _ClassVar[int]
    GROUPING_SETS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VARIADIC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    grouping_sets: _containers.RepeatedCompositeFieldContainer[SpGroupingSets]
    src: SrcPosition
    variadic: bool
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        grouping_sets: _Optional[_Iterable[_Union[SpGroupingSets, _Mapping]]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        variadic: bool = ...,
    ) -> None: ...

class SpGroupingSets(_message.Message):
    __slots__ = ("sets", "src")
    SETS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    sets: ExprArgList
    src: SrcPosition
    def __init__(
        self,
        sets: _Optional[_Union[ExprArgList, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeAnalyticsMovingAgg(_message.Message):
    __slots__ = (
        "aggs",
        "df",
        "formatted_col_names",
        "group_by",
        "order_by",
        "src",
        "window_sizes",
    )
    AGGS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    FORMATTED_COL_NAMES_FIELD_NUMBER: _ClassVar[int]
    GROUP_BY_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    WINDOW_SIZES_FIELD_NUMBER: _ClassVar[int]
    aggs: _containers.RepeatedCompositeFieldContainer[Tuple_String_List_String]
    df: SpDataframeExpr
    formatted_col_names: _containers.RepeatedScalarFieldContainer[str]
    group_by: _containers.RepeatedScalarFieldContainer[str]
    order_by: _containers.RepeatedScalarFieldContainer[str]
    src: SrcPosition
    window_sizes: _containers.RepeatedScalarFieldContainer[int]
    def __init__(
        self,
        aggs: _Optional[_Iterable[_Union[Tuple_String_List_String, _Mapping]]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        formatted_col_names: _Optional[_Iterable[str]] = ...,
        group_by: _Optional[_Iterable[str]] = ...,
        order_by: _Optional[_Iterable[str]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        window_sizes: _Optional[_Iterable[int]] = ...,
    ) -> None: ...

class SpDataframeAnalyticsCumulativeAgg(_message.Message):
    __slots__ = (
        "aggs",
        "df",
        "formatted_col_names",
        "group_by",
        "is_forward",
        "order_by",
        "src",
    )
    AGGS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    FORMATTED_COL_NAMES_FIELD_NUMBER: _ClassVar[int]
    GROUP_BY_FIELD_NUMBER: _ClassVar[int]
    IS_FORWARD_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    aggs: _containers.RepeatedCompositeFieldContainer[Tuple_String_List_String]
    df: SpDataframeExpr
    formatted_col_names: _containers.RepeatedScalarFieldContainer[str]
    group_by: _containers.RepeatedScalarFieldContainer[str]
    is_forward: bool
    order_by: _containers.RepeatedScalarFieldContainer[str]
    src: SrcPosition
    def __init__(
        self,
        aggs: _Optional[_Iterable[_Union[Tuple_String_List_String, _Mapping]]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        formatted_col_names: _Optional[_Iterable[str]] = ...,
        group_by: _Optional[_Iterable[str]] = ...,
        is_forward: bool = ...,
        order_by: _Optional[_Iterable[str]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeAnalyticsComputeLag(_message.Message):
    __slots__ = (
        "cols",
        "df",
        "formatted_col_names",
        "group_by",
        "lags",
        "order_by",
        "src",
    )
    COLS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    FORMATTED_COL_NAMES_FIELD_NUMBER: _ClassVar[int]
    GROUP_BY_FIELD_NUMBER: _ClassVar[int]
    LAGS_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    cols: _containers.RepeatedCompositeFieldContainer[Expr]
    df: SpDataframeExpr
    formatted_col_names: _containers.RepeatedScalarFieldContainer[str]
    group_by: _containers.RepeatedScalarFieldContainer[str]
    lags: _containers.RepeatedScalarFieldContainer[int]
    order_by: _containers.RepeatedScalarFieldContainer[str]
    src: SrcPosition
    def __init__(
        self,
        cols: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        formatted_col_names: _Optional[_Iterable[str]] = ...,
        group_by: _Optional[_Iterable[str]] = ...,
        lags: _Optional[_Iterable[int]] = ...,
        order_by: _Optional[_Iterable[str]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeAnalyticsComputeLead(_message.Message):
    __slots__ = (
        "cols",
        "df",
        "formatted_col_names",
        "group_by",
        "leads",
        "order_by",
        "src",
    )
    COLS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    FORMATTED_COL_NAMES_FIELD_NUMBER: _ClassVar[int]
    GROUP_BY_FIELD_NUMBER: _ClassVar[int]
    LEADS_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    cols: _containers.RepeatedCompositeFieldContainer[Expr]
    df: SpDataframeExpr
    formatted_col_names: _containers.RepeatedScalarFieldContainer[str]
    group_by: _containers.RepeatedScalarFieldContainer[str]
    leads: _containers.RepeatedScalarFieldContainer[int]
    order_by: _containers.RepeatedScalarFieldContainer[str]
    src: SrcPosition
    def __init__(
        self,
        cols: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        formatted_col_names: _Optional[_Iterable[str]] = ...,
        group_by: _Optional[_Iterable[str]] = ...,
        leads: _Optional[_Iterable[int]] = ...,
        order_by: _Optional[_Iterable[str]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeAnalyticsTimeSeriesAgg(_message.Message):
    __slots__ = (
        "aggs",
        "df",
        "formatted_col_names",
        "group_by",
        "sliding_interval",
        "src",
        "time_col",
        "windows",
    )
    AGGS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    FORMATTED_COL_NAMES_FIELD_NUMBER: _ClassVar[int]
    GROUP_BY_FIELD_NUMBER: _ClassVar[int]
    SLIDING_INTERVAL_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    TIME_COL_FIELD_NUMBER: _ClassVar[int]
    WINDOWS_FIELD_NUMBER: _ClassVar[int]
    aggs: _containers.RepeatedCompositeFieldContainer[Tuple_String_List_String]
    df: SpDataframeExpr
    formatted_col_names: _containers.RepeatedScalarFieldContainer[str]
    group_by: _containers.RepeatedScalarFieldContainer[str]
    sliding_interval: str
    src: SrcPosition
    time_col: str
    windows: _containers.RepeatedScalarFieldContainer[str]
    def __init__(
        self,
        aggs: _Optional[_Iterable[_Union[Tuple_String_List_String, _Mapping]]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        formatted_col_names: _Optional[_Iterable[str]] = ...,
        group_by: _Optional[_Iterable[str]] = ...,
        sliding_interval: _Optional[str] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        time_col: _Optional[str] = ...,
        windows: _Optional[_Iterable[str]] = ...,
    ) -> None: ...

class SpMatchedClause(_message.Message):
    __slots__ = (
        "sp_merge_delete_when_matched_clause",
        "sp_merge_insert_when_not_matched_clause",
        "sp_merge_update_when_matched_clause",
    )
    SP_MERGE_DELETE_WHEN_MATCHED_CLAUSE_FIELD_NUMBER: _ClassVar[int]
    SP_MERGE_INSERT_WHEN_NOT_MATCHED_CLAUSE_FIELD_NUMBER: _ClassVar[int]
    SP_MERGE_UPDATE_WHEN_MATCHED_CLAUSE_FIELD_NUMBER: _ClassVar[int]
    sp_merge_delete_when_matched_clause: SpMergeDeleteWhenMatchedClause
    sp_merge_insert_when_not_matched_clause: SpMergeInsertWhenNotMatchedClause
    sp_merge_update_when_matched_clause: SpMergeUpdateWhenMatchedClause
    def __init__(
        self,
        sp_merge_delete_when_matched_clause: _Optional[
            _Union[SpMergeDeleteWhenMatchedClause, _Mapping]
        ] = ...,
        sp_merge_insert_when_not_matched_clause: _Optional[
            _Union[SpMergeInsertWhenNotMatchedClause, _Mapping]
        ] = ...,
        sp_merge_update_when_matched_clause: _Optional[
            _Union[SpMergeUpdateWhenMatchedClause, _Mapping]
        ] = ...,
    ) -> None: ...

class SpMergeUpdateWhenMatchedClause(_message.Message):
    __slots__ = ("condition", "src", "update_assignments")
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    UPDATE_ASSIGNMENTS_FIELD_NUMBER: _ClassVar[int]
    condition: Expr
    src: SrcPosition
    update_assignments: Map_Expr_Expr
    def __init__(
        self,
        condition: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        update_assignments: _Optional[_Union[Map_Expr_Expr, _Mapping]] = ...,
    ) -> None: ...

class SpMergeDeleteWhenMatchedClause(_message.Message):
    __slots__ = ("condition", "src")
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    condition: Expr
    src: SrcPosition
    def __init__(
        self,
        condition: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpMergeInsertWhenNotMatchedClause(_message.Message):
    __slots__ = ("condition", "insert_keys", "insert_values", "src")
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    INSERT_KEYS_FIELD_NUMBER: _ClassVar[int]
    INSERT_VALUES_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    condition: Expr
    insert_keys: List_Expr
    insert_values: List_Expr
    src: SrcPosition
    def __init__(
        self,
        condition: _Optional[_Union[Expr, _Mapping]] = ...,
        insert_keys: _Optional[_Union[List_Expr, _Mapping]] = ...,
        insert_values: _Optional[_Union[List_Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpTableDelete(_message.Message):
    __slots__ = ("block", "condition", "id", "source", "src", "statement_params")
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    block: bool
    condition: Expr
    id: VarId
    source: SpDataframeExpr
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        block: bool = ...,
        condition: _Optional[_Union[Expr, _Mapping]] = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        source: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpTableDropTable(_message.Message):
    __slots__ = ("id", "src")
    ID_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    id: VarId
    src: SrcPosition
    def __init__(
        self,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpTableMerge(_message.Message):
    __slots__ = (
        "block",
        "clauses",
        "id",
        "join_expr",
        "source",
        "src",
        "statement_params",
    )
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    CLAUSES_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    JOIN_EXPR_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    block: bool
    clauses: _containers.RepeatedCompositeFieldContainer[SpMatchedClause]
    id: VarId
    join_expr: Expr
    source: SpDataframeExpr
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        block: bool = ...,
        clauses: _Optional[_Iterable[_Union[SpMatchedClause, _Mapping]]] = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        join_expr: _Optional[_Union[Expr, _Mapping]] = ...,
        source: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpTableSample(_message.Message):
    __slots__ = ("df", "num", "probability_fraction", "sampling_method", "seed", "src")
    DF_FIELD_NUMBER: _ClassVar[int]
    NUM_FIELD_NUMBER: _ClassVar[int]
    PROBABILITY_FRACTION_FIELD_NUMBER: _ClassVar[int]
    SAMPLING_METHOD_FIELD_NUMBER: _ClassVar[int]
    SEED_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    num: _wrappers_pb2.Int64Value
    probability_fraction: _wrappers_pb2.DoubleValue
    sampling_method: _wrappers_pb2.StringValue
    seed: _wrappers_pb2.Int64Value
    src: SrcPosition
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        num: _Optional[_Union[_wrappers_pb2.Int64Value, _Mapping]] = ...,
        probability_fraction: _Optional[
            _Union[_wrappers_pb2.DoubleValue, _Mapping]
        ] = ...,
        sampling_method: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        seed: _Optional[_Union[_wrappers_pb2.Int64Value, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpTableUpdate(_message.Message):
    __slots__ = (
        "assignments",
        "block",
        "condition",
        "id",
        "source",
        "src",
        "statement_params",
    )
    ASSIGNMENTS_FIELD_NUMBER: _ClassVar[int]
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    CONDITION_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    assignments: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    block: bool
    condition: Expr
    id: VarId
    source: SpDataframeExpr
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        assignments: _Optional[_Iterable[_Union[Tuple_String_Expr, _Mapping]]] = ...,
        block: bool = ...,
        condition: _Optional[_Union[Expr, _Mapping]] = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        source: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpDataframeReader(_message.Message):
    __slots__ = (
        "sp_dataframe_reader_init",
        "sp_dataframe_reader_option",
        "sp_dataframe_reader_options",
        "sp_dataframe_reader_schema",
        "sp_dataframe_reader_with_metadata",
    )
    SP_DATAFRAME_READER_INIT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_READER_OPTION_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_READER_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_READER_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_READER_WITH_METADATA_FIELD_NUMBER: _ClassVar[int]
    sp_dataframe_reader_init: SpDataframeReaderInit
    sp_dataframe_reader_option: SpDataframeReaderOption
    sp_dataframe_reader_options: SpDataframeReaderOptions
    sp_dataframe_reader_schema: SpDataframeReaderSchema
    sp_dataframe_reader_with_metadata: SpDataframeReaderWithMetadata
    def __init__(
        self,
        sp_dataframe_reader_init: _Optional[
            _Union[SpDataframeReaderInit, _Mapping]
        ] = ...,
        sp_dataframe_reader_option: _Optional[
            _Union[SpDataframeReaderOption, _Mapping]
        ] = ...,
        sp_dataframe_reader_options: _Optional[
            _Union[SpDataframeReaderOptions, _Mapping]
        ] = ...,
        sp_dataframe_reader_schema: _Optional[
            _Union[SpDataframeReaderSchema, _Mapping]
        ] = ...,
        sp_dataframe_reader_with_metadata: _Optional[
            _Union[SpDataframeReaderWithMetadata, _Mapping]
        ] = ...,
    ) -> None: ...

class SpDataframeReaderInit(_message.Message):
    __slots__ = ("src",)
    SRC_FIELD_NUMBER: _ClassVar[int]
    src: SrcPosition
    def __init__(self, src: _Optional[_Union[SrcPosition, _Mapping]] = ...) -> None: ...

class SpDataframeReaderOption(_message.Message):
    __slots__ = ("key", "reader", "src", "value")
    KEY_FIELD_NUMBER: _ClassVar[int]
    READER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    key: str
    reader: SpDataframeReader
    src: SrcPosition
    value: Expr
    def __init__(
        self,
        key: _Optional[str] = ...,
        reader: _Optional[_Union[SpDataframeReader, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        value: _Optional[_Union[Expr, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeReaderOptions(_message.Message):
    __slots__ = ("configs", "reader", "src")
    CONFIGS_FIELD_NUMBER: _ClassVar[int]
    READER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    configs: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    reader: SpDataframeReader
    src: SrcPosition
    def __init__(
        self,
        configs: _Optional[_Iterable[_Union[Tuple_String_Expr, _Mapping]]] = ...,
        reader: _Optional[_Union[SpDataframeReader, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeReaderSchema(_message.Message):
    __slots__ = ("reader", "schema", "src")
    READER_FIELD_NUMBER: _ClassVar[int]
    SCHEMA_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    reader: SpDataframeReader
    schema: SpStructType
    src: SrcPosition
    def __init__(
        self,
        reader: _Optional[_Union[SpDataframeReader, _Mapping]] = ...,
        schema: _Optional[_Union[SpStructType, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeReaderWithMetadata(_message.Message):
    __slots__ = ("metadata_columns", "reader", "src")
    METADATA_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    READER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    metadata_columns: ExprArgList
    reader: SpDataframeReader
    src: SrcPosition
    def __init__(
        self,
        metadata_columns: _Optional[_Union[ExprArgList, _Mapping]] = ...,
        reader: _Optional[_Union[SpDataframeReader, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpReadTable(_message.Message):
    __slots__ = ("name", "reader", "src")
    NAME_FIELD_NUMBER: _ClassVar[int]
    READER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    name: SpTableName
    reader: SpDataframeReader
    src: SrcPosition
    def __init__(
        self,
        name: _Optional[_Union[SpTableName, _Mapping]] = ...,
        reader: _Optional[_Union[SpDataframeReader, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpReadCsv(_message.Message):
    __slots__ = ("path", "reader", "src")
    PATH_FIELD_NUMBER: _ClassVar[int]
    READER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    path: str
    reader: SpDataframeReader
    src: SrcPosition
    def __init__(
        self,
        path: _Optional[str] = ...,
        reader: _Optional[_Union[SpDataframeReader, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpReadJson(_message.Message):
    __slots__ = ("path", "reader", "src")
    PATH_FIELD_NUMBER: _ClassVar[int]
    READER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    path: str
    reader: SpDataframeReader
    src: SrcPosition
    def __init__(
        self,
        path: _Optional[str] = ...,
        reader: _Optional[_Union[SpDataframeReader, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpReadAvro(_message.Message):
    __slots__ = ("path", "reader", "src")
    PATH_FIELD_NUMBER: _ClassVar[int]
    READER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    path: str
    reader: SpDataframeReader
    src: SrcPosition
    def __init__(
        self,
        path: _Optional[str] = ...,
        reader: _Optional[_Union[SpDataframeReader, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpReadOrc(_message.Message):
    __slots__ = ("path", "reader", "src")
    PATH_FIELD_NUMBER: _ClassVar[int]
    READER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    path: str
    reader: SpDataframeReader
    src: SrcPosition
    def __init__(
        self,
        path: _Optional[str] = ...,
        reader: _Optional[_Union[SpDataframeReader, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpReadParquet(_message.Message):
    __slots__ = ("path", "reader", "src")
    PATH_FIELD_NUMBER: _ClassVar[int]
    READER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    path: str
    reader: SpDataframeReader
    src: SrcPosition
    def __init__(
        self,
        path: _Optional[str] = ...,
        reader: _Optional[_Union[SpDataframeReader, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpReadXml(_message.Message):
    __slots__ = ("path", "reader", "src")
    PATH_FIELD_NUMBER: _ClassVar[int]
    READER_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    path: str
    reader: SpDataframeReader
    src: SrcPosition
    def __init__(
        self,
        path: _Optional[str] = ...,
        reader: _Optional[_Union[SpDataframeReader, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeWriter(_message.Message):
    __slots__ = (
        "trait_sp_dataframe_writer_options",
        "trait_sp_dataframe_writer_save_mode",
        "trait_sp_write_file",
        "sp_write_copy_into_location",
        "sp_write_csv",
        "sp_write_json",
        "sp_write_parquet",
        "sp_write_table",
    )
    TRAIT_SP_DATAFRAME_WRITER_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_DATAFRAME_WRITER_SAVE_MODE_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_WRITE_FILE_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_COPY_INTO_LOCATION_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_CSV_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_JSON_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_PARQUET_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_TABLE_FIELD_NUMBER: _ClassVar[int]
    trait_sp_dataframe_writer_options: SpDataframeWriterOptions
    trait_sp_dataframe_writer_save_mode: SpDataframeWriterSaveMode
    trait_sp_write_file: SpWriteFile
    sp_write_copy_into_location: SpWriteCopyIntoLocation
    sp_write_csv: SpWriteCsv
    sp_write_json: SpWriteJson
    sp_write_parquet: SpWriteParquet
    sp_write_table: SpWriteTable
    def __init__(
        self,
        trait_sp_dataframe_writer_options: _Optional[
            _Union[SpDataframeWriterOptions, _Mapping]
        ] = ...,
        trait_sp_dataframe_writer_save_mode: _Optional[
            _Union[SpDataframeWriterSaveMode, _Mapping]
        ] = ...,
        trait_sp_write_file: _Optional[_Union[SpWriteFile, _Mapping]] = ...,
        sp_write_copy_into_location: _Optional[
            _Union[SpWriteCopyIntoLocation, _Mapping]
        ] = ...,
        sp_write_csv: _Optional[_Union[SpWriteCsv, _Mapping]] = ...,
        sp_write_json: _Optional[_Union[SpWriteJson, _Mapping]] = ...,
        sp_write_parquet: _Optional[_Union[SpWriteParquet, _Mapping]] = ...,
        sp_write_table: _Optional[_Union[SpWriteTable, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeWriterSaveMode(_message.Message):
    __slots__ = ("dummy",)
    DUMMY_FIELD_NUMBER: _ClassVar[int]
    dummy: bool
    def __init__(self, dummy: bool = ...) -> None: ...

class SpDataframeWriterOptions(_message.Message):
    __slots__ = ("dummy",)
    DUMMY_FIELD_NUMBER: _ClassVar[int]
    dummy: bool
    def __init__(self, dummy: bool = ...) -> None: ...

class SpDataframeWrite(_message.Message):
    __slots__ = ("df", "save_mode", "src")
    DF_FIELD_NUMBER: _ClassVar[int]
    SAVE_MODE_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    save_mode: SpSaveMode
    src: SrcPosition
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        save_mode: _Optional[_Union[SpSaveMode, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpWriteFile(_message.Message):
    __slots__ = (
        "sp_write_copy_into_location",
        "sp_write_csv",
        "sp_write_json",
        "sp_write_parquet",
    )
    SP_WRITE_COPY_INTO_LOCATION_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_CSV_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_JSON_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_PARQUET_FIELD_NUMBER: _ClassVar[int]
    sp_write_copy_into_location: SpWriteCopyIntoLocation
    sp_write_csv: SpWriteCsv
    sp_write_json: SpWriteJson
    sp_write_parquet: SpWriteParquet
    def __init__(
        self,
        sp_write_copy_into_location: _Optional[
            _Union[SpWriteCopyIntoLocation, _Mapping]
        ] = ...,
        sp_write_csv: _Optional[_Union[SpWriteCsv, _Mapping]] = ...,
        sp_write_json: _Optional[_Union[SpWriteJson, _Mapping]] = ...,
        sp_write_parquet: _Optional[_Union[SpWriteParquet, _Mapping]] = ...,
    ) -> None: ...

class SpWriteCsv(_message.Message):
    __slots__ = (
        "block",
        "copy_options",
        "format_type_options",
        "header",
        "id",
        "location",
        "partition_by",
        "src",
        "statement_params",
    )
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    COPY_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    FORMAT_TYPE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    HEADER_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    PARTITION_BY_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    block: bool
    copy_options: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    format_type_options: _containers.RepeatedCompositeFieldContainer[
        Tuple_String_String
    ]
    header: bool
    id: VarId
    location: str
    partition_by: Expr
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        block: bool = ...,
        copy_options: _Optional[_Iterable[_Union[Tuple_String_Expr, _Mapping]]] = ...,
        format_type_options: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
        header: bool = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        location: _Optional[str] = ...,
        partition_by: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpWriteJson(_message.Message):
    __slots__ = (
        "block",
        "copy_options",
        "format_type_options",
        "header",
        "id",
        "location",
        "partition_by",
        "src",
        "statement_params",
    )
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    COPY_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    FORMAT_TYPE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    HEADER_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    PARTITION_BY_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    block: bool
    copy_options: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    format_type_options: _containers.RepeatedCompositeFieldContainer[
        Tuple_String_String
    ]
    header: bool
    id: VarId
    location: str
    partition_by: Expr
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        block: bool = ...,
        copy_options: _Optional[_Iterable[_Union[Tuple_String_Expr, _Mapping]]] = ...,
        format_type_options: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
        header: bool = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        location: _Optional[str] = ...,
        partition_by: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpWriteParquet(_message.Message):
    __slots__ = (
        "block",
        "copy_options",
        "format_type_options",
        "header",
        "id",
        "location",
        "partition_by",
        "src",
        "statement_params",
    )
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    COPY_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    FORMAT_TYPE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    HEADER_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    PARTITION_BY_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    block: bool
    copy_options: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    format_type_options: _containers.RepeatedCompositeFieldContainer[
        Tuple_String_String
    ]
    header: bool
    id: VarId
    location: str
    partition_by: Expr
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        block: bool = ...,
        copy_options: _Optional[_Iterable[_Union[Tuple_String_Expr, _Mapping]]] = ...,
        format_type_options: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
        header: bool = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        location: _Optional[str] = ...,
        partition_by: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpWriteTable(_message.Message):
    __slots__ = (
        "block",
        "change_tracking",
        "clustering_keys",
        "column_order",
        "comment",
        "copy_grants",
        "create_temp_table",
        "data_retention_time",
        "enable_schema_evolution",
        "iceberg_config",
        "id",
        "max_data_extension_time",
        "mode",
        "src",
        "statement_params",
        "table_name",
        "table_type",
    )
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    CHANGE_TRACKING_FIELD_NUMBER: _ClassVar[int]
    CLUSTERING_KEYS_FIELD_NUMBER: _ClassVar[int]
    COLUMN_ORDER_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    COPY_GRANTS_FIELD_NUMBER: _ClassVar[int]
    CREATE_TEMP_TABLE_FIELD_NUMBER: _ClassVar[int]
    DATA_RETENTION_TIME_FIELD_NUMBER: _ClassVar[int]
    ENABLE_SCHEMA_EVOLUTION_FIELD_NUMBER: _ClassVar[int]
    ICEBERG_CONFIG_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    MAX_DATA_EXTENSION_TIME_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TABLE_TYPE_FIELD_NUMBER: _ClassVar[int]
    block: bool
    change_tracking: _wrappers_pb2.BoolValue
    clustering_keys: List_Expr
    column_order: str
    comment: _wrappers_pb2.StringValue
    copy_grants: bool
    create_temp_table: bool
    data_retention_time: _wrappers_pb2.Int64Value
    enable_schema_evolution: _wrappers_pb2.BoolValue
    iceberg_config: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    id: VarId
    max_data_extension_time: _wrappers_pb2.Int64Value
    mode: SpSaveMode
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    table_name: SpTableName
    table_type: str
    def __init__(
        self,
        block: bool = ...,
        change_tracking: _Optional[_Union[_wrappers_pb2.BoolValue, _Mapping]] = ...,
        clustering_keys: _Optional[_Union[List_Expr, _Mapping]] = ...,
        column_order: _Optional[str] = ...,
        comment: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        copy_grants: bool = ...,
        create_temp_table: bool = ...,
        data_retention_time: _Optional[
            _Union[_wrappers_pb2.Int64Value, _Mapping]
        ] = ...,
        enable_schema_evolution: _Optional[
            _Union[_wrappers_pb2.BoolValue, _Mapping]
        ] = ...,
        iceberg_config: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        max_data_extension_time: _Optional[
            _Union[_wrappers_pb2.Int64Value, _Mapping]
        ] = ...,
        mode: _Optional[_Union[SpSaveMode, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
        table_name: _Optional[_Union[SpTableName, _Mapping]] = ...,
        table_type: _Optional[str] = ...,
    ) -> None: ...

class SpWriteCopyIntoLocation(_message.Message):
    __slots__ = (
        "block",
        "copy_options",
        "file_format_name",
        "file_format_type",
        "format_type_options",
        "header",
        "id",
        "location",
        "partition_by",
        "src",
        "statement_params",
    )
    BLOCK_FIELD_NUMBER: _ClassVar[int]
    COPY_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    FILE_FORMAT_NAME_FIELD_NUMBER: _ClassVar[int]
    FILE_FORMAT_TYPE_FIELD_NUMBER: _ClassVar[int]
    FORMAT_TYPE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    HEADER_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    LOCATION_FIELD_NUMBER: _ClassVar[int]
    PARTITION_BY_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    block: bool
    copy_options: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    file_format_name: _wrappers_pb2.StringValue
    file_format_type: _wrappers_pb2.StringValue
    format_type_options: _containers.RepeatedCompositeFieldContainer[
        Tuple_String_String
    ]
    header: bool
    id: VarId
    location: str
    partition_by: Expr
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        block: bool = ...,
        copy_options: _Optional[_Iterable[_Union[Tuple_String_Expr, _Mapping]]] = ...,
        file_format_name: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        file_format_type: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        format_type_options: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
        header: bool = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        location: _Optional[str] = ...,
        partition_by: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpDataframeCreateOrReplaceView(_message.Message):
    __slots__ = ("comment", "df", "is_temp", "name", "src", "statement_params")
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    IS_TEMP_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    comment: _wrappers_pb2.StringValue
    df: SpDataframeExpr
    is_temp: bool
    name: _containers.RepeatedScalarFieldContainer[str]
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        comment: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        is_temp: bool = ...,
        name: _Optional[_Iterable[str]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpDataframeCreateOrReplaceDynamicTable(_message.Message):
    __slots__ = (
        "clustering_keys",
        "comment",
        "data_retention_time",
        "df",
        "initialize",
        "is_transient",
        "lag",
        "max_data_extension_time",
        "mode",
        "name",
        "refresh_mode",
        "src",
        "statement_params",
        "warehouse",
    )
    CLUSTERING_KEYS_FIELD_NUMBER: _ClassVar[int]
    COMMENT_FIELD_NUMBER: _ClassVar[int]
    DATA_RETENTION_TIME_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    INITIALIZE_FIELD_NUMBER: _ClassVar[int]
    IS_TRANSIENT_FIELD_NUMBER: _ClassVar[int]
    LAG_FIELD_NUMBER: _ClassVar[int]
    MAX_DATA_EXTENSION_TIME_FIELD_NUMBER: _ClassVar[int]
    MODE_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    REFRESH_MODE_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    WAREHOUSE_FIELD_NUMBER: _ClassVar[int]
    clustering_keys: List_Expr
    comment: _wrappers_pb2.StringValue
    data_retention_time: _wrappers_pb2.Int64Value
    df: SpDataframeExpr
    initialize: _wrappers_pb2.StringValue
    is_transient: bool
    lag: str
    max_data_extension_time: _wrappers_pb2.Int64Value
    mode: SpSaveMode
    name: _containers.RepeatedScalarFieldContainer[str]
    refresh_mode: _wrappers_pb2.StringValue
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    warehouse: str
    def __init__(
        self,
        clustering_keys: _Optional[_Union[List_Expr, _Mapping]] = ...,
        comment: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        data_retention_time: _Optional[
            _Union[_wrappers_pb2.Int64Value, _Mapping]
        ] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        initialize: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        is_transient: bool = ...,
        lag: _Optional[str] = ...,
        max_data_extension_time: _Optional[
            _Union[_wrappers_pb2.Int64Value, _Mapping]
        ] = ...,
        mode: _Optional[_Union[SpSaveMode, _Mapping]] = ...,
        name: _Optional[_Iterable[str]] = ...,
        refresh_mode: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
        warehouse: _Optional[str] = ...,
    ) -> None: ...

class SpDataframeCopyIntoTable(_message.Message):
    __slots__ = (
        "copy_options",
        "df",
        "files",
        "format_type_options",
        "iceberg_config",
        "pattern",
        "src",
        "statement_params",
        "table_name",
        "target_columns",
        "transformations",
        "validation_mode",
    )
    COPY_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    FILES_FIELD_NUMBER: _ClassVar[int]
    FORMAT_TYPE_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    ICEBERG_CONFIG_FIELD_NUMBER: _ClassVar[int]
    PATTERN_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    TABLE_NAME_FIELD_NUMBER: _ClassVar[int]
    TARGET_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    TRANSFORMATIONS_FIELD_NUMBER: _ClassVar[int]
    VALIDATION_MODE_FIELD_NUMBER: _ClassVar[int]
    copy_options: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    df: SpDataframeExpr
    files: _containers.RepeatedScalarFieldContainer[str]
    format_type_options: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    iceberg_config: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    pattern: _wrappers_pb2.StringValue
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    table_name: _containers.RepeatedScalarFieldContainer[str]
    target_columns: _containers.RepeatedScalarFieldContainer[str]
    transformations: _containers.RepeatedCompositeFieldContainer[Expr]
    validation_mode: _wrappers_pb2.StringValue
    def __init__(
        self,
        copy_options: _Optional[_Iterable[_Union[Tuple_String_Expr, _Mapping]]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        files: _Optional[_Iterable[str]] = ...,
        format_type_options: _Optional[
            _Iterable[_Union[Tuple_String_Expr, _Mapping]]
        ] = ...,
        iceberg_config: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
        pattern: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
        table_name: _Optional[_Iterable[str]] = ...,
        target_columns: _Optional[_Iterable[str]] = ...,
        transformations: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        validation_mode: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeCacheResult(_message.Message):
    __slots__ = ("df", "src", "statement_params")
    DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    df: SpDataframeExpr
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpDataframeStatApproxQuantile(_message.Message):
    __slots__ = ("cols", "id", "percentile", "src", "statement_params")
    COLS_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    PERCENTILE_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    cols: _containers.RepeatedCompositeFieldContainer[Expr]
    id: VarId
    percentile: _containers.RepeatedScalarFieldContainer[float]
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        cols: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        percentile: _Optional[_Iterable[float]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpDataframeStatCorr(_message.Message):
    __slots__ = ("col1", "col2", "id", "src", "statement_params")
    COL1_FIELD_NUMBER: _ClassVar[int]
    COL2_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    col1: Expr
    col2: Expr
    id: VarId
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        col1: _Optional[_Union[Expr, _Mapping]] = ...,
        col2: _Optional[_Union[Expr, _Mapping]] = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpDataframeStatCov(_message.Message):
    __slots__ = ("col1", "col2", "id", "src", "statement_params")
    COL1_FIELD_NUMBER: _ClassVar[int]
    COL2_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    col1: Expr
    col2: Expr
    id: VarId
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        col1: _Optional[_Union[Expr, _Mapping]] = ...,
        col2: _Optional[_Union[Expr, _Mapping]] = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpDataframeStatCrossTab(_message.Message):
    __slots__ = ("col1", "col2", "id", "src", "statement_params")
    COL1_FIELD_NUMBER: _ClassVar[int]
    COL2_FIELD_NUMBER: _ClassVar[int]
    ID_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    STATEMENT_PARAMS_FIELD_NUMBER: _ClassVar[int]
    col1: Expr
    col2: Expr
    id: VarId
    src: SrcPosition
    statement_params: _containers.RepeatedCompositeFieldContainer[Tuple_String_String]
    def __init__(
        self,
        col1: _Optional[_Union[Expr, _Mapping]] = ...,
        col2: _Optional[_Union[Expr, _Mapping]] = ...,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        statement_params: _Optional[
            _Iterable[_Union[Tuple_String_String, _Mapping]]
        ] = ...,
    ) -> None: ...

class SpDataframeStatSampleBy(_message.Message):
    __slots__ = ("col", "df", "fractions", "src")
    COL_FIELD_NUMBER: _ClassVar[int]
    DF_FIELD_NUMBER: _ClassVar[int]
    FRACTIONS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    col: Expr
    df: SpDataframeExpr
    fractions: _containers.RepeatedCompositeFieldContainer[Tuple_Expr_Float]
    src: SrcPosition
    def __init__(
        self,
        col: _Optional[_Union[Expr, _Mapping]] = ...,
        df: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        fractions: _Optional[_Iterable[_Union[Tuple_Expr_Float, _Mapping]]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpRelationalGroupedDataframeExpr(_message.Message):
    __slots__ = (
        "sp_dataframe_cube",
        "sp_dataframe_group_by",
        "sp_dataframe_group_by_grouping_sets",
        "sp_dataframe_group_by__columns",
        "sp_dataframe_group_by__strings",
        "sp_dataframe_pivot",
        "sp_dataframe_rollup",
        "sp_dataframe_rollup__columns",
        "sp_dataframe_rollup__strings",
        "sp_relational_grouped_dataframe_ref",
    )
    SP_DATAFRAME_CUBE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_GROUP_BY_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_GROUP_BY_GROUPING_SETS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_GROUP_BY__COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_GROUP_BY__STRINGS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_PIVOT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ROLLUP_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ROLLUP__COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ROLLUP__STRINGS_FIELD_NUMBER: _ClassVar[int]
    SP_RELATIONAL_GROUPED_DATAFRAME_REF_FIELD_NUMBER: _ClassVar[int]
    sp_dataframe_cube: SpDataframeCube
    sp_dataframe_group_by: SpDataframeGroupBy
    sp_dataframe_group_by_grouping_sets: SpDataframeGroupByGroupingSets
    sp_dataframe_group_by__columns: SpDataframeGroupBy_Columns
    sp_dataframe_group_by__strings: SpDataframeGroupBy_Strings
    sp_dataframe_pivot: SpDataframePivot
    sp_dataframe_rollup: SpDataframeRollup
    sp_dataframe_rollup__columns: SpDataframeRollup_Columns
    sp_dataframe_rollup__strings: SpDataframeRollup_Strings
    sp_relational_grouped_dataframe_ref: SpRelationalGroupedDataframeRef
    def __init__(
        self,
        sp_dataframe_cube: _Optional[_Union[SpDataframeCube, _Mapping]] = ...,
        sp_dataframe_group_by: _Optional[_Union[SpDataframeGroupBy, _Mapping]] = ...,
        sp_dataframe_group_by_grouping_sets: _Optional[
            _Union[SpDataframeGroupByGroupingSets, _Mapping]
        ] = ...,
        sp_dataframe_group_by__columns: _Optional[
            _Union[SpDataframeGroupBy_Columns, _Mapping]
        ] = ...,
        sp_dataframe_group_by__strings: _Optional[
            _Union[SpDataframeGroupBy_Strings, _Mapping]
        ] = ...,
        sp_dataframe_pivot: _Optional[_Union[SpDataframePivot, _Mapping]] = ...,
        sp_dataframe_rollup: _Optional[_Union[SpDataframeRollup, _Mapping]] = ...,
        sp_dataframe_rollup__columns: _Optional[
            _Union[SpDataframeRollup_Columns, _Mapping]
        ] = ...,
        sp_dataframe_rollup__strings: _Optional[
            _Union[SpDataframeRollup_Strings, _Mapping]
        ] = ...,
        sp_relational_grouped_dataframe_ref: _Optional[
            _Union[SpRelationalGroupedDataframeRef, _Mapping]
        ] = ...,
    ) -> None: ...

class SpRelationalGroupedDataframeRef(_message.Message):
    __slots__ = ("id", "src")
    ID_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    id: VarId
    src: SrcPosition
    def __init__(
        self,
        id: _Optional[_Union[VarId, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpRelationalGroupedDataframeAgg(_message.Message):
    __slots__ = ("exprs", "grouped_df", "src")
    EXPRS_FIELD_NUMBER: _ClassVar[int]
    GROUPED_DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    exprs: ExprArgList
    grouped_df: SpRelationalGroupedDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        exprs: _Optional[_Union[ExprArgList, _Mapping]] = ...,
        grouped_df: _Optional[_Union[SpRelationalGroupedDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpRelationalGroupedDataframeBuiltin(_message.Message):
    __slots__ = ("agg_name", "cols", "grouped_df", "src")
    AGG_NAME_FIELD_NUMBER: _ClassVar[int]
    COLS_FIELD_NUMBER: _ClassVar[int]
    GROUPED_DF_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    agg_name: str
    cols: ExprArgList
    grouped_df: SpRelationalGroupedDataframeExpr
    src: SrcPosition
    def __init__(
        self,
        agg_name: _Optional[str] = ...,
        cols: _Optional[_Union[ExprArgList, _Mapping]] = ...,
        grouped_df: _Optional[_Union[SpRelationalGroupedDataframeExpr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpRelationalGroupedDataframeApplyInPandas(_message.Message):
    __slots__ = ("func", "grouped_df", "kwargs", "output_schema", "src")
    FUNC_FIELD_NUMBER: _ClassVar[int]
    GROUPED_DF_FIELD_NUMBER: _ClassVar[int]
    KWARGS_FIELD_NUMBER: _ClassVar[int]
    OUTPUT_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    func: SpCallable
    grouped_df: SpRelationalGroupedDataframeExpr
    kwargs: _containers.RepeatedCompositeFieldContainer[Tuple_String_Expr]
    output_schema: SpStructType
    src: SrcPosition
    def __init__(
        self,
        func: _Optional[_Union[SpCallable, _Mapping]] = ...,
        grouped_df: _Optional[_Union[SpRelationalGroupedDataframeExpr, _Mapping]] = ...,
        kwargs: _Optional[_Iterable[_Union[Tuple_String_Expr, _Mapping]]] = ...,
        output_schema: _Optional[_Union[SpStructType, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpRelationalGroupedDataframePivot(_message.Message):
    __slots__ = ("default_on_null", "grouped_df", "pivot_col", "src", "values")
    DEFAULT_ON_NULL_FIELD_NUMBER: _ClassVar[int]
    GROUPED_DF_FIELD_NUMBER: _ClassVar[int]
    PIVOT_COL_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    default_on_null: Expr
    grouped_df: SpRelationalGroupedDataframeExpr
    pivot_col: Expr
    src: SrcPosition
    values: SpPivotValue
    def __init__(
        self,
        default_on_null: _Optional[_Union[Expr, _Mapping]] = ...,
        grouped_df: _Optional[_Union[SpRelationalGroupedDataframeExpr, _Mapping]] = ...,
        pivot_col: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        values: _Optional[_Union[SpPivotValue, _Mapping]] = ...,
    ) -> None: ...

class SpRow(_message.Message):
    __slots__ = ("names", "src", "vs")
    NAMES_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    VS_FIELD_NUMBER: _ClassVar[int]
    names: List_String
    src: SrcPosition
    vs: _containers.RepeatedCompositeFieldContainer[Expr]
    def __init__(
        self,
        names: _Optional[_Union[List_String, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
        vs: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
    ) -> None: ...

class ExprArgList(_message.Message):
    __slots__ = ("args", "variadic")
    ARGS_FIELD_NUMBER: _ClassVar[int]
    VARIADIC_FIELD_NUMBER: _ClassVar[int]
    args: _containers.RepeatedCompositeFieldContainer[Expr]
    variadic: bool
    def __init__(
        self,
        args: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        variadic: bool = ...,
    ) -> None: ...

class SpTableFnCallOver(_message.Message):
    __slots__ = ("lhs", "order_by", "partition_by", "src")
    LHS_FIELD_NUMBER: _ClassVar[int]
    ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    PARTITION_BY_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    lhs: Expr
    order_by: _containers.RepeatedCompositeFieldContainer[Expr]
    partition_by: _containers.RepeatedCompositeFieldContainer[Expr]
    src: SrcPosition
    def __init__(
        self,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        order_by: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        partition_by: _Optional[_Iterable[_Union[Expr, _Mapping]]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpTableFnCallAlias(_message.Message):
    __slots__ = ("aliases", "lhs", "src")
    ALIASES_FIELD_NUMBER: _ClassVar[int]
    LHS_FIELD_NUMBER: _ClassVar[int]
    SRC_FIELD_NUMBER: _ClassVar[int]
    aliases: ExprArgList
    lhs: Expr
    src: SrcPosition
    def __init__(
        self,
        aliases: _Optional[_Union[ExprArgList, _Mapping]] = ...,
        lhs: _Optional[_Union[Expr, _Mapping]] = ...,
        src: _Optional[_Union[SrcPosition, _Mapping]] = ...,
    ) -> None: ...

class SpDataframeType(_message.Message):
    __slots__ = ("columns", "tys")
    COLUMNS_FIELD_NUMBER: _ClassVar[int]
    TYS_FIELD_NUMBER: _ClassVar[int]
    columns: _containers.RepeatedScalarFieldContainer[str]
    tys: _containers.RepeatedCompositeFieldContainer[Type]
    def __init__(
        self,
        columns: _Optional[_Iterable[str]] = ...,
        tys: _Optional[_Iterable[_Union[Type, _Mapping]]] = ...,
    ) -> None: ...

class SpGroupedDataframeType(_message.Message):
    __slots__ = ("inner_columns", "outer_columns")
    INNER_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    OUTER_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    inner_columns: _containers.RepeatedScalarFieldContainer[str]
    outer_columns: _containers.RepeatedScalarFieldContainer[str]
    def __init__(
        self,
        inner_columns: _Optional[_Iterable[str]] = ...,
        outer_columns: _Optional[_Iterable[str]] = ...,
    ) -> None: ...

class SpWindowType(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class SpColExprType(_message.Message):
    __slots__ = ("typ",)
    TYP_FIELD_NUMBER: _ClassVar[int]
    typ: Type
    def __init__(self, typ: _Optional[_Union[Type, _Mapping]] = ...) -> None: ...

class HasSrcPosition(_message.Message):
    __slots__ = (
        "trait_bin_op",
        "trait_const",
        "trait_expr",
        "trait_fn_id_ref_expr",
        "trait_fn_name_ref_expr",
        "trait_fn_ref_expr",
        "trait_sp_column_expr",
        "trait_sp_column_fn",
        "trait_sp_dataframe_expr",
        "trait_sp_dataframe_reader",
        "trait_sp_dataframe_writer",
        "trait_sp_dataframe_writer_options",
        "trait_sp_dataframe_writer_save_mode",
        "trait_sp_matched_clause",
        "trait_sp_relational_grouped_dataframe_expr",
        "trait_sp_window_spec_expr",
        "trait_sp_write_file",
        "trait_unary_op",
        "add",
        "apply_expr",
        "big_decimal_val",
        "big_int_val",
        "binary_val",
        "bit_and",
        "bit_or",
        "bit_xor",
        "bool_val",
        "builtin_fn",
        "call_table_function_expr",
        "cast_expr",
        "date_val",
        "div",
        "eq",
        "float64_val",
        "fn_val",
        "geq",
        "gt",
        "if_expr",
        "indirect_table_fn_id_ref",
        "indirect_table_fn_name_ref",
        "int32_val",
        "int64_val",
        "leq",
        "list_val",
        "lt",
        "mod",
        "mul",
        "neg",
        "neq",
        "none_val",
        "null_val",
        "object_get_item",
        "pd_dataframe",
        "pd_dataframe_get_item",
        "pd_dataframe_i_loc",
        "pd_dataframe_loc",
        "pd_dataframe_set_item",
        "pd_drop_na",
        "pd_repr",
        "pow",
        "python_date_val",
        "python_time_val",
        "python_timestamp_val",
        "range_val",
        "ref",
        "seq_map_val",
        "some_val",
        "sp_case_expr",
        "sp_column_alias",
        "sp_column_apply__int",
        "sp_column_apply__string",
        "sp_column_asc",
        "sp_column_between",
        "sp_column_case_when",
        "sp_column_cast",
        "sp_column_desc",
        "sp_column_equal_nan",
        "sp_column_equal_null",
        "sp_column_in__dataframe",
        "sp_column_in__seq",
        "sp_column_is_not_null",
        "sp_column_is_null",
        "sp_column_name",
        "sp_column_over",
        "sp_column_ref",
        "sp_column_sql_expr",
        "sp_column_string_collate",
        "sp_column_string_contains",
        "sp_column_string_ends_with",
        "sp_column_string_like",
        "sp_column_string_regexp",
        "sp_column_string_starts_with",
        "sp_column_string_substr",
        "sp_column_try_cast",
        "sp_column_within_group",
        "sp_create_dataframe",
        "sp_dataframe_agg",
        "sp_dataframe_alias",
        "sp_dataframe_analytics_compute_lag",
        "sp_dataframe_analytics_compute_lead",
        "sp_dataframe_analytics_cumulative_agg",
        "sp_dataframe_analytics_moving_agg",
        "sp_dataframe_analytics_time_series_agg",
        "sp_dataframe_apply",
        "sp_dataframe_cache_result",
        "sp_dataframe_col",
        "sp_dataframe_collect",
        "sp_dataframe_copy_into_table",
        "sp_dataframe_count",
        "sp_dataframe_create_or_replace_dynamic_table",
        "sp_dataframe_create_or_replace_view",
        "sp_dataframe_cross_join",
        "sp_dataframe_cube",
        "sp_dataframe_describe",
        "sp_dataframe_distinct",
        "sp_dataframe_drop",
        "sp_dataframe_drop_duplicates",
        "sp_dataframe_except",
        "sp_dataframe_filter",
        "sp_dataframe_first",
        "sp_dataframe_flatten",
        "sp_dataframe_group_by",
        "sp_dataframe_group_by_grouping_sets",
        "sp_dataframe_group_by__columns",
        "sp_dataframe_group_by__strings",
        "sp_dataframe_intersect",
        "sp_dataframe_join",
        "sp_dataframe_join_table_function",
        "sp_dataframe_join__dataframe__join_exprs",
        "sp_dataframe_join__dataframe__using_columns",
        "sp_dataframe_limit",
        "sp_dataframe_na_drop__python",
        "sp_dataframe_na_drop__scala",
        "sp_dataframe_na_fill",
        "sp_dataframe_na_replace",
        "sp_dataframe_natural_join",
        "sp_dataframe_pivot",
        "sp_dataframe_random_split",
        "sp_dataframe_reader_init",
        "sp_dataframe_reader_option",
        "sp_dataframe_reader_options",
        "sp_dataframe_reader_schema",
        "sp_dataframe_reader_with_metadata",
        "sp_dataframe_ref",
        "sp_dataframe_rename",
        "sp_dataframe_rollup",
        "sp_dataframe_rollup__columns",
        "sp_dataframe_rollup__strings",
        "sp_dataframe_sample",
        "sp_dataframe_select__columns",
        "sp_dataframe_select__exprs",
        "sp_dataframe_show",
        "sp_dataframe_sort",
        "sp_dataframe_stat_approx_quantile",
        "sp_dataframe_stat_corr",
        "sp_dataframe_stat_cov",
        "sp_dataframe_stat_cross_tab",
        "sp_dataframe_stat_sample_by",
        "sp_dataframe_to_df",
        "sp_dataframe_to_local_iterator",
        "sp_dataframe_to_pandas",
        "sp_dataframe_to_pandas_batches",
        "sp_dataframe_union",
        "sp_dataframe_union_all",
        "sp_dataframe_union_all_by_name",
        "sp_dataframe_union_by_name",
        "sp_dataframe_unpivot",
        "sp_dataframe_where",
        "sp_dataframe_with_column",
        "sp_dataframe_with_column_renamed",
        "sp_dataframe_with_columns",
        "sp_dataframe_write",
        "sp_datatype_val",
        "sp_flatten",
        "sp_fn_ref",
        "sp_generator",
        "sp_grouping_sets",
        "sp_merge_delete_when_matched_clause",
        "sp_merge_insert_when_not_matched_clause",
        "sp_merge_update_when_matched_clause",
        "sp_range",
        "sp_read_avro",
        "sp_read_csv",
        "sp_read_json",
        "sp_read_orc",
        "sp_read_parquet",
        "sp_read_table",
        "sp_read_xml",
        "sp_relational_grouped_dataframe_agg",
        "sp_relational_grouped_dataframe_apply_in_pandas",
        "sp_relational_grouped_dataframe_builtin",
        "sp_relational_grouped_dataframe_pivot",
        "sp_relational_grouped_dataframe_ref",
        "sp_row",
        "sp_session_table_function",
        "sp_sql",
        "sp_stored_procedure",
        "sp_table",
        "sp_table_delete",
        "sp_table_drop_table",
        "sp_table_fn_call_alias",
        "sp_table_fn_call_over",
        "sp_table_merge",
        "sp_table_sample",
        "sp_table_update",
        "sp_window_spec_empty",
        "sp_window_spec_order_by",
        "sp_window_spec_partition_by",
        "sp_window_spec_range_between",
        "sp_window_spec_rows_between",
        "sp_write_copy_into_location",
        "sp_write_csv",
        "sp_write_json",
        "sp_write_pandas",
        "sp_write_parquet",
        "sp_write_table",
        "stored_procedure",
        "string_val",
        "sub",
        "time_val",
        "timestamp_val",
        "tuple_val",
        "udaf",
        "udf",
        "udtf",
    )
    TRAIT_BIN_OP_FIELD_NUMBER: _ClassVar[int]
    TRAIT_CONST_FIELD_NUMBER: _ClassVar[int]
    TRAIT_EXPR_FIELD_NUMBER: _ClassVar[int]
    TRAIT_FN_ID_REF_EXPR_FIELD_NUMBER: _ClassVar[int]
    TRAIT_FN_NAME_REF_EXPR_FIELD_NUMBER: _ClassVar[int]
    TRAIT_FN_REF_EXPR_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_COLUMN_EXPR_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_COLUMN_FN_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_DATAFRAME_EXPR_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_DATAFRAME_READER_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_DATAFRAME_WRITER_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_DATAFRAME_WRITER_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_DATAFRAME_WRITER_SAVE_MODE_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_MATCHED_CLAUSE_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_RELATIONAL_GROUPED_DATAFRAME_EXPR_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_WINDOW_SPEC_EXPR_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SP_WRITE_FILE_FIELD_NUMBER: _ClassVar[int]
    TRAIT_UNARY_OP_FIELD_NUMBER: _ClassVar[int]
    ADD_FIELD_NUMBER: _ClassVar[int]
    AND_FIELD_NUMBER: _ClassVar[int]
    APPLY_EXPR_FIELD_NUMBER: _ClassVar[int]
    BIG_DECIMAL_VAL_FIELD_NUMBER: _ClassVar[int]
    BIG_INT_VAL_FIELD_NUMBER: _ClassVar[int]
    BINARY_VAL_FIELD_NUMBER: _ClassVar[int]
    BIT_AND_FIELD_NUMBER: _ClassVar[int]
    BIT_OR_FIELD_NUMBER: _ClassVar[int]
    BIT_XOR_FIELD_NUMBER: _ClassVar[int]
    BOOL_VAL_FIELD_NUMBER: _ClassVar[int]
    BUILTIN_FN_FIELD_NUMBER: _ClassVar[int]
    CALL_TABLE_FUNCTION_EXPR_FIELD_NUMBER: _ClassVar[int]
    CAST_EXPR_FIELD_NUMBER: _ClassVar[int]
    DATE_VAL_FIELD_NUMBER: _ClassVar[int]
    DIV_FIELD_NUMBER: _ClassVar[int]
    EQ_FIELD_NUMBER: _ClassVar[int]
    FLOAT64_VAL_FIELD_NUMBER: _ClassVar[int]
    FN_VAL_FIELD_NUMBER: _ClassVar[int]
    GEQ_FIELD_NUMBER: _ClassVar[int]
    GT_FIELD_NUMBER: _ClassVar[int]
    IF_EXPR_FIELD_NUMBER: _ClassVar[int]
    INDIRECT_TABLE_FN_ID_REF_FIELD_NUMBER: _ClassVar[int]
    INDIRECT_TABLE_FN_NAME_REF_FIELD_NUMBER: _ClassVar[int]
    INT32_VAL_FIELD_NUMBER: _ClassVar[int]
    INT64_VAL_FIELD_NUMBER: _ClassVar[int]
    LEQ_FIELD_NUMBER: _ClassVar[int]
    LIST_VAL_FIELD_NUMBER: _ClassVar[int]
    LT_FIELD_NUMBER: _ClassVar[int]
    MOD_FIELD_NUMBER: _ClassVar[int]
    MUL_FIELD_NUMBER: _ClassVar[int]
    NEG_FIELD_NUMBER: _ClassVar[int]
    NEQ_FIELD_NUMBER: _ClassVar[int]
    NONE_VAL_FIELD_NUMBER: _ClassVar[int]
    NOT_FIELD_NUMBER: _ClassVar[int]
    NULL_VAL_FIELD_NUMBER: _ClassVar[int]
    OBJECT_GET_ITEM_FIELD_NUMBER: _ClassVar[int]
    OR_FIELD_NUMBER: _ClassVar[int]
    PD_DATAFRAME_FIELD_NUMBER: _ClassVar[int]
    PD_DATAFRAME_GET_ITEM_FIELD_NUMBER: _ClassVar[int]
    PD_DATAFRAME_I_LOC_FIELD_NUMBER: _ClassVar[int]
    PD_DATAFRAME_LOC_FIELD_NUMBER: _ClassVar[int]
    PD_DATAFRAME_SET_ITEM_FIELD_NUMBER: _ClassVar[int]
    PD_DROP_NA_FIELD_NUMBER: _ClassVar[int]
    PD_REPR_FIELD_NUMBER: _ClassVar[int]
    POW_FIELD_NUMBER: _ClassVar[int]
    PYTHON_DATE_VAL_FIELD_NUMBER: _ClassVar[int]
    PYTHON_TIME_VAL_FIELD_NUMBER: _ClassVar[int]
    PYTHON_TIMESTAMP_VAL_FIELD_NUMBER: _ClassVar[int]
    RANGE_VAL_FIELD_NUMBER: _ClassVar[int]
    REF_FIELD_NUMBER: _ClassVar[int]
    SEQ_MAP_VAL_FIELD_NUMBER: _ClassVar[int]
    SOME_VAL_FIELD_NUMBER: _ClassVar[int]
    SP_CASE_EXPR_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_ALIAS_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_APPLY__INT_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_APPLY__STRING_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_ASC_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_BETWEEN_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_CASE_WHEN_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_CAST_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_DESC_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_EQUAL_NAN_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_EQUAL_NULL_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_IN__DATAFRAME_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_IN__SEQ_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_IS_NOT_NULL_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_IS_NULL_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_NAME_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_OVER_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_REF_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_SQL_EXPR_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_COLLATE_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_CONTAINS_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_ENDS_WITH_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_LIKE_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_REGEXP_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_STARTS_WITH_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_STRING_SUBSTR_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_TRY_CAST_FIELD_NUMBER: _ClassVar[int]
    SP_COLUMN_WITHIN_GROUP_FIELD_NUMBER: _ClassVar[int]
    SP_CREATE_DATAFRAME_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_AGG_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ALIAS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ANALYTICS_COMPUTE_LAG_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ANALYTICS_COMPUTE_LEAD_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ANALYTICS_CUMULATIVE_AGG_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ANALYTICS_MOVING_AGG_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ANALYTICS_TIME_SERIES_AGG_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_APPLY_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_CACHE_RESULT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_COL_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_COLLECT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_COPY_INTO_TABLE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_COUNT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_CREATE_OR_REPLACE_DYNAMIC_TABLE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_CREATE_OR_REPLACE_VIEW_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_CROSS_JOIN_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_CUBE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_DESCRIBE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_DISTINCT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_DROP_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_DROP_DUPLICATES_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_EXCEPT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_FILTER_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_FIRST_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_FLATTEN_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_GROUP_BY_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_GROUP_BY_GROUPING_SETS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_GROUP_BY__COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_GROUP_BY__STRINGS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_INTERSECT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_JOIN_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_JOIN_TABLE_FUNCTION_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_JOIN__DATAFRAME__JOIN_EXPRS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_JOIN__DATAFRAME__USING_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_LIMIT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_NA_DROP__PYTHON_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_NA_DROP__SCALA_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_NA_FILL_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_NA_REPLACE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_NATURAL_JOIN_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_PIVOT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_RANDOM_SPLIT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_READER_INIT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_READER_OPTION_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_READER_OPTIONS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_READER_SCHEMA_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_READER_WITH_METADATA_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_REF_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_RENAME_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ROLLUP_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ROLLUP__COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_ROLLUP__STRINGS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SAMPLE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SELECT__COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SELECT__EXPRS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SHOW_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_SORT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_STAT_APPROX_QUANTILE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_STAT_CORR_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_STAT_COV_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_STAT_CROSS_TAB_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_STAT_SAMPLE_BY_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_TO_DF_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_TO_LOCAL_ITERATOR_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_TO_PANDAS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_TO_PANDAS_BATCHES_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_UNION_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_UNION_ALL_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_UNION_ALL_BY_NAME_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_UNION_BY_NAME_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_UNPIVOT_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_WHERE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_WITH_COLUMN_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_WITH_COLUMN_RENAMED_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_WITH_COLUMNS_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_WRITE_FIELD_NUMBER: _ClassVar[int]
    SP_DATATYPE_VAL_FIELD_NUMBER: _ClassVar[int]
    SP_FLATTEN_FIELD_NUMBER: _ClassVar[int]
    SP_FN_REF_FIELD_NUMBER: _ClassVar[int]
    SP_GENERATOR_FIELD_NUMBER: _ClassVar[int]
    SP_GROUPING_SETS_FIELD_NUMBER: _ClassVar[int]
    SP_MERGE_DELETE_WHEN_MATCHED_CLAUSE_FIELD_NUMBER: _ClassVar[int]
    SP_MERGE_INSERT_WHEN_NOT_MATCHED_CLAUSE_FIELD_NUMBER: _ClassVar[int]
    SP_MERGE_UPDATE_WHEN_MATCHED_CLAUSE_FIELD_NUMBER: _ClassVar[int]
    SP_RANGE_FIELD_NUMBER: _ClassVar[int]
    SP_READ_AVRO_FIELD_NUMBER: _ClassVar[int]
    SP_READ_CSV_FIELD_NUMBER: _ClassVar[int]
    SP_READ_JSON_FIELD_NUMBER: _ClassVar[int]
    SP_READ_ORC_FIELD_NUMBER: _ClassVar[int]
    SP_READ_PARQUET_FIELD_NUMBER: _ClassVar[int]
    SP_READ_TABLE_FIELD_NUMBER: _ClassVar[int]
    SP_READ_XML_FIELD_NUMBER: _ClassVar[int]
    SP_RELATIONAL_GROUPED_DATAFRAME_AGG_FIELD_NUMBER: _ClassVar[int]
    SP_RELATIONAL_GROUPED_DATAFRAME_APPLY_IN_PANDAS_FIELD_NUMBER: _ClassVar[int]
    SP_RELATIONAL_GROUPED_DATAFRAME_BUILTIN_FIELD_NUMBER: _ClassVar[int]
    SP_RELATIONAL_GROUPED_DATAFRAME_PIVOT_FIELD_NUMBER: _ClassVar[int]
    SP_RELATIONAL_GROUPED_DATAFRAME_REF_FIELD_NUMBER: _ClassVar[int]
    SP_ROW_FIELD_NUMBER: _ClassVar[int]
    SP_SESSION_TABLE_FUNCTION_FIELD_NUMBER: _ClassVar[int]
    SP_SQL_FIELD_NUMBER: _ClassVar[int]
    SP_STORED_PROCEDURE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_DELETE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_DROP_TABLE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_FN_CALL_ALIAS_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_FN_CALL_OVER_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_MERGE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_SAMPLE_FIELD_NUMBER: _ClassVar[int]
    SP_TABLE_UPDATE_FIELD_NUMBER: _ClassVar[int]
    SP_WINDOW_SPEC_EMPTY_FIELD_NUMBER: _ClassVar[int]
    SP_WINDOW_SPEC_ORDER_BY_FIELD_NUMBER: _ClassVar[int]
    SP_WINDOW_SPEC_PARTITION_BY_FIELD_NUMBER: _ClassVar[int]
    SP_WINDOW_SPEC_RANGE_BETWEEN_FIELD_NUMBER: _ClassVar[int]
    SP_WINDOW_SPEC_ROWS_BETWEEN_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_COPY_INTO_LOCATION_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_CSV_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_JSON_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_PANDAS_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_PARQUET_FIELD_NUMBER: _ClassVar[int]
    SP_WRITE_TABLE_FIELD_NUMBER: _ClassVar[int]
    STORED_PROCEDURE_FIELD_NUMBER: _ClassVar[int]
    STRING_VAL_FIELD_NUMBER: _ClassVar[int]
    SUB_FIELD_NUMBER: _ClassVar[int]
    TIME_VAL_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_VAL_FIELD_NUMBER: _ClassVar[int]
    TUPLE_VAL_FIELD_NUMBER: _ClassVar[int]
    UDAF_FIELD_NUMBER: _ClassVar[int]
    UDF_FIELD_NUMBER: _ClassVar[int]
    UDTF_FIELD_NUMBER: _ClassVar[int]
    trait_bin_op: BinOp
    trait_const: Const
    trait_expr: Expr
    trait_fn_id_ref_expr: FnIdRefExpr
    trait_fn_name_ref_expr: FnNameRefExpr
    trait_fn_ref_expr: FnRefExpr
    trait_sp_column_expr: SpColumnExpr
    trait_sp_column_fn: SpColumnFn
    trait_sp_dataframe_expr: SpDataframeExpr
    trait_sp_dataframe_reader: SpDataframeReader
    trait_sp_dataframe_writer: SpDataframeWriter
    trait_sp_dataframe_writer_options: SpDataframeWriterOptions
    trait_sp_dataframe_writer_save_mode: SpDataframeWriterSaveMode
    trait_sp_matched_clause: SpMatchedClause
    trait_sp_relational_grouped_dataframe_expr: SpRelationalGroupedDataframeExpr
    trait_sp_window_spec_expr: SpWindowSpecExpr
    trait_sp_write_file: SpWriteFile
    trait_unary_op: UnaryOp
    add: Add
    apply_expr: ApplyExpr
    big_decimal_val: BigDecimalVal
    big_int_val: BigIntVal
    binary_val: BinaryVal
    bit_and: BitAnd
    bit_or: BitOr
    bit_xor: BitXor
    bool_val: BoolVal
    builtin_fn: BuiltinFn
    call_table_function_expr: CallTableFunctionExpr
    cast_expr: CastExpr
    date_val: DateVal
    div: Div
    eq: Eq
    float64_val: Float64Val
    fn_val: FnVal
    geq: Geq
    gt: Gt
    if_expr: IfExpr
    indirect_table_fn_id_ref: IndirectTableFnIdRef
    indirect_table_fn_name_ref: IndirectTableFnNameRef
    int32_val: Int32Val
    int64_val: Int64Val
    leq: Leq
    list_val: ListVal
    lt: Lt
    mod: Mod
    mul: Mul
    neg: Neg
    neq: Neq
    none_val: NoneVal
    null_val: NullVal
    object_get_item: ObjectGetItem
    pd_dataframe: PdDataframe
    pd_dataframe_get_item: PdDataframeGetItem
    pd_dataframe_i_loc: PdDataframeILoc
    pd_dataframe_loc: PdDataframeLoc
    pd_dataframe_set_item: PdDataframeSetItem
    pd_drop_na: PdDropNa
    pd_repr: PdRepr
    pow: Pow
    python_date_val: PythonDateVal
    python_time_val: PythonTimeVal
    python_timestamp_val: PythonTimestampVal
    range_val: RangeVal
    ref: Ref
    seq_map_val: SeqMapVal
    some_val: SomeVal
    sp_case_expr: SpCaseExpr
    sp_column_alias: SpColumnAlias
    sp_column_apply__int: SpColumnApply_Int
    sp_column_apply__string: SpColumnApply_String
    sp_column_asc: SpColumnAsc
    sp_column_between: SpColumnBetween
    sp_column_case_when: SpColumnCaseWhen
    sp_column_cast: SpColumnCast
    sp_column_desc: SpColumnDesc
    sp_column_equal_nan: SpColumnEqualNan
    sp_column_equal_null: SpColumnEqualNull
    sp_column_in__dataframe: SpColumnIn_Dataframe
    sp_column_in__seq: SpColumnIn_Seq
    sp_column_is_not_null: SpColumnIsNotNull
    sp_column_is_null: SpColumnIsNull
    sp_column_name: SpColumnName
    sp_column_over: SpColumnOver
    sp_column_ref: SpColumnRef
    sp_column_sql_expr: SpColumnSqlExpr
    sp_column_string_collate: SpColumnStringCollate
    sp_column_string_contains: SpColumnStringContains
    sp_column_string_ends_with: SpColumnStringEndsWith
    sp_column_string_like: SpColumnStringLike
    sp_column_string_regexp: SpColumnStringRegexp
    sp_column_string_starts_with: SpColumnStringStartsWith
    sp_column_string_substr: SpColumnStringSubstr
    sp_column_try_cast: SpColumnTryCast
    sp_column_within_group: SpColumnWithinGroup
    sp_create_dataframe: SpCreateDataframe
    sp_dataframe_agg: SpDataframeAgg
    sp_dataframe_alias: SpDataframeAlias
    sp_dataframe_analytics_compute_lag: SpDataframeAnalyticsComputeLag
    sp_dataframe_analytics_compute_lead: SpDataframeAnalyticsComputeLead
    sp_dataframe_analytics_cumulative_agg: SpDataframeAnalyticsCumulativeAgg
    sp_dataframe_analytics_moving_agg: SpDataframeAnalyticsMovingAgg
    sp_dataframe_analytics_time_series_agg: SpDataframeAnalyticsTimeSeriesAgg
    sp_dataframe_apply: SpDataframeApply
    sp_dataframe_cache_result: SpDataframeCacheResult
    sp_dataframe_col: SpDataframeCol
    sp_dataframe_collect: SpDataframeCollect
    sp_dataframe_copy_into_table: SpDataframeCopyIntoTable
    sp_dataframe_count: SpDataframeCount
    sp_dataframe_create_or_replace_dynamic_table: SpDataframeCreateOrReplaceDynamicTable
    sp_dataframe_create_or_replace_view: SpDataframeCreateOrReplaceView
    sp_dataframe_cross_join: SpDataframeCrossJoin
    sp_dataframe_cube: SpDataframeCube
    sp_dataframe_describe: SpDataframeDescribe
    sp_dataframe_distinct: SpDataframeDistinct
    sp_dataframe_drop: SpDataframeDrop
    sp_dataframe_drop_duplicates: SpDataframeDropDuplicates
    sp_dataframe_except: SpDataframeExcept
    sp_dataframe_filter: SpDataframeFilter
    sp_dataframe_first: SpDataframeFirst
    sp_dataframe_flatten: SpDataframeFlatten
    sp_dataframe_group_by: SpDataframeGroupBy
    sp_dataframe_group_by_grouping_sets: SpDataframeGroupByGroupingSets
    sp_dataframe_group_by__columns: SpDataframeGroupBy_Columns
    sp_dataframe_group_by__strings: SpDataframeGroupBy_Strings
    sp_dataframe_intersect: SpDataframeIntersect
    sp_dataframe_join: SpDataframeJoin
    sp_dataframe_join_table_function: SpDataframeJoinTableFunction
    sp_dataframe_join__dataframe__join_exprs: SpDataframeJoin_Dataframe_JoinExprs
    sp_dataframe_join__dataframe__using_columns: SpDataframeJoin_Dataframe_UsingColumns
    sp_dataframe_limit: SpDataframeLimit
    sp_dataframe_na_drop__python: SpDataframeNaDrop_Python
    sp_dataframe_na_drop__scala: SpDataframeNaDrop_Scala
    sp_dataframe_na_fill: SpDataframeNaFill
    sp_dataframe_na_replace: SpDataframeNaReplace
    sp_dataframe_natural_join: SpDataframeNaturalJoin
    sp_dataframe_pivot: SpDataframePivot
    sp_dataframe_random_split: SpDataframeRandomSplit
    sp_dataframe_reader_init: SpDataframeReaderInit
    sp_dataframe_reader_option: SpDataframeReaderOption
    sp_dataframe_reader_options: SpDataframeReaderOptions
    sp_dataframe_reader_schema: SpDataframeReaderSchema
    sp_dataframe_reader_with_metadata: SpDataframeReaderWithMetadata
    sp_dataframe_ref: SpDataframeRef
    sp_dataframe_rename: SpDataframeRename
    sp_dataframe_rollup: SpDataframeRollup
    sp_dataframe_rollup__columns: SpDataframeRollup_Columns
    sp_dataframe_rollup__strings: SpDataframeRollup_Strings
    sp_dataframe_sample: SpDataframeSample
    sp_dataframe_select__columns: SpDataframeSelect_Columns
    sp_dataframe_select__exprs: SpDataframeSelect_Exprs
    sp_dataframe_show: SpDataframeShow
    sp_dataframe_sort: SpDataframeSort
    sp_dataframe_stat_approx_quantile: SpDataframeStatApproxQuantile
    sp_dataframe_stat_corr: SpDataframeStatCorr
    sp_dataframe_stat_cov: SpDataframeStatCov
    sp_dataframe_stat_cross_tab: SpDataframeStatCrossTab
    sp_dataframe_stat_sample_by: SpDataframeStatSampleBy
    sp_dataframe_to_df: SpDataframeToDf
    sp_dataframe_to_local_iterator: SpDataframeToLocalIterator
    sp_dataframe_to_pandas: SpDataframeToPandas
    sp_dataframe_to_pandas_batches: SpDataframeToPandasBatches
    sp_dataframe_union: SpDataframeUnion
    sp_dataframe_union_all: SpDataframeUnionAll
    sp_dataframe_union_all_by_name: SpDataframeUnionAllByName
    sp_dataframe_union_by_name: SpDataframeUnionByName
    sp_dataframe_unpivot: SpDataframeUnpivot
    sp_dataframe_where: SpDataframeWhere
    sp_dataframe_with_column: SpDataframeWithColumn
    sp_dataframe_with_column_renamed: SpDataframeWithColumnRenamed
    sp_dataframe_with_columns: SpDataframeWithColumns
    sp_dataframe_write: SpDataframeWrite
    sp_datatype_val: SpDatatypeVal
    sp_flatten: SpFlatten
    sp_fn_ref: SpFnRef
    sp_generator: SpGenerator
    sp_grouping_sets: SpGroupingSets
    sp_merge_delete_when_matched_clause: SpMergeDeleteWhenMatchedClause
    sp_merge_insert_when_not_matched_clause: SpMergeInsertWhenNotMatchedClause
    sp_merge_update_when_matched_clause: SpMergeUpdateWhenMatchedClause
    sp_range: SpRange
    sp_read_avro: SpReadAvro
    sp_read_csv: SpReadCsv
    sp_read_json: SpReadJson
    sp_read_orc: SpReadOrc
    sp_read_parquet: SpReadParquet
    sp_read_table: SpReadTable
    sp_read_xml: SpReadXml
    sp_relational_grouped_dataframe_agg: SpRelationalGroupedDataframeAgg
    sp_relational_grouped_dataframe_apply_in_pandas: SpRelationalGroupedDataframeApplyInPandas
    sp_relational_grouped_dataframe_builtin: SpRelationalGroupedDataframeBuiltin
    sp_relational_grouped_dataframe_pivot: SpRelationalGroupedDataframePivot
    sp_relational_grouped_dataframe_ref: SpRelationalGroupedDataframeRef
    sp_row: SpRow
    sp_session_table_function: SpSessionTableFunction
    sp_sql: SpSql
    sp_stored_procedure: SpStoredProcedure
    sp_table: SpTable
    sp_table_delete: SpTableDelete
    sp_table_drop_table: SpTableDropTable
    sp_table_fn_call_alias: SpTableFnCallAlias
    sp_table_fn_call_over: SpTableFnCallOver
    sp_table_merge: SpTableMerge
    sp_table_sample: SpTableSample
    sp_table_update: SpTableUpdate
    sp_window_spec_empty: SpWindowSpecEmpty
    sp_window_spec_order_by: SpWindowSpecOrderBy
    sp_window_spec_partition_by: SpWindowSpecPartitionBy
    sp_window_spec_range_between: SpWindowSpecRangeBetween
    sp_window_spec_rows_between: SpWindowSpecRowsBetween
    sp_write_copy_into_location: SpWriteCopyIntoLocation
    sp_write_csv: SpWriteCsv
    sp_write_json: SpWriteJson
    sp_write_pandas: SpWritePandas
    sp_write_parquet: SpWriteParquet
    sp_write_table: SpWriteTable
    stored_procedure: StoredProcedure
    string_val: StringVal
    sub: Sub
    time_val: TimeVal
    timestamp_val: TimestampVal
    tuple_val: TupleVal
    udaf: Udaf
    udf: Udf
    udtf: Udtf
    def __init__(
        self,
        trait_bin_op: _Optional[_Union[BinOp, _Mapping]] = ...,
        trait_const: _Optional[_Union[Const, _Mapping]] = ...,
        trait_expr: _Optional[_Union[Expr, _Mapping]] = ...,
        trait_fn_id_ref_expr: _Optional[_Union[FnIdRefExpr, _Mapping]] = ...,
        trait_fn_name_ref_expr: _Optional[_Union[FnNameRefExpr, _Mapping]] = ...,
        trait_fn_ref_expr: _Optional[_Union[FnRefExpr, _Mapping]] = ...,
        trait_sp_column_expr: _Optional[_Union[SpColumnExpr, _Mapping]] = ...,
        trait_sp_column_fn: _Optional[_Union[SpColumnFn, _Mapping]] = ...,
        trait_sp_dataframe_expr: _Optional[_Union[SpDataframeExpr, _Mapping]] = ...,
        trait_sp_dataframe_reader: _Optional[_Union[SpDataframeReader, _Mapping]] = ...,
        trait_sp_dataframe_writer: _Optional[_Union[SpDataframeWriter, _Mapping]] = ...,
        trait_sp_dataframe_writer_options: _Optional[
            _Union[SpDataframeWriterOptions, _Mapping]
        ] = ...,
        trait_sp_dataframe_writer_save_mode: _Optional[
            _Union[SpDataframeWriterSaveMode, _Mapping]
        ] = ...,
        trait_sp_matched_clause: _Optional[_Union[SpMatchedClause, _Mapping]] = ...,
        trait_sp_relational_grouped_dataframe_expr: _Optional[
            _Union[SpRelationalGroupedDataframeExpr, _Mapping]
        ] = ...,
        trait_sp_window_spec_expr: _Optional[_Union[SpWindowSpecExpr, _Mapping]] = ...,
        trait_sp_write_file: _Optional[_Union[SpWriteFile, _Mapping]] = ...,
        trait_unary_op: _Optional[_Union[UnaryOp, _Mapping]] = ...,
        add: _Optional[_Union[Add, _Mapping]] = ...,
        apply_expr: _Optional[_Union[ApplyExpr, _Mapping]] = ...,
        big_decimal_val: _Optional[_Union[BigDecimalVal, _Mapping]] = ...,
        big_int_val: _Optional[_Union[BigIntVal, _Mapping]] = ...,
        binary_val: _Optional[_Union[BinaryVal, _Mapping]] = ...,
        bit_and: _Optional[_Union[BitAnd, _Mapping]] = ...,
        bit_or: _Optional[_Union[BitOr, _Mapping]] = ...,
        bit_xor: _Optional[_Union[BitXor, _Mapping]] = ...,
        bool_val: _Optional[_Union[BoolVal, _Mapping]] = ...,
        builtin_fn: _Optional[_Union[BuiltinFn, _Mapping]] = ...,
        call_table_function_expr: _Optional[
            _Union[CallTableFunctionExpr, _Mapping]
        ] = ...,
        cast_expr: _Optional[_Union[CastExpr, _Mapping]] = ...,
        date_val: _Optional[_Union[DateVal, _Mapping]] = ...,
        div: _Optional[_Union[Div, _Mapping]] = ...,
        eq: _Optional[_Union[Eq, _Mapping]] = ...,
        float64_val: _Optional[_Union[Float64Val, _Mapping]] = ...,
        fn_val: _Optional[_Union[FnVal, _Mapping]] = ...,
        geq: _Optional[_Union[Geq, _Mapping]] = ...,
        gt: _Optional[_Union[Gt, _Mapping]] = ...,
        if_expr: _Optional[_Union[IfExpr, _Mapping]] = ...,
        indirect_table_fn_id_ref: _Optional[
            _Union[IndirectTableFnIdRef, _Mapping]
        ] = ...,
        indirect_table_fn_name_ref: _Optional[
            _Union[IndirectTableFnNameRef, _Mapping]
        ] = ...,
        int32_val: _Optional[_Union[Int32Val, _Mapping]] = ...,
        int64_val: _Optional[_Union[Int64Val, _Mapping]] = ...,
        leq: _Optional[_Union[Leq, _Mapping]] = ...,
        list_val: _Optional[_Union[ListVal, _Mapping]] = ...,
        lt: _Optional[_Union[Lt, _Mapping]] = ...,
        mod: _Optional[_Union[Mod, _Mapping]] = ...,
        mul: _Optional[_Union[Mul, _Mapping]] = ...,
        neg: _Optional[_Union[Neg, _Mapping]] = ...,
        neq: _Optional[_Union[Neq, _Mapping]] = ...,
        none_val: _Optional[_Union[NoneVal, _Mapping]] = ...,
        null_val: _Optional[_Union[NullVal, _Mapping]] = ...,
        object_get_item: _Optional[_Union[ObjectGetItem, _Mapping]] = ...,
        pd_dataframe: _Optional[_Union[PdDataframe, _Mapping]] = ...,
        pd_dataframe_get_item: _Optional[_Union[PdDataframeGetItem, _Mapping]] = ...,
        pd_dataframe_i_loc: _Optional[_Union[PdDataframeILoc, _Mapping]] = ...,
        pd_dataframe_loc: _Optional[_Union[PdDataframeLoc, _Mapping]] = ...,
        pd_dataframe_set_item: _Optional[_Union[PdDataframeSetItem, _Mapping]] = ...,
        pd_drop_na: _Optional[_Union[PdDropNa, _Mapping]] = ...,
        pd_repr: _Optional[_Union[PdRepr, _Mapping]] = ...,
        pow: _Optional[_Union[Pow, _Mapping]] = ...,
        python_date_val: _Optional[_Union[PythonDateVal, _Mapping]] = ...,
        python_time_val: _Optional[_Union[PythonTimeVal, _Mapping]] = ...,
        python_timestamp_val: _Optional[_Union[PythonTimestampVal, _Mapping]] = ...,
        range_val: _Optional[_Union[RangeVal, _Mapping]] = ...,
        ref: _Optional[_Union[Ref, _Mapping]] = ...,
        seq_map_val: _Optional[_Union[SeqMapVal, _Mapping]] = ...,
        some_val: _Optional[_Union[SomeVal, _Mapping]] = ...,
        sp_case_expr: _Optional[_Union[SpCaseExpr, _Mapping]] = ...,
        sp_column_alias: _Optional[_Union[SpColumnAlias, _Mapping]] = ...,
        sp_column_apply__int: _Optional[_Union[SpColumnApply_Int, _Mapping]] = ...,
        sp_column_apply__string: _Optional[
            _Union[SpColumnApply_String, _Mapping]
        ] = ...,
        sp_column_asc: _Optional[_Union[SpColumnAsc, _Mapping]] = ...,
        sp_column_between: _Optional[_Union[SpColumnBetween, _Mapping]] = ...,
        sp_column_case_when: _Optional[_Union[SpColumnCaseWhen, _Mapping]] = ...,
        sp_column_cast: _Optional[_Union[SpColumnCast, _Mapping]] = ...,
        sp_column_desc: _Optional[_Union[SpColumnDesc, _Mapping]] = ...,
        sp_column_equal_nan: _Optional[_Union[SpColumnEqualNan, _Mapping]] = ...,
        sp_column_equal_null: _Optional[_Union[SpColumnEqualNull, _Mapping]] = ...,
        sp_column_in__dataframe: _Optional[
            _Union[SpColumnIn_Dataframe, _Mapping]
        ] = ...,
        sp_column_in__seq: _Optional[_Union[SpColumnIn_Seq, _Mapping]] = ...,
        sp_column_is_not_null: _Optional[_Union[SpColumnIsNotNull, _Mapping]] = ...,
        sp_column_is_null: _Optional[_Union[SpColumnIsNull, _Mapping]] = ...,
        sp_column_name: _Optional[_Union[SpColumnName, _Mapping]] = ...,
        sp_column_over: _Optional[_Union[SpColumnOver, _Mapping]] = ...,
        sp_column_ref: _Optional[_Union[SpColumnRef, _Mapping]] = ...,
        sp_column_sql_expr: _Optional[_Union[SpColumnSqlExpr, _Mapping]] = ...,
        sp_column_string_collate: _Optional[
            _Union[SpColumnStringCollate, _Mapping]
        ] = ...,
        sp_column_string_contains: _Optional[
            _Union[SpColumnStringContains, _Mapping]
        ] = ...,
        sp_column_string_ends_with: _Optional[
            _Union[SpColumnStringEndsWith, _Mapping]
        ] = ...,
        sp_column_string_like: _Optional[_Union[SpColumnStringLike, _Mapping]] = ...,
        sp_column_string_regexp: _Optional[
            _Union[SpColumnStringRegexp, _Mapping]
        ] = ...,
        sp_column_string_starts_with: _Optional[
            _Union[SpColumnStringStartsWith, _Mapping]
        ] = ...,
        sp_column_string_substr: _Optional[
            _Union[SpColumnStringSubstr, _Mapping]
        ] = ...,
        sp_column_try_cast: _Optional[_Union[SpColumnTryCast, _Mapping]] = ...,
        sp_column_within_group: _Optional[_Union[SpColumnWithinGroup, _Mapping]] = ...,
        sp_create_dataframe: _Optional[_Union[SpCreateDataframe, _Mapping]] = ...,
        sp_dataframe_agg: _Optional[_Union[SpDataframeAgg, _Mapping]] = ...,
        sp_dataframe_alias: _Optional[_Union[SpDataframeAlias, _Mapping]] = ...,
        sp_dataframe_analytics_compute_lag: _Optional[
            _Union[SpDataframeAnalyticsComputeLag, _Mapping]
        ] = ...,
        sp_dataframe_analytics_compute_lead: _Optional[
            _Union[SpDataframeAnalyticsComputeLead, _Mapping]
        ] = ...,
        sp_dataframe_analytics_cumulative_agg: _Optional[
            _Union[SpDataframeAnalyticsCumulativeAgg, _Mapping]
        ] = ...,
        sp_dataframe_analytics_moving_agg: _Optional[
            _Union[SpDataframeAnalyticsMovingAgg, _Mapping]
        ] = ...,
        sp_dataframe_analytics_time_series_agg: _Optional[
            _Union[SpDataframeAnalyticsTimeSeriesAgg, _Mapping]
        ] = ...,
        sp_dataframe_apply: _Optional[_Union[SpDataframeApply, _Mapping]] = ...,
        sp_dataframe_cache_result: _Optional[
            _Union[SpDataframeCacheResult, _Mapping]
        ] = ...,
        sp_dataframe_col: _Optional[_Union[SpDataframeCol, _Mapping]] = ...,
        sp_dataframe_collect: _Optional[_Union[SpDataframeCollect, _Mapping]] = ...,
        sp_dataframe_copy_into_table: _Optional[
            _Union[SpDataframeCopyIntoTable, _Mapping]
        ] = ...,
        sp_dataframe_count: _Optional[_Union[SpDataframeCount, _Mapping]] = ...,
        sp_dataframe_create_or_replace_dynamic_table: _Optional[
            _Union[SpDataframeCreateOrReplaceDynamicTable, _Mapping]
        ] = ...,
        sp_dataframe_create_or_replace_view: _Optional[
            _Union[SpDataframeCreateOrReplaceView, _Mapping]
        ] = ...,
        sp_dataframe_cross_join: _Optional[
            _Union[SpDataframeCrossJoin, _Mapping]
        ] = ...,
        sp_dataframe_cube: _Optional[_Union[SpDataframeCube, _Mapping]] = ...,
        sp_dataframe_describe: _Optional[_Union[SpDataframeDescribe, _Mapping]] = ...,
        sp_dataframe_distinct: _Optional[_Union[SpDataframeDistinct, _Mapping]] = ...,
        sp_dataframe_drop: _Optional[_Union[SpDataframeDrop, _Mapping]] = ...,
        sp_dataframe_drop_duplicates: _Optional[
            _Union[SpDataframeDropDuplicates, _Mapping]
        ] = ...,
        sp_dataframe_except: _Optional[_Union[SpDataframeExcept, _Mapping]] = ...,
        sp_dataframe_filter: _Optional[_Union[SpDataframeFilter, _Mapping]] = ...,
        sp_dataframe_first: _Optional[_Union[SpDataframeFirst, _Mapping]] = ...,
        sp_dataframe_flatten: _Optional[_Union[SpDataframeFlatten, _Mapping]] = ...,
        sp_dataframe_group_by: _Optional[_Union[SpDataframeGroupBy, _Mapping]] = ...,
        sp_dataframe_group_by_grouping_sets: _Optional[
            _Union[SpDataframeGroupByGroupingSets, _Mapping]
        ] = ...,
        sp_dataframe_group_by__columns: _Optional[
            _Union[SpDataframeGroupBy_Columns, _Mapping]
        ] = ...,
        sp_dataframe_group_by__strings: _Optional[
            _Union[SpDataframeGroupBy_Strings, _Mapping]
        ] = ...,
        sp_dataframe_intersect: _Optional[_Union[SpDataframeIntersect, _Mapping]] = ...,
        sp_dataframe_join: _Optional[_Union[SpDataframeJoin, _Mapping]] = ...,
        sp_dataframe_join_table_function: _Optional[
            _Union[SpDataframeJoinTableFunction, _Mapping]
        ] = ...,
        sp_dataframe_join__dataframe__join_exprs: _Optional[
            _Union[SpDataframeJoin_Dataframe_JoinExprs, _Mapping]
        ] = ...,
        sp_dataframe_join__dataframe__using_columns: _Optional[
            _Union[SpDataframeJoin_Dataframe_UsingColumns, _Mapping]
        ] = ...,
        sp_dataframe_limit: _Optional[_Union[SpDataframeLimit, _Mapping]] = ...,
        sp_dataframe_na_drop__python: _Optional[
            _Union[SpDataframeNaDrop_Python, _Mapping]
        ] = ...,
        sp_dataframe_na_drop__scala: _Optional[
            _Union[SpDataframeNaDrop_Scala, _Mapping]
        ] = ...,
        sp_dataframe_na_fill: _Optional[_Union[SpDataframeNaFill, _Mapping]] = ...,
        sp_dataframe_na_replace: _Optional[
            _Union[SpDataframeNaReplace, _Mapping]
        ] = ...,
        sp_dataframe_natural_join: _Optional[
            _Union[SpDataframeNaturalJoin, _Mapping]
        ] = ...,
        sp_dataframe_pivot: _Optional[_Union[SpDataframePivot, _Mapping]] = ...,
        sp_dataframe_random_split: _Optional[
            _Union[SpDataframeRandomSplit, _Mapping]
        ] = ...,
        sp_dataframe_reader_init: _Optional[
            _Union[SpDataframeReaderInit, _Mapping]
        ] = ...,
        sp_dataframe_reader_option: _Optional[
            _Union[SpDataframeReaderOption, _Mapping]
        ] = ...,
        sp_dataframe_reader_options: _Optional[
            _Union[SpDataframeReaderOptions, _Mapping]
        ] = ...,
        sp_dataframe_reader_schema: _Optional[
            _Union[SpDataframeReaderSchema, _Mapping]
        ] = ...,
        sp_dataframe_reader_with_metadata: _Optional[
            _Union[SpDataframeReaderWithMetadata, _Mapping]
        ] = ...,
        sp_dataframe_ref: _Optional[_Union[SpDataframeRef, _Mapping]] = ...,
        sp_dataframe_rename: _Optional[_Union[SpDataframeRename, _Mapping]] = ...,
        sp_dataframe_rollup: _Optional[_Union[SpDataframeRollup, _Mapping]] = ...,
        sp_dataframe_rollup__columns: _Optional[
            _Union[SpDataframeRollup_Columns, _Mapping]
        ] = ...,
        sp_dataframe_rollup__strings: _Optional[
            _Union[SpDataframeRollup_Strings, _Mapping]
        ] = ...,
        sp_dataframe_sample: _Optional[_Union[SpDataframeSample, _Mapping]] = ...,
        sp_dataframe_select__columns: _Optional[
            _Union[SpDataframeSelect_Columns, _Mapping]
        ] = ...,
        sp_dataframe_select__exprs: _Optional[
            _Union[SpDataframeSelect_Exprs, _Mapping]
        ] = ...,
        sp_dataframe_show: _Optional[_Union[SpDataframeShow, _Mapping]] = ...,
        sp_dataframe_sort: _Optional[_Union[SpDataframeSort, _Mapping]] = ...,
        sp_dataframe_stat_approx_quantile: _Optional[
            _Union[SpDataframeStatApproxQuantile, _Mapping]
        ] = ...,
        sp_dataframe_stat_corr: _Optional[_Union[SpDataframeStatCorr, _Mapping]] = ...,
        sp_dataframe_stat_cov: _Optional[_Union[SpDataframeStatCov, _Mapping]] = ...,
        sp_dataframe_stat_cross_tab: _Optional[
            _Union[SpDataframeStatCrossTab, _Mapping]
        ] = ...,
        sp_dataframe_stat_sample_by: _Optional[
            _Union[SpDataframeStatSampleBy, _Mapping]
        ] = ...,
        sp_dataframe_to_df: _Optional[_Union[SpDataframeToDf, _Mapping]] = ...,
        sp_dataframe_to_local_iterator: _Optional[
            _Union[SpDataframeToLocalIterator, _Mapping]
        ] = ...,
        sp_dataframe_to_pandas: _Optional[_Union[SpDataframeToPandas, _Mapping]] = ...,
        sp_dataframe_to_pandas_batches: _Optional[
            _Union[SpDataframeToPandasBatches, _Mapping]
        ] = ...,
        sp_dataframe_union: _Optional[_Union[SpDataframeUnion, _Mapping]] = ...,
        sp_dataframe_union_all: _Optional[_Union[SpDataframeUnionAll, _Mapping]] = ...,
        sp_dataframe_union_all_by_name: _Optional[
            _Union[SpDataframeUnionAllByName, _Mapping]
        ] = ...,
        sp_dataframe_union_by_name: _Optional[
            _Union[SpDataframeUnionByName, _Mapping]
        ] = ...,
        sp_dataframe_unpivot: _Optional[_Union[SpDataframeUnpivot, _Mapping]] = ...,
        sp_dataframe_where: _Optional[_Union[SpDataframeWhere, _Mapping]] = ...,
        sp_dataframe_with_column: _Optional[
            _Union[SpDataframeWithColumn, _Mapping]
        ] = ...,
        sp_dataframe_with_column_renamed: _Optional[
            _Union[SpDataframeWithColumnRenamed, _Mapping]
        ] = ...,
        sp_dataframe_with_columns: _Optional[
            _Union[SpDataframeWithColumns, _Mapping]
        ] = ...,
        sp_dataframe_write: _Optional[_Union[SpDataframeWrite, _Mapping]] = ...,
        sp_datatype_val: _Optional[_Union[SpDatatypeVal, _Mapping]] = ...,
        sp_flatten: _Optional[_Union[SpFlatten, _Mapping]] = ...,
        sp_fn_ref: _Optional[_Union[SpFnRef, _Mapping]] = ...,
        sp_generator: _Optional[_Union[SpGenerator, _Mapping]] = ...,
        sp_grouping_sets: _Optional[_Union[SpGroupingSets, _Mapping]] = ...,
        sp_merge_delete_when_matched_clause: _Optional[
            _Union[SpMergeDeleteWhenMatchedClause, _Mapping]
        ] = ...,
        sp_merge_insert_when_not_matched_clause: _Optional[
            _Union[SpMergeInsertWhenNotMatchedClause, _Mapping]
        ] = ...,
        sp_merge_update_when_matched_clause: _Optional[
            _Union[SpMergeUpdateWhenMatchedClause, _Mapping]
        ] = ...,
        sp_range: _Optional[_Union[SpRange, _Mapping]] = ...,
        sp_read_avro: _Optional[_Union[SpReadAvro, _Mapping]] = ...,
        sp_read_csv: _Optional[_Union[SpReadCsv, _Mapping]] = ...,
        sp_read_json: _Optional[_Union[SpReadJson, _Mapping]] = ...,
        sp_read_orc: _Optional[_Union[SpReadOrc, _Mapping]] = ...,
        sp_read_parquet: _Optional[_Union[SpReadParquet, _Mapping]] = ...,
        sp_read_table: _Optional[_Union[SpReadTable, _Mapping]] = ...,
        sp_read_xml: _Optional[_Union[SpReadXml, _Mapping]] = ...,
        sp_relational_grouped_dataframe_agg: _Optional[
            _Union[SpRelationalGroupedDataframeAgg, _Mapping]
        ] = ...,
        sp_relational_grouped_dataframe_apply_in_pandas: _Optional[
            _Union[SpRelationalGroupedDataframeApplyInPandas, _Mapping]
        ] = ...,
        sp_relational_grouped_dataframe_builtin: _Optional[
            _Union[SpRelationalGroupedDataframeBuiltin, _Mapping]
        ] = ...,
        sp_relational_grouped_dataframe_pivot: _Optional[
            _Union[SpRelationalGroupedDataframePivot, _Mapping]
        ] = ...,
        sp_relational_grouped_dataframe_ref: _Optional[
            _Union[SpRelationalGroupedDataframeRef, _Mapping]
        ] = ...,
        sp_row: _Optional[_Union[SpRow, _Mapping]] = ...,
        sp_session_table_function: _Optional[
            _Union[SpSessionTableFunction, _Mapping]
        ] = ...,
        sp_sql: _Optional[_Union[SpSql, _Mapping]] = ...,
        sp_stored_procedure: _Optional[_Union[SpStoredProcedure, _Mapping]] = ...,
        sp_table: _Optional[_Union[SpTable, _Mapping]] = ...,
        sp_table_delete: _Optional[_Union[SpTableDelete, _Mapping]] = ...,
        sp_table_drop_table: _Optional[_Union[SpTableDropTable, _Mapping]] = ...,
        sp_table_fn_call_alias: _Optional[_Union[SpTableFnCallAlias, _Mapping]] = ...,
        sp_table_fn_call_over: _Optional[_Union[SpTableFnCallOver, _Mapping]] = ...,
        sp_table_merge: _Optional[_Union[SpTableMerge, _Mapping]] = ...,
        sp_table_sample: _Optional[_Union[SpTableSample, _Mapping]] = ...,
        sp_table_update: _Optional[_Union[SpTableUpdate, _Mapping]] = ...,
        sp_window_spec_empty: _Optional[_Union[SpWindowSpecEmpty, _Mapping]] = ...,
        sp_window_spec_order_by: _Optional[_Union[SpWindowSpecOrderBy, _Mapping]] = ...,
        sp_window_spec_partition_by: _Optional[
            _Union[SpWindowSpecPartitionBy, _Mapping]
        ] = ...,
        sp_window_spec_range_between: _Optional[
            _Union[SpWindowSpecRangeBetween, _Mapping]
        ] = ...,
        sp_window_spec_rows_between: _Optional[
            _Union[SpWindowSpecRowsBetween, _Mapping]
        ] = ...,
        sp_write_copy_into_location: _Optional[
            _Union[SpWriteCopyIntoLocation, _Mapping]
        ] = ...,
        sp_write_csv: _Optional[_Union[SpWriteCsv, _Mapping]] = ...,
        sp_write_json: _Optional[_Union[SpWriteJson, _Mapping]] = ...,
        sp_write_pandas: _Optional[_Union[SpWritePandas, _Mapping]] = ...,
        sp_write_parquet: _Optional[_Union[SpWriteParquet, _Mapping]] = ...,
        sp_write_table: _Optional[_Union[SpWriteTable, _Mapping]] = ...,
        stored_procedure: _Optional[_Union[StoredProcedure, _Mapping]] = ...,
        string_val: _Optional[_Union[StringVal, _Mapping]] = ...,
        sub: _Optional[_Union[Sub, _Mapping]] = ...,
        time_val: _Optional[_Union[TimeVal, _Mapping]] = ...,
        timestamp_val: _Optional[_Union[TimestampVal, _Mapping]] = ...,
        tuple_val: _Optional[_Union[TupleVal, _Mapping]] = ...,
        udaf: _Optional[_Union[Udaf, _Mapping]] = ...,
        udf: _Optional[_Union[Udf, _Mapping]] = ...,
        udtf: _Optional[_Union[Udtf, _Mapping]] = ...,
        **kwargs
    ) -> None: ...

class Stmt(_message.Message):
    __slots__ = ("assign", "eval")
    ASSIGN_FIELD_NUMBER: _ClassVar[int]
    EVAL_FIELD_NUMBER: _ClassVar[int]
    assign: Assign
    eval: Eval
    def __init__(
        self,
        assign: _Optional[_Union[Assign, _Mapping]] = ...,
        eval: _Optional[_Union[Eval, _Mapping]] = ...,
    ) -> None: ...

class Assign(_message.Message):
    __slots__ = ("expr", "symbol", "uid", "var_id")
    EXPR_FIELD_NUMBER: _ClassVar[int]
    SYMBOL_FIELD_NUMBER: _ClassVar[int]
    UID_FIELD_NUMBER: _ClassVar[int]
    VAR_ID_FIELD_NUMBER: _ClassVar[int]
    expr: Expr
    symbol: _wrappers_pb2.StringValue
    uid: int
    var_id: VarId
    def __init__(
        self,
        expr: _Optional[_Union[Expr, _Mapping]] = ...,
        symbol: _Optional[_Union[_wrappers_pb2.StringValue, _Mapping]] = ...,
        uid: _Optional[int] = ...,
        var_id: _Optional[_Union[VarId, _Mapping]] = ...,
    ) -> None: ...

class Eval(_message.Message):
    __slots__ = ("uid", "var_id")
    UID_FIELD_NUMBER: _ClassVar[int]
    VAR_ID_FIELD_NUMBER: _ClassVar[int]
    uid: int
    var_id: VarId
    def __init__(
        self,
        uid: _Optional[int] = ...,
        var_id: _Optional[_Union[VarId, _Mapping]] = ...,
    ) -> None: ...

class Type(_message.Message):
    __slots__ = (
        "trait_numeric_type",
        "trait_scalar_type",
        "any_type",
        "bool_type",
        "float64_type",
        "fn_type",
        "int32_type",
        "int64_type",
        "list_type",
        "map_type",
        "nothing_type",
        "number_type",
        "option_type",
        "pd_repr_type",
        "sp_col_expr_type",
        "sp_dataframe_type",
        "sp_grouped_dataframe_type",
        "sp_window_type",
        "string_type",
        "tuple_type",
        "ty_var",
        "unit_type",
        "unknown_type",
    )
    TRAIT_NUMERIC_TYPE_FIELD_NUMBER: _ClassVar[int]
    TRAIT_SCALAR_TYPE_FIELD_NUMBER: _ClassVar[int]
    ANY_TYPE_FIELD_NUMBER: _ClassVar[int]
    BOOL_TYPE_FIELD_NUMBER: _ClassVar[int]
    FLOAT64_TYPE_FIELD_NUMBER: _ClassVar[int]
    FN_TYPE_FIELD_NUMBER: _ClassVar[int]
    INT32_TYPE_FIELD_NUMBER: _ClassVar[int]
    INT64_TYPE_FIELD_NUMBER: _ClassVar[int]
    LIST_TYPE_FIELD_NUMBER: _ClassVar[int]
    MAP_TYPE_FIELD_NUMBER: _ClassVar[int]
    NOTHING_TYPE_FIELD_NUMBER: _ClassVar[int]
    NUMBER_TYPE_FIELD_NUMBER: _ClassVar[int]
    OPTION_TYPE_FIELD_NUMBER: _ClassVar[int]
    PD_REPR_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_COL_EXPR_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_DATAFRAME_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_GROUPED_DATAFRAME_TYPE_FIELD_NUMBER: _ClassVar[int]
    SP_WINDOW_TYPE_FIELD_NUMBER: _ClassVar[int]
    STRING_TYPE_FIELD_NUMBER: _ClassVar[int]
    TUPLE_TYPE_FIELD_NUMBER: _ClassVar[int]
    TY_VAR_FIELD_NUMBER: _ClassVar[int]
    UNIT_TYPE_FIELD_NUMBER: _ClassVar[int]
    UNKNOWN_TYPE_FIELD_NUMBER: _ClassVar[int]
    trait_numeric_type: NumericType
    trait_scalar_type: ScalarType
    any_type: AnyType
    bool_type: BoolType
    float64_type: Float64Type
    fn_type: FnType
    int32_type: Int32Type
    int64_type: Int64Type
    list_type: ListType
    map_type: MapType
    nothing_type: NothingType
    number_type: NumberType
    option_type: OptionType
    pd_repr_type: PdReprType
    sp_col_expr_type: SpColExprType
    sp_dataframe_type: SpDataframeType
    sp_grouped_dataframe_type: SpGroupedDataframeType
    sp_window_type: SpWindowType
    string_type: StringType
    tuple_type: TupleType
    ty_var: TyVar
    unit_type: UnitType
    unknown_type: UnknownType
    def __init__(
        self,
        trait_numeric_type: _Optional[_Union[NumericType, _Mapping]] = ...,
        trait_scalar_type: _Optional[_Union[ScalarType, _Mapping]] = ...,
        any_type: _Optional[_Union[AnyType, _Mapping]] = ...,
        bool_type: _Optional[_Union[BoolType, _Mapping]] = ...,
        float64_type: _Optional[_Union[Float64Type, _Mapping]] = ...,
        fn_type: _Optional[_Union[FnType, _Mapping]] = ...,
        int32_type: _Optional[_Union[Int32Type, _Mapping]] = ...,
        int64_type: _Optional[_Union[Int64Type, _Mapping]] = ...,
        list_type: _Optional[_Union[ListType, _Mapping]] = ...,
        map_type: _Optional[_Union[MapType, _Mapping]] = ...,
        nothing_type: _Optional[_Union[NothingType, _Mapping]] = ...,
        number_type: _Optional[_Union[NumberType, _Mapping]] = ...,
        option_type: _Optional[_Union[OptionType, _Mapping]] = ...,
        pd_repr_type: _Optional[_Union[PdReprType, _Mapping]] = ...,
        sp_col_expr_type: _Optional[_Union[SpColExprType, _Mapping]] = ...,
        sp_dataframe_type: _Optional[_Union[SpDataframeType, _Mapping]] = ...,
        sp_grouped_dataframe_type: _Optional[
            _Union[SpGroupedDataframeType, _Mapping]
        ] = ...,
        sp_window_type: _Optional[_Union[SpWindowType, _Mapping]] = ...,
        string_type: _Optional[_Union[StringType, _Mapping]] = ...,
        tuple_type: _Optional[_Union[TupleType, _Mapping]] = ...,
        ty_var: _Optional[_Union[TyVar, _Mapping]] = ...,
        unit_type: _Optional[_Union[UnitType, _Mapping]] = ...,
        unknown_type: _Optional[_Union[UnknownType, _Mapping]] = ...,
    ) -> None: ...

class UnknownType(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class AnyType(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ScalarType(_message.Message):
    __slots__ = (
        "trait_numeric_type",
        "bool_type",
        "float64_type",
        "int32_type",
        "int64_type",
        "number_type",
        "unit_type",
    )
    TRAIT_NUMERIC_TYPE_FIELD_NUMBER: _ClassVar[int]
    BOOL_TYPE_FIELD_NUMBER: _ClassVar[int]
    FLOAT64_TYPE_FIELD_NUMBER: _ClassVar[int]
    INT32_TYPE_FIELD_NUMBER: _ClassVar[int]
    INT64_TYPE_FIELD_NUMBER: _ClassVar[int]
    NUMBER_TYPE_FIELD_NUMBER: _ClassVar[int]
    UNIT_TYPE_FIELD_NUMBER: _ClassVar[int]
    trait_numeric_type: NumericType
    bool_type: BoolType
    float64_type: Float64Type
    int32_type: Int32Type
    int64_type: Int64Type
    number_type: NumberType
    unit_type: UnitType
    def __init__(
        self,
        trait_numeric_type: _Optional[_Union[NumericType, _Mapping]] = ...,
        bool_type: _Optional[_Union[BoolType, _Mapping]] = ...,
        float64_type: _Optional[_Union[Float64Type, _Mapping]] = ...,
        int32_type: _Optional[_Union[Int32Type, _Mapping]] = ...,
        int64_type: _Optional[_Union[Int64Type, _Mapping]] = ...,
        number_type: _Optional[_Union[NumberType, _Mapping]] = ...,
        unit_type: _Optional[_Union[UnitType, _Mapping]] = ...,
    ) -> None: ...

class NumericType(_message.Message):
    __slots__ = ("float64_type", "int32_type", "int64_type", "number_type")
    FLOAT64_TYPE_FIELD_NUMBER: _ClassVar[int]
    INT32_TYPE_FIELD_NUMBER: _ClassVar[int]
    INT64_TYPE_FIELD_NUMBER: _ClassVar[int]
    NUMBER_TYPE_FIELD_NUMBER: _ClassVar[int]
    float64_type: Float64Type
    int32_type: Int32Type
    int64_type: Int64Type
    number_type: NumberType
    def __init__(
        self,
        float64_type: _Optional[_Union[Float64Type, _Mapping]] = ...,
        int32_type: _Optional[_Union[Int32Type, _Mapping]] = ...,
        int64_type: _Optional[_Union[Int64Type, _Mapping]] = ...,
        number_type: _Optional[_Union[NumberType, _Mapping]] = ...,
    ) -> None: ...

class NumberType(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class NothingType(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class UnitType(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class BoolType(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Int32Type(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Int64Type(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class Float64Type(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class StringType(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class PdReprType(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class FnType(_message.Message):
    __slots__ = ("params", "ret")
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    RET_FIELD_NUMBER: _ClassVar[int]
    params: _containers.RepeatedCompositeFieldContainer[Type]
    ret: Type
    def __init__(
        self,
        params: _Optional[_Iterable[_Union[Type, _Mapping]]] = ...,
        ret: _Optional[_Union[Type, _Mapping]] = ...,
    ) -> None: ...

class OptionType(_message.Message):
    __slots__ = ("typ",)
    TYP_FIELD_NUMBER: _ClassVar[int]
    typ: Type
    def __init__(self, typ: _Optional[_Union[Type, _Mapping]] = ...) -> None: ...

class TupleType(_message.Message):
    __slots__ = ("tys",)
    TYS_FIELD_NUMBER: _ClassVar[int]
    tys: _containers.RepeatedCompositeFieldContainer[Type]
    def __init__(
        self, tys: _Optional[_Iterable[_Union[Type, _Mapping]]] = ...
    ) -> None: ...

class ListType(_message.Message):
    __slots__ = ("typ",)
    TYP_FIELD_NUMBER: _ClassVar[int]
    typ: Type
    def __init__(self, typ: _Optional[_Union[Type, _Mapping]] = ...) -> None: ...

class MapType(_message.Message):
    __slots__ = ("k", "v")
    K_FIELD_NUMBER: _ClassVar[int]
    V_FIELD_NUMBER: _ClassVar[int]
    k: Type
    v: Type
    def __init__(
        self,
        k: _Optional[_Union[Type, _Mapping]] = ...,
        v: _Optional[_Union[Type, _Mapping]] = ...,
    ) -> None: ...

class TyVar(_message.Message):
    __slots__ = ("id",)
    ID_FIELD_NUMBER: _ClassVar[int]
    id: str
    def __init__(self, id: _Optional[str] = ...) -> None: ...
