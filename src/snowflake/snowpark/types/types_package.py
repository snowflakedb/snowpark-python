from array import ArrayType

from .sf_types import BinaryType, BooleanType, DataType, DateType, IntegerType, LongType, DoubleType, \
    FloatType, ShortType, ByteType, DecimalType, StringType, TimeType, VariantType, TimestampType, \
    StructType, MapType

from .sp_data_types import DataType as SPDataType, BooleanType as SPBooleanType, \
    StructType as SPStructType, StructField as SPStructField, StringType as SPStringType, \
    ByteType as SPByteType, ShortType as SPShortType, IntegerType as SPIntegerType, \
    LongType as SPLongType, FloatType as SPFloatType, DoubleType as SPDoubleType, \
    DateType as SPDateType, TimeType as SPTimeType, TimestampType as SPTimestampType, \
    BinaryType as SPBinaryType, ArrayType as SPArrayType, MapType as SPMapType, \
    VariantType as SPVariantType, DecimalType as SPDecimalType


def udf_option_supported(data_type: DataType) -> bool:
    if type(data_type) in [IntegerType, LongType, DoubleType, FloatType, ShortType, ByteType,
                           BooleanType]:
        return True
    else:
        return False


# TODO revisit when dealing with Java UDFs. We'll probably hard-code the return values.
def to_java_type(datatype):
    pass


# TODO revisit when dealing with Java UDFs. We'll probably hard-code the return values.
def to_udf_argument_type(datatype) -> str:
    pass


# TODO maybe change to isinstance()
def convert_to_sf_type(data_type: DataType) -> str:
    if type(data_type) is DecimalType:
        return f"NUMBER(${data_type.precision}, ${data_type.scale})"
    if type(data_type) is IntegerType:
        return "INT"
    if type(data_type) is ShortType:
        return "SMALLINT"
    if type(data_type) is ByteType:
        return "BYTEINT"
    if type(data_type) is LongType:
        return "BIGINT"
    if type(data_type) is FloatType:
        return "FLOAT"
    if type(data_type) is DoubleType:
        return "DOUBLE"
    if type(data_type) is StringType:
        return "STRING"
    if type(data_type) is BooleanType:
        return "BOOLEAN"
    if type(data_type) is DateType:
        return "DATE"
    if type(data_type) is TimeType:
        return "TIME"
    if type(data_type) is TimestampType:
        return "TIMESTAMP"
    if type(data_type) is BinaryType:
        return "BINARY"
    if type(data_type) is ArrayType:
        return "ARRAY"
    if type(data_type) is VariantType:
        return "VARIANT"
    # if type(data_type) is GeographyType:
    #    return "GEOGRAPHY"
    raise Exception(f"Unsupported data type: {data_type.type_name}")


def snow_type_to_sp_type(datatype: DataType) -> SPDataType:
    """ Mapping from snowflake data-types, to SP data-types """
    if type(datatype) == BooleanType:
        return SPBooleanType()
    if type(datatype) == StringType:
        return SPStringType()
    if type(datatype) == StructType:
        return SPStructType([
            SPStructField(field.name, snow_type_to_sp_type(field.dataType),
                          field.nullable)
            for field in datatype.fields])
    if type(datatype) == ByteType:
        return SPByteType()
    if type(datatype) == ShortType:
        return SPShortType()
    if type(datatype) == IntegerType:
        return SPIntegerType()
    if type(datatype) == LongType:
        return SPLongType()
    if type(datatype) == FloatType:
        return SPFloatType()
    if type(datatype) == DoubleType:
        return SPDoubleType()
    if type(datatype) == DateType():
        return SPDateType()
    if type(datatype) == TimeType:
        return SPTimeType()
    if type(datatype) == TimestampType:
        return SPTimestampType()
    if type(datatype) == BinaryType:
        return SPBinaryType()
    if type(datatype) == ArrayType:
        return SPArrayType(snow_type_to_sp_type(datatype.element_type),
                           contains_null=True)
    if type(datatype) == MapType:
        return SPMapType(snow_type_to_sp_type(datatype.key_type),
                         snow_type_to_sp_type(datatype.value_type),
                         value_contains_null=True)
    if type(datatype) == VariantType:
        return SPVariantType()
    if type(datatype) == DecimalType:
        return SPDecimalType(datatype.precision, datatype.scale)
    # if type(datatype) == GeographyType:
    #    return SPGeographyType(snowTypeToSpType(valueType))
    return None


def to_sp_struct_type(struct_type: StructType) -> SPStructType:
    return snow_type_to_sp_type(struct_type)


# TODO
def sp_type_to_snow_type(data_type: SPDateType) -> DataType:
    pass


def to_snow_struct_type(struct_type: SPStructType) -> StructType:
    return sp_type_to_snow_type(struct_type)
