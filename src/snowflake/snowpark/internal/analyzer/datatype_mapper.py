from array import ArrayType

from ...types.sf_types import BinaryType, BooleanType, DateType, NumericType, StringType, VariantType


class DataTypeMapper:
    MILLIS_PER_DAY = 24 * 3600 * 1000
    MICROS_PER_MILLIS = 1000

    @staticmethod
    # TODO
    def to_sql(value, spark_data_type):
        pass

    @staticmethod
    def schema_expression(data_type, is_nullable):
        if is_nullable:
            # if isinstance(data_type) == GeographType:
            # return "TRY_TO_GEOGRAPHY(NULL)"
            return "NULL :: " + convert_to_sf_type(data_type)

        if isinstance(data_type, NumericType):
            return "0 :: " + convert_to_sf_type(dataType)
        if isinstance(data_type, StringType):
            return "'a' :: STRING"
        if isinstance(data_type, BinaryType):
            return "to_binary(hex_encode(1))"
        if isinstance(data_type, DateType):
            return "date('2020-9-16')"
        if isinstance(data_type, BooleanType):
            return "true"
        if isinstance(data_type, TimeType):
            return "to_time('04:15:29.999')"
        if isinstance(data_type, TimestampType):
            return "to_timestamp_ntz('2020-09-16 06:30:00')"
        if isinstance(data_type, ArrayType):
            return "to_array(0)"
        if isinstance(data_type, MapType):
            return "to_object(parse_json('0'))"
        if isinstance(data_type, VariantType):
            return "to_variant(0)"
        #if isinstance(data_type, GeographyType):
        #    return "true"
        raise Exception(f"Unsupported data type: {data_type.type_name()}")

    @staticmethod
    def to_sql_without_cast(value, data_type):
        if not value:
            return "NULL"
        if isinstance(data_type, SPStringType):
            return f"""{value}"""
        return str(value)