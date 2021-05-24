class AbstractDataType:
    pass


class DataType(AbstractDataType):
    @property
    def type_name(self):
        """ Returns a data type name. """
        return self.__class__.__name__

    @property
    def to_string(self):
        """ Returns a data type name. Alias of [[type_name]] """
        return self.type_name


# Data types
class AtomicType(DataType):
    pass


class ArrayType(DataType):
    def __init__(self, element_type: DataType, contains_null: bool):
        self.element_type = element_type
        self.contains_null = contains_null


class MapType(DataType):
    def __init__(self, key_type: DataType, value_type: DataType, value_contains_null: bool):
        self.key_type = key_type
        self.value_type = value_type
        self.value_contains_null = value_contains_null


# TODO might require more work
class StructType(DataType):
    def __init__(self, fields: list):
        self.fields = fields


# TODO might require more work
class StructField:
    def __init__(self, name: str, data_type: DataType, nullable: bool, metadata=None):
        self.name = name
        self.data_type = data_type
        self.nullable = nullable
        self.metadata = metadata


class VariantType(DataType):
    def sql(self):
        return "VARIANT"

    def simple_string(self):
        return "variant"

    def catalog_string(self):
        return "variant"


class GeographyType(DataType):
    pass


# Atomic Types

class BooleanType(AtomicType):
    pass


class StringType(AtomicType):
    pass


class NumericType(AtomicType):
    pass


class DateType(AtomicType):
    pass


class TimestampType(AtomicType):
    pass


class TimeType(DataType):
    pass


class BinaryType(AtomicType):
    pass


# Numeric Types
class IntegralType(NumericType):
    pass


class FractionalType(NumericType):
    pass


# Integral types
class ShortType(IntegralType):
    pass


class ByteType(IntegralType):
    pass


class IntegerType(IntegralType):
    pass


class LongType(IntegralType):
    pass


# Fractional types
class FloatType(FractionalType):
    pass


class DoubleType(FractionalType):
    pass


class DecimalType(FractionalType):
    def __init__(self, precision, scale):
        self.precision = precision
        self.scale = scale
