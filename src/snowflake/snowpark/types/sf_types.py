import re


class DataType:

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


class MapType(DataType):
    def __init__(self, key_type, value_type):
        self.key_type = key_type
        self.value_type = value_type

    def to_string(self):
        return f"MapType[{self.key_type.to_string()},{self.value_type.to_string()}]"


class VariantType(DataType):
    pass


# See also StructType in the end of this file.

# Atomic types

class BinaryType(AtomicType):
    pass


class BooleanType(AtomicType):
    """Boolean data type. Mapped to BOOLEAN Snowflake data type."""
    pass


class DateType(AtomicType):
    pass


class StringType(AtomicType):
    pass


class NumericType(AtomicType):
    pass


class TimestampType(AtomicType):
    pass


class TimeType(AtomicType):
    pass


# Numeric types
class IntegralType(NumericType):
    pass


class FractionalType(NumericType):
    pass


class ByteType(IntegralType):
    """Byte data type. Mapped to TINYINT Snowflake date type."""
    pass


class ShortType(IntegralType):
    """Short integer data type. Mapped to SMALLINT Snowflake date type."""
    pass


class IntegerType(IntegralType):
    """Integer data type. Mapped to INT Snowflake date type."""
    pass


class LongType(IntegralType):
    """Long integer data type. Mapped to BIGINT Snowflake date type."""
    pass


class FloatType(FractionalType):
    """Float data type. Mapped to FLOAT Snowflake date type."""
    pass


class DoubleType(FractionalType):
    """Double data type. Mapped to DOUBLE Snowflake date type."""
    pass


class DecimalType(FractionalType):
    """Decimal data type. Mapped to NUMBER Snowflake date type."""

    MAX_PRECISION = 38
    MAX_SCALE = 38

    def __init__(self, precision, scale):
        self.precision = precision
        self.scale = scale

    def to_string(self):
        """Returns Decimal Info. Decimal(precision, scale)"""
        return f"Decimal({self.precision},{self.scale})"

    def type_name(self):
        """Returns Decimal Info. Decimal(precision, scale), Alias of [[toString]]"""
        return self.to_string()


class StructType(DataType):

    def __init__(self, fields):
        self.fields = fields

    @classmethod
    def from_attributes(cls, attributes: list):
        cls([StructField(a.name, a.data_type, a.nullable) for a in attributes])

    def to_attributes(self):
        raise Exception("Not implemented StructType.toAttributes()")


# TODO complete
class ColumnIdentifier:
    def __init__(self, normalized_name):
        self.normalized_name = normalized_name

    def name(self) -> str:
        return ColumnIdentifier.strip_unnecessary_quotes(self.normalized_name)

    @staticmethod
    def strip_unnecessary_quotes(string: str) -> str:
        remove_quote = re.compile("^\"(([_A-Z]+[_A-Z0-9$]*)|(\\$\\d+))\"$")
        result = remove_quote.search(string)
        # TODO maybe need to parse result to provide string output
        return result if result else string


# TODO complete
class StructField:
    def __init__(self, column_identifier: ColumnIdentifier, data_type: DataType,
                 nullable: bool = True):
        self.column_identifier = column_identifier
        self.data_type = data_type
        self.nullable = nullable

    def name(self):
        return self.column_identifier.name()

    def to_string(self):
        return f"StructField({self.name()}, {self.data_type}, Nullable={self.nullable})"

    # TODO
    def tree_string(self, layer: int):
        raise Exception("Not Implemented tree_string()")
