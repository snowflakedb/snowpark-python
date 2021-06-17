from src.snowflake.snowpark.types.sf_types import DataType


class Attribute:
    """ Snowflake version of Attribute."""
    def __init__(self, name: str, datatype: DataType, nullable=True):
        self.name = name
        self.datatype = datatype
        self.nullable = nullable
