from src.snowflake.snowpark.types.sf_types import DataType


class Attribute:
    """ Snowflake version of Attribute."""
    def __init__(self, name: str, data_type: DataType, nullable=True):
        self.name = name
        self.data_type = data_type
        self.nullable = nullable
