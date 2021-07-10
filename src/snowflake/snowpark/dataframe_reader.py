from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark.internal.analyzer.sf_attribute import Attribute
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.sf_types import StructType, VariantType

from typing import Dict


class DataFrameReader:
    def __init__(self, session):
        self.session = session
        self.__user_schema = None
        self.__cur_options = {}

    def table(self, name: str) -> DataFrame:
        return self.session.table(name)

    def schema(self, schema: StructType) -> "DataFrameReader":
        self.__user_schema = schema
        return self

    def csv(self, path: str) -> DataFrame:
        if not self.__user_schema:
            raise SnowparkClientException("Must provide user schema before reading file")
        return DataFrame(self.session, self.session._Session__plan_builder.read_file(
            path, "csv", self.__cur_options, self.session.getFullyQualifiedCurrentSchema(),
            self.__user_schema.to_attributes()
        ))

    def json(self, path: str) -> DataFrame:
        return self.__read_semi_structured_file(path, "JSON")

    def avro(self, path: str) -> DataFrame:
        return self.__read_semi_structured_file(path, "AVRO")

    def parquet(self, path: str) -> DataFrame:
        return self.__read_semi_structured_file(path, "PARQUET")

    def orc(self, path: str) -> DataFrame:
        return self.__read_semi_structured_file(path, "ORC")

    def xml(self, path: str) -> DataFrame:
        return self.__read_semi_structured_file(path, "XML")

    def option(self, key: str, value) -> "DataFrameReader":
        self.__cur_options[key.upper()] = self.__parse_value(value)
        return self

    def options(self, configs: Dict) -> "DataFrameReader":
        for k, v in configs.items():
            self.option(k, v)
        return self

    def __parse_value(self, v) -> str:
        if type(v) in [bool, int]:
            return str(v)
        elif type(v) == str and v.lower() in ['true', 'false']:
            return v
        else:
            return AnalyzerPackage.single_quote(str(v))

    def __read_semi_structured_file(self, path: str, format: str) -> "DataFrame":
        if self.__user_schema:
            raise ValueError(f"Read {format} does not support user schema")
        return DataFrame(self.session,
                         self.session._Session__plan_builder.read_file(
                            path, format, self.__cur_options,
                            self.session.getFullyQualifiedCurrentSchema(),
                            [Attribute("\"$1\"", VariantType())]
        ))