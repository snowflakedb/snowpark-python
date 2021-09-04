#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from typing import Any, Dict, List, Optional

from snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark.internal.analyzer.sf_attribute import Attribute
from snowflake.snowpark.internal.analyzer.snowflake_plan import Query, SnowflakePlan
from snowflake.snowpark.internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.sf_types import StructType, VariantType


class StagedFileReader:
    # for read_file()
    __copy_option = {
        "ON_ERROR",
        "SIZE_LIMIT",
        "PURGE",
        "RETURN_FAILED_ONLY",
        "MATCH_BY_COLUMN_NAME",
        "ENFORCE_LENGTH",
        "TRUNCATECOLUMNS",
        "FORCE",
        "LOAD_UNCERTAIN_FILES",
    }

    __supported_file_types = {"CSV", "JSON", "PARQUET", "AVRO", "ORC", "XML"}

    def __init__(self, session: "Session", cur_options:Dict[str,str], stage_location:str, format_type: str, fully_qualified_schema:str, user_schema:Optional[StructType], table_name:Optional[str]):
        self.__session = session
        self.__cur_options = cur_options
        self.stage_location = stage_location
        self.format_type = format_type
        self.fully_qualified_schema = fully_qualified_schema
        self.__user_schema = user_schema
        self.table_name = table_name

    @classmethod
    def from_session(cls, session:"Session"):
        return cls(session,{},"","CSV", "", None, None)

    @classmethod
    def from_staged_file_reade(cls, reader):
        return cls(reader.__session, reader.__cur_options, reader.stage_location, reader.format_type, reader.fully_qualified_schema, reader.__user_schema, reader.table_name)

    def path(self, stage_location: str) -> "StagedFileReader":
        self.stage_location = stage_location
        return self

    def format(self, format_type: str) -> "StagedFileReader":
        upper_format_type = format_type.upper()
        if upper_format_type in self.__supported_file_types:
            self.format_type = upper_format_type
        else:
            raise SnowparkClientExceptionMessages.PLAN_UNSUPPORTED_FILE_FORMAT_TYPE(
                format_type
            )
        return self

    def option(self, key: str, value: Any) -> "StagedFileReader":
        # upper case to deduplicate
        self.__cur_options[key.upper()] = self.__parse_value(value)
        return self

    def options(self, configs: Dict) -> "DataFrameReader":
        for k, v in configs.items():
            self.option(k, v)
        return self

    def __parse_value(self, v) -> str:
        if type(v) in [bool, int]:
            return str(v)
        elif type(v) == str and v.lower() in ["true", "false"]:
            return v
        else:
            return AnalyzerPackage.single_quote(str(v))

    def database_schema(
        self, fully_qualified_schema: str
    ) -> "StagedFileReader":
        self.fully_qualified_schema = fully_qualified_schema

    def user_schema(self, schema: StructType) -> "StagedFileReader":
        self.__user_schema = schema
        return self

    def table(self, table_name:str) -> "StagedFileReader":
        self.table_name = table_name
        return self

    def create_snowflake_plan(self) -> "SnowflakePlan":
        if self.table_name:
            return self.__session._Session__plan_builder.copy_into(
                self.table_name,
                self.stage_location,
                self.format_type,
                self.__cur_options,
                self.fully_qualified_schema,
                self.__user_schema.to_attributes()
            )

        elif self.format_type == "CSV":
            if not self.__user_schema:
                raise SnowparkClientException(
                    "Must provide user schema before reading file"
                )
            return self.__session._Session__plan_builder.read_file(
                self.stage_location,
                self.format_type,
                self.__cur_options,
                self.fully_qualified_schema,
                self.__user_schema.to_attributes(),
            )
        else:
            if self.__user_schema:
                raise ValueError(f"Read {format} does not support user schema")

            return self.__session._Session__plan_builder.read_file(
                self.stage_location,
                self.format_type,
                self.__cur_options,
                self.fully_qualified_schema,
                [Attribute('"$1"', VariantType())],
            )

