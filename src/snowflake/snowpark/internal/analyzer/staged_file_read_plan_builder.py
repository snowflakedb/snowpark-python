#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from typing import Any, Dict, List

from snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark.internal.analyzer.sf_attribute import Attribute
from snowflake.snowpark.internal.analyzer.snowflake_plan import Query, SnowflakePlan
from snowflake.snowpark.internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.sf_types import StructType, VariantType


class StagedFileReadPlanBuilder:
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

    def __init__(self, session: "Session"):
        self.__session = session

        self.__cur_options = {}
        self.stage_location = ""
        self.format_type = ""
        self.fully_qualified_schema = ""
        self.__user_schema = None

    def path(self, stage_location: str) -> "StagedFileReadPlanBuilder":
        self.stage_location = stage_location
        return self

    def format(self, format_type: str) -> "StagedFileReadPlanBuilder":
        upper_format_type = format_type.upper()
        if upper_format_type in self.__supported_file_types:
            self.format_type = upper_format_type
        else:
            raise SnowparkClientExceptionMessages.PLAN_UNSUPPORTED_FILE_FORMAT_TYPE(
                format_type
            )
        return self

    def option(self, key: str, value: Any) -> "StagedFileReadPlanBuilder":
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
    ) -> "StagedFileReadPlanBuilder":
        self.fully_qualified_schema = fully_qualified_schema

    def user_schema(self, schema: StructType) -> "StagedFileReadPlanBuilder":
        self.__user_schema = schema
        return self

    def create_snowflake_plan(self) -> "SnowflakePlan":
        if self.format_type == "CSV":
            if not self.__user_schema:
                raise SnowparkClientException(
                    "Must provide user schema before reading file"
                )
            return self.read_file(
                self.stage_location,
                self.format_type,
                self.__cur_options,
                self.fully_qualified_schema,
                self.__user_schema.to_attributes(),
            )
        else:
            if self.__user_schema:
                raise ValueError(f"Read {format} does not support user schema")

            return self.read_file(
                self.stage_location,
                self.format_type,
                self.__cur_options,
                self.fully_qualified_schema,
                [Attribute('"$1"', VariantType())],
            )

    def read_file(
        self,
        path: str,
        format: str,
        options: Dict,
        fully_qualified_schema: str,
        schema: List[Attribute],
    ):
        copy_options = {}
        format_type_options = {}

        for k, v in options.items():
            if k != "PATTERN":
                if k in self.__copy_option:
                    copy_options[k] = v
                else:
                    format_type_options[k] = v

        pattern = options.get("PATTERN", None)
        # TODO track usage of pattern, will refactor this function in future
        # Telemetry: https://snowflakecomputing.atlassian.net/browse/SNOW-363951
        # if pattern:
        #   session.conn.telemetry.reportUsageOfCopyPattern()

        temp_object_name = (
            fully_qualified_schema + "." + AnalyzerPackage.random_name_for_temp_object()
        )

        pkg = AnalyzerPackage()
        if not copy_options:  # use select
            queries = [
                Query(
                    pkg.create_file_format_statement(
                        temp_object_name,
                        format,
                        format_type_options,
                        temp=True,
                        if_not_exist=True,
                    )
                ),
                Query(
                    pkg.select_from_path_with_format_statement(
                        pkg.schema_cast_seq(schema), path, temp_object_name, pattern
                    )
                ),
            ]
            return SnowflakePlan(
                queries,
                pkg.schema_value_statement(schema),
                [],
                {},
                self.__session,
                None,
            )
        else:  # otherwise use COPY
            if "FORCE" in copy_options and copy_options["FORCE"].lower() != "true":
                raise SnowparkClientException(
                    f"Copy option 'FORCE = {copy_options['FORCE']}' is not supported. Snowpark doesn't skip any loaded files in COPY."
                )

            # set force to true.
            # it is useless since we always create new temp table.
            # setting it helps users to understand generated queries.

            copy_options_with_force = {**copy_options, "FORCE": "TRUE"}

            temp_table_schema = []
            for index, att in enumerate(schema):
                temp_table_schema.append(
                    Attribute(f'"COL{index}"', att.datatype, att.nullable)
                )

            queries = [
                Query(
                    pkg.create_temp_table_statement(
                        temp_object_name,
                        pkg.attribute_to_schema_string(temp_table_schema),
                    )
                ),
                Query(
                    pkg.copy_into_table(
                        temp_object_name,
                        path,
                        format,
                        format_type_options,
                        copy_options_with_force,
                        pattern,
                    )
                ),
                Query(
                    pkg.project_statement(
                        [
                            f"{new_att.name} AS {input_att.name}"
                            for new_att, input_att in zip(temp_table_schema, schema)
                        ],
                        temp_object_name,
                    )
                ),
            ]

            post_actions = [pkg.drop_table_if_exists_statement(temp_object_name)]
            return SnowflakePlan(
                queries,
                pkg.schema_value_statement(schema),
                post_actions,
                {},
                self.__session,
                None,
            )
