#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import List

import snowflake.snowpark
from snowflake.connector.constants import FIELD_ID_TO_NAME
from snowflake.connector.cursor import ResultMetadata
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    quote_name_without_upper_casing,
)
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.type_utils import convert_sf_to_sp_type
from snowflake.snowpark.types import DecimalType, LongType, StringType


def command_attributes() -> List[Attribute]:
    return [Attribute('"status"', StringType())]


def list_stage_attributes() -> List[Attribute]:
    return [
        Attribute('"name"', StringType()),
        Attribute('"size"', LongType()),
        Attribute('"md5"', StringType()),
        Attribute('"last_modified"', StringType()),
    ]


def remove_state_file_attributes() -> List[Attribute]:
    return [Attribute('"name"', StringType()), Attribute('"result"', StringType())]


def put_attributes() -> List[Attribute]:
    return [
        Attribute('"source"', StringType(), nullable=False),
        Attribute('"target"', StringType(), nullable=False),
        Attribute('"source_size"', DecimalType(10, 0), nullable=False),
        Attribute('"target_size"', DecimalType(10, 0), nullable=False),
        Attribute('"source_compression"', StringType(), nullable=False),
        Attribute('"target_compression"', StringType(), nullable=False),
        Attribute('"status"', StringType(), nullable=False),
        Attribute('"encryption"', StringType(), nullable=False),
        Attribute('"message"', StringType(), nullable=False),
    ]


def get_attributes() -> List[Attribute]:
    return [
        Attribute('"file"', StringType(), nullable=False),
        Attribute('"size"', DecimalType(10, 0), nullable=False),
        Attribute('"status"', StringType(), nullable=False),
        Attribute('"encryption"', StringType(), nullable=False),
        Attribute('"message"', StringType(), nullable=False),
    ]


def analyze_attributes(
    sql: str, session: "snowflake.snowpark.session.Session"
) -> List[Attribute]:
    lowercase = sql.strip().lower()

    # SQL commands which cannot be prepared
    # https://docs.snowflake.com/en/user-guide/sql-prepare.html
    if lowercase.startswith(
        ("alter", "drop", "use", "create", "grant", "revoke", "comment")
    ):
        return command_attributes()
    if lowercase.startswith(("ls", "list")):
        return list_stage_attributes()
    if lowercase.startswith(("rm", "remove")):
        return remove_state_file_attributes()
    if lowercase.startswith("put"):
        return put_attributes()
    if lowercase.startswith("get"):
        return get_attributes()
    if lowercase.startswith("describe"):
        session._run_query(sql)
        return convert_result_meta_to_attribute(session._conn._cursor.description)

    return session._get_result_attributes(sql)


def convert_result_meta_to_attribute(meta: List[ResultMetadata]) -> List[Attribute]:
    attributes = []
    for column_name, type_value, _, _, precision, scale, nullable in meta:
        quoted_name = quote_name_without_upper_casing(column_name)
        attributes.append(
            Attribute(
                quoted_name,
                convert_sf_to_sp_type(FIELD_ID_TO_NAME[type_value], precision, scale),
                nullable,
            )
        )
    return attributes
