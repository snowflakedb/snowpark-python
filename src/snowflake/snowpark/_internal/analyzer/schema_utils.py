#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from __future__ import annotations

from typing import TYPE_CHECKING

import snowflake.snowpark
from snowflake.connector.cursor import ResultMetadata, SnowflakeCursor
from snowflake.snowpark._internal.analyzer.analyzer_utils import (
    quote_name_without_upper_casing,
)
from snowflake.snowpark._internal.analyzer.expression import Attribute
from snowflake.snowpark._internal.type_utils import convert_metadata_to_sp_type
from snowflake.snowpark.types import DecimalType, LongType, StringType

if TYPE_CHECKING:
    import snowflake.snowpark.session

    try:
        from snowflake.connector.cursor import ResultMetadataV2
    except ImportError:
        ResultMetadataV2 = ResultMetadata


def command_attributes() -> list[Attribute]:
    return [Attribute('"status"', StringType())]


def list_stage_attributes() -> list[Attribute]:
    return [
        Attribute('"name"', StringType()),
        Attribute('"size"', LongType()),
        Attribute('"md5"', StringType()),
        Attribute('"last_modified"', StringType()),
    ]


def remove_state_file_attributes() -> list[Attribute]:
    return [Attribute('"name"', StringType()), Attribute('"result"', StringType())]


def put_attributes() -> list[Attribute]:
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


def get_attributes() -> list[Attribute]:
    return [
        Attribute('"file"', StringType(), nullable=False),
        Attribute('"size"', DecimalType(10, 0), nullable=False),
        Attribute('"status"', StringType(), nullable=False),
        Attribute('"encryption"', StringType(), nullable=False),
        Attribute('"message"', StringType(), nullable=False),
    ]


def analyze_attributes(
    sql: str, session: snowflake.snowpark.session.Session
) -> list[Attribute]:
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


def convert_result_meta_to_attribute(
    meta: list[ResultMetadata] | list[ResultMetadataV2],  # pyright: ignore
) -> list[Attribute]:
    # ResultMetadataV2 may not currently be a type, depending on the connector
    # version, so the argument types are pyright ignored

    attributes = []
    for column_metadata in meta:
        quoted_name = quote_name_without_upper_casing(column_metadata.name)
        attributes.append(
            Attribute(
                quoted_name,
                convert_metadata_to_sp_type(column_metadata),
                column_metadata.is_nullable,
            )
        )
    return attributes


def get_new_description_if_exists(
    cursor: SnowflakeCursor,
) -> list[ResultMetadata] | list[ResultMetadataV2]:  # pyright: ignore
    """Return the description of a cursor, using the new result metadata format if possible."""

    # ResultMetadataV2 may not currently be a type, depending on the connector
    # version, so the argument types are pyright ignored

    if hasattr(cursor, "_description_internal"):
        # Pyright does not perform narrowing here
        return cursor._description_internal  # pyright: ignore
    else:
        return cursor.description


def run_new_describe_if_exists(
    cursor: SnowflakeCursor, query: str
) -> list[ResultMetadata] | list[ResultMetadataV2]:  # pyright: ignore
    """Execute a describe() on a cursor, using the new result metadata format if possible."""

    # ResultMetadataV2 may not currently be a type, depending on the connector
    # version, so the argument types are pyright ignored

    if hasattr(cursor, "_describe_internal"):
        # Pyright does not perform narrowing here
        return cursor._describe_internal(query)  # pyright: ignore
    else:
        return cursor.describe(query)
