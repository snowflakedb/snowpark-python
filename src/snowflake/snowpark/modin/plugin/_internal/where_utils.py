#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.modin.plugin._internal.frame import InternalFrame
from snowflake.snowpark.types import BooleanType


def validate_expected_boolean_data_columns(frame: InternalFrame) -> None:
    """
    Checks if the data column types of the frame are all boolean types.  If not, will raise an exception.

    Args:
        frame: The internal frame

    Returns:
        None
    """
    frame_snowflake_identifier_to_data_type_map = (
        frame.quoted_identifier_to_snowflake_type()
    )

    if not all(
        isinstance(
            frame_snowflake_identifier_to_data_type_map.get(
                snowflake_quoted_identifier
            ),
            BooleanType,
        )
        for snowflake_quoted_identifier in frame.data_column_snowflake_quoted_identifiers
    ):
        raise ValueError("Boolean array expected for the condition, not object")
