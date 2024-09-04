#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from enum import Enum


class SkipLargeQueryBreakdownCategory(Enum):
    ACTIVE_TRANSACTION = "active transaction"
    VIEW_DYNAMIC_TABLE = "view or dynamic table command"
    NO_ACTIVE_DB_SCHEMA = "no active database or schema"
