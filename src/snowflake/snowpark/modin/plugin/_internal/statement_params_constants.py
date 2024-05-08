#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

# keys used in the statement parameters for Snowpark pandas
SNOWPARK_API = "SNOWPARK_API"
# state params keys used by read only table
# the reason why temp table creation is triggered for creating read only table
MATERIALIZATION_REASON = "MATERIALIZATION_REASON"
# the name for the source table (can be view or others) that is used to create the temp table from
MATERIALIZATION_SOURCE_TABLE_NAME = "MATERIALIZATION_SOURCE_TABLE_NAME"
# the name for the temp table created
MATERIALIZATION_TABLE_NAME = "MATERIALIZATION_TABLE_NAME"
# the source table that the readonly table is created on top of.
READONLY_SOURCE_TABLE_NAME = "READONLY_SOURCE_TABLE_NAME"
# the read only table created
READONLY_TABLE_NAME = "READONLY_TABLE_NAME"

# values used in the statement parameters for Snowpark pandas
PANDAS_API = "pandas"
UNKNOWN = "UNKNOWN"
CONTAINS_ORDER_BY = "CONTAINS_ORDER_BY"
