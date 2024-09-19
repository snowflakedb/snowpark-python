#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.session import Session

session = Session.builder.getOrCreate()

session.ast_enabled = True

ans = session.create_dataframe([1, 2, 3, 4]).collect()

print(ans)
