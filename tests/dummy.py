#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

try:
    import modin.pandas as pd

    import snowflake.snowpark.modin.plugin  # noqa: F401
    from snowflake.snowpark.session import Session

    session = Session.builder.create()

    index = pd.Index([0, 1, 2])

    index.any()

    print("all ok")
except Exception as e:
    print(e)
