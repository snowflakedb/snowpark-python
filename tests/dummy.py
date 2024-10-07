#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

try:
    import modin.pandas as pd
    import numpy as np

    import snowflake.snowpark.modin.plugin  # noqa: F401
    from snowflake.snowpark.session import Session

    session = Session.builder.create()

    df = pd.DataFrame(
        {
            "col1": ["a", "a", "b", "b", "a"],
            "col2": [1.0, 2.0, 3.0, np.nan, 5.0],
            "col3": [1.0, 2.0, 3.0, 4.0, 5.0],
        },
        columns=["col1", "col2", "col3"],
    )

    df2 = df.copy()
    df2.loc[0, "col1"] = "c"
    df2.loc[2, "col3"] = 4.0

    df.compare(df2)

    print("all ok")
except Exception as e:
    print(e)
