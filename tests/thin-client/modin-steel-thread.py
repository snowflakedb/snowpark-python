#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark import Session

session = Session.builder.create()
df = pd.DataFrame(
    [[1, 2, 3, 4, 5, 6, 7], [8, 9, 10, 11, 12, 13, 14], [15, 16, 17, 18, 19, 20, 21]],
    columns=["A", "B", "C", "D", "E", "F", "G"],
)
df = df.iloc[3, 4]
df.show()
