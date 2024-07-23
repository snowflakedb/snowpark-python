import modin.pandas as pd
import snowflake.snowpark.modin.plugin
import numpy as np
import pandas as native_pd
from snowflake.snowpark.session import Session; session = Session.builder.create()
import logging; logging.getLogger("snowflake.snowpark").setLevel(logging.DEBUG)


### Section 1. Timedelta
df = pd.DataFrame([[pd.Timestamp(year=2020,month=11,day=11,second=30), pd.Timestamp(year=2019,month=10,day=10,second=1)]])

# check we can print dataframe
print(df)

# The schema has the correct snowpark types.
print(df._query_compiler._modin_frame.ordered_dataframe._dataframe_ref.snowpark_dataframe.schema)

timedelta_result = df[0] - df[1]

# The timedelta type shows up as the last column in the schema!
print(timedelta_result._query_compiler._modin_frame.ordered_dataframe._dataframe_ref.snowpark_dataframe.schema)


# However, Snowflake still raises a type error because the expression types aren't available at the point where we generate the SQL,
# so we can't decide to use datediff instead of regular subtraction.
print(df[0] - df[1])

# adding timestamp to timedelta

# adding two timedelta

# timestamp + (timedelta + timedelta)

### Section 2. Interval

# Interval
# df = pd.DataFrame([pd.Interval(1, 3, closed='left'), pd.Interval(5, 7, closed='both')])
# print(df)
# dfp = df._to_pandas()
# print(dfp)
# print(list(dfp[0]))
