import modin.pandas as pd
import snowflake.snowpark.modin.plugin
import numpy as np
import pandas as native_pd
from snowflake.snowpark.session import Session; session = Session.builder.create()
import logging; logging.getLogger("snowflake.snowpark").setLevel(logging.DEBUG)

### Section 1. Timedelta
df = pd.DataFrame(
    [
        [pd.Timestamp(year=2020,month=11,day=11,second=30),
         pd.Timestamp(year=2019,month=10,day=10,second=1),
         pd.Timestamp(year=2024, month=7, day=24)
         ]])

# check we can print dataframe
print(df)

# subracting two timestamps.
time_delta = df[0] - df[1]

# The timedelta type shows up as the last column in the schema!
print(time_delta._query_compiler._modin_frame.ordered_dataframe._dataframe_ref.snowpark_dataframe.schema)

print(time_delta)

# adding timestamp to timedelta
print(df[2] + time_delta)


# adding two timedelta to get a new timedelta

time_delta_sum = time_delta + time_delta
print(time_delta_sum)

# add the resulting timedelta to a timestamp again

print(df[0] + time_delta_sum)


# FAIL: take max of timedelta column-- doesn't work yet because we have to
# convert all the functions and Column methods used for transpose into
# SnowparkPandasColumn versions.
print(time_delta.max())

### Section 2. Interval

# Interval
# df = pd.DataFrame([pd.Interval(1, 3, closed='left'), pd.Interval(5, 7, closed='both')])
# print(df)
# dfp = df._to_pandas()
# print(dfp)
# print(list(dfp[0]))