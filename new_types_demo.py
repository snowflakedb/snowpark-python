import modin.pandas as pd
import snowflake.snowpark.modin.plugin
import numpy as np
import pandas as native_pd
from snowflake.snowpark.session import Session; session = Session.builder.create()
import logging; logging.getLogger("snowflake.snowpark").setLevel(logging.DEBUG)

# Interval
df = pd.DataFrame([pd.Interval(1, 3, closed='left'), pd.Interval(5, 7, closed='both')])
print(df)
dfp = df._to_pandas()
print(dfp)
print(list(dfp[0]))

# Timedelta
df = pd.DataFrame([[pd.Timestamp(year=2020,month=11,day=11,second=30), pd.Timestamp(year=2019,month=10,day=10,second=1)]])
print(df)
print(df[0] - df[1])