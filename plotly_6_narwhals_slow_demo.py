import modin.pandas as pd
import snowflake.snowpark.modin.plugin
import numpy as np
import datetime
import pandas as native_pd
import logging; logging.getLogger("snowflake.snowpark").setLevel(logging.DEBUG)
from snowflake.snowpark.session import Session; session = Session.builder.create()
import plotly.express as px

logging.basicConfig()

df = pd.DataFrame([[1,2]])
px.scatter(pd.DataFrame([[1, 2], [3, 4]] * 1), x=0, y=1)
