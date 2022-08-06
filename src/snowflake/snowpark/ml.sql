CREATE OR REPLACE PROCEDURE helper(sql_query STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import pandas
import os, time
def run(session, sql_query):

    session.sql(sql_query).collect()
    id = session.sql('select last_query_id()').collect()
    return id[0][0]
$$;


CREATE OR REPLACE PROCEDURE proc_3(query_id STRING, cols ARRAY)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import pandas
import os, time
from snowflake.snowpark.functions import lit
def run(session, query_id, cols):

    pandas_df = session.table_function('result_scan', lit(query_id)).to_pandas()
    ans = []
    for col in cols:
        ans.append(list(pandas_df.corrwith(pandas_df[col])))
    return str(ans)

$$;

