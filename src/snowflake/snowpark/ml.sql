CREATE OR REPLACE PROCEDURE corr_proc(sql_query STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS
$$
import pandas
import os, time
def run(session, sql_query):

    start_time = time.time()
    df = session.sql(sql_query).to_pandas()
    pandas_creation_time = time.time() - start_time

    start_time = time.time()
    corr_matrix = df.corr().round(3)
    mat_creation_time = time.time() - start_time

    file_path = '/tmp/'

    file_name = 'adddsd'

    corr_matrix.to_csv(file_path + file_name)

    start_time = time.time()
    session.file.put(file_path + file_name, "@s1", auto_compress = False, overwrite = True)
    put_command_time = time.time() - start_time

    file = open("/tmp/metrics", "w")
    file.write("Pandas df creation time is %s, correlation df creation time is %s, put command execution time is %s \n" %(pandas_creation_time, mat_creation_time, put_command_time))
    file.close()

    session.file.put("/tmp/metrics", "@s1", auto_compress = False, overwrite = True)
    return file_name
$$;


CREATE OR REPLACE PROCEDURE proc_3(query_id STRING, cols1 ARRAY, cols2 ARRAY)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
EXECUTE AS CALLER
AS
$$
import os, time
from snowflake.snowpark.functions import lit
def run(session, query_id, cols1, cols2):

    cols = list(set(cols1).union(cols2))
    start_time = time.time()
    pandas_df = session.table_function('result_scan', lit(query_id)).select(cols).to_pandas()
    time1 = time.time() - start_time

    pandas_df1 = pandas_df[cols1]

    start_time = time.time()
    ans = []
    for col in cols2:
        ans.append(list(pandas_df1.corrwith(pandas_df[col])))
    time2 = time.time() - start_time

    ans.append(["%s,%s" %(time1, time2)])
    return str(ans)

$$;


CREATE OR REPLACE PROCEDURE snow_gridsearch(input_cols ARRAY, output_col ARRAY, sql_query STRING, file_name STRING)
RETURNS STRING
LANGUAGE PYTHON
RUNTIME_VERSION = '3.8'
PACKAGES = ('snowflake-snowpark-python', 'scikit-learn', 'pandas')
HANDLER = 'run'
AS
$$
import pickle, numpy
def run(session, input_cols, output_col, query, file_name):


    input_cols =[i.upper() for i in input_cols]
    output_col = [i.upper() for i in output_col]

    session.file.get("@s2/" + file_name, "/tmp")
    dbfile = open('/tmp/' + file_name, 'rb')
    gs_obj = pickle.load(dbfile)
    dbfile.close()
    df = session.sql(query).to_pandas()
    x = df[input_cols]
    y = df[output_col]

    gs_obj.fit(x, numpy.ravel(y))

    dbfile = open('/tmp/' + file_name, 'wb')
    pickle.dump(gs_obj, dbfile)
    dbfile.close()

    session.file.put('/tmp/' + file_name, "@s2", auto_compress = False, overwrite = True)
    return
$$;

