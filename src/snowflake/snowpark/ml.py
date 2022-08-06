import snowflake.snowpark
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.session import Session
from snowflake.connector.options import pandas
from concurrent.futures import ThreadPoolExecutor, wait
import numpy as np
import time, os, shutil
from threading import current_thread
import ast

import sklearn, os, pickle, numpy
from sklearn.model_selection import GridSearchCV
from sklearn.neighbors import KNeighborsClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import MinMaxScaler
from sklearn.svm import SVC
import random, math, itertools


from snowflake.snowpark.functions import corr as corr_func

CONNECTION_PM3 = {
    'account': 'snowflake',
    'user': 'admin',
    'password': 'test',
    'protocol': 'http',
    'host': 'snowflake.qa1.int.snowflakecomputing.com',
    'port': '8084',
    'schema': 'CM_SCHEMA',
    'database': 'CM_DB',
    'warehouse': 'CM_WH',
}


# Target function for the multithreading approach
def tgt(i, j, n, res, ans, size, session_dict):
    query_res = []

    session_new = session_dict[current_thread().name]
    res = session_new.sql(res)

    temp_var = False

    # Handling any errors that might occur.
    while temp_var or len(query_res) == 0 or (len(query_res) == 1 and len(query_res[0]) != size):
        try:
            # Execute the query
            query_res = res.collect()
        # Need to handle exceptions more carefully
        except ValueError:
            temp_var = True
            print("exception encountered")
            continue
        temp_var = False

    # Store the query results in list of lists
    for k in range(j, min(j + n, len(ans))):
        if len(query_res) == 0 or k >= len(query_res[0]) + j:
            break
        ans[i][k] = query_res[0][k - j]
        ans[k][i] = query_res[0][k - j]


# Thread initialization function. Associates a session object with a thread.
def initializer_worker(session_dict):
    name = current_thread().name
    if name not in session_dict:
        try:
            session_new = Session.builder.configs(CONNECTION_PM3).create()
        except Exception as e:
            print(e)

        session_dict[name] = session_new


def corr_mt(df):
    # Hardcode number of calls to Snowflake's corr( ) in a query
    n = 10
    # Retrieve the column names as a list
    cols = df.columns
    # Dictionary to store mapping between thread names and session objects.
    session_dict = {}

    # A mapping from matrix index to name of the column
    dict_mapping = {ind: col for ind, col in enumerate(cols)}

    # Initialize a list of lists to store the results
    ans = [[None for i in range(len(cols))] for j in range(len(cols))]

    # A list, which will be used by ThreadPoolExecutor to store the future objects
    futures = []

    # Using a context manager to work with the ThreadPoolExecutor. It will automatically call the shutdown()
    # function in the end. It will also wait for all the threads to finish execution. The max_workers is set
    # to the minimum of 1000 and total number of queries generated.
    with ThreadPoolExecutor(max_workers=min(300, len(cols) * (len(cols) + 1) // (2 * n)), initializer=initializer_worker, initargs=tuple([session_dict])) as executor:
        for i in range(0, len(cols)):
            for j in range(i, len(cols), n):
                res = df.select(
                    [corr_func(dict_mapping[i], dict_mapping[k]) for k in range(j, min(j + n, len(ans)))])
                futures.append(executor.submit(tgt, i, j, n, res.queries['queries'][0], ans, min(j + n, len(ans)) - j,
                                               session_dict))

        wait(futures)

    # Remove quotations from the names of the columns returned by self.columns
    for ind, col in enumerate(cols):
        if col.startswith("'") or col.startswith('"'):
            cols[ind] = cols[ind][1:-1]

    # Create a Pandas dataframe from list of lists. Replace any np.nan with None objects.
    pandas_df = pandas.DataFrame(ans, columns=cols, index=cols).replace({np.nan: None})

    # Delete all the session objects.
    session_dict.clear()
    return pandas_df


# The target function for the mixed approach
def corr_mixed_tgt(session_dict, query_id, cols):
    session_new = session_dict[current_thread().name]

    ans = session_new.call('proc_3', query_id, cols)

    return ans


# Mixed approach
def corr_mixed(session, df):

    n = 10
    session_dict = {}
    cols = list(df.columns)

    ans = []
    futures = []

    query_id = session.call('helper', df.queries['queries'][0])
    # query_id = session.sql('select last_query_id()').collect()
    with ThreadPoolExecutor(max_workers=300, initializer=initializer_worker,
                            initargs=tuple([session_dict])) as executor:
        for i in range(0, len(cols), n):
            futures.append(executor.submit(corr_mixed_tgt, session_dict, query_id, cols[i:min(i + n, len(cols))]))

        wait(futures)
        for future in futures:
            res = future.result()
            ans = ans + ast.literal_eval(res)

    return pandas.DataFrame(ans, columns=cols, index=cols).replace({numpy.nan: None})


def hp_sp(input_cols, output_col, query, file_name):

    dbfile = open(file_name, 'rb')
    gs_obj = pickle.load(dbfile)
    dbfile.close()
    session = Session.builder.configs(CONNECTION_PM3).create()
    x = session.sql(query).select(input_cols + output_col).to_pandas()
    y = x[output_col]

    gs_obj.fit(x, numpy.ravel(y))

    dbfile = open(file_name, 'wb')
    pickle.dump(gs_obj, dbfile)
    dbfile.close()

    return


class SnowflakeGridSearch(GridSearchCV):

    def initializer_worker(self, session_dict):
        name = current_thread().name
        if name not in session_dict:
            try:
                session_new = Session.builder.configs(CONNECTION_PM3).create()
            except Exception as e:
                print(e)

            session_dict[name] = session_new

    def tgt(self, grid, input_cols, output_col, query):

        session_new = self.session_dict[current_thread().name]
        gs_obj = GridSearchCV(self.estimator, grid, scoring=self.scoring, n_jobs=self.n_jobs,
                              refit=self.refit, cv=self.cv, verbose=self.verbose, pre_dispatch=self.pre_dispatch,
                              error_score=self.error_score, return_train_score=self.return_train_score)

        file_name = './tmp/' + current_thread().name
        dbfile = open(file_name, 'wb')
        pickle.dump(gs_obj, dbfile)
        dbfile.close()
        hp_sp(input_cols, output_col, query, file_name)

        return file_name

    def split2(self, options, n_jobs):

        keys = options.keys()
        values = (options[key] for key in keys)
        combinations = [dict(zip(keys, combination)) for combination in itertools.product(*values)]
        result = []

        for i in range(0, len(combinations), math.ceil(len(combinations) / n_jobs)):
            temp = {}
            for j in range(i, min(len(combinations), i + math.ceil(len(combinations) / n_jobs))):
                for key in combinations[j].keys():
                    if key not in temp:
                        temp[key] = []
                    if combinations[j][key] not in temp[key]:
                        temp[key].append(combinations[j][key])
            result.append(temp)
        return result

    def split(self, tuned_params_list, n_jobs=5):

        jobs_size = []
        total_size = 0

        for tuned_params in tuned_params_list:

            size = 1

            for key in tuned_params:
                size *= len(tuned_params[key])

            jobs_size.append(size)
            total_size += size

        for i in range(0, len(jobs_size)):
            jobs_size[i] = math.ceil((jobs_size[i] / total_size) * n_jobs)

        grid = []
        print(jobs_size)
        for i in range(0, len(jobs_size)):
            grid += self.split2(tuned_params_list[i], jobs_size[i])

        return grid

    def fit(self, input_cols, output_col, query):

        n_jobs = 5
        grid_list = self.split(self.param_grid, n_jobs)
        self.session_dict = {}
        futures = []
        file_names = []

        with ThreadPoolExecutor(max_workers=n_jobs, initializer=initializer_worker,
                                initargs=tuple([self.session_dict])) as executor:
            for grid in grid_list:
                futures.append(executor.submit(tgt, grid, input_cols, output_col, query))

            wait(futures)
            for future in futures:
                file_names.append(future.result())

        for file_name in file_names:
            dbfile = open(file_name, 'rb')
            gs_obj = pickle.load(dbfile)
            dbfile.close()
            print(gs_obj.best_params_, gs_obj.best_score_)
            #return gs_obj






