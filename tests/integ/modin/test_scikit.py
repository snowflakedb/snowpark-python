#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import MaxAbsScaler

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=5)
def test_maxabs():
    data = [[1.0, -1.0, 2.0], [2.0, 0.0, 0.0], [0.0, 1.0, -1.0]]
    X = pd.DataFrame(data)
    MaxAbsScaler().fit_transform(X)


@sql_count_checker(query_count=3)
def test_pca():
    data = [[1.0, -1.0, 2.0], [2.0, 0.0, 0.0], [0.0, 1.0, -1.0]]
    X = pd.DataFrame(data)
    pca = PCA()
    pca.fit(X)
