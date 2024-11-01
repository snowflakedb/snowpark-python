#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
from sklearn.decomposition import PCA
from sklearn.preprocessing import MaxAbsScaler

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import SqlCounter


# SNOW-1344931 Support MaxAbsScalar on earlier versions of
# scikit-learn by supporting numpy.may_share_memory
def test_scikit_maxabs():
    data = [[1.0, -1.0, 2.0], [2.0, 0.0, 0.0], [0.0, 1.0, -1.0]]
    X = pd.DataFrame(data)
    # the following will result in a TypeError on earlier versions
    # of scikit-learn if `numpy.may_share_memory' is not implemented as an
    # array function. In later versions a NotImplementedError is
    # thrown.
    with SqlCounter(query_count=0):
        try:
            MaxAbsScaler().fit_transform(X)
            raise AssertionError()
        except NotImplementedError:
            # scikit-learn 1.5.2 will throw an error for the DF
            # interchange protocol.
            pass


# SNOW-1518382 Support PCA  on earlier versions of
# scikit-learn by supporting numpy.may_share_memory
def test_scikit_pca():
    data = [[1.0, -1.0, 2.0], [2.0, 0.0, 0.0], [0.0, 1.0, -1.0]]
    X = pd.DataFrame(data)
    with SqlCounter(query_count=0):
        pca = PCA()
        try:
            pca.fit(X)
            raise AssertionError()
        except NotImplementedError:
            # scikit-learn 1.5.2 will throw an error for the DF
            # interchange protocol.
            pass
