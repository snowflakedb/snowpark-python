#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
from numpy.testing import assert_array_equal

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import SqlCounter


# SNOW-1344931 Support MaxAbsScalar by supporting numpy.may_share_memory
def test_scikit_maxabs():
    from sklearn.preprocessing import MaxAbsScaler

    data = [[1.0, -1.0, 2.0], [2.0, 0.0, 0.0], [0.0, 1.0, -1.0]]
    X = pd.DataFrame(data)
    X_native = native_pd.DataFrame(data)
    # the following will result in a TypeError if
    # `numpy.may_share_memory' is not implemented as an
    # array function. Similar calls are made in other scikit learn
    # functions
    with SqlCounter(query_count=3):
        result = MaxAbsScaler().fit_transform(X)
        native_result = MaxAbsScaler().fit_transform(X_native)
        assert_array_equal(np.array(result), np.array(native_result))


# SNOW-1518382 Support PCA by supporting numpy.may_share_memory
# similar to the test for MaxAbsScalar
def test_scikit_pca():
    from sklearn.decomposition import PCA

    data = [[1.0, -1.0, 2.0], [2.0, 0.0, 0.0], [0.0, 1.0, -1.0]]
    X = pd.DataFrame(data)
    X_native = native_pd.DataFrame(data)
    with SqlCounter(query_count=2):
        pca = PCA()
        result = pca.fit(X)
        native_result = pca.fit(X_native)
        assert_array_equal(np.array(result), np.array(native_result))
