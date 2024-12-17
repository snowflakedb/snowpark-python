#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import math

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

from tests.integ.modin.utils import assert_frame_equal, assert_series_equal
from tests.integ.utils.sql_counter import sql_count_checker, SqlCounter
from tests.utils import running_on_jenkins


@sql_count_checker(query_count=4)
def test_apply_sin():
    from snowflake.snowpark.functions import sin

    native_s = native_pd.Series([0.00, -1.23, 10, math.pi, math.pi / 2])
    s = pd.Series(native_s)

    assert_series_equal(s.apply(sin), native_s.apply(math.sin))
    assert_series_equal(s.map(sin), native_s.map(math.sin))
    assert_frame_equal(
        s.to_frame().applymap(sin), native_s.to_frame().applymap(math.sin)
    )
    assert_frame_equal(
        s.to_frame().apply(sin),
        native_s.to_frame().apply(np.sin),  # Note math.sin does not work with df.apply
    )


@sql_count_checker(query_count=4)
def test_apply_log10():
    from snowflake.snowpark.functions import _log10

    native_s = native_pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])
    s = pd.Series(native_s)

    assert_series_equal(s.apply(_log10), native_s.apply(np.log10))
    assert_series_equal(s.map(_log10), native_s.map(np.log10))
    assert_frame_equal(
        s.to_frame().applymap(_log10), native_s.to_frame().applymap(np.log10)
    )
    assert_frame_equal(
        s.to_frame().apply(_log10),
        native_s.to_frame().apply(
            np.log10
        ),  # Note math.sin does not work with df.apply
    )


@sql_count_checker(query_count=0)
def test_apply_snowpark_python_function_not_implemented():
    from snowflake.snowpark.functions import desc, asc

    with pytest.raises(NotImplementedError):
        pd.Series([1, 2, 3]).apply(desc)
    with pytest.raises(NotImplementedError):
        pd.Series([1, 2, 3]).to_frame().apply(asc, na_action="ignore")
    with pytest.raises(NotImplementedError):
        pd.Series([1, 2, 3]).to_frame().applymap(asc, args=[1, 2])
    with pytest.raises(NotImplementedError):
        pd.DataFrame({"a": [1, 2, 3]}).apply(desc)
    with pytest.raises(NotImplementedError):
        pd.DataFrame({"a": [1, 2, 3]}).apply(asc, raw=True)
    with pytest.raises(NotImplementedError):
        pd.DataFrame({"a": [1, 2, 3]}).apply(asc, axis=1)
    with pytest.raises(NotImplementedError):
        pd.DataFrame({"a": [1, 2, 3]}).apply(asc, args=(1, 2))


@pytest.mark.skipif(
    running_on_jenkins(),
    reason="TODO: SNOW-1859087 snowflake.cortex.summarize SSL error",
)
def test_apply_snowflake_cortex_summarize(session):
    from snowflake.snowpark.functions import snowflake_cortex_summarize

    # TODO: SNOW-1758914 snowflake.cortex.summarize error on GCP
    with SqlCounter(query_count=0):
        if session.connection.host == "sfctest0.us-central1.gcp.snowflakecomputing.com":
            return

    with SqlCounter(query_count=1):
        content = """pandas on Snowflake lets you run your pandas code in a distributed manner directly on your data in
        Snowflake. Just by changing the import statement and a few lines of code, you can get the familiar pandas experience
        you know and love with the scalability and security benefits of Snowflake. With pandas on Snowflake, you can work
        with much larger datasets and avoid the time and expense of porting your pandas pipelines to other big data
        frameworks or provisioning large and expensive machines. It runs workloads natively in Snowflake through
        transpilation to SQL, enabling it to take advantage of parallelization and the data governance and security
        benefits of Snowflake. pandas on Snowflake is delivered through the Snowpark pandas API as part of the Snowpark
        Python library, which enables scalable data processing of Python code within the Snowflake platform.
        """
        s = pd.Series([content])
        summary = s.apply(snowflake_cortex_summarize).iloc[0]
        # this length check is to get around the fact that this function may not be deterministic
        assert 0 < len(summary) < len(content)


@pytest.mark.skipif(
    running_on_jenkins(),
    reason="TODO: SNOW-1859087 snowflake.cortex.sentiment SSL error",
)
def test_apply_snowflake_cortex_sentiment(session):
    from snowflake.snowpark.functions import snowflake_cortex_sentiment

    # TODO: SNOW-1758914 snowflake.cortex.sentiment error on GCP
    with SqlCounter(query_count=0):
        if session.connection.host == "sfctest0.us-central1.gcp.snowflakecomputing.com":
            return

    with SqlCounter(query_count=1):
        content = "A very very bad review!"
        s = pd.Series([content])
        sentiment = s.apply(snowflake_cortex_sentiment).iloc[0]
        assert -1 <= sentiment <= 0
