#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pytest
from pytest import param


from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import running_on_jenkins

# snowflake-ml-python, which provides snowflake.cortex, may not be available in
# the test environment. If it's not available, skip all tests in this module.
cortex = pytest.importorskip("snowflake.cortex")
Sentiment = cortex.Sentiment
Summarize = cortex.Summarize
Translate = cortex.Translate


@pytest.mark.skipif(
    running_on_jenkins(),
    reason="TODO: SNOW-1859087 snowflake.cortex.summarize SSL error",
)
def test_apply_snowflake_cortex_summarize(session):

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
        summary = s.apply(Summarize).iloc[0]
        # this length check is to get around the fact that this function may not be deterministic
        assert 0 < len(summary) < len(content)


@pytest.mark.skipif(
    running_on_jenkins(),
    reason="TODO: SNOW-1859087 snowflake.cortex.sentiment SSL error",
)
def test_apply_snowflake_cortex_sentiment_series(session):

    # TODO: SNOW-1758914 snowflake.cortex.sentiment error on GCP
    with SqlCounter(query_count=0):
        if session.connection.host == "sfctest0.us-central1.gcp.snowflakecomputing.com":
            return

    with SqlCounter(query_count=1):
        content = "A very very bad review!"
        s = pd.Series([content])
        sentiment = s.apply(Sentiment).iloc[0]
        assert -1 <= sentiment <= 0


@pytest.mark.skipif(
    running_on_jenkins(),
    reason="TODO: SNOW-1859087 snowflake.cortex.sentiment SSL error",
)
def test_apply_snowflake_cortex_sentiment_df(session):

    # TODO: SNOW-1758914 snowflake.cortex.sentiment error on GCP
    with SqlCounter(query_count=0):
        if session.connection.host == "sfctest0.us-central1.gcp.snowflakecomputing.com":
            return
    text_list = [
        "A first row of text.",
        "This is a very bad test.",
        "This is the best test ever.",
    ]

    content_frame = pd.DataFrame(text_list, columns=["content"])
    with SqlCounter(query_count=4):
        res = content_frame.apply(Sentiment)
        sent_row_2 = res["content"][1]
        sent_row_3 = res["content"][2]
        assert -1 <= sent_row_2 <= 0
        assert 0 <= sent_row_3 <= 1


@pytest.mark.skipif(
    running_on_jenkins(),
    reason="TODO: SNOW-1859087 snowflake.cortex.sentiment SSL error",
)
@sql_count_checker(query_count=0)
@pytest.mark.parametrize(
    "is_series, operation",
    [
        param(
            True,
            (lambda s: s.apply(Translate, source_language="en", target_language="de")),
            id="series_cortex_unsupported_function_translate",
        ),
        param(
            False,
            (
                lambda df: df.apply(
                    Translate, source_language="en", target_language="de"
                )
            ),
            id="df_cortex_unsupported_function_translate",
        ),
        param(
            True,
            (lambda s: s.apply(Sentiment, args=("hello",))),
            id="series_cortex_unsupported_args",
        ),
        param(
            False,
            (lambda df: df.apply(Sentiment, args=("hello",))),
            id="df_cortex_unsupported_args",
        ),
        param(
            True,
            (lambda s: s.apply(Sentiment, extra="hello")),
            id="series_cortex_unsupported_kwargs",
        ),
        param(
            False,
            (lambda df: df.apply(Sentiment, extra="hello")),
            id="df_cortex_unsupported_kwargs",
        ),
        param(
            True,
            (lambda s: s.apply(Sentiment, na_action="ignore")),
            id="series_cortex_unsupported_na_action",
        ),
        param(
            False,
            (lambda df: df.apply(Sentiment, raw=True)),
            id="df_cortex_unsupported_raw",
        ),
        param(
            False,
            (lambda df: df.apply(Sentiment, axis=1)),
            id="df_cortex_unsupported_axis_1",
        ),
    ],
)
def test_apply_snowflake_cortex_negative(session, is_series, operation):

    # TODO: SNOW-1758914 snowflake.cortex.sentiment error on GCP
    if session.connection.host == "sfctest0.us-central1.gcp.snowflakecomputing.com":
        return

    content = "One day I will see the world."
    modin_input = (pd.Series if is_series else pd.DataFrame)([content])
    with pytest.raises(NotImplementedError):
        operation(modin_input)
