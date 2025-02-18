#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark import QueryListener
from snowflake.snowpark.query_history import QueryRecord
from snowflake.snowpark.session import Session
from tests.integ.modin.conftest import IRIS_DF
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker

# expected_query_count is for test_repr_html paramterized SqlCounter test
_DATAFRAMES_TO_TEST = [
    (
        native_pd.DataFrame(
            {
                "Animal": ["Falcon", "Falcon", "Parrot", "Parrot"],
                "Max Speed": [380.0, 370.0, 24.0, 26.0],
                "Timedelta": [
                    pd.Timedelta(1, unit="ns"),
                    pd.Timedelta(microseconds=1),
                    pd.Timedelta(milliseconds=-1),
                    pd.Timedelta(days=9999, hours=10, minutes=30, seconds=10),
                ],
            }
        ),
        1,
    ),
    (
        native_pd.DataFrame([1, 2], index=[pd.Timedelta(1), pd.Timedelta(-1)]),
        1,
    ),
    (
        IRIS_DF,
        4,
    ),
    (
        native_pd.DataFrame(),
        1,
    ),
    (
        native_pd.DataFrame(
            {"A": list(range(10000)), "B": np.random.normal(size=10000)}
        ),
        4,
    ),
    (
        native_pd.DataFrame(columns=["A", "B", "C", "D", "C", "B", "A"]),
        1,
    ),
    # one large dataframe to test many columns
    (
        native_pd.DataFrame(columns=[f"x{i}" for i in range(300)]),
        1,
    ),
    # one large dataframe to test both columns/rows
    (
        native_pd.DataFrame(
            data=np.zeros(shape=(300, 300)), columns=[f"x{i}" for i in range(300)]
        ),
        4,
    ),
]


@pytest.mark.parametrize("native_df, expected_query_count", _DATAFRAMES_TO_TEST)
def test_repr(native_df, expected_query_count):
    snow_df = pd.DataFrame(native_df)

    native_str = repr(native_df)
    # only measure select statements here, creation of dfs may yield a couple
    # CREATE TEMPORARY TABLE/INSERT INTO queries
    with SqlCounter(query_count=expected_query_count, select_count=1):
        snow_str = repr(snow_df)

        assert native_str == snow_str


@pytest.mark.parametrize("native_df, expected_query_count", _DATAFRAMES_TO_TEST)
def test_repr_html(native_df, expected_query_count):

    # TODO: SNOW-916596 Test this with Jupyter notebooks.
    # joins due to temp table creation
    snow_df = pd.DataFrame(native_df)

    # in Snowpark Pandas, we set "display.max_columns" to 20 to limit the query, simulate this behavior here
    # because Pandas uses default value of 0

    native_html = native_df._repr_html_()
    snow_html = snow_df._repr_html_()

    native_html = native_df._repr_html_()

    # 10 of these are related to stored procs, inserts, alter session query tag.
    with SqlCounter(query_count=expected_query_count, select_count=1):
        snow_html = snow_df._repr_html_()

    assert native_html == snow_html


class ReprQueryListener(QueryListener):
    """A context manager that listens to and records SQL queries that are pushed down to the Snowflake database
    if they are used for `repr` or `_repr_html_`.

    See also:
        :meth:`snowflake.snowpark.Session.query_history`.
    """

    def __init__(self, session: Session) -> None:
        self.session = session
        self.session._conn.add_query_listener(self)
        self._queries = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.session._conn.remove_query_listener(self)

    def _notify(self, query_record: QueryRecord, **kwargs):
        # Any query for `repr` or `_repr_html_` will include
        # `<= 31` in the SQL text.
        if "<= 31" in query_record.sql_text:
            self._queries.append(query_record)

    @property
    def queries(self) -> list[QueryRecord]:
        return [query.sql_text for query in self._queries]


@pytest.mark.parametrize("native_df, expected_query_count", _DATAFRAMES_TO_TEST)
def test_repr_and_repr_html_issue_same_query(native_df, expected_query_count):
    """This test ensures that the same query is issued for both `repr` and `repr_html`
    in order to take advantage of Snowflake server side caching when both are called back
    to back (as in the case with displaying a DataFrame in a Jupyter notebook)."""

    native_df = native_pd.DataFrame([[1, 2, 3], [4, 5, 6], [7, 8, 9]])
    snow_df = pd.DataFrame(native_df)

    with ReprQueryListener(pd.session) as listener:
        with SqlCounter(query_count=1, select_count=1):
            repr_str = repr(snow_df)
        with SqlCounter(query_count=1, select_count=1):
            repr_html = snow_df._repr_html_()

    assert repr_str == repr(native_df)
    assert repr_html == native_df._repr_html_()

    assert len(listener.queries) == 2
    assert listener.queries[0] == listener.queries[1]


class TestWithGlobalSettings:
    def setup_class(self):
        self.native_num_rows = native_pd.get_option("display.max_rows")
        self.native_num_cols = native_pd.get_option("display.max_columns")

        self.num_rows = pd.get_option("display.max_rows")
        self.num_cols = pd.get_option("display.max_columns")

        self.native_df = native_pd.DataFrame(
            data=np.zeros(shape=(40, 40)), columns=[f"x{i}" for i in range(40)]
        )
        self.snow_df = pd.DataFrame(self.native_df)

    def teardown_class(self):
        native_pd.set_option("display.max_rows", self.native_num_rows)
        native_pd.set_option("display.max_columns", self.native_num_cols)
        pd.set_option("display.max_rows", self.num_rows)
        pd.set_option("display.max_columns", self.num_cols)

    def test_with_max_columns(self):
        native_pd.set_option("display.max_columns", 10)
        pd.set_option("display.max_columns", 10)

        # This test should only issue 6 SQL queries given dataframe creation from large
        # local data: 1) Creating temp table, 2) Setting query tag, 3) Inserting into temp table,
        # 4) Unsetting query tag, 5) Select Columns, 6) Drop temp table.
        # However, an additional 6 queries are issued to eagerly get the row count.
        # for now, track only SELECT count here
        with SqlCounter(select_count=1):
            snow_str = repr(self.snow_df)
        native_str = repr(self.native_df)

        assert snow_str == native_str
        self.teardown_class()

    def test_with_max_rows_none(self):
        native_pd.set_option("display.max_rows", None)
        pd.set_option("display.max_rows", None)

        with SqlCounter(select_count=2):
            snow_str = repr(self.snow_df)
        native_str = repr(self.native_df)

        assert snow_str == native_str
        self.teardown_class()

    def test_with_max_columns_none(self):
        native_pd.set_option("display.max_columns", None)
        pd.set_option("display.max_columns", None)

        with SqlCounter(select_count=1):
            snow_str = repr(self.snow_df)
        native_str = repr(self.native_df)

        assert snow_str == native_str


def test_repr_deviating_behavior():
    native_df = native_pd.DataFrame(index=list(range(10000)))
    snow_df = pd.DataFrame(native_df)

    # This test should only issue 6 SQL queries given dataframe creation from large
    # local data: 1) Creating temp table, 2) Setting query tag, 3) Inserting into temp table,
    # 4) Unsetting query tag, 5) Select Columns, 6) Drop temp table.
    # However, an additional 6 queries are issued to eagerly get the row count.
    # for now, track only SELECT count here
    with SqlCounter(select_count=1):
        snow_str = repr(snow_df)

    native_str = repr(native_df)

    # Snowpark pandas and pandas deviate here, as pandas displays an index specific number of elements up to the
    # maximum width, which is here up to 100 elements, i.e.
    # Empty DataFrame Columns [] Index: [0, 1, 2, 3, ...]
    # whereas Snowpark pandas uses the same format as in other APIs and will display
    # up to pandas.get_option("display.max_rows")
    # # Empty DataFrame Columns [] Index: [0, 1, 2, 3, ... ]

    # therefore check here only 2/5 of the snow_str
    N = (2 * len(snow_str)) // 5
    assert native_str[:N] == snow_str[:N]


@sql_count_checker(query_count=2, union_count=1)
def test_repr_of_multiindex_df():
    tuples = [
        ("cobra", "mark i"),
        ("cobra", "mark ii"),
        ("sidewinder", "mark i"),
        ("sidewinder", "mark ii"),
        ("viper", "mark ii"),
        ("viper", "mark iii"),
    ]
    index = pd.MultiIndex.from_tuples(tuples)
    values = [[12, 2], [0, 4], [10, 20], [1, 4], [7, 1], [16, 36]]
    df = pd.DataFrame(values, columns=["max_speed", "shield"], index=index)
    native_df = native_pd.DataFrame(
        values, columns=["max_speed", "shield"], index=index
    )

    # Note: There's a type mismatch here between pandas and Snowpark pandas
    # I.e., Snowpark pandas returns Name: ('cobra', 'mark ii'), dtype: int8 whereas pandas returns Name: (cobra, mark ii), dtype: int64
    ans = repr(df.loc[("cobra", "mark ii")])
    native_ans = repr(native_df.loc[("cobra", "mark ii")])

    # Remove ' and change int8 -> int64
    ans = ans.replace("'", "").replace("int8", "int64")

    assert ans == native_ans
