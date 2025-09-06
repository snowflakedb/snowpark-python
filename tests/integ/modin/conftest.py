#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import pathlib
import re
from datetime import datetime

import modin.pandas as pd
import numpy as np
import pandas
import pytest
from pandas._typing import Frequency
from pandas.core.indexing import IndexingError
from pytest import fail
from typing import Generator

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.apply_utils import (
    clear_session_udf_and_udtf_caches,
)
from tests.integ.modin.pandas_api_coverage import PandasAPICoverageGenerator
from tests.integ.utils.sql_counter import (
    SqlCounter,
    clear_sql_counter_called,
    generate_sql_count_report,
    is_sql_counter_called,
)
from tests.utils import Utils, running_on_jenkins

from modin.config import AutoSwitchBackend
from tests.conftest import set_skip_sql_count_check

MODIN_HYBRID_TEST_MODE_ENABLED = False


@pytest.fixture(scope="session", autouse=True)
def setup_modin_hybrid_mode(pytestconfig):
    global MODIN_HYBRID_TEST_MODE_ENABLED
    hybrid_mode = pytestconfig.getoption("enable_modin_hybrid_mode")
    if hybrid_mode:
        AutoSwitchBackend.enable()
        MODIN_HYBRID_TEST_MODE_ENABLED = True
        set_skip_sql_count_check(True)
    else:
        AutoSwitchBackend.disable()
        MODIN_HYBRID_TEST_MODE_ENABLED = False


def read_hybrid_known_failures():
    """
    Read the modin_hybrid_integ_results.csv file and create a pandas
    dataframe filtered down to only the failed tests. You can regenerate
    this file by:
    * Collecting the hybrid test results with pytest:
        pytest tests/integ/modin -n 10
               --enable_modin_hybrid_mode
               --csv tests/integ/modin/modin_hybrid_integ_results.csv
    * (Recommended) Pre-Filtering the results to reduce the file size:
      import pandas as pd
      df = pd.read_csv("tests/integ/modin/modin_hybrid_integ_results.csv")
      filtered = df[["module", "name", "message", "status"]][
               df["status"].isin(["failed", "xfailed", "error"])
            ]
      filtered.to_csv("tests/integ/modin/modin_hybrid_integ_results.csv")
    """
    HYBRID_RESULTS_PATH = os.path.normpath(
        os.path.join(
            os.path.dirname(__file__), "../modin/modin_hybrid_integ_results.csv"
        )
    )
    df = pandas.read_csv(HYBRID_RESULTS_PATH)
    return df[["module", "name", "message", "status"]][
        df["status"].isin(["failed", "xfailed", "error"])
    ]


HYBRID_KNOWN_FAILURES = read_hybrid_known_failures()


def is_hybrid_known_failure(module_name, test_name) -> dict[bool, str]:
    """
    Determine whether the module/test is a known hybrid mode failure
    and return the result along with the error message if applicable.
    """
    module_mask = HYBRID_KNOWN_FAILURES.module == module_name
    testname_mask = HYBRID_KNOWN_FAILURES.name == test_name
    test_data = HYBRID_KNOWN_FAILURES[module_mask & testname_mask]
    failed = len(test_data) >= 1
    msg = None
    if failed:
        msg = test_data["message"].iloc[0]
    return (failed, msg)


def pytest_runtest_setup(item):
    """
    pytest hook to filter out tests when running under hybrid mode
    """
    config = item.config
    if not config.option.enable_modin_hybrid_mode:
        return
    # When a test is annotated with @pytest.mark.no_hybrid it will be skipped
    if len(list(item.iter_markers(name="no_hybrid"))) > 0:
        pytest.skip("Skipped for Hybrid: pytest.mark.no_hybrid")
    # Check the known failure list as of 2025-09-04 and skip those with a message
    (failed, msg) = is_hybrid_known_failure(item.module.__name__, item.name)
    if failed:
        pytest.skip(f"Skipped for Hybrid: {msg}")


@pytest.fixture(scope="module", autouse=True)
def f(session):
    # create a snowpark pandas dataframe so that modin keeps an empty query compiler
    pd.DataFrame()


INTEG_PANDAS_SUBPATH = "tests/integ/modin/"

# use a sample size to generate query result in multiple chunks (default chunk size is 4K rows in Snowflake)
INDEX_SAMPLE_SIZE = 5000
HALF_INDEX_SAMPLE_SIZE = 2500

nullable_int_sample = np.random.choice(
    np.append(np.arange(10), [None]), size=INDEX_SAMPLE_SIZE
)
nullable_bool_sample = np.random.choice([True, False, None], size=INDEX_SAMPLE_SIZE)


# The dataframe/series aggregation methods supported with Snowpark pandas
skipna_agg_methods = ["max", "min", "sum", "std", "var", "mean", "median"]
agg_methods = skipna_agg_methods + ["count"]


def pytest_addoption(parser):
    parser.addoption(
        "--generate_pandas_api_coverage", action="store_true", default=False
    )


@pytest.fixture(scope="session", autouse=True)
def setup_pandas_api_coverage_generator(pytestconfig):
    enable_coverage = pytestconfig.getoption("generate_pandas_api_coverage")
    if enable_coverage:
        PandasAPICoverageGenerator()


@pytest.fixture(scope="function", autouse=True)
def clear_udf_and_udtf_caches():
    # UDF/UDTFs are persisted across the entire session for performance reasons. To ensure tests
    # remain independent from each other, we must clear the caches between runs.
    clear_session_udf_and_udtf_caches()


@pytest.fixture(scope="function")
def sql_counter():
    """Return a sql counter as pytest fixture"""
    return SqlCounter()


# The following autouse pytest fixture will run the sql count checker in record mode when uncommented.  This will
# capture the sql count results of each test which are then written into the source files along with a status report
# for any sql counts that can't be automated.  This method may be handy when adding bulk Snowpark pandas tests or
# extending the sql counts in the future.
#
# The following line must be commented when merged into main.
# @pytest.fixture(autouse=True)
def auto_annotate_sql_counter(request):
    counter = SqlCounter()

    # Set record mode so existing sql_count assumptions will *not* fail (will be skipped) if we're running auto here.
    SqlCounter.set_record_mode(True)
    with counter:
        yield

    generate_sql_count_report(request, counter)


@pytest.fixture(autouse=True)
def check_sql_counter_invoked(request):
    from tests.conftest import SKIP_SQL_COUNT_CHECK

    do_check = (
        INTEG_PANDAS_SUBPATH in request.node.location[0]
        # Originally, we ran this check in Github Actions but not when running
        # tests locally or on Jenkins. It turned out to be more convenient to
        # have the local development experience match the Github Actions
        # experience, so now we run the check for local development, but we
        # still don't run it on Jenkins. We may eventually want to run the check
        # on Jenkins.
        and not running_on_jenkins()
        and not SKIP_SQL_COUNT_CHECK
    )

    if do_check:
        clear_sql_counter_called()

    yield

    if (
        do_check
        # We only need to check the SQL counts if the test has passed so far.
        and request.node.rep_call.passed
        and not is_sql_counter_called()
    ):
        test_file, line_no, test_name = request.node.location
        fail(
            reason=f"Sql counter checker decorator or inline was not run in test '{test_name}' "
            + f"\n\nTest file: {test_file}\nTest name: {test_name}\nLine no: {line_no}\n\n"
            + "Please add for test to pass.",
            pytrace=False,
        )


@pytest.hookimpl(tryfirst=True, hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """
    rep_setup - setup result
    rep_call - test result
    rep_teardown - teardown result
    Use this solution to set rep_call to the result of running a test case.
    Older versions of pytest had request.node.rep_call, but currently we have
    to add it ourselves.
    source: https://stackoverflow.com/a/74492150
    """
    outcome = yield
    rep = outcome.get_result()
    setattr(item, "rep_" + rep.when, rep)


@pytest.fixture(params=skipna_agg_methods)
def skipna_agg_method(request):
    """Fixture for parametrization of result compatible aggregation methods."""
    return request.param


@pytest.fixture(params=agg_methods)
def agg_method(request):
    """Fixture for parametrization of result compatible aggregation methods."""
    return request.param


def create_multiindex() -> pd.MultiIndex:
    """
    MultiIndex used to test the general functionality of this object:
    MultiIndex([('foo', 'one'),
            ('foo', 'two'),
            ('bar', 'one'),
            ('baz', 'two'),
            ('qux', 'one'),
            ('qux', 'two')],
           names=['first', 'second'])
    """

    major_axis = pandas.Index(["foo", "bar", "baz", "qux"])
    minor_axis = pandas.Index(["one", "two"])

    major_codes = np.array([0, 0, 1, 2, 3, 3])
    minor_codes = np.array([0, 1, 0, 1, 0, 1])
    index_names = ["first", "second"]
    return pd.MultiIndex(
        levels=[major_axis, minor_axis],
        codes=[major_codes, minor_codes],
        names=index_names,
        verify_integrity=False,
    )


def create_multiindex_with_dt64tz_level() -> pd.MultiIndex:
    """
    MultiIndex with a level that is a tzaware DatetimeIndex:
    MultiIndex([(1, 'a', '2013-01-01 00:00:00-05:00'),
            (1, 'a', '2013-01-02 00:00:00-05:00'),
            (1, 'a', '2013-01-03 00:00:00-05:00'),
            (1, 'b', '2013-01-01 00:00:00-05:00'),
            (1, 'b', '2013-01-02 00:00:00-05:00'),
            (1, 'b', '2013-01-03 00:00:00-05:00'),
            (2, 'a', '2013-01-01 00:00:00-05:00'),
            (2, 'a', '2013-01-02 00:00:00-05:00'),
            (2, 'a', '2013-01-03 00:00:00-05:00'),
            (2, 'b', '2013-01-01 00:00:00-05:00'),
            (2, 'b', '2013-01-02 00:00:00-05:00'),
            (2, 'b', '2013-01-03 00:00:00-05:00')],
           names=['one', 'two', 'three'])
    """
    # GH#8367 round trip with pickle
    return pandas.MultiIndex.from_product(
        [
            [1, 2],
            ["a", "b"],
            pandas.date_range("20130101", periods=3, tz="US/Eastern"),
        ],
        names=["one", "two", "three"],
    )


@pytest.fixture(scope="session")
def indices_dict():
    return {
        "string": pandas.Index(
            [f"i-{i}" for i in range(INDEX_SAMPLE_SIZE)], dtype=object
        ),
        "int": pandas.Index(np.arange(INDEX_SAMPLE_SIZE), dtype="int16"),
        "range": pandas.RangeIndex(0, INDEX_SAMPLE_SIZE, 1),
        "float": pandas.Index(np.arange(INDEX_SAMPLE_SIZE), dtype="float"),
        "repeats": pandas.Index([0, 0, 1, 1, 2, 2] * int(INDEX_SAMPLE_SIZE / 6)),
        "bool-dtype": pandas.Index(np.random.randn(INDEX_SAMPLE_SIZE) < 0),
        "tuples": pandas.MultiIndex.from_tuples(zip(["foo", "bar", "baz"], [1, 2, 3])),
        "multi": create_multiindex(),
        # NumericIndex is a pandas 2.x feature
        "num_int64": pandas.Index(np.arange(INDEX_SAMPLE_SIZE), dtype="int64"),
        "num_float64": pandas.Index(np.arange(INDEX_SAMPLE_SIZE), dtype="float64"),
        "empty": pandas.Index([]),
        "bool-object": pandas.Index(
            [True, False] * HALF_INDEX_SAMPLE_SIZE, dtype=object
        ),
        "string-python": pandas.Index(
            pd.array(
                pandas.Index(
                    [f"i-{i}" for i in range(INDEX_SAMPLE_SIZE)], dtype=object
                ),
                dtype="string[python]",
            )
        ),
        "nullable_int": pandas.Index(nullable_int_sample, dtype="Int64"),
        "nullable_uint": pandas.Index(nullable_int_sample, dtype="UInt16"),
        "nullable_float": pandas.Index(nullable_int_sample, dtype="Float32"),
        "nullable_bool": pandas.Index(
            nullable_bool_sample.astype(bool),
            dtype="boolean",
        ),
        "uint": pandas.Index(np.arange(INDEX_SAMPLE_SIZE), dtype="uint"),
        "uint-small": pandas.Index([1, 2, 3], dtype="uint64"),
        "timedelta": pandas.timedelta_range(
            start="1 day", periods=INDEX_SAMPLE_SIZE, freq="D"
        ),
        "multi-with-dt64tz-level": create_multiindex_with_dt64tz_level(),
        # NumericIndex is a pandas 2.x feature
        "num_int32": pandas.Index(np.arange(INDEX_SAMPLE_SIZE), dtype="int32"),
        "num_int16": pandas.Index(np.arange(INDEX_SAMPLE_SIZE), dtype="int16"),
        "num_int8": pandas.Index(np.arange(INDEX_SAMPLE_SIZE)).astype("int8"),
        "num_uint64": pandas.Index(np.arange(INDEX_SAMPLE_SIZE), dtype="uint64"),
        "num_uint32": pandas.Index(np.arange(INDEX_SAMPLE_SIZE), dtype="uint32"),
        "num_uint16": pandas.Index(np.arange(INDEX_SAMPLE_SIZE), dtype="uint16"),
        "num_uint8": pandas.Index(np.arange(INDEX_SAMPLE_SIZE)).astype("uint8"),
        "num_float32": pandas.Index(np.arange(INDEX_SAMPLE_SIZE), dtype="float32"),
        "categorical": pandas.Index(list("abcde") * 20, dtype="category"),
        "interval": pandas.IntervalIndex.from_breaks(np.linspace(0, 100, num=101)),
        "complex64": pandas.Index(np.arange(100)).astype("complex64"),
        "complex128": pandas.Index(np.arange(100)).astype("complex128"),
        "period": pandas.period_range(
            start=datetime(2000, 1, 1), periods=100, freq="D", name="period[B]"
        ),
        # failed due to no
        "datetime": pandas.DatetimeIndex(
            pandas.bdate_range(
                datetime(2000, 1, 1), periods=INDEX_SAMPLE_SIZE, freq="B"
            )
        ),
        "datetime-tz": pandas.DatetimeIndex(
            pandas.bdate_range(
                datetime(2000, 1, 1), periods=INDEX_SAMPLE_SIZE, freq="B"
            ),
            tz="US/Pacific",
        ),
    }


@pytest.fixture(scope="module", autouse=True)
def session(session):
    session._disable_multiline_queries()
    return session


@pytest.fixture(scope="function")
def test_table_name(session) -> str:
    test_table_name = f"{Utils.random_table_name()}TESTTABLENAME"
    try:
        yield test_table_name
    finally:
        Utils.drop_table(session, test_table_name)


@pytest.fixture(scope="session")
def float_frame() -> pandas.DataFrame:
    """
    Fixture for DataFrame of floats with index of unique strings

    Columns are ['A', 'B', 'C', 'D'].

                       A         B         C         D
    P7GACiRnxd -0.465578 -0.361863  0.886172 -0.053465
    qZKh6afn8n -0.466693 -0.373773  0.266873  1.673901
    tkp0r6Qble  0.148691 -0.059051  0.174817  1.598433
    wP70WOCtv8  0.133045 -0.581994 -0.992240  0.261651
    M2AeYQMnCz -1.207959 -0.185775  0.588206  0.563938
    QEPzyGDYDo -0.381843 -0.758281  0.502575 -0.565053
    r78Jwns6dn -0.653707  0.883127  0.682199  0.206159
    ...              ...       ...       ...       ...
    IHEGx9NO0T -0.277360  0.113021 -1.018314  0.196316
    lPMj8K27FA -1.313667 -0.604776 -1.305618 -0.863999
    qa66YMWQa5  1.110525  0.475310 -0.747865  0.032121
    yOa0ATsmcE -0.431457  0.067094  0.096567 -0.264962
    65znX3uRNG  1.528446  0.160416 -0.109635 -0.032987
    eCOBvKqf3e  0.235281  1.622222  0.781255  0.392871
    xSucinXxuV -1.263557  0.252799 -0.552247  0.400426

    [30 rows x 4 columns]
    """
    return pandas.DataFrame(
        np.random.default_rng(2).standard_normal((30, 4)),
        index=pandas.Index([f"foo_{i}" for i in range(30)], dtype=object),
        columns=pandas.Index(list("ABCD"), dtype=object),
    )


@pytest.fixture(scope="session")
def float_string_frame():
    """
    Fixture for DataFrame of floats and strings with index of unique strings

    Columns are ['A', 'B', 'C', 'D', 'foo'].

                       A         B         C         D  foo
    w3orJvq07g -1.594062 -1.084273 -1.252457  0.356460  bar
    PeukuVdmz2  0.109855 -0.955086 -0.809485  0.409747  bar
    ahp2KvwiM8 -1.533729 -0.142519 -0.154666  1.302623  bar
    3WSJ7BUCGd  2.484964  0.213829  0.034778 -2.327831  bar
    khdAmufk0U -0.193480 -0.743518 -0.077987  0.153646  bar
    LE2DZiFlrE -0.193566 -1.343194 -0.107321  0.959978  bar
    HJXSJhVn7b  0.142590  1.257603 -0.659409 -0.223844  bar
    ...              ...       ...       ...       ...  ...
    9a1Vypttgw -1.316394  1.601354  0.173596  1.213196  bar
    h5d1gVFbEy  0.609475  1.106738 -0.155271  0.294630  bar
    mK9LsTQG92  1.303613  0.857040 -1.019153  0.369468  bar
    oOLksd9gKH  0.558219 -0.134491 -0.289869 -0.951033  bar
    9jgoOjKyHg  0.058270 -0.496110 -0.413212 -0.852659  bar
    jZLDHclHAO  0.096298  1.267510  0.549206 -0.005235  bar
    lR0nxDp1C2 -2.119350 -0.794384  0.544118  0.145849  bar

    [30 rows x 5 columns]
    """
    df = pandas.DataFrame(
        np.random.default_rng(2).standard_normal((30, 4)),
        index=pandas.Index([f"foo_{i}" for i in range(30)], dtype=object),
        columns=pandas.Index(list("ABCD"), dtype=object),
    )
    df["foo"] = "bar"
    return df


@pytest.fixture(scope="session")
def datetime_series(nper=30, freq: Frequency = "B", name=None) -> pandas.Series:
    """
    Fixture for Series of floats with DatetimeIndex
    """
    return pandas.Series(
        np.random.default_rng(2).standard_normal(30),
        index=pandas.date_range(start="2000-01-01", periods=nper, freq=freq),
        name=name,
    )


@pytest.fixture(scope="function")
def iloc_snowpark_pandas_input_map():
    return {
        "categorical[int]": pd.Categorical([1, 3, 4]),
        "Index": pd.Index([-0.9, -1.0, -1.1, 0.0, 1.0, 0.9, 1.1, 1]),
        "Series": pd.Series([-0.9, -1.0, -1.1, 0.0, 1.0, 0.9, 1.1, -1]),
        "Series[positive_int]": pd.Series([0, 1]),
        "Series_all_positive_int": pd.Series([1, 1, 2]),
        "RangeIndex": pd.RangeIndex(1, 4),
        "Index[bool]": pd.Index([True, True, False, False, False, True, True]),
        # In pandas 1.5.x the default type was float, but in 2.x is object
        "emptyFloatSeries": pd.Series(dtype=float),
        "multi_index_Series": pd.Series(
            [2, 2, 4],
            index=pandas.MultiIndex.from_tuples(
                [(1, "A"), (1, "B"), (2, "C")],
                name=["Index1", "Index2"],
            ),
        ),
    }


@pytest.fixture(scope="function")
def negative_iloc_snowpark_pandas_input_map():
    return {
        "dataframe": (pd.DataFrame(), pandas.DataFrame()),
    }


@pytest.fixture(scope="function")
def loc_snowpark_pandas_input_map():
    return {
        "empty_series": (pd.Series(), pandas.Series()),
        "multi_index_series_row": (
            pd.Series(
                ["c", "a", "a", "b", "a"],
                index=pd.MultiIndex.from_tuples(
                    [(1, "A"), (1, "B"), (2, "A"), (2, "C"), (2, "B")],
                    name=["Index1", "Index2"],
                ),
            ),
            pandas.Series(
                ["c", "a", "a", "b", "a"],
                index=pandas.MultiIndex.from_tuples(
                    [(1, "A"), (1, "B"), (2, "A"), (2, "C"), (2, "B")],
                    name=["Index1", "Index2"],
                ),
            ),
        ),
        "multi_index_series_col": (
            pd.Series(
                ["C", "A", "A", "B", "A"],
                index=pd.MultiIndex.from_tuples(
                    [(1, "A"), (1, "B"), (2, "A"), (2, "C"), (2, "B")],
                    name=["Index1", "Index2"],
                ),
            ),
            pandas.Series(
                ["C", "A", "A", "B", "A"],
                index=pandas.MultiIndex.from_tuples(
                    [(1, "A"), (1, "B"), (2, "A"), (2, "C"), (2, "B")],
                    name=["Index1", "Index2"],
                ),
            ),
        ),
        "series[label]_col": (
            pd.Series(["C", "A", "A", "B", "A"]),
            pandas.Series(["C", "A", "A", "B", "A"]),
        ),
        "series[bool]_col": (
            pd.Series(
                [True, True, True, False, False, True, True],
                index=["A", "B", "x", "D", "F", "E", "C"],
            ),
            pandas.Series(
                [True, True, True, False, False, True, True],
                index=pandas.Index(["A", "B", "x", "D", "F", "E", "C"]),
            ),
        ),
        "int_float": (
            pd.Series(
                [
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                ],
                index=[0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
            ),
            pandas.Series(
                [
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                ],
                index=pandas.Index([0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0]),
            ),
        ),
    }


@pytest.fixture(scope="function")
def negative_loc_snowpark_pandas_input_map():
    return {
        "dataframe": (pd.DataFrame(), pandas.DataFrame()),
        "series[bool]_less": (
            pd.Series(
                [
                    True,
                    True,
                    False,
                    False,
                    False,
                    True,
                ],
                index=pandas.Index(["a", "b", "c", "d", "e", "g"]),
            ),
            pandas.Series(
                [
                    True,
                    True,
                    False,
                    False,
                    False,
                    True,
                ],
                index=pandas.Index(["a", "b", "c", "d", "e", "g"]),
            ),
        ),
        "series[bool]_empty_index": (
            pd.Series(
                [
                    True,
                ]
            ),
            pandas.Series(
                [
                    True,
                ]
            ),
        ),
        "series[bool]_empty": (
            pd.Series([], dtype=bool),
            pandas.Series([], dtype=bool),
        ),
        "int_float": (
            pd.Series(
                [
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                ],
                index=pandas.Index([0.9, 1.1, 2.0, 3.0, 4.0, 5.0, 6.0]),
            ),
            pandas.Series(
                [
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                ],
                index=pandas.Index([0.9, 1.1, 2.0, 3.0, 4.0, 5.0, 6.0]),
            ),
        ),
        "int_string": (
            pd.Series(
                [
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                ],
                index=pandas.Index(["0", "1", "2", "3", "4", "5", "6"]),
            ),
            pandas.Series(
                [
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                ],
                index=pandas.Index(["0", "1", "2", "3", "4", "5", "6"]),
            ),
        ),
        "series[bool]_date_index": (
            pd.Series(
                [
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                ],
                index=pd.date_range("2023-01-01", periods=7, freq="D"),
            ),
            pandas.Series(
                [
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                    True,
                ],
                index=pd.date_range("2023-01-01", periods=7, freq="D"),
            ),
        ),
    }


@pytest.fixture(scope="function")
def negative_loc_diff2native_snowpark_pandas_input_map():
    return {
        "not_in_index_series[str]": (
            pd.Series(["c", "a", "a", "y", "y"]),
            KeyError,
            r"None of \[\'y\', \'y\'\] are in the index",
        ),
        "series[bool]_dup": (
            pd.Series(
                [
                    True,
                    True,
                    False,
                    False,
                    False,
                    True,
                    True,
                ],
                index=pandas.Index(
                    [
                        "a",
                        "b",
                        "a",
                        "d",
                        "f",
                        "e",
                        "c",
                    ]
                ),
            ),
            IndexingError,
            re.escape(
                "Unalignable boolean Series provided as indexer (index of the boolean Series and of the indexed object do not match)."
            ),
        ),
        # This test raises the same error as pandas 2.0.0 when key index that is not in df index, e.g. `x` has
        # duplicates, but pandas 1.5.3 wil not raise such error. We decided to follow 2.0.0 because 1) it seems like
        # an edge case 1.5.3 forgot about since 1.5.3 will also raise such error msg if key index that is in df index
        # e.g. `a` has duplicates. 2) This is a rare edge case that adding additional check will slow down other cases.
        "series[bool]_dup_out_bound": (
            pd.Series(
                [True, True, False, False, False, True, True, True],
                index=pandas.Index(["a", "b", "x", "d", "f", "e", "c", "x"]),
            ),
            IndexingError,
            re.escape(
                "Unalignable boolean Series provided as indexer (index of the boolean Series and of the indexed object do not match)."
            ),
        ),
    }


@pytest.fixture(scope="function")
def iloc_setitem_snowpark_pandas_pair_map():
    return {
        "series": (
            (
                pd.Series([3, 1, 2]),
                pd.DataFrame([["991"] * 7, ["992"] * 7, ["993"] * 7]),
            ),
            (
                pandas.Series([3, 1, 2]),
                pandas.DataFrame([["991"] * 7, ["992"] * 7, ["993"] * 7]),
            ),
        ),
        "series_broadcast": (
            (pd.Series([3, 1, 2]), pd.Series(["991"] * 7)),
            (pandas.Series([3, 1, 2]), pandas.Series(["991"] * 7)),
        ),
        "df_broadcast": (
            (pd.Series([3, 1, 2]), pd.DataFrame([["991"] * 7])),
            (pandas.Series([3, 1, 2]), pandas.DataFrame([["991"] * 7])),
        ),
    }


@pytest.fixture(scope="function")
def negative_iloc_setitem_snowpark_pandas_pair_map():
    return {
        "7x1": (
            pd.Series([3, 1, 2]),
            pd.DataFrame([["991"]] * 7),
            ValueError,
            re.escape(
                "shape mismatch: value array of shape (7, 1) could not be broadcast to indexing result of shape (3, 7)"
            ),
        ),
        "1x1": (
            pd.Series([3, 1, 2]),
            pd.Series(["991"]),
            ValueError,
            "cannot set using a list-like indexer with a different length than the value",
        ),
        "7x3": (
            pd.Series([3, 1, 2]),
            pd.DataFrame([["991"] * 3 for _ in range(7)]),
            ValueError,
            re.escape(
                "shape mismatch: value array of shape (7, 3) could not be broadcast to indexing result of shape (3, 7)"
            ),
        ),
    }


IRIS_FILE_PATH = pathlib.Path(__file__).parent.parent.parent / "resources" / "iris.csv"
IRIS_DF = pandas.read_csv(IRIS_FILE_PATH)


@pytest.fixture(scope="function")
def numeric_test_data_4x4():
    return {
        "A": [5, 8, 11, 14],
        "B": [6, 9, 12, 15],
        "C": [7, 10, 13, 16],
        "D": [8, 11, 14, 17],
    }


@pytest.fixture
def timedelta_native_df() -> pandas.DataFrame:
    return pandas.DataFrame(
        {
            "A": [
                pd.Timedelta(days=1),
                pd.Timedelta(days=2),
                pd.Timedelta(days=3),
                pd.Timedelta(days=4),
            ],
            "B": [
                pd.Timedelta(minutes=-1),
                pd.Timedelta(minutes=0),
                pd.Timedelta(minutes=5),
                pd.Timedelta(minutes=6),
            ],
            "C": [
                None,
                pd.Timedelta(nanoseconds=5),
                pd.Timedelta(nanoseconds=0),
                pd.Timedelta(nanoseconds=4),
            ],
            "D": pandas.to_timedelta([pd.NaT] * 4),
        }
    )


@pytest.fixture
def testing_dfs_from_read_snowflake(
    request, session, test_table_name
) -> Generator[tuple[pd.DataFrame, pandas.DataFrame], None, None]:
    pandas_df = request.param
    session.write_pandas(
        df=pandas_df,
        table_name=test_table_name,
        overwrite=True,
        table_type="temp",
        auto_create_table=True,
    )
    return pd.read_snowflake(test_table_name), pandas_df
