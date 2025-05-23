#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import gzip
import os
import tempfile
from typing import Any, List, Tuple

import modin.pandas as pd
import pandas as native_pd
import pytest
from numpy.testing import assert_equal

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker
from tests.utils import Utils

temp_dir = tempfile.TemporaryDirectory()
TEMP_DIR_NAME = temp_dir.name


SERIES_TEST_CASES = [
    ({}, False),
    ({"header": True}, False),
    ({"header": False}, False),
    ({"index": True}, False),
    ({"index": False}, False),
    ({"index_label": "INDEX"}, False),
    ({"sep": "|"}, False),
    ({"na_rep": "NULL_VALUE"}, False),
    ({"compression": "infer", "ext": "gz"}, True),
    ({"compression": "infer", "ext": "csv"}, False),
    ({"compression": "infer", "ext": None}, False),
    ({"compression": None}, False),
    ({"compression": "gzip"}, True),
    ({"compression": {"method": "gzip"}}, True),
    ({"chunksize": 10}, False),
]


DATAFRAME_TEST_CASES = SERIES_TEST_CASES + [
    ({"columns": ["B"]}, False),
    ({"columns": ["B", "A"]}, False),
]


@pytest.fixture
def sf_stage(session) -> str:
    stage_name = Utils.random_stage_name()
    Utils.create_stage(session, stage_name, is_temporary=True)
    return stage_name


def assert_file_equal(
    path_actual: str, path_expected: str, is_compressed: bool
) -> None:
    """
    Args:
        path_actual: Path to file with actual output.
        path_expected: Path to file with expected output.
        is_compressed: True if outfile files are gzip compressed.
    """

    def read_file_content(path: str) -> List[str]:
        if is_compressed:
            with gzip.open(path) as f:
                return f.read().decode().splitlines()
        else:
            with open(path) as f:
                return f.readlines()

    assert_equal(read_file_content(path_actual), read_file_content(path_expected))


def get_filepaths(kwargs: Any, test_name: str) -> Tuple[str, str]:
    ext = kwargs.get("ext", "csv")
    if ext:
        native_path = os.path.join(TEMP_DIR_NAME, f"native_{test_name}.{ext}")
        snow_path = os.path.join(TEMP_DIR_NAME, f"snow_{test_name}.{ext}")
    else:
        native_path = os.path.join(TEMP_DIR_NAME, f"native_{test_name}")
        snow_path = os.path.join(TEMP_DIR_NAME, f"snow_{test_name}")
    # Remove files if exits.
    if os.path.exists(native_path):
        os.remove(native_path)
    if os.path.exists(snow_path):
        os.remove(snow_path)
    return native_path, snow_path


@pytest.mark.parametrize("kwargs, is_compressed", SERIES_TEST_CASES)
@sql_count_checker(query_count=1)
def test_to_csv_series_local(kwargs, is_compressed):
    native_series = native_pd.Series(["one", None, "", "two"], name="A")
    native_path, snow_path = get_filepaths(kwargs, "series_local")

    kwargs = kwargs.copy()
    kwargs.pop("ext", None)
    # Write csv with native pandas.
    native_series.to_csv(native_path, **kwargs)
    # Write csv with snowpark pandas.
    pd.Series(native_series).to_csv(snow_path, **kwargs)

    assert_file_equal(snow_path, native_path, is_compressed)


@pytest.mark.parametrize("kwargs, is_compressed", DATAFRAME_TEST_CASES)
@sql_count_checker(query_count=1)
def test_to_csv_dataframe_local(kwargs, is_compressed):
    native_df = native_pd.DataFrame({"A": ["one", "", "two", None], "B": [4, 1, 2, 3]})
    native_path, snow_path = get_filepaths(kwargs, "dataframe_local")

    kwargs = kwargs.copy()
    kwargs.pop("ext", None)
    # Write csv with native pandas.
    native_df.to_csv(native_path, **kwargs)
    # Write csv with snowpark pandas.
    pd.DataFrame(native_df).to_csv(snow_path, **kwargs)

    assert_file_equal(snow_path, native_path, is_compressed)


@pytest.mark.parametrize("kwargs, is_compressed", SERIES_TEST_CASES)
@sql_count_checker(query_count=2)
def test_to_csv_series_stage(sf_stage, session, kwargs, is_compressed):
    native_series = native_pd.Series(["one", "", None, "two"], name="A")
    # None index name is not supported when writing to snowflake stage.
    native_series.index.name = "X"
    native_path, snow_path = get_filepaths(kwargs, "series_stage")

    kwargs = kwargs.copy()
    kwargs.pop("ext", None)
    # Write csv with native pandas.
    native_series.to_csv(native_path, **kwargs)
    # Write csv to snowflake stage.

    stage_location = f"@{sf_stage}/{os.path.basename(snow_path)}"
    if kwargs.get("index", True) is False:
        # Writing single column with null values has different behavior in snowpark
        # pandas as compared to native pandas. Null values are written as empty string
        # in snowpark pandas but in native pandas null values are written as a pair of
        # double quotes "". Add na_rep param to match behavior in test.
        # Bug filed in pandas: https://github.com/pandas-dev/pandas/issues/59116
        pytest.skip(
            "Snowpark pandas and native pandas behave differently in this case."
        )
    pd.Series(native_series).to_csv(stage_location, **kwargs)

    # Download csv file from stage.
    session.file.get(stage_location, TEMP_DIR_NAME)
    # Compare content.
    assert_file_equal(snow_path, native_path, is_compressed)


@pytest.mark.parametrize("kwargs, is_compressed", DATAFRAME_TEST_CASES)
@sql_count_checker(query_count=2)
def test_to_csv_dataframe_stage(sf_stage, session, kwargs, is_compressed):
    native_df = native_pd.DataFrame({"A": ["one", "two", None, ""], "B": [1, 2, 3, 0]})
    # None index name is not supported when writing to snowflake stage.
    native_df.index.set_names(["X"], inplace=True)
    native_path, snow_path = get_filepaths(kwargs, "dataframe_stage")

    kwargs = kwargs.copy()
    kwargs.pop("ext", None)
    # Write csv with native pandas.
    native_df.to_csv(native_path, **kwargs)
    # Write csv to snowflake stage.
    stage_location = f"@{sf_stage}/{os.path.basename(snow_path)}"
    pd.DataFrame(native_df).to_csv(stage_location, **kwargs)

    # Download csv file from stage.
    session.file.get(stage_location, TEMP_DIR_NAME)
    # Compare content.
    assert_file_equal(snow_path, native_path, is_compressed)


@sql_count_checker(query_count=1)
def test_to_csv_none_path():
    native_df = native_pd.DataFrame({"A": ["one", "", "two", None], "B": [1, 2, 3, 5]})
    # to_csv with None path_or_buf returns csv representation as string.
    native_result = native_df.to_csv(path_or_buf=None)
    snow_result = pd.DataFrame(native_df).to_csv(path_or_buf=None)
    assert_equal(snow_result, native_result)


@sql_count_checker(query_count=0)
def test_to_csv_unknown_compression(sf_stage, session):
    native_df = native_pd.DataFrame({"A": ["one", "", "two", None], "B": [1, 4, 2, 3]})
    kwargs = {"compression": "piedpiper"}
    native_path, snow_path = get_filepaths(kwargs, "unknown_compression")

    # Write csv with native  pandas.
    msg = "Unrecognized compression type: piedpiper"
    with pytest.raises(ValueError, match=msg):
        native_df.to_csv(native_path, **kwargs)

    # Write csv to snowflake stage.
    stage_location = f"@{sf_stage}/{os.path.basename(snow_path)}"
    with pytest.raises(ValueError, match=msg):
        pd.DataFrame(native_df).to_csv(stage_location, **kwargs)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"float_format": None},
        {"mode": "w"},
        {"encoding": None},
        {"quoting": None},
        {"quotechar": '"'},
        {"lineterminator": None},
        {"doublequote": True},
        {"decimal": "."},
    ],
)
@sql_count_checker(query_count=2)
def test_to_csv_unsupported_params_default_value(sf_stage, session, kwargs):
    native_df = native_pd.DataFrame({"A": ["one", "", "two", None], "B": [1, 2, 5, 3]})
    # None index name is not supported when writing to snowflake stage.
    native_df.index.set_names(["X"], inplace=True)
    native_path, snow_path = get_filepaths(kwargs, "unsupported_params_default_value")

    # Write csv with native pandas.
    native_df.to_csv(native_path, **kwargs)
    # Write csv to snowflake stage.
    stage_location = f"@{sf_stage}/{os.path.basename(snow_path)}"
    pd.DataFrame(native_df).to_csv(stage_location, **kwargs)

    # Download csv file from stage.
    session.file.get(stage_location, TEMP_DIR_NAME)
    # Compare content.
    assert_file_equal(snow_path, native_path, False)


@pytest.mark.parametrize(
    "kwargs",
    [
        {"float_format": "2d"},
        {"mode": "a"},
        {"encoding": "utf-16"},
        {"quoting": "nonnumeric"},
        {"quotechar": "|"},
        {"lineterminator": "\r"},
        {"doublequote": False},
        {"decimal": ","},
    ],
)
@sql_count_checker(query_count=0)
def test_to_csv_unsupported_params_error(sf_stage, session, kwargs):
    native_df = native_pd.DataFrame({"A": ["one", "", "two", None], "B": [9, 1, 2, 3]})
    # None index name is not supported when writing to snowflake stage.
    native_df.index.set_names(["X"], inplace=True)
    native_path, snow_path = get_filepaths(kwargs, "unsupported_params_default_value")

    stage_location = f"@{sf_stage}/{os.path.basename(snow_path)}"
    msg = f"Snowpark pandas method to_csv does not yet support the '{list(kwargs.keys())[0]}' parameter"
    with pytest.raises(NotImplementedError, match=msg):
        # Write csv to snowflake stage.
        pd.DataFrame(native_df).to_csv(stage_location, **kwargs)


@sql_count_checker(query_count=1)
def test_timedelta_to_csv_series_local():
    native_series = native_pd.Series(
        native_pd.timedelta_range("1 day", periods=3), name="A"
    )
    native_path, snow_path = get_filepaths(kwargs={}, test_name="series_local")

    # Write csv with native pandas.
    native_series.to_csv(native_path)
    # Write csv with snowpark pandas.
    pd.Series(native_series).to_csv(snow_path)

    assert_file_equal(snow_path, native_path, is_compressed=False)


@sql_count_checker(query_count=1)
def test_timedeltaindex_to_csv_dataframe_local():
    native_df = native_pd.DataFrame(
        {
            "A": native_pd.to_timedelta(["1 days 06:05:01.00003", "15.5us", "nan"]),
            "B": [10, 8, 12],
            "C": ["bond", "james", "bond"],
        }
    )
    native_df = native_df.groupby("A").min()
    native_path, snow_path = get_filepaths(kwargs={}, test_name="series_local")

    # Write csv with native pandas.
    native_df.to_csv(native_path)
    # Write csv with snowpark pandas.
    pd.DataFrame(native_df).to_csv(snow_path)

    assert_file_equal(snow_path, native_path, is_compressed=False)
