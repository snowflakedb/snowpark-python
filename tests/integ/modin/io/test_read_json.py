#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
import io
import json
import logging
import os
import tempfile
from typing import Any, Union

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import Utils

# Note: read_json operations have a high query count.
# if the file is local, an additional query is needed to upload it to a stage.
# 6 queries are needed to get the metadata required to create a Snowpark dataframe.
# 2 queries is needed to create the Snowpandas dataframe.

tmp_stage_name1 = Utils.random_stage_name()

temp_dir = tempfile.TemporaryDirectory()
TEMP_DIR_NAME = temp_dir.name

TEST_JSON_FILE_1 = "test_read_json.json"
TEST_JSON_FILE_2 = "test_read_json_2.json"


def write_ndjson_file(
    filepath: str, jsondata: Union[list[dict[str, Any]], dict[str, Any]]
) -> None:
    """
    Writes a list of Python dictionaries to newline-delimited json (NDJSON) file.

    Parameters
    ----------
    filepath : str
    File to write to. If the file does not exist, it will be created.

    jsondata : list of dictionaries.
    Dictionaries to write into ndjson format.

    Raises
    ------
    ValueError if jsondata is not a list of dictionaries or a single dictionary.

    Examples
    --------
    List of dictionaries:
    data = [{"a": 1, "b": 3}, {"b": 2, "c": 3}]
    write_ndjson_file("snowpark_pandas.json", data)

    snowpark_pandas.json:
    {"a": 1, "b": 3}\n{"b": 2, "c": 3}

    Single dictionary:
    data = {"a": 1, "b": 3}
    write_ndjson_file("snowpark_pandas2.json", data)

    snowpark_pandas2.json
    {"a": 1, "b": 3}
    """
    if isinstance(jsondata, dict):
        jsondata = [jsondata]

    if not isinstance(jsondata, list):
        raise ValueError("jsondata must be a list")

    with open(filepath, "w") as f:
        for d in jsondata:
            json.dump(d, f)
            f.write("\n")


def sf_read_ndjson_data(jsondata: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Returns a list of dictionaries `l` with the following properties:
    1. len(`l`) == len(`jsondata`).
    1. `l`[i]'s key are the UNION of all keys of each dictionary in `jsondata`.
    2. if `jsondata`[i] is missing a key `k`, `l`[i][k] is None.

    Parameters
    ----------
    jsondata : List[Dict[str, Any]
    List of dictionaries.

    Returns
    -------
    List[Dict[str, Any]
    List of dictionaries with the properties as listed above.


    Examples
    --------
    Subset of keys: fill missing keys with None.
    data = [{"a": 1, "b": 3}, {"b": 2, "c": 3}]
    output: [{"a": 1, "b": 3, "c": None}, {"a": None, "b": 2, "c": 3}]

    No overlap in keys:
    data = [{"a": 1, "b": 3}, {"c": 2, "d": 3}, {"e": 2, "f": 3}]
    output: [
        {"a": 1, "b": 3, "c": None, "d": None, "e": None},
        {"a": None, "b": None, "c": 2, "d": 3, "e": None, "f": None},
        {"a": None, "b": None, "c": None, "d": None, "e": 2, "f": 3}
    ]
    """

    all_keys = set()
    for dict_ in jsondata:
        for key in dict_:
            all_keys.add(key)

    outer_joined_data = []
    for dict_ in jsondata:
        copied_dict = copy.deepcopy(dict_)

        for key in all_keys:
            if key not in copied_dict:
                copied_dict[key] = None

        outer_joined_data.append(copied_dict)

    return outer_joined_data


@pytest.fixture(scope="module", autouse=True)
def setup(session, json_data):
    Utils.create_stage(session, tmp_stage_name1, is_temporary=True)

    test_files = [TEST_JSON_FILE_1, TEST_JSON_FILE_2]

    for file_name in test_files:

        with open(f"{TEMP_DIR_NAME}/{file_name}", "w") as f:
            json.dump(json_data, f)

        Utils.upload_to_stage(
            session,
            "@" + tmp_stage_name1,
            f"{TEMP_DIR_NAME}/{file_name}",
            compress=False,
        )

    yield
    # tear down the resources after yield (pytest fixture feature)
    # https://docs.pytest.org/en/6.2.x/fixture.html#yield-fixtures-recommended
    session.sql(f"DROP STAGE IF EXISTS {tmp_stage_name1}").collect()
    temp_dir.cleanup()


@pytest.fixture(scope="module")
def json_data():
    return {"A": "snowpark!", "B": 3, "C": np.float64(12.33)}


def test_read_json_basic(json_data):

    with SqlCounter(query_count=9):
        df = pd.read_json(f"{TEMP_DIR_NAME}/{TEST_JSON_FILE_1}")

    expected = native_pd.DataFrame(json_data, index=[0])

    assert_frame_equal(df, expected, check_dtype=False)


def test_read_json_single_ndjson_file():

    data = [
        {
            "a": 1,
            "b": "string!",
            "c": [1, 2],
            "d": {"key": "value", "key2": 2},
            "e": None,
            "f": True,
        },
        {
            "a": 100,
            "b": "string2!",
            "c": [100, 200],
            "d": {"key2": "value2", "key3": 3},
            "e": None,
            "f": False,
        },
    ]
    write_ndjson_file(f"{TEMP_DIR_NAME}/test_read_json_single_ndjson_file.json", data)

    with SqlCounter(query_count=9):
        df = pd.read_json(f"{TEMP_DIR_NAME}/test_read_json_single_ndjson_file.json")

    expected = native_pd.DataFrame(data, index=[0, 1])

    assert_frame_equal(df, expected, check_dtype=False)


# Tests for subset of keys, no overlapping keys, keys with some overlap.
@pytest.mark.parametrize(
    "ndjsondata",
    [
        [{"a": 100}],
        [
            {
                "a_alt": 1,
                "b_alt": "string!",
                "c_alt": [1, 2],
                "d_alt": {"key": "value", "key2": 2},
                "e_alt": None,
                "f_alt": True,
            },
            {
                "a_alt": 2,
                "b_alt": "string2!",
                "c_alt": [100, 200],
                "d_alt": {"key3": "value2", "key4": 200},
                "e_alt": None,
                "f_alt": True,
            },
        ],
        [
            {
                "a": 101,
                "b": "str1",
                "new_key1": {"a_alt": 1, "b_alt": "string!", "c_alt": True},
            }
        ],
    ],
)
def test_read_json_ndjson_different_keys(ndjsondata):
    original_data = [
        {
            "a": 1,
            "b": "string!",
            "c": [1, 2],
            "d": {"key": "value", "key2": 2},
            "e": None,
            "f": True,
        },
    ]

    data = original_data + ndjsondata

    write_ndjson_file(
        f"{TEMP_DIR_NAME}/test_read_json_single_ndjson_different_keys.json", data
    )

    with SqlCounter(query_count=9):
        snow_df = pd.read_json(
            f"{TEMP_DIR_NAME}/test_read_json_single_ndjson_different_keys.json"
        )

    expected_data = sf_read_ndjson_data(data)
    expected = native_pd.DataFrame(
        expected_data, index=[i for i in range(len(expected_data))]
    )

    # We set the order of the expected DataFrame using the order of the Snowpark pandas
    # dataframe since the order is undeterministic.
    expected_df = expected.reindex(snow_df.columns, axis=1)

    assert_frame_equal(snow_df, expected_df, check_dtype=False)


def test_read_json_staged_file(json_data):
    with SqlCounter(query_count=8):
        snow_df = pd.read_json(f"@{tmp_stage_name1}/{TEST_JSON_FILE_1}")

    expected = native_pd.DataFrame(json_data, index=[0])

    assert_frame_equal(snow_df, expected, check_dtype=False)


def test_read_json_staged_folder():

    with SqlCounter(query_count=8):
        snow_df = pd.read_json(f"@{tmp_stage_name1}")

    expected = native_pd.DataFrame(
        {
            "A": ["snowpark!", "snowpark!"],
            "B": [3, 3],
            "C": [np.float64(12.33), np.float64(12.33)],
        },
        index=[0, 1],
    )
    assert_frame_equal(snow_df, expected, check_dtype=False)


@sql_count_checker(query_count=5)
@pytest.mark.xfail(
    reason="SNOW-1336174: Remove xfail by handling empty JSON files", strict=True
)
def test_read_json_empty_file():
    with open("test_read_json_empty_file.json", "w"):
        pass
    try:
        snow_df = pd.read_json("test_read_json_empty_file.json")
        assert len(snow_df) == 0
    finally:
        os.remove("test_read_json_empty_file.json")


@sql_count_checker(query_count=3)
def test_read_json_malformed_file_negative():

    with open("test_read_json_malformed_file.json", "w") as f:
        f.write("{a: 3, key_no_value}")

    with pytest.raises(AssertionError):
        pd.read_json("test_read_json_malformed_file.json")

    os.remove("test_read_json_malformed_file.json")


@pytest.mark.parametrize(
    "parameter, argument",
    [
        ("orient", "records"),
        ("typ", "series"),
        ("dtype", True),
        ("convert_axes", True),
        ("convert_dates", True),
        ("keep_default_dates", True),
        ("precise_float", True),
        ("date_unit", "s"),
        ("encoding_errors", "ignore"),
        ("lines", True),
        ("chunksize", 100),
        ("nrows", 10),
    ],
)
@sql_count_checker(query_count=0)
def test_read_json_unimplemented_parameter_negative(parameter, argument):
    with pytest.raises(NotImplementedError, match=f"{parameter} is not implemented."):
        pd.read_json("file.json", **{parameter: argument})


@pytest.mark.parametrize(
    "parameter, argument", [("storage_options", "random_option"), ("engine", "ujson")]
)
@sql_count_checker(
    query_count=10,
    high_count_expected=True,
    high_count_reason="Expected high count read_json",
)
def test_read_json_warning(caplog, parameter, argument, json_data):
    warning_msg = f"The argument `{parameter}` of `pd.read_json` has been ignored by Snowpark pandas API"

    caplog.clear()

    with caplog.at_level(logging.WARNING):
        snow_df = pd.read_json(
            f"{TEMP_DIR_NAME}/{TEST_JSON_FILE_1}", **{parameter: argument}
        )

    assert warning_msg in caplog.text

    expected = native_pd.DataFrame(json_data, index=[0])
    assert_frame_equal(snow_df, expected, check_dtype=False)


@sql_count_checker(query_count=0)
def test_read_json_filepath_negative(json_data):
    buffer = io.StringIO()
    json.dump(json_data, buffer)

    with pytest.raises(
        NotImplementedError,
        match="'path' must be a path to a file or folder stored locally or on a Snowflake stage.",
    ):
        pd.read_json(buffer)
