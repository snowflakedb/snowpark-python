#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime
import os
import uuid
from io import StringIO

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.sql_counter import SqlCounter, sql_count_checker
from tests.integ.modin.utils import assert_frame_equal
from tests.utils import IS_WINDOWS, TestFiles, Utils

tmp_stage_name1 = Utils.random_stage_name()
test_file_csv = "testCSV.csv"


# these tests have high query_counts since
# ...


@pytest.fixture(scope="module", autouse=True)
def setup(session, resources_path):
    test_files = TestFiles(resources_path)
    Utils.create_stage(session, tmp_stage_name1, is_temporary=True)
    Utils.upload_to_stage(
        session, "@" + tmp_stage_name1, test_files.test_file_csv, compress=False
    )

    yield
    # tear down the resources after yield (pytest fixture feature)
    # https://docs.pytest.org/en/6.2.x/fixture.html#yield-fixtures-recommended
    session.sql(f"DROP STAGE IF EXISTS {tmp_stage_name1}").collect()


@sql_count_checker(query_count=9)
def test_read_csv():
    df = native_pd.DataFrame({"c1": [1, 2], "c2": ["qwe", 3], "c3": [4, 5]})
    filename = f"test_read_csv_{str(uuid.uuid4())}"
    try:
        df.to_csv(filename, index=False)
        assert_frame_equal(
            pd.read_csv(filename),
            native_pd.read_csv(filename),
            check_dtype=False,
        )
    finally:
        if os.path.exists(filename):
            os.remove(filename)


def test_read_csv_header(resources_path):
    test_files = TestFiles(resources_path)

    filename = test_files.test_file_csv_header

    with SqlCounter(query_count=9):
        assert_frame_equal(
            pd.read_csv(filename, header=None),
            native_pd.read_csv(filename, header=None),
            check_dtype=False,
        )

    with SqlCounter(query_count=9):
        assert_frame_equal(
            pd.read_csv(filename, header=0),
            native_pd.read_csv(filename, header=0),
            check_dtype=False,
        )


@pytest.mark.parametrize("header", [[0, 1, 3], 2])
@sql_count_checker(query_count=0)
def test_read_csv_header_negative(resources_path, header):
    test_files = TestFiles(resources_path)

    with pytest.raises(NotImplementedError, match="header is not implemented."):
        pd.read_csv(test_files.test_file_csv, header=header)


@sql_count_checker(query_count=0)
def test_read_csv_header_skiprows_negative(resources_path):
    test_files = TestFiles(resources_path)

    with pytest.raises(NotImplementedError, match="header is not implemented."):
        pd.read_csv(test_files.test_file_csv, header=1, skiprows=1)


@pytest.mark.parametrize(
    "names",
    [
        ["c1", "c2", "c3"],
        [1, "2", 3],
        [1, 2, 3],
        ["long test name", ("tuple_val", 3), 190],
        ("c1", "c2", "c3"),
        native_pd.Series(["c1", "c2", "c3"]),
        native_pd.Index(["c1", "c2", "c3"]),
        ["c1", "c2"],
        ["UPPER_CASE_NAME", "UPPER_CASE_NAME_2", "UPPER_CASE_NAME_3"],
        [
            'UPPER_"QUOTED_NAME"',
            "UPPER_CASE_NAME WHITESPACE",
            'UPPER_CASE_" QUOTED   WHITESPACE"  ',
        ],
    ],
)
@sql_count_checker(query_count=9)
def test_read_csv_names(resources_path, names):
    test_files = TestFiles(resources_path)

    expected = native_pd.read_csv(test_files.test_file_csv, names=names)
    got = pd.read_csv(test_files.test_file_csv, names=names)

    assert_frame_equal(expected, got, check_dtype=False, check_index_type=False)


@sql_count_checker(query_count=9)
def test_read_csv_names_overwrite_header(resources_path):
    test_files = TestFiles(resources_path)

    expected = native_pd.read_csv(
        test_files.test_file_csv_header, names=["c1", "c2", "c3"], header=0
    )
    got = pd.read_csv(
        test_files.test_file_csv_header, names=["c1", "c2", "c3"], header=0
    )

    assert_frame_equal(expected, got, check_dtype=False, check_index_type=False)


@pytest.mark.parametrize(
    "names, error_msg, expected_query_count",
    [
        (
            ("c1", "c2", "c3", "c4"),
            "Too many columns specified: expected 4 and found 3",
            8,
        ),
        (
            native_pd.Series(["c1", "c2", "c3", "c4"]),
            "Too many columns specified: expected 4 and found 3",
            8,
        ),
        (
            native_pd.Index(["c2", "c3", "c4", "c5"]),
            "Too many columns specified: expected 4 and found 3",
            8,
        ),
        (["c1", "c1"], "Duplicate names are not allowed.", 0),
        (native_pd.Index(["c1", "c1"]), "Duplicate names are not allowed.", 0),
        (native_pd.Series(["c1", "c1"]), "Duplicate names are not allowed.", 0),
        (["c1", "c2", "c2"], "Duplicate names are not allowed.", 0),
    ],
)
def test_read_csv_name_negative(resources_path, names, error_msg, expected_query_count):
    test_files = TestFiles(resources_path)

    with SqlCounter(query_count=expected_query_count):
        with pytest.raises(ValueError, match=error_msg):
            pd.read_csv(test_files.test_file_csv_header, names=names)


@sql_count_checker(query_count=0)
def test_read_csv_name_invalid_type_negative(resources_path):
    test_files = TestFiles(resources_path)

    names = [1, [2, 3], 4]

    with pytest.raises(TypeError, match="unhashable type: 'list'"):
        pd.read_csv(test_files.test_file_csv_header, names=names)


@sql_count_checker(query_count=9)
def test_read_csv_diff_dataypes():

    df = native_pd.DataFrame(
        [
            ("c1", "c2", "c3", "c4"),
            (
                [datetime.date(2023, 1, 1), None],
                str,
                "str",
                native_pd.Series(["2023-01-01 00:00:00", "NaT"]),
            ),
            (
                [datetime.date(2023, 1, 1), None],
                type,
                "str",
                native_pd.Series(
                    [
                        "<class 'pandas._libs.tslibs.timestamps.Timestamp'>",
                        "<class 'pandas._libs.tslibs.nattype.NaTType'>",
                    ]
                ),
            ),
            # data in TIME column will be encoded as pd.Timedelta (instead of pd.Timestamp!),
            # and NULL will be encoded as pd.NaT in vectorized udf
            (
                [datetime.time(1, 2, 3), None],
                str,
                "str",
                native_pd.Series(["0 days 01:02:03", "NaT"]),
            ),
            (
                [datetime.time(1, 2, 3), None],
                type,
                "str",
                native_pd.Series(
                    [
                        "<class 'pandas._libs.tslibs.timedeltas.Timedelta'>",
                        "<class 'pandas._libs.tslibs.nattype.NaTType'>",
                    ]
                ),
            ),
            (
                [datetime.datetime(2023, 1, 1, 1, 2, 3), None],
                str,
                "str",
                native_pd.Series(["2023-01-01 01:02:03", "NaT"]),
            ),
            (
                [datetime.datetime(2023, 1, 1, 1, 2, 3), None],
                type,
                "str",
                native_pd.Series(
                    [
                        "<class 'pandas._libs.tslibs.timestamps.Timestamp'>",
                        "<class 'pandas._libs.tslibs.nattype.NaTType'>",
                    ]
                ),
            ),
            # data in TIMESTAMP_TZ column will be encoded as pd.Timestamp,
            # and NULL will be encoded as None in vectorized udf
            (
                [
                    datetime.datetime(
                        2023, 1, 1, 1, 2, 3, tzinfo=datetime.timezone.utc
                    ),
                    datetime.datetime(2023, 1, 1, 1, 2, 3),
                    None,
                ],
                str,
                "str",
                native_pd.Series(
                    ["2023-01-01 01:02:03+00:00", "2023-01-01 01:02:03-08:00", "None"]
                ),
            ),
            (
                [
                    datetime.datetime(
                        2023, 1, 1, 1, 2, 3, tzinfo=datetime.timezone.utc
                    ),
                    None,
                ],
                type,
                "str",
                native_pd.Series(
                    [
                        "<class 'pandas._libs.tslibs.timestamps.Timestamp'>",
                        "<class 'NoneType'>",
                    ]
                ),
            ),
        ]
    )
    filename = f"test_read_csv_diff_datatypes_{str(uuid.uuid4())}"
    try:
        df.to_csv(filename, index=False)
        assert_frame_equal(
            pd.read_csv(filename),
            native_pd.read_csv(filename),
            check_dtype=False,
        )
    finally:
        if os.path.exists(filename):
            os.remove(filename)


@pytest.mark.skipif(
    IS_WINDOWS,
    reason="files cannot be named with certain reserved characters in Windows",
)
@pytest.mark.parametrize("wildcard", ["*", "?"])
@sql_count_checker(query_count=9)
def test_read_csv_filepath_glob_pattern(wildcard):
    df = native_pd.DataFrame({"c1": [1, 2], "c2": ["qwe", 3], "c3": [4, 5]})
    filename_a = f"test_read_csv_b_filepath_glob_pattern_{str(uuid.uuid4())}"
    filename_b = f"test_read_csv_a_filepath_glob_pattern_{str(uuid.uuid4())}"
    filename_wildcard = (
        f"test_read_csv_{wildcard}_filepath_glob_pattern_{str(uuid.uuid4())}"
    )

    try:
        df.to_csv(filename_a, index=False)
        df.to_csv(filename_b, index=False)
        df.to_csv(filename_wildcard, index=False)
        assert_frame_equal(
            pd.read_csv(filename_wildcard),
            native_pd.read_csv(filename_wildcard),
            check_dtype=False,
        )
    finally:
        if os.path.exists(filename_a):
            os.remove(filename_a)

        if os.path.exists(filename_b):
            os.remove(filename_b)

        if os.path.exists(filename_wildcard):
            os.remove(filename_wildcard)


@sql_count_checker(query_count=9)
def test_read_csv_filepath_starting_with_stage_symbol():
    df = native_pd.DataFrame({"c1": [1, 2], "c2": ["qwe", 3], "c3": [4, 5]})
    filename = f"@test_read_csv_backslash_{str(uuid.uuid4())}"

    try:
        df.to_csv(filename, index=False)
        assert_frame_equal(
            pd.read_csv(rf"\{filename}"),
            native_pd.read_csv(filename),
            check_dtype=False,
        )
    finally:
        if os.path.exists(filename):
            os.remove(filename)


@sql_count_checker(query_count=0)
def test_read_csv_filepath_negative():
    with pytest.raises(
        NotImplementedError,
        match="filepath_or_buffer must be a path to a file or folder stored locally or on a Snowflake stage.",
    ):
        pd.read_csv(StringIO("a,b\n1,2"))


@pytest.mark.parametrize(
    "param,arg",
    [
        ("engine", "c"),
        ("cache_dates", True),
        ("infer_datetime_format", True),
        ("chunksize", 1000),
        ("memory_map", True),
        ("storage_options", {}),
        ("low_memory", True),
        ("float_precision", "high"),
        ("dtype_backend", "numpy_nullable"),
    ],
)
@sql_count_checker(query_count=9)
def test_read_csv_with_warning_params(param, arg):

    df = native_pd.DataFrame({"c1": [1, 2], "c2": ["qwe", 3], "c3": [4, 5]})
    filename = f"test_read_csv_with_warning_params_{str(uuid.uuid4())}"
    try:
        df.to_csv(filename, index=False)
        assert_frame_equal(
            pd.read_csv(filename, **{param: arg}),
            native_pd.read_csv(filename),
            check_dtype=False,
        )
    finally:
        if os.path.exists(filename):
            os.remove(filename)


@sql_count_checker(query_count=9)
def test_read_csv_no_sep():
    df = native_pd.DataFrame({"c1": [1, 2], "c2": ["qwe", 3], "c3": [4, 5]})
    filename = f"test_read_csv_no_sep_{str(uuid.uuid4())}"

    try:
        df.to_csv(filename, index=False, sep=",")
        assert_frame_equal(
            pd.read_csv(filename),
            native_pd.read_csv(filename),
            check_dtype=False,
        )
    finally:
        if os.path.exists(filename):
            os.remove(filename)


@sql_count_checker(query_count=9)
def test_read_csv_delimiter():
    df = native_pd.DataFrame({"c1": [1, 2], "c2": ["qwe", 3], "c3": [4, 5]})
    filename = f"test_read_csv_delimiter_{str(uuid.uuid4())}"

    try:
        df.to_csv(filename, index=False, sep=";")
        assert_frame_equal(
            pd.read_csv(filename, delimiter=";"),
            native_pd.read_csv(filename, delimiter=";"),
            check_dtype=False,
        )
    finally:
        if os.path.exists(filename):
            os.remove(filename)


@sql_count_checker(query_count=0)
def test_read_csv_sep_delimiter_negative(resources_path):
    test_files = TestFiles(resources_path)
    with pytest.raises(
        ValueError, match="Specified a sep and a delimiter; you can only specify one."
    ):
        pd.read_csv(test_files.test_file_csv_colon, sep=";", delimiter=";")


@sql_count_checker(query_count=9)
def test_read_csv_misc_parameters(resources_path):
    test_files = TestFiles(resources_path)
    got = pd.read_csv(
        test_files.test_file_csv_colon,
        sep=";",
        encoding="utf-8",
        na_values=("one", "two"),
        compression="infer",
        skiprows=1,
        header=None,
    )
    expected = native_pd.read_csv(
        test_files.test_file_csv_colon,
        sep=";",
        encoding="utf-8",
        compression="infer",
        na_values=("one", "two"),
        skiprows=1,
        header=None,
    )
    assert_frame_equal(got, expected, check_dtype=False)


@sql_count_checker(query_count=8)
def test_read_csv_stage(resources_path):
    got = pd.read_csv(f"@{tmp_stage_name1}/{test_file_csv}")
    test_files = TestFiles(resources_path)
    expected = native_pd.read_csv(test_files.test_file_csv)

    assert_frame_equal(got, expected, check_dtype=False)


@pytest.mark.parametrize(
    "param,arg",
    [
        ("verbose", True),
        ("dayfirst", False),
        ("date_parser", True),
        ("date_format", "%Y-%m-%d"),
        ("keep_date_col", True),
        ("parse_dates", True),
        ("iterator", True),
        ("na_filter", True),
        ("skipfooter", 3),
        ("nrows", 100),
        ("thousands", ","),
        ("decimal", ","),
        ("lineterminator", "q"),
        ("dialect", "excel"),
        ("quoting", 0),
        ("doublequote", True),
        ("encoding_errors", "strict"),
        ("comment", "#"),
        ("converters", {"c1": lambda x: x * 2}),
        ("true_values", ["qwe"]),
        ("false_values", ["qwe"]),
        ("keep_default_na", False),
        ("delim_whitespace", True),
        ("skipinitialspace", True),
        ("on_bad_lines", "skip"),
    ],
)
@sql_count_checker(query_count=0)
def test_read_csv_negative(param, arg):
    with pytest.raises(NotImplementedError, match=f"{param} is not implemented."):
        pd.read_csv("file.csv", **{param: arg})


@pytest.mark.parametrize(
    "usecols",
    [
        ("id", "name", "rating"),
        ("name", "id", "rating"),
        ("rating", "id"),
        lambda x: x.startswith("rat"),
        lambda x: "i" in x,
        [0, 1, 2],
        range(0, 3),
        [2, 1],
        native_pd.Series(["rating", "name"]),
        native_pd.Series([0, 1]),
        native_pd.Index(["rating", "id"]),
    ],
)
@sql_count_checker(query_count=9)
def test_read_csv_usecols(resources_path, usecols):
    test_files = TestFiles(resources_path)

    expected = native_pd.read_csv(test_files.test_file_csv_header, usecols=usecols)
    got = pd.read_csv(test_files.test_file_csv_header, usecols=usecols)

    assert_frame_equal(expected, got, check_dtype=False, check_index_type=False)


@sql_count_checker(query_count=1)
def test_read_csv_usecols_empty(resources_path):
    test_files = TestFiles(resources_path)

    expected = native_pd.read_csv(test_files.test_file_csv_header, usecols=[])
    got = pd.read_csv(test_files.test_file_csv_header, usecols=[])

    assert_frame_equal(expected, got, check_dtype=False, check_index_type=False)


@pytest.mark.parametrize(
    "usecols",
    [
        ("rating"),
        [1, "rating"],
        [1, [2, 3], 4],
        native_pd.MultiIndex.from_arrays([["rating", "id", "name"]]),
        [12.33, np.float64(13.2333), np.double(2.5)],
        [datetime.time(1, 2, 3)],
        [datetime.date(2021, 1, 9), datetime.datetime(2023, 1, 1, 1, 2, 3)],
    ],
)
@sql_count_checker(query_count=0)
def test_read_csv_usecols_invalid_types_negative(resources_path, usecols):
    test_files = TestFiles(resources_path)

    with pytest.raises(
        ValueError,
        match="'usecols' must either be list-like of all strings, all integers or a callable.",
    ):
        pd.read_csv(test_files.test_file_csv, usecols=usecols)


@pytest.mark.parametrize(
    "usecols",
    [["non_existent_col"], ["rating", "non_existent_col"], [-1], [0, 4]],
)
@sql_count_checker(query_count=8)
def test_read_csv_usecols_nonexistent_negative(resources_path, usecols):
    test_files = TestFiles(resources_path)

    with pytest.raises(
        ValueError,
        match="'usecols' do not match columns, columns expected but not found",
    ):
        pd.read_csv(test_files.test_file_csv, usecols=usecols)


@pytest.mark.parametrize(
    "usecols",
    [["c1", "c2"], ["c3"], ["c3", "c2"], [0, 2], [2, 1], [1]],
)
@sql_count_checker(query_count=9)
def test_read_csv_usecols_with_names(resources_path, usecols):
    test_files = TestFiles(resources_path)

    expected = native_pd.read_csv(
        test_files.test_file_csv_header, names=["c1", "c2", "c3"], usecols=usecols
    )
    got = pd.read_csv(
        test_files.test_file_csv_header, names=["c1", "c2", "c3"], usecols=usecols
    )
    assert_frame_equal(expected, got, check_index_type=False)


@pytest.mark.parametrize(
    "usecols",
    [
        ["UPPER_CASE", '"QUOTED    " name  with whitespace '],
        ['"QUOTED NAME with , #" special characters'],
        [
            '"QUOTED NAME with , #" special characters',
            '"QUOTED    " name  with whitespace ',
        ],
        [0, 2],
        [2, 1],
        [1],
    ],
)
@sql_count_checker(query_count=9)
def test_read_csv_usecols_with_special_names(resources_path, usecols):
    test_files = TestFiles(resources_path)
    names = [
        "UPPER_CASE",
        '"QUOTED    " name  with whitespace ',
        '"QUOTED NAME with , #" special characters',
    ]
    expected = native_pd.read_csv(
        test_files.test_file_csv_header, names=names, usecols=usecols
    )
    got = pd.read_csv(test_files.test_file_csv_header, names=names, usecols=usecols)
    assert_frame_equal(expected, got, check_index_type=False)


def test_read_csv_usecols_with_names_negative(resources_path):
    test_files = TestFiles(resources_path)

    with SqlCounter(query_count=8):
        with pytest.raises(
            ValueError,
            match="'usecols' do not match columns, columns expected but not found",
        ):
            pd.read_csv(
                test_files.test_file_csv_header,
                names=["c1", "c2", "c3"],
                usecols=["id"],
            )

    with SqlCounter(query_count=8):
        with pytest.raises(
            ValueError,
            match="'usecols' do not match columns, columns expected but not found",
        ):
            pd.read_csv(test_files.test_file_csv_header, names=["c1"], usecols=[1])


@pytest.mark.parametrize(
    "dtype",
    [
        str,
        {"id": str},
        {"id": float, "rating": str},
        {"rating": np.float64},
        {"id": np.int64},
        {"id": "Int64", "rating": "Float64"},
        {"non_existent_col": int},
        {},
    ],
)
@sql_count_checker(query_count=9)
def test_read_csv_dtype(resources_path, dtype):
    test_files = TestFiles(resources_path)
    expected = native_pd.read_csv(test_files.test_file_csv_header, dtype=dtype)
    got = pd.read_csv(test_files.test_file_csv_header, dtype=dtype)
    assert_frame_equal(expected, got, check_dtype=False)


@pytest.mark.parametrize(
    "dtype,expected_error,expected_error_msg",
    [
        ({"id": [str]}, TypeError, "unhashable type: 'list'"),
        (
            {"rating": "non_existent_type"},
            NotImplementedError,
            "pandas type non_existent_type is not implemented",
        ),
    ],
)
@sql_count_checker(query_count=8)
def test_read_csv_dtype_negative(
    resources_path, dtype, expected_error, expected_error_msg
):

    test_files = TestFiles(resources_path)

    with pytest.raises(expected_error, match=expected_error_msg):
        pd.read_csv(test_files.test_file_csv_header, dtype=dtype).to_pandas()


@sql_count_checker(query_count=9)
def test_read_csv_dtype_usecols(resources_path):
    test_files = TestFiles(resources_path)
    expected = native_pd.read_csv(
        test_files.test_file_csv_header, usecols=["id", "rating"], dtype=np.float64
    )
    got = pd.read_csv(
        test_files.test_file_csv_header, usecols=["id", "rating"], dtype=np.float64
    )
    assert_frame_equal(expected, got, check_dtype=False)


@pytest.mark.parametrize(
    "index_col",
    [
        "id",
        "rating",
        ["id", "rating"],
        ("rating", "id", "name"),
        [],
        1,
        (0, 1, 2),
        [2, 0],
        [-1],
    ],
)
@sql_count_checker(query_count=9)
def test_read_csv_index_col(resources_path, index_col):
    test_files = TestFiles(resources_path)
    expected = native_pd.read_csv(test_files.test_file_csv_header, index_col=index_col)
    got = pd.read_csv(test_files.test_file_csv_header, index_col=index_col)
    assert_frame_equal(expected, got, check_dtype=False, check_index_type=False)


def test_read_csv_index_col_name(resources_path):
    test_files = TestFiles(resources_path)
    expected = native_pd.read_csv(
        test_files.test_file_csv_header, names=["c1", "c2", "c3"], index_col=["c3"]
    )
    with SqlCounter(query_count=9):
        got = pd.read_csv(
            test_files.test_file_csv_header, names=["c1", "c2", "c3"], index_col=["c3"]
        )
        assert_frame_equal(expected, got, check_dtype=False, check_index_type=False)

    test_files = TestFiles(resources_path)
    expected = native_pd.read_csv(
        test_files.test_file_csv_header,
        names=["c1", "c2", "c3"],
        index_col=["c3", "c1"],
    )
    with SqlCounter(query_count=9):
        got = pd.read_csv(
            test_files.test_file_csv_header,
            names=["c1", "c2", "c3"],
            index_col=["c3", "c1"],
        )
        assert_frame_equal(expected, got, check_dtype=False, check_index_type=False)


@pytest.mark.parametrize(
    "index_col,expected_error_type,expected_error_msg",
    [
        ({"rating"}, TypeError, "list indices must be integers or slices, not set"),
        (
            [1, {"nested_example_non_existent"}, 2],
            TypeError,
            "list indices must be integers or slices, not set",
        ),
        (
            [1, {"nested_example_non_existent"}, 2],
            TypeError,
            "list indices must be integers or slices, not set",
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_read_csv_index_col_frontend_negative(
    resources_path, index_col, expected_error_type, expected_error_msg
):
    test_files = TestFiles(resources_path)

    with pytest.raises(expected_error_type, match=expected_error_msg):
        pd.read_csv(test_files.test_file_csv_header, index_col=index_col).to_pandas()


@pytest.mark.parametrize(
    "index_col,expected_error_type,expected_error_msg",
    [
        (["non_existent_col", "a"], ValueError, "Index non_existent_col invalid"),
        ([-5], IndexError, "list index is out of range"),
        ((4), IndexError, "list index is out of range"),
        ([0, 0], ValueError, "Duplicate columns in index_col are not allowed."),
        ([1, "name"], ValueError, "Duplicate columns in index_col are not allowed."),
    ],
)
@sql_count_checker(query_count=8)
def test_read_csv_index_col_negative(
    resources_path, index_col, expected_error_type, expected_error_msg
):
    test_files = TestFiles(resources_path)

    with pytest.raises(expected_error_type, match=expected_error_msg):
        pd.read_csv(test_files.test_file_csv_header, index_col=index_col).to_pandas()
