#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
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
from snowflake.snowpark._internal.utils import generate_random_alphanumeric
from tests.integ.modin.utils import assert_frame_equal
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import IS_WINDOWS, TestFiles, Utils

tmp_stage_name1 = Utils.random_stage_name()
test_file_csv = "testCSV.csv"


# Explicitly redefine here to make it work on precommit tests
@pytest.fixture(scope="session")
def resources_path() -> str:
    return os.path.normpath(
        os.path.join(os.path.dirname(__file__), "../../../resources")
    )


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


@sql_count_checker(query_count=2)
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


def test_read_csv_header_none(resources_path):
    test_files = TestFiles(resources_path)

    filename = test_files.test_file_csv_header

    with SqlCounter(query_count=2):
        assert_frame_equal(
            pd.read_csv(filename, header=None),
            native_pd.read_csv(filename, header=None),
            check_dtype=False,
        )


@pytest.mark.parametrize("header", [0, 1])
@sql_count_checker(query_count=2)
def test_read_csv_header_simple(resources_path, header):
    test_files = TestFiles(resources_path)

    expected = native_pd.read_csv(test_files.test_file_csv_header, header=header)
    got = pd.read_csv(test_files.test_file_csv_header, header=header)
    assert_frame_equal(expected, got, check_dtype=False, check_index_type=False)


@pytest.mark.modin_sp_precommit
@pytest.mark.parametrize("engine", ["c", "python", "pyarrow"])
@sql_count_checker(query_count=2)
def test_read_csv_engine_local(resources_path, engine):
    test_files = TestFiles(resources_path)

    expected = native_pd.read_csv(test_files.test_file_csv_header)
    got = pd.read_csv(test_files.test_file_csv_header, engine=engine)
    assert_frame_equal(expected, got, check_dtype=False, check_index_type=False)


@pytest.mark.modin_sp_precommit
@sql_count_checker(query_count=9)
def test_read_csv_engine_snowflake(resources_path):
    test_files = TestFiles(resources_path)

    expected = native_pd.read_csv(test_files.test_file_csv_header)
    got = pd.read_csv(test_files.test_file_csv_header, engine="snowflake")
    assert_frame_equal(expected, got, check_dtype=False, check_index_type=False)


@sql_count_checker(query_count=2)
def test_read_csv_header_skiprows(resources_path):
    test_files = TestFiles(resources_path)

    expected = native_pd.read_csv(test_files.test_file_csv_header, header=1, skiprows=1)
    got = pd.read_csv(test_files.test_file_csv_header, header=1, skiprows=1)

    assert_frame_equal(expected, got, check_dtype=False, check_index_type=False)


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
@sql_count_checker(query_count=2)
def test_read_csv_names(resources_path, names):
    test_files = TestFiles(resources_path)

    expected = native_pd.read_csv(test_files.test_file_csv, names=names)
    got = pd.read_csv(test_files.test_file_csv, names=names)

    assert_frame_equal(expected, got, check_dtype=False, check_index_type=False)


@sql_count_checker(query_count=2)
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


@sql_count_checker(query_count=1)
def test_read_csv_name_invalid_type_negative(resources_path):
    test_files = TestFiles(resources_path)

    names = [1, [2, 3], 4]

    with pytest.raises(TypeError, match="unhashable type: 'list'"):
        pd.read_csv(test_files.test_file_csv_header, names=names)


@sql_count_checker(query_count=2)
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
@sql_count_checker(query_count=2)
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


@sql_count_checker(query_count=2)
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
@sql_count_checker(query_count=8)
def test_read_csv_with_warning_params(param, resources_path, arg):
    test_files = TestFiles(resources_path)
    staging_filename = f"@{tmp_stage_name1}/{test_file_csv}"
    local_filename = test_files.test_file_csv

    assert_frame_equal(
        pd.read_csv(staging_filename, **{param: arg}),
        native_pd.read_csv(local_filename),
        check_dtype=False,
    )


@sql_count_checker(query_count=2)
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


@sql_count_checker(query_count=2)
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


@sql_count_checker(query_count=2)
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
        ("dayfirst", True),
        ("date_parser", True),
        ("date_format", "%Y-%m-%d"),
        ("keep_date_col", True),
        ("parse_dates", True),
        ("iterator", True),
        ("na_filter", False),
        ("skipfooter", 3),
        ("nrows", 100),
        ("thousands", ","),
        ("decimal", ","),
        ("lineterminator", "q"),
        ("dialect", "excel"),
        ("quoting", 2),
        ("doublequote", False),
        ("encoding_errors", "replace"),
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
def test_read_staged_csv_negative(param, arg):
    with pytest.raises(NotImplementedError, match=f"{param} is not implemented."):
        pd.read_csv(f"@{tmp_stage_name1}/{test_file_csv}", **{param: arg})


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
@sql_count_checker(query_count=2)
def test_read_csv_usecols(resources_path, usecols):
    test_files = TestFiles(resources_path)

    expected = native_pd.read_csv(test_files.test_file_csv_header, usecols=usecols)
    got = pd.read_csv(test_files.test_file_csv_header, usecols=usecols).to_pandas()
    assert_frame_equal(expected, got, check_dtype=False, check_index_type=False)


@sql_count_checker(query_count=0)
def test_read_csv_usecols_empty_negative(resources_path):
    test_files = TestFiles(resources_path)

    with pytest.raises(NotImplementedError, match="usecols"):
        pd.read_csv(test_files.test_file_csv_header, usecols=[])


@pytest.mark.parametrize(
    "usecols",
    [
        [1, "rating"],
        [1, [2, 3], 4],
        native_pd.MultiIndex.from_arrays([["rating", "id", "name"]]),
        [12.33, np.float64(13.2333), np.double(2.5)],
        [datetime.time(1, 2, 3)],
        [datetime.date(2021, 1, 9), datetime.datetime(2023, 1, 1, 1, 2, 3)],
    ],
)
@sql_count_checker(query_count=1)
def test_read_csv_usecols_invalid_types_negative(resources_path, usecols):
    test_files = TestFiles(resources_path)

    with pytest.raises(
        ValueError,
        match="'usecols' must either be list-like of all strings, all unicode, all integers or a callable.",
    ):
        pd.read_csv(test_files.test_file_csv, usecols=usecols)


@pytest.mark.parametrize(
    "usecols",
    [["non_existent_col"], ["rating", "non_existent_col"], [-1], [0, 4], ("rating")],
)
@sql_count_checker(query_count=1)
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
@sql_count_checker(query_count=2)
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
@sql_count_checker(query_count=2)
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

    with SqlCounter(query_count=1):
        with pytest.raises(
            ValueError,
            match="'usecols' do not match columns, columns expected but not found",
        ):
            pd.read_csv(
                test_files.test_file_csv_header,
                names=["c1", "c2", "c3"],
                usecols=["id"],
            )

    with SqlCounter(query_count=1):
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
@sql_count_checker(query_count=2)
def test_read_csv_dtype(resources_path, dtype):
    test_files = TestFiles(resources_path)
    expected = native_pd.read_csv(test_files.test_file_csv_header, dtype=dtype)
    got = pd.read_csv(test_files.test_file_csv_header, dtype=dtype)
    assert_frame_equal(expected, got, check_dtype=False)


@pytest.mark.parametrize(
    "dtype,expected_error,expected_error_msg",
    [
        (
            {"id": [str]},
            TypeError,
            "Field elements must be 2- or 3-tuples, got '<class 'str'>",
        ),
        (
            {"rating": "non_existent_type"},
            TypeError,
            "data type 'non_existent_type' not understood",
        ),
    ],
)
@sql_count_checker(query_count=0)
def test_read_csv_dtype_negative(
    resources_path, dtype, expected_error, expected_error_msg
):

    test_files = TestFiles(resources_path)

    with pytest.raises(expected_error, match=expected_error_msg):
        pd.read_csv(test_files.test_file_csv_header, dtype=dtype).to_pandas()


# usecols is applied after the parsing, so changing the dtypes w/ usecols will
# fail if the original dataset has non-convertible types
@sql_count_checker(query_count=0)
def test_read_csv_dtype_usecols_negative(resources_path):
    test_files = TestFiles(resources_path)
    with pytest.raises(ValueError, match="could not convert string to float: 'one'"):
        pd.read_csv(
            test_files.test_file_csv_header, usecols=["id", "rating"], dtype=np.float64
        )


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
@sql_count_checker(query_count=2)
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
    with SqlCounter(query_count=2):
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
    with SqlCounter(query_count=2):
        got = pd.read_csv(
            test_files.test_file_csv_header,
            names=["c1", "c2", "c3"],
            index_col=["c3", "c1"],
        )
        assert_frame_equal(expected, got, check_dtype=False, check_index_type=False)


@pytest.mark.parametrize(
    "index_col,expected_error_type,expected_error_msg",
    [
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
@sql_count_checker(query_count=1)
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
        ([-5], IndexError, "list index out of range"),
        ((4), IndexError, "list index out of range"),
        ([0, 0], ValueError, "Duplicate columns in index_col are not allowed."),
        ([1, "name"], ValueError, "Duplicate columns in index_col are not allowed."),
    ],
)
@sql_count_checker(query_count=1)
def test_read_csv_index_col_negative(
    resources_path, index_col, expected_error_type, expected_error_msg
):
    test_files = TestFiles(resources_path)

    with pytest.raises(expected_error_type, match=expected_error_msg):
        pd.read_csv(test_files.test_file_csv_header, index_col=index_col).to_pandas()


@sql_count_checker(query_count=2)
def test_read_csv_empty_header_snow_1272443():
    # generate a random temp name so these tests can be run in parallel
    temp_file_name = f"test_read_csv_empty_header_{generate_random_alphanumeric(4)}.csv"
    expected_df = native_pd.DataFrame({"": [1, 2], "a": ["qwe", 3], "b": [4, 5]})
    expected_df.to_csv(temp_file_name, index=False)
    # reading an empty header w/ pandas will result in "Unamed: 0" as a column header
    # even though this is not in the original, written CSV. We account for this by
    # re-reading the CSV with native pandas for a proper comparison.
    expected_df = native_pd.read_csv(temp_file_name)
    got_df = pd.read_csv(temp_file_name)
    os.remove(temp_file_name)
    assert_frame_equal(expected_df, got_df, check_dtype=False, check_index_type=False)


@sql_count_checker(query_count=4)
def test_read_csv_dateparse():
    # generate a random temp name so these tests can be run in parallel
    temp_file_name = f"test_read_csv_dateparse_{generate_random_alphanumeric(4)}.csv"
    df = native_pd.DataFrame({"date": ["1/1/2019", "1/2/2019", "1/3/2019"]})
    df.to_csv(temp_file_name, index=False)

    expected_df = native_pd.read_csv(temp_file_name, parse_dates=["date"])
    got_df = pd.read_csv(temp_file_name, parse_dates=["date"])
    assert_frame_equal(expected_df, got_df, check_dtype=False, check_index_type=False)

    expected_df = native_pd.read_csv(
        temp_file_name, parse_dates=["date"], dayfirst=True
    )
    got_df = pd.read_csv(temp_file_name, parse_dates=["date"], dayfirst=True)
    assert_frame_equal(expected_df, got_df, check_dtype=False, check_index_type=False)

    os.remove(temp_file_name)


@sql_count_checker(query_count=2)
def test_read_csv_dateparse_multiple_columns():
    # generate a random temp name so these tests can be run in parallel
    temp_file_name = f"test_read_csv_dateparse_{generate_random_alphanumeric(4)}.csv"
    df = native_pd.DataFrame(
        {"day": [1, 2, 3], "month": [3, 2, 1], "year": [2021, 2022, 2023]}
    )
    df.to_csv(temp_file_name, index=False)

    expected_df = native_pd.read_csv(
        temp_file_name, parse_dates={"date": ["year", "month", "day"]}
    )
    got_df = pd.read_csv(temp_file_name, parse_dates={"date": ["year", "month", "day"]})
    assert_frame_equal(expected_df, got_df, check_dtype=False, check_index_type=False)

    os.remove(temp_file_name)


def test_read_csv_s3():
    host = pd.session.connection.host
    if any(platform in host.split(".") for platform in ["gcp", "azure"]):
        pytest.skip(reason="Skipping test for Azure and GCP deployment")
    if IS_WINDOWS:
        pytest.skip(
            reason="Skipping test for Windows because the schema cannot be found"
        )
    with SqlCounter(query_count=8):
        df = pd.read_csv(
            "s3://sfquickstarts/frostbyte_tastybytes/analytics/menu_item_aggregate_v.csv"
        )
    assert len(df.columns) == 12
