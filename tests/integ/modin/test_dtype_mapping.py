#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import json
import re
from datetime import date, time
from decimal import Decimal
from json import JSONDecodeError

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest
from numpy import dtype
from pandas import Timestamp
from pandas._testing import assert_frame_equal

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import SqlCounter
from tests.utils import Utils


@pytest.mark.modin_sp_precommit
@pytest.mark.parametrize(
    "col_name_type, samples, expected_dtype, expected_to_pandas_dtype, expected_to_pandas",
    [
        (
            "int8 number",
            "values (-128),(127)",
            dtype("int64"),
            dtype("int8"),
            native_pd.DataFrame(
                [-128, 127], index=native_pd.Index([0, 1]), columns=["INT8"]
            ).astype("int8"),
        ),
        (
            "int16 number",
            "values (-32768),(32767)",
            dtype("int64"),
            dtype("int16"),
            native_pd.DataFrame(
                [-32768, 32767], index=native_pd.Index([0, 1]), columns=["INT16"]
            ).astype("int16"),
        ),
        (
            "int32 number",
            "values (-2147483648),(2147483647)",
            dtype("int64"),
            dtype("int32"),
            native_pd.DataFrame(
                [-2147483648, 2147483647],
                index=native_pd.Index([0, 1]),
                columns=["INT32"],
            ).astype("int32"),
        ),
        (
            "int64 number",
            "values (-9223372036854775808),(9223372036854775807)",
            dtype("int64"),
            dtype("int64"),
            native_pd.DataFrame(
                [-9223372036854775808, 9223372036854775807],
                index=native_pd.Index([0, 1]),
                columns=["INT64"],
            ),
        ),
        (
            '">int64" number',
            "values (-99999999999999999999999999999999999999),(99999999999999999999999999999999999999)",
            dtype("int64"),
            dtype("float64"),
            native_pd.DataFrame(
                [
                    -99999999999999999999999999999999999999,
                    99999999999999999999999999999999999999,
                ],
                index=native_pd.Index([0, 1]),
                columns=[">int64"],
            ).astype("float64"),
        ),
        (
            "decimal number(5,2)",
            "values (-128.02),(127.99)",
            dtype("float64"),
            dtype("float64"),
            native_pd.DataFrame(
                [-128.02, 127.99], index=native_pd.Index([0, 1]), columns=["DECIMAL"]
            ).astype("float64"),
        ),
        # SNOW-990542 changed behavior to explicitly downcast to float. This means, the resulting
        # value is -1e36 and 1e36 for this test. Follow Snowpark Python behavior here.
        #         (
        #             "large_decimal number(38,2)",
        #             "values (-999999999999999999999999999999999999.99),(999999999999999999999999999999999999.99)",
        #             dtype("float64"),
        #             dtype("float64"),
        #             native_pd.DataFrame(
        #                 [
        #                     Decimal("-999999999999999999999999999999999999.99"),
        #                     Decimal("999999999999999999999999999999999999.99"),
        #                 ],
        #                 index=pd.Index([0, 1]),
        #                 columns=["LARGE_DECIMAL"],
        #             ),
        #         ),  # python decimal object
        (
            "bool boolean",
            "values (true),(false)",
            dtype("bool"),
            dtype("bool"),
            native_pd.DataFrame(
                [True, False], index=native_pd.Index([0, 1]), columns=["BOOL"]
            ),
        ),
        (
            "bool_with_null boolean",
            "values (true),(null)",
            dtype("bool"),
            dtype("O"),
            native_pd.DataFrame(
                [True, None], index=native_pd.Index([0, 1]), columns=["BOOL_WITH_NULL"]
            ),
        ),
        (
            "float64 float",
            "values (3.14),(9.999999)",
            dtype("float64"),
            dtype("float64"),
            native_pd.DataFrame(
                [3.14, 9.999999], index=native_pd.Index([0, 1]), columns=["FLOAT64"]
            ),
        ),
        (
            "str text",
            "values ('abc'),('xxyyzz')",
            dtype("O"),
            dtype("O"),
            native_pd.DataFrame(
                ["abc", "xxyyzz"], index=native_pd.Index([0, 1]), columns=["STR"]
            ),
        ),
        (
            "bin binary",
            "values ('48454C50'),('48454C50')",
            dtype("O"),
            dtype("O"),
            native_pd.DataFrame(
                [b"HELP", b"HELP"], index=native_pd.Index([0, 1]), columns=["BIN"]
            ),
        ),
        (
            "variant variant",
            """select PARSE_JSON(' { "key1": "value1", "key2": NULL } ')""",
            dtype("O"),
            dtype("O"),
            native_pd.DataFrame(
                np.array([{"key1": "value1", "key2": None}]),
                index=native_pd.Index([0]),
                columns=["VARIANT"],
            ),
        ),
        (
            "float_in_variant variant",
            """values (3.14::float), (9.99999::float)""",
            dtype("O"),
            dtype("float64"),
            native_pd.DataFrame(
                np.array([3.14, 9.99999]),
                index=native_pd.Index([0, 1]),
                columns=["FLOAT_IN_VARIANT"],
            ),
        ),
        (
            "multitype variant, multitype2 variant",
            [
                """select PARSE_JSON(' { "key1": "value1", "key2": NULL } '), 'string'::variant""",
                """select ARRAY_CONSTRUCT('Washington', 'Oregon', 'California'), 1""",
                """select OBJECT_CONSTRUCT('first_name', 'Mickey', 'last_name', 'Mouse'), to_variant(to_timestamp('2111','YYYY'))""",
                """select to_variant(to_time('01:23:45')), to_variant(to_date('2016-05-01'))""",
                """select 'string'::variant, to_variant(to_timestamp('2111','YYYY'))""",
                """select to_variant(to_timestamp('2111','YYYY')), 1""",
            ],
            [dtype("O"), dtype("O")],
            [dtype("O"), dtype("O")],
            native_pd.DataFrame(
                np.array(
                    [
                        [{"key1": "value1", "key2": None}, "string"],
                        [["Washington", "Oregon", "California"], 1],
                        [
                            {"first_name": "Mickey", "last_name": "Mouse"},
                            native_pd.to_datetime("2111-01-01 00:00:00"),
                        ],
                        [time(1, 23, 45), date(2016, 5, 1)],
                        ["string", native_pd.to_datetime("2111-01-01 00:00:00")],
                        [native_pd.to_datetime("2111-01-01 00:00:00"), 1],
                    ],
                    dtype="object",
                ),
                index=native_pd.Index([0, 1, 2, 3, 4, 5]),
                columns=["MULTITYPE", "MULTITYPE2"],
            ),
        ),
        (
            "obj object",
            """select PARSE_JSON(' { "key1": "value1", "key2": NULL } ')""",
            dtype("O"),
            dtype("O"),
            native_pd.DataFrame(
                np.array([{"key1": "value1", "key2": None}]),
                index=native_pd.Index([0]),
                columns=["OBJ"],
            ),
        ),
        (
            "arr array",
            "select ARRAY_CONSTRUCT(1,2,3, ARRAY_CONSTRUCT(1,2,3))",
            dtype("O"),
            dtype("O"),
            native_pd.DataFrame(
                ["val"],
                index=native_pd.Index([0]),
                columns=["ARR"],
            ).applymap(
                lambda val: [1, 2, 3, [1, 2, 3]]
            ),  # use `applymap` to create an array cell
        ),
        (
            "date date",
            "values ('2016-05-01'::date), ('2016-05-02'::date)",
            dtype("O"),
            dtype("O"),
            native_pd.DataFrame(
                [date(2016, 5, 1), date(2016, 5, 2)],
                index=native_pd.Index([0, 1]),
                columns=["DATE"],
            ),
        ),
        (
            "time time",
            "values ('00:00:01'::time), ('23:59:59'::time)",
            dtype("O"),
            dtype("O"),
            native_pd.DataFrame(
                [time(0, 0, 1), time(23, 59, 59)],
                index=native_pd.Index([0, 1]),
                columns=["TIME"],
            ),
        ),
        (
            "timestamp_ntz timestamp_ntz",
            "values ('2023-01-01 00:00:01.001'), ('2023-12-31 23:59:59.999')",
            dtype("<M8[ns]"),
            dtype("<M8[ns]"),
            native_pd.DataFrame(
                [
                    Timestamp("2023-01-01 00:00:01.001"),
                    Timestamp("2023-12-31 23:59:59.999"),
                ],
                index=native_pd.Index([0, 1]),
                columns=["TIMESTAMP_NTZ"],
            ),
        ),
        (
            "timestamp_ltz timestamp_ltz",
            "values ('2023-01-01 00:00:01.001'), ('2023-12-31 23:59:59.999')",
            "datetime64[ns, America/Los_Angeles]",
            "datetime64[ns, America/Los_Angeles]",
            native_pd.DataFrame(
                [
                    Timestamp("2023-01-01 00:00:01.001", tz="America/Los_Angeles"),
                    Timestamp("2023-12-31 23:59:59.999", tz="America/Los_Angeles"),
                ],
                index=native_pd.Index([0, 1]),
                columns=["TIMESTAMP_LTZ"],
            ),
        ),
        (
            "timestamp_tz timestamp_tz",
            "values ('2023-01-01 00:00:01.001 +0000'), ('2023-12-31 23:59:59.999 +1000')",  # timestamp_tz only supports tz offset
            "object",  # multi timezone case
            "object",
            native_pd.DataFrame(
                [
                    Timestamp("2023-01-01 00:00:01.001 +0000"),
                    Timestamp("2023-12-31 23:59:59.999 +1000"),
                ],
                index=native_pd.Index([0, 1]),
                columns=["TIMESTAMP_TZ"],
            ),
        ),
        (
            "geography geography",
            "select to_geography('POINT(-122.35 37.55)')",
            dtype("O"),
            dtype("O"),
            native_pd.DataFrame(
                ["val"],
                index=native_pd.Index([0]),
                columns=["GEOGRAPHY"],
            ).applymap(
                lambda val: json.dumps(
                    {"coordinates": [-122.35, 37.55], "type": "Point"}, indent=2
                )
            ),  # use `applymap` to create an array cell
        ),
        (
            "geometry geometry",
            "select to_geometry('POINT(-122.35 37.55)')",
            dtype("O"),
            dtype("O"),
            native_pd.DataFrame(
                ["val"],
                index=native_pd.Index([0]),
                columns=["GEOMETRY"],
            ).applymap(
                lambda val: """{
  "coordinates": [
    -1.223500000000000e+02,
    3.755000000000000e+01
  ],
  "type": "Point"
}"""
            ),  # use `applymap` to create an array cell
        ),
    ],
)
def test_read_snowflake_data_types(
    session,
    test_table_name,
    col_name_type,
    samples,
    expected_dtype,
    expected_to_pandas_dtype,
    expected_to_pandas,
):
    expected_query_count = (
        9
        if isinstance(samples, list) and len(samples) > 1
        else 5
        if "timestamp_tz" in col_name_type
        else 4
    )
    with SqlCounter(
        query_count=expected_query_count,
        high_count_expected=True,
        high_count_reason="Getting table schema",
    ):
        Utils.create_table(session, test_table_name, col_name_type, is_temporary=True)
        if not isinstance(samples, list):
            samples = [samples]
        for sample in samples:
            session.sql(f"insert into {test_table_name} {sample}").collect()
        df = pd.read_snowflake(test_table_name, enforce_ordering=True)
        assert (
            df.dtypes.to_list() == [expected_dtype]
            if not isinstance(expected_dtype, list)
            else expected_dtype
        ), f"unexpected dtypes {df.dtypes.to_list()}, expected [{expected_dtype}]"
        to_pandas_df = df.to_pandas()
        assert (
            to_pandas_df.dtypes.to_list() == [expected_to_pandas_dtype]
            if not isinstance(expected_to_pandas_dtype, list)
            else expected_to_pandas_dtype
        ), f"unexpected to_pandas() dtypes {to_pandas_df.dtypes.to_list()}, expected [{expected_to_pandas_dtype}]"
        assert_frame_equal(
            to_pandas_df, expected_to_pandas
        )  # Snowpark pandas APIs use Int index type while pandas use RangeIndex


@pytest.mark.parametrize(
    "col_name_type, samples, expected_dtype, expected_to_pandas_dtype, expected_to_pandas",
    [
        (
            "large_decimal number(38,2)",
            "values (-999999999999999999999999999999999999.99),(999999999999999999999999999999999999.99)",
            dtype("float64"),
            dtype("float64"),
            native_pd.DataFrame(
                [
                    Decimal("-999999999999999999999999999999999999.99"),
                    Decimal("999999999999999999999999999999999999.99"),
                ],
                index=native_pd.Index([0, 1]),
                columns=["LARGE_DECIMAL"],
            ),
        ),  # python decimal object
    ],
)
def test_read_snowflake_data_types_negative(
    session,
    test_table_name,
    col_name_type,
    samples,
    expected_dtype,
    expected_to_pandas_dtype,
    expected_to_pandas,
):
    # SNOW-990542 changed behavior to explicitly downcast to float. This means, the resulting
    # value is -1e36 and 1e36 for this test as the decimal above can not be represented as a floating pointer number.
    # For now to be compatible with the current Snowpark behavior, we track this as a negative test here.
    # However, in the future Snowpark's behavior may change to address this large-scale floating point number case
    # in a different way and we should refresh with their current behavior.

    expected_query_count = 9 if isinstance(samples, list) and len(samples) > 1 else 4
    with SqlCounter(query_count=expected_query_count):
        Utils.create_table(session, test_table_name, col_name_type, is_temporary=True)
        if not isinstance(samples, list):
            samples = [samples]
        for sample in samples:
            session.sql(f"insert into {test_table_name} {sample}").collect()
        df = pd.read_snowflake(test_table_name, enforce_ordering=True)
        assert (
            df.dtypes.to_list() == [expected_dtype]
            if not isinstance(expected_dtype, list)
            else expected_dtype
        ), f"unexpected dtypes {df.dtypes.to_list()}, expected [{expected_dtype}]"
        to_pandas_df = df.to_pandas()
        assert (
            to_pandas_df.dtypes.to_list() == [expected_to_pandas_dtype]
            if not isinstance(expected_to_pandas_dtype, list)
            else expected_to_pandas_dtype
        ), f"unexpected to_pandas() dtypes {to_pandas_df.dtypes.to_list()}, expected [{expected_to_pandas_dtype}]"

        # produces error due to floating point representation gap:
        # E   DataFrame.iloc[:, 0] (column name="LARGE_DECIMAL") values are different (100.0 %)
        # E   [index]: [0, 1]
        # E   [left]:  [-1e+36, 1e+36]
        # E   [right]: [-999999999999999999999999999999999999.99, 999999999999999999999999999999999999.99]
        with pytest.raises(
            AssertionError,
            match=re.escape(
                'DataFrame.iloc[:, 0] (column name="LARGE_DECIMAL") are different'
            ),
        ):
            assert_frame_equal(
                to_pandas_df, expected_to_pandas, check_dtype=False
            )  # Snowpark pandas APIs use Int index type while pandas use RangeIndex


@pytest.mark.parametrize(
    "col_name_type, samples, expected_to_pandas_dtype, expected_to_pandas",
    [
        (
            "multitype variant",
            [
                """select PARSE_JSON(' { "key1": "value1", "key2": NULL } ')""",
                """select ARRAY_CONSTRUCT('Washington', 'Oregon', 'California', NULL)""",
                """select OBJECT_CONSTRUCT('first_name', 'Mickey', 'last_name', 'Mouse')""",
                """select 1""",
                """select 'string'::variant""",
            ],
            dtype("O"),
            native_pd.DataFrame(
                np.array(
                    [
                        '{\n  "key1": "value1",\n  "key2": null\n}',
                        '[\n  "Washington",\n  "Oregon",\n  "California",\n  undefined\n]',
                        '{\n  "first_name": "Mickey",\n  "last_name": "Mouse"\n}',
                        "1",
                        '"string"',
                    ],
                ),
                index=native_pd.Index([0, 1, 2, 3, 4]),
                columns=["MULTITYPE"],
            ),
        ),
        (
            "arr array",
            "select ARRAY_CONSTRUCT(1,2,3,NULL)",
            dtype("O"),
            native_pd.DataFrame(
                ["[\n  1,\n  2,\n  3,\n  undefined\n]"],
                index=native_pd.Index([0]),
                columns=["ARR"],
            ),
        ),
    ],
)
def test_read_snowflake_data_types_array_undefined_negative(
    session,
    test_table_name,
    col_name_type,
    samples,
    expected_to_pandas_dtype,
    expected_to_pandas,
):
    expected_query_count = 7 if isinstance(samples, list) and len(samples) > 1 else 3
    with SqlCounter(query_count=expected_query_count):
        Utils.create_table(session, test_table_name, col_name_type, is_temporary=True)
        if not isinstance(samples, list):
            samples = [samples]
        for sample in samples:
            session.sql(f"insert into {test_table_name} {sample}").collect()
        # to_pandas will fail when `undefined` exists in array
        with pytest.raises(JSONDecodeError, match="Expecting value"):
            pd.read_snowflake(test_table_name).to_pandas()
