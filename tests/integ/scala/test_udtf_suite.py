#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import datetime
import decimal
import sys
from collections import Counter
from typing import Dict, List, Tuple

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.exceptions import (
    SnowparkInvalidObjectNameException,
    SnowparkSQLException,
)
from snowflake.snowpark.functions import col, lit, udtf
from snowflake.snowpark.types import (
    IntegerType,
    StringType,
    StructField,
    StructType,
    Variant,
)
from tests.utils import Utils

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

pytestmark = pytest.mark.udf

wordcount_table_name = Utils.random_table_name()


@pytest.fixture(scope="module", autouse=True)
def setup_data(session):
    df = session.sql(
        "SELECT $1 AS \"C1\", $2 AS \"C2\" FROM  VALUES ('w1 w2', 'g1'), ('w1 w1 w1', 'g2')"
    )
    df.write.save_as_table(wordcount_table_name)
    yield
    Utils.drop_table(session, wordcount_table_name)


def test_basic_udtf_word_count_without_end_partition(session):
    func_name = Utils.random_function_name()

    class MyWordCount:
        def process(self, s1: str) -> Iterable[Tuple[str, int]]:
            counter = Counter(s1.split())
            return counter.items()

    wordcount_udtf = session.udtf.register(
        MyWordCount, ["word", "count"], name=func_name, is_permanent=False, replace=True
    )

    wordcount_udtf_with_statemenet_params = session.udtf.register(
        MyWordCount,
        ["word", "count"],
        name=func_name,
        is_permanent=False,
        replace=True,
        statement_params={"SF_PARTNER": "FAKE_PARTNER"},
    )

    try:
        # Use table function name to query the new udtf
        df1 = session.table_function(func_name, lit("w1 w2 w2 w3 w3 w3"))
        Utils.check_answer(df1, [Row("w1", 1), Row("w2", 2), Row("w3", 3)], sort=True)
        assert df1.columns == ["WORD", "COUNT"]

        # Use returned result to query the new udtf
        df2 = session.table_function(wordcount_udtf(lit("w1 w2 w2 w3 w3 w3")))
        Utils.check_answer(df2, [Row("w1", 1), Row("w2", 2), Row("w3", 3)], sort=True)
        assert df2.columns == ["WORD", "COUNT"]

        df2_with_statement_params = session.table_function(
            wordcount_udtf_with_statemenet_params(lit("w1 w2 w2 w3 w3 w3"))
        )
        Utils.check_answer(
            df2_with_statement_params,
            [Row("w1", 1), Row("w2", 2), Row("w3", 3)],
            sort=True,
        )
        assert df2_with_statement_params.columns == ["WORD", "COUNT"]

        # Call the UDTF with funcName and named parameters, result should be the same
        df3 = session.table_function(wordcount_udtf(arg1=lit("w1 w2 w2 w3 w3 w3")))
        Utils.check_answer(df3, [Row("w1", 1), Row("w2", 2), Row("w3", 3)], sort=True)
        assert df3.columns == ["WORD", "COUNT"]

        # Use UDTF with table join
        df4 = session.table(wordcount_table_name).join_table_function(
            wordcount_udtf(col("c1")).over(partition_by="c2")
        )
        Utils.check_answer(
            df4,
            [
                Row("w1 w2", "g1", "w2", 1),
                Row("w1 w2", "g1", "w1", 1),
                Row("w1 w1 w1", "g2", "w1", 3),
            ],
        )
        assert df4.columns == ["C1", "C2", "WORD", "COUNT"]
    finally:
        Utils.drop_function(session, f"{func_name}(string)")


def test_basic_udtf_word_count_with_end_partition(session):
    func_name = Utils.random_function_name()

    class MyWordCount:
        def __init__(self) -> None:
            self._total_per_partition = 0

        def process(self, s1: str) -> Iterable[Tuple[str, int]]:
            words = s1.split()
            self._total_per_partition = len(words)
            counter = Counter(words)
            return counter.items()

        def end_partition(self):
            yield ("W_Total", self._total_per_partition)

    session.udtf.register(
        MyWordCount, ["word", "count"], name=func_name, is_permanent=False, replace=True
    )

    try:
        # Use table function name to query the new udtf
        df1 = session.table_function(func_name, lit("w1 w2 w2 w3 w3 w3"))
        Utils.check_answer(
            df1,
            [Row("w1", 1), Row("w2", 2), Row("w3", 3), Row("W_Total", 6)],
            sort=True,
        )
        assert df1.columns == ["WORD", "COUNT"]
    finally:
        Utils.drop_function(session, f"{func_name}(string)")


def test_register_a_udtf_multiple_times(session):
    func_name = Utils.random_function_name()
    func_name2 = Utils.random_function_name()

    class MyDuplicateRegister:
        def process(self, s1):
            counter = Counter(s1.split())
            return counter.items()

    try:
        output_schema = StructType(
            [StructField("word", StringType()), StructField("count", IntegerType())]
        )
        session.udtf.register(
            MyDuplicateRegister,
            output_schema,
            input_types=[StringType()],
            name=func_name,
        )
        session.udtf.register(
            MyDuplicateRegister,
            output_schema,
            input_types=[StringType()],
            name=func_name2,
        )
    finally:
        Utils.drop_function(session, f"{func_name}(string)")
        Utils.drop_function(session, f"{func_name2}(string)")


def test_negative_test_with_invalid_output_column_name(session):
    class MyInvalidNameUDTF:
        def process(self, s1):
            return [(s1,)]

    with pytest.raises(SnowparkInvalidObjectNameException) as invalid_exp:
        session.udtf.register(
            MyInvalidNameUDTF,
            output_schema=StructType([StructField("bad name", StringType())]),
            input_types=[StringType()],
        )
    assert "is invalid" in invalid_exp.value.message

    with pytest.raises(ValueError) as ve:
        session.udtf.register(
            MyInvalidNameUDTF, output_schema=["c1"], input_types=[StringType()]
        )
    assert (
        "The return type hint is not set but 'output_schema' has only column names. "
        "You can either use a StructType instance for 'output_schema', or usea "
        "combination of a return type hint for method 'process' and column names for "
        "'output_schema'." in str(ve.value)
    )

    class MyInvalidUDTF2:
        def process(self, s1) -> Iterable[Tuple[str]]:
            return [(s1,)]

    with pytest.raises(ValueError) as ve:
        session.udtf.register(
            MyInvalidUDTF2, output_schema=["c1", "c2"], input_types=[StringType()]
        )
    assert "output_schema' has 2 names while type hints Tuple has only 1." in str(
        ve.value
    )

    class MyWrongReturnTypeUDTF:
        def process(self) -> int:
            return [(1,)]

    with pytest.raises(ValueError) as ve:
        session.udtf.register(MyWrongReturnTypeUDTF, output_schema=["c1"])
    assert (
        "The return type hint for a UDTF handler must but a collection type or a PandasDataFrame. <class 'int'> is used."
        in str(ve.value)
    )

    class MyWrongReturnTypeNoTupleUDTF:
        def process(self) -> Iterable[List[int]]:
            return [[1]]

    with pytest.raises(ValueError) as ve:
        session.udtf.register(MyWrongReturnTypeNoTupleUDTF, output_schema=["c1"])
    assert (
        "The return type hint of method 'MyWrongReturnTypeNoTupleUDTF.process' must be a collection of tuple"
        in str(ve.value)
    )


@pytest.mark.skip(
    "Python UDTF doesn't support Optional in type hints for out_schema yet. And Optional has no impact."
)
def test_input_type_basic_types(session):
    output_schema = [
        "i1_str",
        "f1_str",
        "b1_str",
        "decimal1_str",
        "str1_str",
        "bytes1_str",
        "bytearray1_str",
    ]

    @udtf(output_schema=output_schema)
    class UDTFInputBasicTypes:
        def process(
            self,
            int_: int,
            float_: float,
            bool_: bool,
            decimal_: decimal.Decimal,
            str_: str,
            bytes_: bytes,
            byte_array_: bytearray,
        ) -> Iterable[Tuple[str, ...]]:
            return [
                (
                    str(int_),
                    str(float_),
                    str(bool_),
                    str(decimal_),
                    str_,
                    bytes_.decode("utf-8"),
                    byte_array_.decode("utf-8"),
                )
            ]

    try:
        df1 = session.table_function(
            UDTFInputBasicTypes(
                lit(1),
                lit(2.2),
                lit(True),
                lit(decimal.Decimal("3.33")),
                lit("python"),
                lit(b"bytes"),
                lit(bytearray("bytearray", "utf-8")),
            )
        )
        assert df1.columns == [x.upper() for x in output_schema]
        Utils.check_answer(
            df1,
            [
                Row(
                    "1",
                    "2.2",
                    "True",
                    "3.330000000000000000",
                    "python",
                    "bytes",
                    "bytearray",
                )
            ],
        )
    finally:
        Utils.drop_function(
            session,
            f"{UDTFInputBasicTypes.name}(bigint, float, boolean, number(38, 18), string, binary, binary)",
        )


def test_input_type_date_time_timestamp(session):
    output_schema = ["data_str", "time_str", "timestamp_str"]

    @udtf(output_schema=output_schema)
    class UDTFInputTimestampTypes:
        def process(
            self,
            date1: datetime.date,
            time1: datetime.time,
            datetime1: datetime.datetime,
        ) -> Iterable[Tuple[str, ...]]:
            return [(str(date1), str(time1), str(datetime1))]

    try:
        df = session.sql(
            "select date'2022-01-25' as date, time'12:13:14.123' as time, '2017-01-01 12:00:00'::timestamp_ntz as ts"
        )
        df = df.join_table_function(
            UDTFInputTimestampTypes.name, col("date"), col("time"), col("ts")
        )
        assert df.columns == ["DATE", "TIME", "TS", *(x.upper() for x in output_schema)]
        Utils.check_answer(
            df,
            [
                Row(
                    datetime.date(2022, 1, 25),
                    datetime.time(12, 13, 14, 123000),
                    datetime.datetime(2017, 1, 1, 12),
                    "2022-01-25",
                    "12:13:14.123000",
                    "2017-01-01 12:00:00",
                )
            ],
        )
    finally:
        Utils.drop_function(
            session, f"{UDTFInputTimestampTypes.name}(date, time, timestamp_ntz)"
        )


def test_input_type_variant_array_object(session):
    output_schema = ["V_STR", "A1_STR", "A2_STR", "M1_STR", "M2_STR"]

    @udtf(output_schema=output_schema)
    class VariangArrayMapUDTF:
        def process(
            self,
            v: "Variant",
            a1: List[str],
            a2: List["Variant"],
            m1: Dict[str, str],
            m2: Dict[str, "Variant"],
        ) -> Iterable[Tuple[str, ...]]:
            return [(str(v), str(a1), str(a2), str(m1), str(m2))]

    try:
        df = session.sql(
            "select to_variant('v1') as v, array_construct('a1', 'a1') as a1, array_construct('a2', 'a2') as a2, object_construct('m1', 'one') as m1, object_construct('m2', 'two') as m2"
        )
        df = df.join_table_function(
            VariangArrayMapUDTF("v", "A1", "A2", "M1", "M2")
        ).select(output_schema)
        assert df.columns == output_schema
        Utils.check_answer(
            df,
            [
                Row(
                    V_STR="v1",
                    A1_STR="['a1', 'a1']",
                    A2_STR="['a2', 'a2']",
                    M1_STR="{'m1': 'one'}",
                    M2_STR="{'m2': 'two'}",
                )
            ],
        )
    finally:
        Utils.drop_function(
            session,
            f"{VariangArrayMapUDTF.name}(variant, array, array, object, object)",
        )


def test_return_large_amount_of_return_columns_for_udtf(session):
    def create_udtf(output_column_count):
        @udtf(output_schema=["c" + str(i) for i in range(output_column_count)])
        class ReturnManyColumns:
            def process(self, data: int) -> Iterable[Tuple[int, ...]]:
                return [tuple(i + data for i in range(output_column_count))]

            def end_partition(self):
                return [tuple(range(output_column_count))]

        return ReturnManyColumns

    for column_count in [3, 100, 200]:
        tf = create_udtf(column_count)
        try:
            df3 = session.table_function(tf(lit(10)))
            assert len(df3.schema.fields) == column_count
            Utils.check_answer(
                df3,
                [
                    Row(*(10 + i for i in range(column_count))),
                    Row(*(i for i in range(column_count))),
                ],
                sort=False,
            )
        finally:
            Utils.drop_function(
                session, f"{tf.name}({','.join(['string'] * column_count)})"
            )


def test_output_type_basic_types(session):
    output_schema = ["int", "float", "bool", "decimal", "str", "bytes", "bytearray"]
    return_result = (
        1,
        1.1,
        True,
        2.2,
        "Hello str",
        b"Hello bytes",
        bytearray("Hello bytearray", "utf-8"),
    )

    @udtf(output_schema=output_schema)
    class ReturnBasicTypes:
        def process(
            self,
        ) -> Iterable[Tuple[int, float, bool, decimal.Decimal, str, bytes, bytearray]]:
            return [return_result]

    try:
        df = session.table_function(ReturnBasicTypes())
        assert df.columns == [x.upper() for x in output_schema]
        Utils.check_answer(df, [Row(*return_result)])
    finally:
        Utils.drop_function(session, f"{ReturnBasicTypes.name}()")


def test_output_type_date_time_timestamp(session):
    output_schema = ["time", "date", "timestamp"]
    return_result = (
        datetime.date(2022, 1, 25),
        datetime.time(12, 13, 14, 123000),
        datetime.datetime(2017, 1, 1, 12),
    )

    @udtf(output_schema=output_schema)
    class ReturnBasicTypes:
        def process(
            self,
        ) -> Iterable[Tuple[datetime.date, datetime.time, datetime.datetime]]:
            return [return_result]

    try:
        df = session.table_function(ReturnBasicTypes())
        assert df.columns == [x.upper() for x in output_schema]
        Utils.check_answer(df, [Row(*return_result)])
    finally:
        Utils.drop_function(session, f"{ReturnBasicTypes.name}()")


def test_output_variant_array_object(session):
    output_schema = [
        "variant",
        "string_array",
        "variant_array",
        "string_map",
        "variant_map",
    ]
    return_result = (1, ["a", "b"], ["v1", "v2"], {"k1": "v1"}, {"k2": "v2"})

    @udtf(output_schema=output_schema)
    class ReturnBasicTypes:
        def process(
            self,
        ) -> Iterable[
            Tuple[Variant, List[str], List[Variant], Dict[str, str], Dict[str, Variant]]
        ]:
            return [return_result]

    try:
        df = session.table_function(ReturnBasicTypes())
        assert df.columns == [x.upper() for x in output_schema]
        Utils.check_answer(
            df,
            [
                Row(
                    VARIANT="1",
                    STRING_ARRAY='[\n  "a",\n  "b"\n]',
                    VARIANT_ARRAY='[\n  "v1",\n  "v2"\n]',
                    STRING_MAP='{\n  "k1": "v1"\n}',
                    VARIANT_MAP='{\n  "k2": "v2"\n}',
                )
            ],
        )
    finally:
        Utils.drop_function(session, f"{ReturnBasicTypes.name}()")


def test_negative_non_exist_package(session):
    func_name = Utils.random_function_name()

    class MyWordCount:
        def process(self, s1: str) -> Iterable[Tuple[str, int]]:
            counter = Counter(s1.split())
            return counter.items()

    with pytest.raises(SnowparkSQLException) as exec_info:
        session.udtf.register(
            MyWordCount,
            ["word", "count"],
            name=func_name,
            replace=True,
            imports=["@non_exist_stage/a"],
        )
    assert "does not exist or not authorized" in exec_info.value.message
