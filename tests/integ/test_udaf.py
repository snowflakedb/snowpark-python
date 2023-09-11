#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import datetime
import decimal
import logging
from typing import Any, Dict, List

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import udaf
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import IntegerType, Variant
from tests.utils import IS_IN_STORED_PROC, TestFiles, Utils

pytestmark = pytest.mark.udf


def test_basic_udaf(session):
    class PythonSumUDAFHandler:
        def __init__(self) -> None:
            self._sum = 0

        @property
        def aggregate_state(self):
            return self._sum

        def accumulate(self, input_value):
            self._sum += input_value

        def merge(self, other_sum):
            self._sum += other_sum

        def finish(self):
            return self._sum

    sum_udaf = udaf(
        PythonSumUDAFHandler,
        return_type=IntegerType(),
        input_types=[IntegerType()],
        immutable=True,
    )
    df = session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df("a", "b")
    Utils.check_answer(df.agg(sum_udaf("a")), [Row(6)])
    Utils.check_answer(df.group_by("a").agg(sum_udaf("b")), [Row(1, 7), Row(2, 11)])


# TODO: use data class as state. This triggers a bug in UDF server during pickling/unpickling of a state.
def test_int(session):
    @udaf
    class IntSum:
        def __init__(self) -> None:
            self._agg_state = 0

        @property
        def aggregate_state(self) -> int:
            return self._agg_state

        def accumulate(self, int_: int) -> None:
            self._agg_state += int_

        def merge(self, other_state: int) -> None:
            self._agg_state += other_state

        def finish(self) -> int:
            return self._agg_state

    df = session.create_dataframe([[1], [2], [3]]).to_df("int")
    Utils.check_answer(df.agg(IntSum("int")), [Row(6)])


def test_float(session):
    @udaf
    class FloatSum:
        def __init__(self) -> None:
            self._agg_state = 0

        @property
        def aggregate_state(self) -> float:
            return self._agg_state

        def accumulate(self, float_: float) -> None:
            self._agg_state += float_

        def merge(self, other_state: float) -> None:
            self._agg_state += other_state

        def finish(self) -> float:
            return self._agg_state

    df = session.create_dataframe([[1.1], [2.2], [3.3]]).to_df("float")
    Utils.check_answer(df.agg(FloatSum("float")), [Row(6.6)])


def test_bool(session):
    @udaf
    class BoolAnd:
        def __init__(self) -> None:
            self._agg_state = True

        @property
        def aggregate_state(self) -> bool:
            return self._agg_state

        def accumulate(self, bool_: bool) -> None:
            self._agg_state = self._agg_state and bool_

        def merge(self, other_state: bool) -> None:
            self._agg_state = self._agg_state and other_state

        def finish(self) -> bool:
            return self._agg_state

    df = session.create_dataframe([[True], [True], [False]]).to_df("bool")
    Utils.check_answer(df.agg(BoolAnd("bool")), [Row(False)])


def test_decimal(session):
    @udaf
    class DecimalSum:
        def __init__(self) -> None:
            self._agg_state = decimal.Decimal("0")

        @property
        def aggregate_state(self) -> decimal.Decimal:
            return self._agg_state

        def accumulate(self, decimal_: decimal.Decimal) -> None:
            self._agg_state += decimal_

        def merge(self, other_state: decimal.Decimal) -> None:
            self._agg_state += other_state

        def finish(self) -> decimal.Decimal:
            return self._agg_state

    df = session.create_dataframe(
        [[decimal.Decimal("1.1")], [decimal.Decimal("2.2")], [decimal.Decimal("3.3")]]
    ).to_df("decimal")
    Utils.check_answer(df.agg(DecimalSum("decimal")), [Row(decimal.Decimal("6.6"))])


def test_str(session):
    @udaf
    class StringMax:
        def __init__(self) -> None:
            self._agg_state = ""

        @property
        def aggregate_state(self) -> str:
            return self._agg_state

        def accumulate(self, str_: str) -> None:
            self._agg_state = max(self._agg_state, str_)

        def merge(self, other_state: str) -> None:
            self._agg_state = max(self._agg_state, other_state)

        def finish(self) -> str:
            return self._agg_state

    df = session.create_dataframe([["a"], ["b"], ["c"]]).to_df("str")
    Utils.check_answer(df.agg(StringMax("str")), [Row("c")])


def test_bytes(session):
    @udaf
    class BytesMax:
        def __init__(self) -> None:
            self._agg_state = b""

        @property
        def aggregate_state(self) -> bytes:
            return self._agg_state

        def accumulate(self, bytes_: bytes) -> None:
            self._agg_state = max(self._agg_state, bytes_)

        def merge(self, other_state: bytes) -> None:
            self._agg_state = max(self._agg_state, other_state)

        def finish(self) -> bytes:
            return self._agg_state

    df = session.create_dataframe([[b"a"], [b"b"], [b"c"]]).to_df("bytes")
    Utils.check_answer(df.agg(BytesMax("bytes")), [Row(b"c")])


def test_bytearray(session):
    @udaf
    class BytearrayMax:
        def __init__(self) -> None:
            self._agg_state = bytearray(b"")

        @property
        def aggregate_state(self) -> bytearray:
            return self._agg_state

        def accumulate(self, bytearray_: bytearray) -> None:
            self._agg_state = max(self._agg_state, bytearray_)

        def merge(self, other_state: bytearray) -> None:
            self._agg_state = max(self._agg_state, other_state)

        def finish(self) -> bytearray:
            return self._agg_state

    df = session.create_dataframe(
        [[bytearray(b"a")], [bytearray(b"b")], [bytearray(b"c")]]
    ).to_df("bytearray")
    Utils.check_answer(df.agg(BytearrayMax("bytearray")), [Row(bytearray(b"c"))])


def test_date(session):
    @udaf
    class DateMax:
        def __init__(self) -> None:
            self._agg_state = datetime.date(1970, 1, 1)

        @property
        def aggregate_state(self) -> datetime.date:
            return self._agg_state

        def accumulate(self, date_: datetime.date) -> None:
            self._agg_state = max(self._agg_state, date_)

        def merge(self, other_state: datetime.date) -> None:
            self._agg_state = max(self._agg_state, other_state)

        def finish(self) -> datetime.date:
            return self._agg_state

    df = session.create_dataframe(
        [
            [datetime.date(2023, 7, 1)],
            [datetime.date(2023, 7, 2)],
            [datetime.date(2023, 7, 3)],
        ]
    ).to_df("date")
    Utils.check_answer(df.agg(DateMax("date")), [Row(datetime.date(2023, 7, 3))])


def test_time(session):
    @udaf
    class TimeMax:
        def __init__(self) -> None:
            self._agg_state = datetime.time(0, 0, 0)

        @property
        def aggregate_state(self) -> datetime.time:
            return self._agg_state

        def accumulate(self, time_: datetime.time) -> None:
            self._agg_state = max(self._agg_state, time_)

        def merge(self, other_state: datetime.time) -> None:
            self._agg_state = max(self._agg_state, other_state)

        def finish(self) -> datetime.time:
            return self._agg_state

    df = session.create_dataframe(
        [[datetime.time(1, 1, 1)], [datetime.time(2, 2, 2)], [datetime.time(3, 3, 3)]]
    ).to_df("time")
    Utils.check_answer(df.agg(TimeMax("time")), [Row(datetime.time(3, 3, 3))])


def test_datetime(session):
    @udaf
    class DatetimeMax:
        def __init__(self) -> None:
            self._agg_state = datetime.datetime(1970, 1, 1, 0, 0, 0)

        @property
        def aggregate_state(self) -> datetime.datetime:
            return self._agg_state

        def accumulate(self, datetime_: datetime.datetime) -> None:
            self._agg_state = max(self._agg_state, datetime_)

        def merge(self, other_state: datetime.datetime) -> None:
            self._agg_state = max(self._agg_state, other_state)

        def finish(self) -> datetime.datetime:
            return self._agg_state

    df = session.create_dataframe(
        [
            [datetime.datetime(2023, 7, 1, 1, 1, 1)],
            [datetime.datetime(2023, 7, 2, 2, 2, 2)],
            [datetime.datetime(2023, 7, 3, 3, 3, 3)],
        ]
    ).to_df("datetime")
    Utils.check_answer(
        df.agg(DatetimeMax("datetime")), [Row(datetime.datetime(2023, 7, 3, 3, 3, 3))]
    )


def test_variant(session):
    @udaf
    class VariantMax:
        def __init__(self) -> None:
            self._agg_state = ""

        @property
        def aggregate_state(self) -> Variant:
            return self._agg_state

        def accumulate(self, variant: Variant) -> None:
            self._agg_state = max(self._agg_state, variant)

        def merge(self, other_state: Variant) -> None:
            self._agg_state = max(self._agg_state, other_state)

        def finish(self) -> Variant:
            return self._agg_state

    df = session.sql("select to_variant('v1') as v")
    Utils.check_answer(df.agg(VariantMax("v")), [Row('"v1"')])


def test_array(session):
    @udaf
    class ArrayAgg:
        def __init__(self) -> None:
            self._agg_state = []

        @property
        def aggregate_state(self) -> List[Any]:
            return self._agg_state

        def accumulate(
            self,
            list_: List[str],
            list_of_variants: List[Variant],
        ) -> None:
            self._agg_state += list_ + list_of_variants

        def merge(self, other_state: List[Any]) -> None:
            self._agg_state += other_state

        def finish(self) -> List[str]:
            return self._agg_state

    df = session.sql(
        "select array_construct('a1', 'a1') as a1, array_construct(to_variant('a2'), to_variant('a2')) as a2"
    )
    Utils.check_answer(
        df.agg(ArrayAgg("a1", "a2")),
        [Row('[\n  "a1",\n  "a1",\n  "a2",\n  "a2"\n]')],
    )


def test_object(session):
    @udaf
    class ObjectAgg:
        def __init__(self) -> None:
            self._agg_state = {}

        @property
        def aggregate_state(self) -> Dict[str, str]:
            return self._agg_state

        def accumulate(
            self,
            dict_: Dict[str, str],
            dict_str_to_variant: Dict[str, Variant],
        ) -> None:
            self._agg_state.update(dict_)
            self._agg_state.update(dict_str_to_variant)

        def merge(self, other_state: Dict[str, str]) -> None:
            self._agg_state.update(other_state)

        def finish(self) -> Dict[str, str]:
            return self._agg_state

    df = session.sql(
        "select object_construct('o1', 'o1') as o1, object_construct('o2', to_variant('v2')) as o2"
    )
    Utils.check_answer(
        df.agg(ObjectAgg("o1", "o2")),
        [Row('{\n  "o1": "o1",\n  "o2": "v2"\n}')],
    )


def test_register_udaf_from_file_without_type_hints(session, resources_path):
    test_files = TestFiles(resources_path)
    sum_udaf = session.udaf.register_from_file(
        test_files.test_udaf_py_file,
        "MyUDAFWithoutTypeHints",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        immutable=True,
    )
    df = session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df("a", "b")
    Utils.check_answer(df.agg(sum_udaf("a")), [Row(6)])
    Utils.check_answer(df.group_by("a").agg(sum_udaf("b")), [Row(1, 7), Row(2, 11)])


def test_register_udaf_from_file_with_type_hints(session, resources_path):
    test_files = TestFiles(resources_path)
    sum_udaf = session.udaf.register_from_file(
        test_files.test_udaf_py_file,
        "MyUDAFWithTypeHints",
    )
    df = session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df("a", "b")
    Utils.check_answer(df.agg(sum_udaf("a")), [Row(6)])
    Utils.check_answer(df.group_by("a").agg(sum_udaf("b")), [Row(1, 7), Row(2, 11)])


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_permanent_udaf_negative(session, db_parameters, caplog):
    stage_name = Utils.random_stage_name()
    udaf_name = Utils.random_name_for_temp_object(TempObjectType.AGGREGATE_FUNCTION)
    df1 = session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df("a", "b")

    class PythonSumUDAFHandler:
        def __init__(self) -> None:
            self._sum = 0

        @property
        def aggregate_state(self):
            return self._sum

        def accumulate(self, input_value):
            self._sum += input_value

        def merge(self, other_sum):
            self._sum += other_sum

        def finish(self):
            return self._sum

    with Session.builder.configs(db_parameters).create() as new_session:
        new_session.sql_simplifier_enabled = session.sql_simplifier_enabled
        df2 = new_session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df(
            "a", "b"
        )
        try:
            with caplog.at_level(logging.WARN):
                sum_udaf = udaf(
                    PythonSumUDAFHandler,
                    return_type=IntegerType(),
                    input_types=[IntegerType()],
                    name=udaf_name,
                    is_permanent=False,
                    stage_location=stage_name,
                    session=new_session,
                )
            assert (
                "is_permanent is False therefore stage_location will be ignored"
                in caplog.text
            )

            with pytest.raises(
                SnowparkSQLException, match=f"Unknown function {udaf_name}"
            ):
                df1.agg(sum_udaf("a")).collect()

            Utils.check_answer(df2.agg(sum_udaf("a")), [Row(6)])
        finally:
            new_session._run_query(f"drop function if exists {udaf_name}(int)")


def test_udaf_negative(session):
    with pytest.raises(TypeError, match="Invalid handler: expecting a class type"):
        session.udaf.register(1)

    class PythonSumUDAFHandler:
        def __init__(self) -> None:
            self._sum = 0

        @property
        def aggregate_state(self):
            return self._sum

        def accumulate(self, input_value):
            self._sum += input_value

        def merge(self, other_sum):
            self._sum += other_sum

        def finish(self):
            return self._sum

    sum_udaf = udaf(
        PythonSumUDAFHandler,
        return_type=IntegerType(),
        input_types=[IntegerType()],
    )
    df = session.create_dataframe([[1, 3], [1, 4], [2, 5], [2, 6]]).to_df("a", "b")

    with pytest.raises(TypeError, match="must be Column or column name"):
        df.agg(sum_udaf(1))

    with pytest.raises(
        ValueError, match="Incorrect number of arguments passed to the UDAF"
    ):
        df.agg(sum_udaf("a", "b"))
