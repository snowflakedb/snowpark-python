#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import decimal
import sys
from typing import Tuple

import pytest

from snowflake.snowpark import Row, Table
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import lit, udtf
from snowflake.snowpark.types import (
    BinaryType,
    BooleanType,
    DecimalType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from snowflake.snowpark.udtf import UserDefinedTableFunction
from tests.utils import IS_IN_STORED_PROC, TestFiles, Utils

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

try:
    import pandas

    is_pandas_available = True
    from snowflake.snowpark.types import PandasDataFrame, PandasDataFrameType
except ImportError:
    is_pandas_available = False

pytestmark = pytest.mark.udf


@pytest.fixture(scope="module")
def vectorized_udtf_test_table(session) -> str:
    # Input tabular data
    table_name = Utils.random_table_name()
    session.create_dataframe(
        [
            ("x", 3, 35.9),
            ("x", 9, 20.5),
            ("x", 12, 93.8),
            ("x", 15, 95.4),
            ("y", 5, 69.2),
            ("y", 10, 94.3),
            ("y", 15, 36.9),
            ("y", 20, 85.4),
            ("z", 10, 30.4),
            ("z", 20, 85.9),
            ("z", 30, 63.4),
            ("z", 40, 35.8),
        ],
        schema=StructType(
            [
                StructField("id", StringType()),
                StructField("col1", IntegerType()),
                StructField("col2", FloatType()),
            ]
        ),
    ).write.save_as_table(table_name, table_type="temporary")
    yield table_name


def test_register_udtf_from_file_no_type_hints(session, resources_path):
    test_files = TestFiles(resources_path)
    schema = StructType(
        [
            StructField("int_", IntegerType()),
            StructField("float_", FloatType()),
            StructField("bool_", BooleanType()),
            StructField("decimal_", DecimalType(10, 2)),
            StructField("str_", StringType()),
            StructField("bytes_", BinaryType()),
            StructField("bytearray_", BinaryType()),
        ]
    )
    my_udtf = session.udtf.register_from_file(
        test_files.test_udtf_py_file,
        "MyUDTFWithoutTypeHints",
        output_schema=schema,
        input_types=[
            IntegerType(),
            FloatType(),
            BooleanType(),
            DecimalType(10, 2),
            StringType(),
            BinaryType(),
            BinaryType(),
        ],
    )
    assert isinstance(my_udtf.handler, tuple)
    df = session.table_function(
        my_udtf(
            lit(1),
            lit(2.2),
            lit(True),
            lit(decimal.Decimal("3.33")).cast("number(10, 2)"),
            lit("python"),
            lit(b"bytes"),
            lit(bytearray("bytearray", "utf-8")),
        )
    )
    Utils.check_answer(
        df,
        [
            Row(
                1,
                2.2,
                True,
                decimal.Decimal("3.33"),
                "python",
                b"bytes",
                bytearray("bytearray", "utf-8"),
            )
        ],
    )


def test_register_udtf_from_file_with_typehints(session, resources_path):
    test_files = TestFiles(resources_path)
    schema = ["int_", "float_", "bool_", "decimal_", "str_", "bytes_", "bytearray_"]
    my_udtf = session.udtf.register_from_file(
        test_files.test_udtf_py_file,
        "MyUDTFWithTypeHints",
        output_schema=schema,
    )
    assert isinstance(my_udtf.handler, tuple)
    df = session.table_function(
        my_udtf(
            lit(1),
            lit(2.2),
            lit(True),
            lit(decimal.Decimal("3.33")),
            lit("python"),
            lit(b"bytes"),
            lit(bytearray("bytearray", "utf-8")),
        )
    )
    Utils.check_answer(
        df,
        [
            Row(
                1,
                2.2,
                True,
                decimal.Decimal("3.33"),
                "python",
                b"bytes",
                bytearray("bytearray", "utf-8"),
            )
        ],
    )

    my_udtf_with_statement_params = session.udtf.register_from_file(
        test_files.test_udtf_py_file,
        "MyUDTFWithTypeHints",
        output_schema=schema,
        statement_params={"SF_PARTNER": "FAKE_PARTNER"},
    )
    assert isinstance(my_udtf_with_statement_params.handler, tuple)
    df = session.table_function(
        my_udtf_with_statement_params(
            lit(1),
            lit(2.2),
            lit(True),
            lit(decimal.Decimal("3.33")),
            lit("python"),
            lit(b"bytes"),
            lit(bytearray("bytearray", "utf-8")),
        )
    )
    Utils.check_answer(
        df,
        [
            Row(
                1,
                2.2,
                True,
                decimal.Decimal("3.33"),
                "python",
                b"bytes",
                bytearray("bytearray", "utf-8"),
            )
        ],
    )


def test_strict_udtf(session):
    @udtf(output_schema=["num"], strict=True)
    class UDTFEcho:
        def process(
            self,
            num: int,
        ) -> Iterable[Tuple[int]]:
            if num is None:
                raise ValueError("num should not be None")
            return [(num,)]

    df = session.table_function(UDTFEcho(lit(None).cast("int")))
    Utils.check_answer(
        df,
        [Row(None)],
    )


def test_udtf_negative(session):
    with pytest.raises(TypeError, match="Invalid function: not a function or callable"):
        udtf(
            1,
            output_schema=StructType([StructField("col1", IntegerType())]),
            input_types=[IntegerType()],
        )

    with pytest.raises(
        ValueError, match="'output_schema' must be a list of column names or StructType"
    ):

        @udtf(output_schema=18)
        class UDTFOutputSchemaTest:
            def process(self, num: int) -> Iterable[Tuple[int]]:
                return (num,)

    with pytest.raises(
        ValueError, match="name must be specified for permanent table function"
    ):

        @udtf(output_schema=["num"], is_permanent=True)
        class UDTFEcho:
            def process(
                self,
                num: int,
            ) -> Iterable[Tuple[int]]:
                return [(num,)]

    with pytest.raises(ValueError, match="file_path.*does not exist"):
        session.udtf.register_from_file(
            "fake_path",
            "MyUDTFWithTypeHints",
            output_schema=[
                "int_",
                "float_",
                "bool_",
                "decimal_",
                "str_",
                "bytes_",
                "bytearray_",
            ],
        )


def test_secure_udtf(session):
    @udtf(output_schema=["num"], secure=True)
    class UDTFEcho:
        def process(
            self,
            num: int,
        ) -> Iterable[Tuple[int]]:
            return [(num,)]

    df = session.table_function(UDTFEcho(lit(1)))
    Utils.check_answer(
        df,
        [Row(1)],
    )
    ddl_sql = f"select get_ddl('function', '{UDTFEcho.name}(int)')"
    assert "SECURE" in session.sql(ddl_sql).collect()[0][0]


@pytest.mark.xfail(reason="SNOW-757054 flaky test", strict=False)
@pytest.mark.skipif(
    IS_IN_STORED_PROC, reason="Named temporary udf is not supported in stored proc"
)
def test_if_not_exists_udtf(session):
    @udtf(name="test_if_not_exists", output_schema=["num"], if_not_exists=True)
    class UDTFEcho:
        def process(
            self,
            num: int,
        ) -> Iterable[Tuple[int]]:
            return [(num,)]

    df = session.table_function(UDTFEcho(lit(1)))
    Utils.check_answer(
        df,
        [Row(1)],
    )

    # register UDTF with updated return value and don't expect changes
    @udtf(name="test_if_not_exists", output_schema=["num"], if_not_exists=True)
    class UDTFEcho:
        def process(
            self,
            num: int,
        ) -> Iterable[Tuple[int]]:
            return [(num + 1,)]

    df = session.table_function(UDTFEcho(lit(1)))
    Utils.check_answer(
        df,
        [Row(1)],
    )

    # error is raised when we try to recreate udtf without if_not_exists set
    with pytest.raises(SnowparkSQLException, match="already exists"):

        @udtf(name="test_if_not_exists", output_schema=["num"], if_not_exists=False)
        class UDTFEcho:
            def process(
                self,
                num: int,
            ) -> Iterable[Tuple[int]]:
                return [(num,)]

    # error is raised when we try to recreate udtf without if_not_exists set
    with pytest.raises(
        ValueError,
        match="options replace and if_not_exists are incompatible",
    ):

        @udtf(
            name="test_if_not_exists",
            output_schema=["num"],
            replace=True,
            if_not_exists=True,
        )
        class UDTFEcho:
            def process(
                self,
                num: int,
            ) -> Iterable[Tuple[int]]:
                return [(num,)]


def assert_vectorized_udtf_result(source_table: Table, udtf: UserDefinedTableFunction):
    # Assert
    Utils.check_answer(
        source_table.select(udtf("id", "col1", "col2").over(partition_by=["id"])),
        [
            Row(
                COLUMN_NAME="col1",
                COUNT=4,
                MEAN=12.5,
                STD=6.454972243679028,
                MIN=5.0,
                Q1=8.75,
                MEDIAN=12.5,
                Q3=16.25,
                MAX=20.0,
            ),
            Row(
                COLUMN_NAME="col2",
                COUNT=4,
                MEAN=71.45,
                STD=25.268491578775865,
                MIN=36.9,
                Q1=61.125,
                MEDIAN=77.30000000000001,
                Q3=87.625,
                MAX=94.3,
            ),
            Row(
                COLUMN_NAME="col1",
                COUNT=4,
                MEAN=25.0,
                STD=12.909944487358056,
                MIN=10.0,
                Q1=17.5,
                MEDIAN=25.0,
                Q3=32.5,
                MAX=40.0,
            ),
            Row(
                COLUMN_NAME="col2",
                COUNT=4,
                MEAN=53.875,
                STD=25.781824993588025,
                MIN=30.4,
                Q1=34.449999999999996,
                MEDIAN=49.599999999999994,
                Q3=69.025,
                MAX=85.9,
            ),
            Row(
                COLUMN_NAME="col1",
                COUNT=4,
                MEAN=9.75,
                STD=5.123475382979799,
                MIN=3.0,
                Q1=7.5,
                MEDIAN=10.5,
                Q3=12.75,
                MAX=15.0,
            ),
            Row(
                COLUMN_NAME="col2",
                COUNT=4,
                MEAN=61.4,
                STD=38.853657056532874,
                MIN=20.5,
                Q1=32.05,
                MEDIAN=64.85,
                Q3=94.2,
                MAX=95.4,
            ),
        ],
    )


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
@pytest.mark.parametrize("from_file", [True, False])
def test_register_vectorized_udtf_with_output_schema(
    session, vectorized_udtf_test_table, from_file, resources_path
):
    """Test registering and executing a basic vectorized UDTF by specifying input/output types using `input_types` and `input_types`."""

    output_schema = PandasDataFrameType(
        [
            StringType(),
            IntegerType(),
            FloatType(),
            FloatType(),
            FloatType(),
            FloatType(),
            FloatType(),
            FloatType(),
            FloatType(),
        ],
        ["column_name", "count", "mean", "std", "min", "q1", "median", "q3", "max"],
    )
    input_types = [PandasDataFrameType([StringType(), IntegerType(), FloatType()])]

    if from_file:
        my_udtf = session.udtf.register_from_file(
            TestFiles(resources_path).test_vectorized_udtf_py_file,
            "Handler",
            output_schema=output_schema,
            input_types=input_types,
        )
    else:

        class Handler:
            def end_partition(self, df):
                result = df.describe().transpose()
                result.insert(loc=0, column="column_name", value=["col1", "col2"])
                return result

        my_udtf = udtf(
            Handler,
            output_schema=output_schema,
            input_types=input_types,
        )

    assert_vectorized_udtf_result(session.table(vectorized_udtf_test_table), my_udtf)


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_register_vectorized_udtf_with_type_hints_only(
    session, vectorized_udtf_test_table
):
    """
    Test registering and executing a basic vectorized UDTF by specifying input/output type information using type hints only.
    This case cannot be directly registered from file since it requires the UDF to import snowflake.snowpark.PandasDataFrame.
    """

    class Handler:
        def end_partition(
            self, df: PandasDataFrame[str, int, float]
        ) -> PandasDataFrame[str, int, float, float, float, float, float, float, float]:
            result = df.describe().transpose()
            result.insert(loc=0, column="column_name", value=["col1", "col2"])
            return result

    my_udtf = udtf(
        Handler,
        output_schema=[
            "column_name",
            "count",
            "mean",
            "std",
            "min",
            "q1",
            "median",
            "q3",
            "max",
        ],
    )

    assert_vectorized_udtf_result(session.table(vectorized_udtf_test_table), my_udtf)


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
@pytest.mark.parametrize("from_file", [True, False])
def test_register_vectorized_udtf_with_type_hints_and_output_schema(
    session, vectorized_udtf_test_table, from_file, resources_path
):
    """
    Test registering and executing a basic vectorized UDTF by specifying type information using both type hints as well as `output_schema` and `input_types`.
    """

    output_schema = StructType(
        [
            StructField("column_name", StringType()),
            StructField("count", IntegerType()),
            StructField("mean", FloatType()),
            StructField("std", FloatType()),
            StructField("min", FloatType()),
            StructField("q1", FloatType()),
            StructField("median", FloatType()),
            StructField("q3", FloatType()),
            StructField("max", FloatType()),
        ]
    )
    input_types = [StringType(), IntegerType(), FloatType()]

    if from_file:
        my_udtf = session.udtf.register_from_file(
            TestFiles(resources_path).test_vectorized_udtf_py_file,
            "TypeHintedHandler",
            output_schema=output_schema,
            input_types=input_types,
        )
    else:

        class TypeHintedHandler:
            def end_partition(self, df: pandas.DataFrame) -> pandas.DataFrame:
                result = df.describe().transpose()
                result.insert(loc=0, column="column_name", value=["col1", "col2"])
                return result

        my_udtf = udtf(
            TypeHintedHandler,
            output_schema=output_schema,
            input_types=input_types,
        )

    assert_vectorized_udtf_result(session.table(vectorized_udtf_test_table), my_udtf)


@pytest.mark.parametrize("from_file", [True, False])
@pytest.mark.parametrize(
    "output_schema",
    [
        [
            "int_",
        ],
        StructType([StructField("int_", IntegerType())]),
    ],
)
def test_register_udtf_from_type_hints_where_process_returns_None(
    session, resources_path, from_file, output_schema
):
    test_files = TestFiles(resources_path)
    if from_file:
        my_udtf = session.udtf.register_from_file(
            test_files.test_udtf_py_file,
            "ProcessReturnsNone",
            output_schema=output_schema,
        )
        assert isinstance(my_udtf.handler, tuple)
    else:

        class ProcessReturnsNone:
            def process(self, a: int, b: int, c: int) -> None:
                pass

            def end_partition(self) -> Iterable[Tuple[int]]:
                yield (1,)

        my_udtf = udtf(
            ProcessReturnsNone,
            output_schema=output_schema,
        )

    df = session.table_function(
        my_udtf(
            lit(1),
            lit(2),
            lit(3),
        )
    )
    Utils.check_answer(df, [Row(INT_=1)])
