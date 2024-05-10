#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import decimal
import sys
from typing import Tuple

import pytest

from snowflake.snowpark import Row, Table
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import lit, udtf
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import (
    BinaryType,
    BooleanType,
    DecimalType,
    DoubleType,
    FloatType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)
from snowflake.snowpark.udtf import UserDefinedTableFunction
from tests.utils import IS_IN_STORED_PROC, IS_NOT_ON_GITHUB, TestFiles, Utils

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

try:
    import pandas as pd

    is_pandas_available = True
    from snowflake.snowpark.types import PandasDataFrame, PandasDataFrameType
except ImportError:
    is_pandas_available = False

pytestmark = [
    pytest.mark.udf,
    pytest.mark.skipif(
        "config.getvalue('local_testing_mode')",
        reason="UDTF not supported in Local Testing",
    ),
]


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
        immutable=True,
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

    query_tag = f"QUERY_TAG_{Utils.random_alphanumeric_str(10)}"
    my_udtf_with_statement_params = session.udtf.register_from_file(
        test_files.test_udtf_py_file,
        "MyUDTFWithTypeHints",
        output_schema=schema,
        statement_params={"QUERY_TAG": query_tag},
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
    Utils.assert_executed_with_query_tag(session, query_tag)


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


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_apply_in_pandas(session):
    # test with element wise operation
    def convert(pdf):
        return pdf.assign(TEMP_F=lambda x: x.TEMP_C * 9 / 5 + 32)

    df = session.createDataFrame(
        [("SF", 21.0), ("SF", 17.5), ("SF", 24.0), ("NY", 30.9), ("NY", 33.6)],
        schema=["location", "temp_c"],
    )

    df = df.group_by("location").apply_in_pandas(
        convert,
        output_schema=StructType(
            [
                StructField("location", StringType()),
                StructField("temp_c", FloatType()),
                StructField("temp_f", FloatType()),
            ]
        ),
    )
    Utils.check_answer(
        df,
        [
            Row("SF", 24.0, 75.2),
            Row("SF", 17.5, 63.5),
            Row("SF", 21.0, 69.8),
            Row("NY", 30.9, 87.61999999999999),
            Row("NY", 33.6, 92.48),
        ],
    )

    # test with group wide opeartion
    df = session.createDataFrame(
        [(1, 1.0), (1, 2.0), (2, 3.0), (2, 5.0), (2, 10.0)], schema=["id", "v"]
    )

    def normalize(pdf):
        V = pdf.V
        return pdf.assign(V=(V - V.mean()) / V.std())

    df = df.group_by("id").applyInPandas(
        normalize,
        output_schema=StructType(
            [StructField("id", IntegerType()), StructField("v", DoubleType())]
        ),
    )

    Utils.check_answer(
        df,
        [
            Row(ID=1, V=0.7071067811865475),
            Row(ID=1, V=-0.7071067811865475),
            Row(ID=2, V=1.1094003924504583),
            Row(ID=2, V=-0.8320502943378437),
            Row(ID=2, V=-0.2773500981126146),
        ],
    )

    # test with multiple columns in group by
    df = session.createDataFrame(
        [("A", 2, 11.0), ("A", 2, 13.9), ("B", 5, 5.0), ("B", 2, 12.1)],
        schema=["grade", "division", "value"],
    )

    def group_sum(pdf):
        return pd.DataFrame(
            [
                (
                    pdf.GRADE.iloc[0],
                    pdf.DIVISION.iloc[0],
                    pdf.VALUE.sum(),
                )
            ]
        )

    df = df.group_by([df.grade, df.division]).applyInPandas(
        group_sum,
        output_schema=StructType(
            [
                StructField("grade", StringType()),
                StructField("division", IntegerType()),
                StructField("sum", DoubleType()),
            ]
        ),
    )
    Utils.check_answer(
        df,
        [
            Row(GRADE="A", DIVISION=2, SUM=24.9),
            Row(GRADE="B", DIVISION=2, SUM=12.1),
            Row(GRADE="B", DIVISION=5, SUM=5.0),
        ],
    )


@pytest.mark.skipif(IS_IN_STORED_PROC, reason="Cannot create session in SP")
def test_permanent_udtf_negative(session, db_parameters):
    stage_name = Utils.random_stage_name()
    udtf_name = Utils.random_name_for_temp_object(TempObjectType.TABLE_FUNCTION)

    class UDTFEcho:
        def process(
            self,
            num: int,
        ) -> Iterable[Tuple[int]]:
            return [(num,)]

    with Session.builder.configs(db_parameters).create() as new_session:
        new_session.sql_simplifier_enabled = session.sql_simplifier_enabled
        try:
            Utils.create_stage(session, stage_name, is_temporary=False)
            echo_udtf = udtf(
                UDTFEcho,
                output_schema=StructType([StructField("A", IntegerType())]),
                input_types=[IntegerType()],
                name=udtf_name,
                is_permanent=False,
                stage_location=stage_name,
                session=new_session,
            )

            with pytest.raises(
                SnowparkSQLException, match=f"Unknown table function {udtf_name}"
            ):
                session.table_function(echo_udtf(lit(1))).collect()

            Utils.check_answer(new_session.table_function(echo_udtf(lit(1))), [Row(1)])
        finally:
            new_session._run_query(f"drop function if exists {udtf_name}(int)")
            Utils.drop_stage(session, stage_name)


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
        immutable=True,
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
            def end_partition(self, df: pd.DataFrame) -> pd.DataFrame:
                result = df.describe().transpose()
                result.insert(loc=0, column="column_name", value=["col1", "col2"])
                return result

        my_udtf = udtf(
            TypeHintedHandler,
            output_schema=output_schema,
            input_types=input_types,
        )

    assert_vectorized_udtf_result(session.table(vectorized_udtf_test_table), my_udtf)


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
@pytest.mark.parametrize("from_file", [True, False])
def test_register_vectorized_udtf_process_basic(session, from_file, resources_path):
    data = [
        Row("x", 3, 35.9),
        Row("x", 9, 20.5),
        Row("x", 12, 93.8),
        Row("y", 5, 69.2),
        Row("y", 10, 94.3),
        Row("y", 15, 36.9),
        Row("y", 20, 85.4),
        Row("z", 10, 30.4),
        Row("z", 20, 85.9),
        Row("z", 30, 63.4),
        Row("z", 40, 35.8),
        Row("z", 50, 95.4),
    ]
    df = session.create_dataframe(data, schema=["id", "col1", "col2"])

    class BasicProcess:
        def process(self, df):
            return df

    output_schema = PandasDataFrameType(
        [StringType(), IntegerType(), FloatType()], ["_id", "_col1", "_col2"]
    )
    input_types = [PandasDataFrameType([StringType(), IntegerType(), FloatType()])]
    if from_file:
        process_udtf = session.udtf.register_from_file(
            TestFiles(resources_path).test_vectorized_udtf_py_file,
            "BasicProcess",
            output_schema=output_schema,
            input_types=input_types,
        )
    else:
        process_udtf = session.udtf.register(
            BasicProcess, output_schema=output_schema, input_types=input_types
        )
    Utils.check_answer(
        df.select(process_udtf("id", "col1", "col2")),
        data,
        statement_params={"PYTHON_UDTF_ENABLE_PROCESS_DATAFRAME_ENCODING": True},
    )


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
@pytest.mark.parametrize("from_file", [True, False])
def test_register_vectorized_udtf_process_basic_with_end_partition(
    session, from_file, resources_path
):
    data = [
        Row("x", 3, 35.9),
        Row("x", 9, 20.5),
        Row("x", 12, 93.8),
        Row("y", 5, 69.2),
        Row("y", 10, 94.3),
        Row("y", 15, 36.9),
        Row("y", 20, 85.4),
        Row("z", 10, 30.4),
        Row("z", 20, 85.9),
        Row("z", 30, 63.4),
        Row("z", 40, 35.8),
        Row("z", 50, 95.4),
    ]
    df = session.create_dataframe(data, schema=["id", "col1", "col2"])

    class BasicProcessWithEndPartition:
        def process(self, df):
            return df

        def end_partition(self):
            yield (["a", "b"], [42, 420], [12.3, 45.6])

    output_schema = PandasDataFrameType(
        [StringType(), IntegerType(), FloatType()], ["_id", "_col1", "_col2"]
    )
    input_types = [PandasDataFrameType([StringType(), IntegerType(), FloatType()])]
    if from_file:
        process_udtf = session.udtf.register_from_file(
            TestFiles(resources_path).test_vectorized_udtf_py_file,
            "BasicProcessWithEndPartition",
            output_schema=output_schema,
            input_types=input_types,
        )
    else:
        process_udtf = session.udtf.register(
            BasicProcessWithEndPartition,
            output_schema=output_schema,
            input_types=input_types,
        )
    expected_data = data.copy()
    expected_data.extend([Row("a", 42, 12.3), Row("b", 420, 45.6)] * 3)
    Utils.check_answer(
        df.select(process_udtf("id", "col1", "col2").over(partition_by="id")),
        expected_data,
        statement_params={"PYTHON_UDTF_ENABLE_PROCESS_DATAFRAME_ENCODING": True},
    )


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
@pytest.mark.parametrize("from_file", [True, False])
def test_register_vectorized_udtf_process_sum_rows(session, from_file, resources_path):
    data = [
        Row("x", 3, 35.9),
        Row("x", 9, 20.5),
        Row("x", 12, 93.8),
        Row("y", 5, 69.2),
        Row("y", 10, 94.3),
        Row("y", 15, 36.9),
        Row("y", 20, 85.4),
        Row("z", 10, 30.4),
        Row("z", 20, 85.9),
        Row("z", 30, 63.4),
        Row("z", 40, 35.8),
        Row("z", 50, 95.4),
    ]
    df = session.create_dataframe(data, schema=["id", "col1", "col2"])

    class SumRows:
        def __init__(self) -> None:
            self.sum = None

        def process(self, df):
            if self.sum is None:
                self.sum = df
            else:
                self.sum += df
            return df

        def end_partition(self):
            return self.sum

    output_schema = PandasDataFrameType([StringType(), IntegerType()], ["_id", "_col1"])
    input_types = [PandasDataFrameType([StringType(), IntegerType()])]
    if from_file:
        process_udtf = session.udtf.register_from_file(
            TestFiles(resources_path).test_vectorized_udtf_py_file,
            "SumRows",
            output_schema=output_schema,
            input_types=input_types,
        )
    else:
        process_udtf = session.udtf.register(
            SumRows,
            output_schema=output_schema,
            input_types=input_types,
            max_batch_size=1,
        )
    expected_data = [Row(row[0], row[1]) for row in data]
    expected_data.extend([Row("xxx", 24), Row("yyyy", 50), Row("zzzzz", 150)])
    Utils.check_answer(
        df.select(process_udtf("id", "col1").over(partition_by="id")),
        expected_data,
        statement_params={"PYTHON_UDTF_ENABLE_PROCESS_DATAFRAME_ENCODING": True},
    )


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
@pytest.mark.parametrize("from_file", [True, False])
def test_register_vectorized_udtf_process_max_batch_size(
    session, from_file, resources_path
):
    data = [
        Row("x", 3, 35.9),
        Row("x", 9, 20.5),
        Row("x", 12, 93.8),
        Row("y", 5, 69.2),
        Row("y", 10, 94.3),
        Row("y", 15, 36.9),
        Row("y", 20, 85.4),
        Row("z", 10, 30.4),
        Row("z", 20, 85.9),
        Row("z", 30, 63.4),
        Row("z", 40, 35.8),
        Row("z", 50, 95.4),
    ]
    df = session.create_dataframe(data, schema=["id", "col1", "col2"])

    class BatchSize:
        def process(self, df):
            return ([len(df)] * len(df),)

    output_schema = PandasDataFrameType([IntegerType()], ["batch_size"])
    input_types = [PandasDataFrameType([StringType(), IntegerType(), FloatType()])]
    if from_file:
        process_udtf = session.udtf.register_from_file(
            TestFiles(resources_path).test_vectorized_udtf_py_file,
            "BatchSize",
            output_schema=output_schema,
            input_types=input_types,
        )
    else:
        process_udtf = session.udtf.register(
            BatchSize,
            output_schema=output_schema,
            input_types=input_types,
            max_batch_size=4,
        )
    expected_data = [Row(3)] * 3 + [Row(4)] * 8 + [Row(1)]
    Utils.check_answer(
        df.select(process_udtf("id", "col1", "col2").over(partition_by="id")),
        expected_data,
        statement_params={"PYTHON_UDTF_ENABLE_PROCESS_DATAFRAME_ENCODING": True},
    )


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_register_vectorized_udtf_process_with_output_schema(session):
    data = [
        Row("x", 3, 35.9),
        Row("x", 9, 20.5),
        Row("x", 12, 93.8),
        Row("y", 5, 69.2),
        Row("y", 10, 94.3),
        Row("y", 15, 36.9),
        Row("y", 20, 85.4),
        Row("z", 10, 30.4),
        Row("z", 20, 85.9),
        Row("z", 30, 63.4),
        Row("z", 40, 35.8),
        Row("z", 50, 95.4),
    ]
    df = session.create_dataframe(data, schema=["id", "col1", "col2"])
    expected_data = [Row(row[0], row[1]) for row in data]
    expected_data.extend([Row("xxx", 24), Row("yyyy", 50), Row("zzzzz", 150)])

    # using output_schema
    class SumRowsNoAnnotations:
        def __init__(self) -> None:
            self.sum = None

        def process(self, df):
            if self.sum is None:
                self.sum = df
            else:
                self.sum += df
            return df

        def end_partition(self):
            return self.sum

    output_schema = PandasDataFrameType([StringType(), IntegerType()], ["_id", "_col1"])
    input_types = [PandasDataFrameType([StringType(), IntegerType()])]
    process_udtf = session.udtf.register(
        SumRowsNoAnnotations,
        output_schema=output_schema,
        input_types=input_types,
        max_batch_size=1,
    )
    Utils.check_answer(
        df.select(process_udtf("id", "col1").over(partition_by="id")),
        expected_data,
        statement_params={"PYTHON_UDTF_ENABLE_PROCESS_DATAFRAME_ENCODING": True},
    )


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_register_vectorized_udtf_process_with_type_hints(session):
    data = [
        Row("x", 3, 35.9),
        Row("x", 9, 20.5),
        Row("x", 12, 93.8),
        Row("y", 5, 69.2),
        Row("y", 10, 94.3),
        Row("y", 15, 36.9),
        Row("y", 20, 85.4),
        Row("z", 10, 30.4),
        Row("z", 20, 85.9),
        Row("z", 30, 63.4),
        Row("z", 40, 35.8),
        Row("z", 50, 95.4),
    ]
    df = session.create_dataframe(data, schema=["id", "col1", "col2"])
    expected_data = [Row(row[0], row[1]) for row in data]
    expected_data.extend([Row("xxx", 24), Row("yyyy", 50), Row("zzzzz", 150)])

    # using type_hints - PandasDataFrame
    class SumRowsFullAnnotations:
        def __init__(self) -> None:
            self.sum = None

        def process(self, df: PandasDataFrame[str, int]) -> PandasDataFrame[str, int]:
            if self.sum is None:
                self.sum = df
            else:
                self.sum += df
            return df

        def end_partition(self):
            return self.sum

    output_schema = ["_id", "_col1"]
    process_udtf = session.udtf.register(
        SumRowsFullAnnotations,
        output_schema=output_schema,
        max_batch_size=1,
    )
    Utils.check_answer(
        df.select(process_udtf("id", "col1").over(partition_by="id")),
        expected_data,
        statement_params={"PYTHON_UDTF_ENABLE_PROCESS_DATAFRAME_ENCODING": True},
    )


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_register_vectorized_udtf_process_with_type_hints_and_output_schema(session):
    data = [
        Row("x", 3, 35.9),
        Row("x", 9, 20.5),
        Row("x", 12, 93.8),
        Row("y", 5, 69.2),
        Row("y", 10, 94.3),
        Row("y", 15, 36.9),
        Row("y", 20, 85.4),
        Row("z", 10, 30.4),
        Row("z", 20, 85.9),
        Row("z", 30, 63.4),
        Row("z", 40, 35.8),
        Row("z", 50, 95.4),
    ]
    df = session.create_dataframe(data, schema=["id", "col1", "col2"])
    expected_data = [Row(row[0], row[1]) for row in data]
    expected_data.extend([Row("xxx", 24), Row("yyyy", 50), Row("zzzzz", 150)])

    # using type_hints and output_schema - pd.DataFrame
    class SumRowsPartialAnnotations:
        def __init__(self) -> None:
            self.sum = None

        def process(self, df: pd.DataFrame) -> pd.DataFrame:
            if self.sum is None:
                self.sum = df
            else:
                self.sum += df
            return df

        def end_partition(self):
            return self.sum

    output_schema = StructType(
        [StructField("_id", StringType()), StructField("_col1", IntegerType())]
    )
    input_types = [StringType(), IntegerType()]
    process_udtf = session.udtf.register(
        SumRowsPartialAnnotations,
        output_schema=output_schema,
        input_types=input_types,
        max_batch_size=1,
    )
    Utils.check_answer(
        df.select(process_udtf("id", "col1").over(partition_by="id")),
        expected_data,
        statement_params={"PYTHON_UDTF_ENABLE_PROCESS_DATAFRAME_ENCODING": True},
    )


def test_udtf_comment(session):
    comment = f"COMMENT_{Utils.random_alphanumeric_str(6)}"

    class EchoUDTF:
        def process(
            self,
            num: int,
        ) -> Iterable[Tuple[int]]:
            return [(num,)]

    echo_udtf = session.udtf.register(
        EchoUDTF,
        output_schema=["num"],
        comment=comment,
    )

    ddl_sql = f"select get_ddl('FUNCTION', '{echo_udtf.name}(number)')"
    assert comment in session.sql(ddl_sql).collect()[0][0]


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


@pytest.mark.skipif(IS_NOT_ON_GITHUB, reason="need resources")
def test_udtf_external_access_integration(session, db_parameters):
    try:

        @udtf(
            output_schema=["num"],
            packages=["requests", "snowflake-snowpark-python"],
            external_access_integrations=[
                db_parameters["external_access_integration1"],
                db_parameters["external_access_integration2"],
            ],
            secrets={
                "cred": f"{db_parameters['external_access_key1']}",
                "cred_2": f"{db_parameters['external_access_key2']}",
            },
        )
        class UDTFEcho:
            def process(
                self,
                num: int,
            ) -> Iterable[Tuple[int]]:
                import _snowflake
                import requests

                token = _snowflake.get_generic_secret_string("cred")
                token_2 = _snowflake.get_generic_secret_string("cred_2")
                if (
                    token == "replace-with-your-api-key"
                    and token_2 == "replace-with-your-api-key_2"
                    and requests.get("https://www.google.com").status_code == 200
                    and requests.get("https://www.microsoft.com").status_code == 200
                ):
                    return [(1,)]
                else:
                    return [(0,)]

        df = session.table_function(UDTFEcho(lit("1").cast("int")))
        Utils.check_answer(
            df,
            [Row(1)],
        )
    except KeyError:
        pytest.skip("External Access Integration is not supported on the deployment.")
