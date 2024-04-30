#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import os
import sys

import pytest

from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.functions import call_udf, col, lit
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import IntegerType


@pytest.mark.localtest
def test_udf_cleanup_on_err(session):
    cur_dir = os.path.dirname(os.path.realpath(__file__))
    test_file = os.path.join(cur_dir, "files", "udf_file.py")

    df = session.create_dataframe([[3, 4], [5, 6]]).to_df("a", "b")
    sys_path_copy = list(sys.path)

    mod5_udf = session.udf.register_from_file(
        test_file,
        "raise_err",
        return_type=IntegerType(),
        input_types=[IntegerType()],
        immutable=True,
    )
    assert isinstance(mod5_udf.func, tuple)
    with pytest.raises(RuntimeError):
        df.select(mod5_udf("a"), mod5_udf("b")).collect()
    assert (
        sys_path_copy == sys.path
    )  # assert sys.path is cleaned up after UDF exits on exception


a = [
    "/Users/aling/Projects/snowpark-python/tests/mock_unit/files",
    "/Users/aling/Projects/snowpark-python/tests/mock_unit",
    "/Users/aling/Projects/snowpark-python",
    "/Users/aling/opt/anaconda3/envs/snowpark-python-env310/bin",
    "/Users/aling/opt/anaconda3/envs/snowpark-python-env310/lib/python310.zip",
    "/Users/aling/opt/anaconda3/envs/snowpark-python-env310/lib/python3.10",
    "/Users/aling/opt/anaconda3/envs/snowpark-python-env310/lib/python3.10/lib-dynload",
    "/Users/aling/opt/anaconda3/envs/snowpark-python-env310/lib/python3.10/site-packages",
]
b = [
    "/Users/aling/Projects/snowpark-python/tests/mock_unit",
    "/Users/aling/Projects/snowpark-python",
    "/Users/aling/opt/anaconda3/envs/snowpark-python-env310/bin",
    "/Users/aling/opt/anaconda3/envs/snowpark-python-env310/lib/python310.zip",
    "/Users/aling/opt/anaconda3/envs/snowpark-python-env310/lib/python3.10",
    "/Users/aling/opt/anaconda3/envs/snowpark-python-env310/lib/python3.10/lib-dynload",
    "/Users/aling/opt/anaconda3/envs/snowpark-python-env310/lib/python3.10/site-packages",
]


@pytest.mark.localtest
def test_registering_udf_with_qualified_identifier(session):
    custom_schema = "test_identifier_schema"

    def add_fn(x: int, y: int) -> int:
        return x + y

    session.udf.register(add_fn, name=f"{custom_schema}.add")

    df = session.create_dataframe([[3, 4]], schema=["num1", "num2"])
    assert (
        df.select(call_udf(f"{custom_schema}.add", col("num1"), col("num2"))).collect()[
            0
        ][0]
        == 7
    )

    session.use_schema(custom_schema)
    assert df.select(call_udf("add", col("num1"), col("num2"))).collect()[0][0] == 7

    session.use_database("test_identifier_database")
    with pytest.raises(NotImplementedError):
        assert (
            df.select(
                call_udf(f"{custom_schema}.add", col("num1"), col("num2"))
            ).collect()[0][0]
            == 7
        )


@pytest.mark.localtest
def test_registering_sproc_with_qualified_identifier(session):
    custom_schema = "test_identifier_schema"
    session.use_database("mock_database")

    def increment_by_one_fn(session: Session, x: int) -> int:
        df = session.create_dataframe([[]]).select((lit(1) + lit(x)).as_("RESULT"))
        return df.collect()[0]["RESULT"]

    session.sproc.register(
        increment_by_one_fn, name=f"{custom_schema}.increment_by_one"
    )
    assert session.call(f"{custom_schema}.increment_by_one", 5) == 6

    session.use_schema(custom_schema)
    assert session.call("increment_by_one", 5) == 6

    session.use_database("test_identifier_database")
    with pytest.raises(SnowparkSQLException):
        assert session.call("increment_by_one", 5) == 6
