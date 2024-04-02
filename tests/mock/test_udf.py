#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import os
import sys

import pytest

from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.session import Session
from snowflake.snowpark.types import IntegerType

session = Session(MockServerConnection())


@pytest.mark.localtest
def test_udf_cleanup_on_err():
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
