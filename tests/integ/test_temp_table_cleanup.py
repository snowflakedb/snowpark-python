#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import copy
import gc

import pytest

from snowflake.snowpark.functions import col

pytestmark = [
    pytest.mark.skipif(
        "config.getoption('local_testing_mode', default=False)",
        reason="Temp table cleanup is not supported in Local Testing",
    ),
]


def test_reference_count_map_basic(session):
    df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).cache_result()
    table_name = df1.table_name
    df1.collect()
    assert session._temp_table_ref_count_map[table_name] == 1
    df2 = df1.select("*").filter(col("a") == 1)
    df2.collect()
    assert session._temp_table_ref_count_map[table_name] == 1
    df3 = df1.union_all(df2)
    df3.collect()
    assert session._temp_table_ref_count_map[table_name] == 1
    del df1
    gc.collect()
    assert session._temp_table_ref_count_map[table_name] == 1
    del df2
    gc.collect()
    assert session._temp_table_ref_count_map[table_name] == 1
    del df3
    gc.collect()
    assert table_name not in session._temp_table_ref_count_map


@pytest.mark.parametrize(
    "copy_function",
    [
        lambda x: copy.copy(x),
        lambda x: x.alias("alias"),
        lambda x: x.na.replace(1, 2, subset=[]),
    ],
)
def test_reference_count_map_copy(session, copy_function):
    df1 = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]).cache_result()
    table_name = df1.table_name
    df1.collect()
    assert session._temp_table_ref_count_map[table_name] == 1
    df2 = copy_function(df1).select("*").filter(col("a") == 1)
    df2.collect()
    assert session._temp_table_ref_count_map[table_name] == 2
    del df1
    gc.collect()
    assert session._temp_table_ref_count_map[table_name] == 1
    del df2
    gc.collect()
    assert table_name not in session._temp_table_ref_count_map
