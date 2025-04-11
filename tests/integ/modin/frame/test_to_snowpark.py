#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import re

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.modin.plugin._internal.utils import (
    extract_pandas_label_from_snowflake_quoted_identifier,
)
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import DoubleType, LongType, StringType
from tests.integ.utils.sql_counter import sql_count_checker
from tests.utils import Utils


@pytest.fixture(scope="function")
def tmp_table_basic(session):
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    Utils.create_table(
        session, table_name, "id integer, foot_size float, shoe_model varchar"
    )
    session.sql(f"insert into {table_name} values (1, 32.0, 'medium')").collect()
    session.sql(f"insert into {table_name} values (2, 27.0, 'small')").collect()
    session.sql(f"insert into {table_name} values (3, 40.0, 'large')").collect()

    try:
        yield table_name
    finally:
        Utils.drop_table(session, table_name)


@pytest.fixture(scope="function")
def native_pandas_df_basic():
    native_df = native_pd.DataFrame(
        {
            "ID": [1, 2, 3],
            "FOOT_SIZE": [32.0, 27.0, 40.0],
            "SHOE_MODEL": ["medium", "small", "large"],
        }
    )
    native_df = native_df.set_index("ID")
    return native_df


@pytest.mark.parametrize("index", [True, False])
@sql_count_checker(query_count=3)
def test_to_snowpark_with_read_snowflake(tmp_table_basic, index) -> None:
    snow_dataframe = pd.read_snowflake(tmp_table_basic)
    index_label = None
    if index:
        index_label = "row_number"
    snowpark_df = snow_dataframe.to_snowpark(index=index, index_label=index_label)

    # verify the default index column row position column is included
    start = 0
    if index:
        assert snowpark_df.schema[start].column_identifier == '"row_number"'
        assert isinstance(snowpark_df.schema[start].datatype, LongType)
        start += 1

    # verify the rest of data columns are included
    assert snowpark_df.schema[start].column_identifier.name == "ID"
    assert snowpark_df.schema[start].column_identifier.quoted_name == '"ID"'
    assert isinstance(snowpark_df.schema[start].datatype, LongType)
    assert snowpark_df.schema[start + 1].column_identifier.name == "FOOT_SIZE"
    assert snowpark_df.schema[start + 1].column_identifier.quoted_name == '"FOOT_SIZE"'
    assert isinstance(snowpark_df.schema[start + 1].datatype, DoubleType)
    assert snowpark_df.schema[start + 2].column_identifier.name == "SHOE_MODEL"
    assert snowpark_df.schema[start + 2].column_identifier.quoted_name == '"SHOE_MODEL"'
    assert isinstance(snowpark_df.schema[start + 2].datatype, StringType)

    # verify the snowpark content
    res = (
        snowpark_df.order_by("ID")
        .select(['"ID"', '"FOOT_SIZE"', '"SHOE_MODEL"'])
        .collect()
    )
    assert res[0] == Row(ID=1, FOOT_SIZE=32.0, SHOE_MODEL="medium")
    assert res[1] == Row(ID=2, FOOT_SIZE=27.0, SHOE_MODEL="small")
    assert res[2] == Row(ID=3, FOOT_SIZE=40.0, SHOE_MODEL="large")


@sql_count_checker(query_count=1)
def test_to_snowpark_from_pandas_df(native_pandas_df_basic) -> None:
    snow_dataframe = pd.DataFrame(native_pandas_df_basic)
    snowpark_df = snow_dataframe.to_snowpark()

    # verify the index column is included
    assert snowpark_df.schema[0].column_identifier.quoted_name == '"ID"'
    assert isinstance(snowpark_df.schema[0].datatype, LongType)

    # verify the rest of data columns are included
    assert snowpark_df.schema[1].column_identifier.name == "FOOT_SIZE"
    assert snowpark_df.schema[1].column_identifier.quoted_name == '"FOOT_SIZE"'
    assert isinstance(snowpark_df.schema[1].datatype, DoubleType)
    assert snowpark_df.schema[2].column_identifier.name == "SHOE_MODEL"
    assert snowpark_df.schema[2].column_identifier.quoted_name == '"SHOE_MODEL"'
    assert isinstance(snowpark_df.schema[2].datatype, StringType)

    # verify the snowpark content
    res = (
        snowpark_df.order_by("ID")
        .select(['"ID"', '"FOOT_SIZE"', '"SHOE_MODEL"'])
        .collect()
    )
    assert res[0] == Row(ID=1, FOOT_SIZE=32.0, SHOE_MODEL="medium")
    assert res[1] == Row(ID=2, FOOT_SIZE=27.0, SHOE_MODEL="small")
    assert res[2] == Row(ID=3, FOOT_SIZE=40.0, SHOE_MODEL="large")


@pytest.mark.parametrize("index", [True, False])
@pytest.mark.parametrize("index_labels", [None, ["index1", "index2"]])
@sql_count_checker(query_count=0)
def test_to_snowpark_with_multiindex(tmp_table_basic, index, index_labels) -> None:
    multiindex = native_pd.MultiIndex.from_arrays(
        [[1, 1, 2, 2], ["red", "blue", "red", "blue"]], names=("number", "color")
    )
    native_df = native_pd.DataFrame(
        [[1] * 2, [2] * 2, [3] * 2, [4] * 2], index=multiindex, columns=["a", "b"]
    )
    snow_dataframe = pd.DataFrame(native_df)
    snowpark_df = snow_dataframe.to_snowpark(index, index_labels)

    start = 0
    if index:
        # verify index columns are first couple schema
        for (i, index_label) in enumerate(multiindex.names):
            quoted_identifier = snowpark_df.schema[i].column_identifier.quoted_name
            label_to_match = index_label if not index_labels else index_labels[i]
            assert (
                label_to_match
                == extract_pandas_label_from_snowflake_quoted_identifier(
                    quoted_identifier
                )
            )
            start += 1
    # verify the data columns
    assert snowpark_df.schema[start].column_identifier.quoted_name == '"a"'
    assert snowpark_df.schema[start + 1].column_identifier.quoted_name == '"b"'


@sql_count_checker(query_count=2)
def test_to_snowpark_with_operations(session, tmp_table_basic) -> None:
    snowpandas_df = pd.read_snowflake(tmp_table_basic)
    # rename FOOT_SIZE to size and SHOE_MODEL to model
    snowpandas_df = snowpandas_df.rename(
        columns={"FOOT_SIZE": "size", "SHOE_MODEL": "model"}
    )
    # set size and ID as new index
    snowpandas_df = snowpandas_df.set_index(["size", "ID"])

    snowpark_df = snowpandas_df.to_snowpark()

    # verify the index column is included, and in order of size and ID
    assert snowpark_df.schema[0].column_identifier.quoted_name == '"size"'
    assert isinstance(snowpark_df.schema[0].datatype, DoubleType)
    assert snowpark_df.schema[1].column_identifier.quoted_name == '"ID"'
    assert isinstance(snowpark_df.schema[1].datatype, LongType)

    # verify the rest of data columns are included
    assert snowpark_df.schema[2].column_identifier.quoted_name == '"model"'
    assert isinstance(snowpark_df.schema[2].datatype, StringType)


@sql_count_checker(query_count=0)
def test_to_snowpark_with_duplicated_columns_raise(native_pandas_df_basic) -> None:
    snow_dataframe = pd.DataFrame(native_pandas_df_basic)
    # rename to columns to have the same column name
    snow_dataframe.columns = ["shoe_info", "shoe_info"]

    message = re.escape(
        "Duplicated labels ['shoe_info'] found in index columns ['ID'] and data columns ['shoe_info', 'shoe_info']. "
        "Snowflake does not allow duplicated identifiers, please rename to make sure there is no duplication among both index and data columns."
    )
    with pytest.raises(ValueError, match=message):
        snow_dataframe.to_snowpark()


@sql_count_checker(query_count=2)
def test_to_snowpark_with_none_index_label(tmp_table_basic) -> None:
    snowpandas_df = pd.read_snowflake(tmp_table_basic)

    snowpark_df = snowpandas_df.to_snowpark()

    # verify the index column is included
    assert snowpark_df.schema[0].column_identifier == '"index"'
    assert isinstance(snowpark_df.schema[0].datatype, LongType)

    # verify the rest of data columns are included
    assert snowpark_df.schema[1].column_identifier.name == "ID"
    assert snowpark_df.schema[1].column_identifier.quoted_name == '"ID"'
    assert isinstance(snowpark_df.schema[1].datatype, LongType)
    assert snowpark_df.schema[2].column_identifier.name == "FOOT_SIZE"
    assert snowpark_df.schema[2].column_identifier.quoted_name == '"FOOT_SIZE"'
    assert isinstance(snowpark_df.schema[2].datatype, DoubleType)
    assert snowpark_df.schema[3].column_identifier.name == "SHOE_MODEL"
    assert snowpark_df.schema[3].column_identifier.quoted_name == '"SHOE_MODEL"'
    assert isinstance(snowpark_df.schema[3].datatype, StringType)


@sql_count_checker(query_count=0)
def test_to_snowpark_with_none_data_label_raises(native_pandas_df_basic) -> None:
    snow_dataframe = pd.DataFrame(native_pandas_df_basic)
    snow_dataframe.columns = ["size", None]

    message = re.escape(
        "Label None is found in the data columns ['size', None], which is invalid in Snowflake. "
        "Please give it a name by set the dataframe columns like df.columns=['A', 'B'], "
        "or set the series name if it is a series like series.name='A'."
    )
    with pytest.raises(ValueError, match=message):
        snow_dataframe.to_snowpark()


@sql_count_checker(query_count=0)
def test_timedelta_to_snowpark(test_table_name, caplog):
    with caplog.at_level(logging.WARNING):
        df = pd.DataFrame(
            {
                "a": [1, 2, 3],
                "b": [4, 5, 6],
                "t": native_pd.timedelta_range("1 days", periods=3),
            }
        )
        sp = df.to_snowpark(index=False)
        assert sp.dtypes[-1] == ('"t"', "bigint")
        assert "`TimedeltaType` may be lost in `to_snowpark`'s result" in caplog.text
