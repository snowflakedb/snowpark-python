#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import logging
import re

import modin.pandas as pd
import numpy as np
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name
from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.modin.plugin._internal.utils import (
    READ_ONLY_TABLE_SUFFIX,
    extract_pandas_label_from_snowflake_quoted_identifier,
)
from snowflake.snowpark.modin.plugin.utils.exceptions import (
    SnowparkPandasErrorCode,
    SnowparkPandasException,
)
from snowflake.snowpark.session import Session
from tests.integ.modin.utils import (
    BASIC_TYPE_DATA1,
    BASIC_TYPE_DATA2,
    SEMI_STRUCTURED_TYPE_DATA,
    VALID_SNOWFLAKE_COLUMN_NAMES,
    assert_frame_equal,
    assert_index_equal,
    assert_series_equal,
    create_table_with_type,
)
from tests.integ.utils.sql_counter import SqlCounter, sql_count_checker
from tests.utils import Utils

paramList = [False, True]


@pytest.fixture(params=paramList)
def setup_use_scoped_object(request, session):
    use_scoped_objects = session._use_scoped_temp_read_only_table
    session._use_scoped_temp_read_only_table = request.param
    yield
    session._use_scoped_temp_read_only_table = use_scoped_objects


def read_snowflake_and_verify_snapshot_creation_if_any(
    session: Session,
    table_name: str,
    as_query: bool,
    materialization_expected: bool,
    enforce_ordering: bool,
    **kwargs,
) -> pd.DataFrame:
    """
    Helper method with following capability:
    1) call `read_snowflake`, either with the table name directly, or with `SELECT * FROM {table_name}`.
    2) check proper read only table is created during read_snowflake.

    Args:
        table_name: The name of the table to call.
        as_query: Whether to call `read_snowflake` with a query or the table name.
        materialization_expected: Whether extra temp table creation is expected. If true, extra check will be applied
                            to verify that extra temp table is created during read_snowflake.
        kwargs: Keyword arguments to pass to `read_snowflake`.

        Returns:
            The resulting Snowpark pandas DataFrame.
    """

    if as_query:
        table_name_or_query = f"SELECT * FROM {table_name}"
    else:
        table_name_or_query = table_name

    with session.query_history() as query_history:
        df = pd.read_snowflake(
            table_name_or_query, **kwargs, enforce_ordering=enforce_ordering
        )

    filtered_query_history = [
        query
        for query in query_history.queries
        if "SHOW PARAMETERS LIKE" not in query.sql_text
        and "SHOW OBJECTS LIKE" not in query.sql_text
    ]

    if not enforce_ordering:
        assert len(filtered_query_history) == 0
    else:
        if materialization_expected:
            # when materialization happens, two queries are executed during read_snowflake:
            # 1) temp table creation out of the current table or query
            # 2) read only temp table creation
            assert len(filtered_query_history) == 2
        else:
            assert len(filtered_query_history) == 1

        # test if the scoped snapshot is created
        scoped_pattern = " SCOPED " if session._use_scoped_temp_read_only_table else " "
        table_create_sql = filtered_query_history[-1].sql_text
        table_create_pattern = f"CREATE OR REPLACE{scoped_pattern}TEMPORARY READ ONLY TABLE SNOWPARK_TEMP_TABLE_[0-9A-Z]+.*{READ_ONLY_TABLE_SUFFIX}.*"
        assert re.match(table_create_pattern, table_create_sql) is not None

        assert READ_ONLY_TABLE_SUFFIX in table_create_sql

    return df


@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_basic(
    setup_use_scoped_object, session, as_query, enforce_ordering
):
    expected_query_count = 5 if enforce_ordering else 3
    with SqlCounter(query_count=expected_query_count):
        # create table
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        fully_qualified_name = [
            session.get_current_database(),
            session.get_current_schema(),
            table_name,
        ]
        session.create_dataframe(
            [BASIC_TYPE_DATA1, BASIC_TYPE_DATA2]
        ).write.save_as_table(table_name, table_type="temp")
        if as_query:
            names_list = [table_name, ".".join(fully_qualified_name)]
        else:
            names_list = [table_name, fully_qualified_name]
        # create snowpark pandas dataframe
        for name in names_list:
            df = read_snowflake_and_verify_snapshot_creation_if_any(
                session, name, as_query, False, enforce_ordering
            )

            pdf = df.to_pandas()
            assert pdf.values[0].tolist() == BASIC_TYPE_DATA1
            assert pdf.values[1].tolist() == BASIC_TYPE_DATA2


@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_semi_structured_types(
    setup_use_scoped_object, session, as_query, enforce_ordering
):
    expected_query_count = 3 if enforce_ordering else 2
    with SqlCounter(query_count=expected_query_count):
        # create table
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        session.create_dataframe([SEMI_STRUCTURED_TYPE_DATA]).write.save_as_table(
            table_name, table_type="temp"
        )

        # create snowpark pandas dataframe
        df = read_snowflake_and_verify_snapshot_creation_if_any(
            session, table_name, as_query, False, enforce_ordering
        )

        pdf = df.to_pandas()
        for res, expected_res in zip(pdf.values[0].tolist(), SEMI_STRUCTURED_TYPE_DATA):
            assert res == expected_res


@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_none_nan(session, as_query, enforce_ordering):
    expected_query_count = 3 if enforce_ordering else 2
    with SqlCounter(query_count=expected_query_count):
        # create table
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        session.create_dataframe([None, float("nan")]).write.save_as_table(
            table_name, table_type="temp"
        )

        # create snowpark pandas dataframe
        df = read_snowflake_and_verify_snapshot_creation_if_any(
            session, table_name, as_query, False, enforce_ordering
        )

        pdf = df.to_pandas()
        assert np.isnan(pdf.values[0][0])
        assert np.isnan(pdf.values[1][0])


@pytest.mark.parametrize("col_name", VALID_SNOWFLAKE_COLUMN_NAMES)
@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_column_names(session, col_name, as_query, enforce_ordering):
    expected_query_count = 3 if enforce_ordering else 2
    with SqlCounter(query_count=expected_query_count):
        # create table
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        Utils.create_table(session, table_name, f"{col_name} int", is_temporary=True)

        # create snowpark pandas dataframe
        df = read_snowflake_and_verify_snapshot_creation_if_any(
            session, table_name, as_query, False, enforce_ordering
        )

        pdf = df.to_pandas()
        assert pdf.index.dtype == np.int64
        assert pdf.columns[0] == extract_pandas_label_from_snowflake_quoted_identifier(
            quote_name(col_name)
        )


@pytest.mark.parametrize(
    "col_name1, col_name2",
    list(zip(VALID_SNOWFLAKE_COLUMN_NAMES, VALID_SNOWFLAKE_COLUMN_NAMES[::-1])),
)
@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_index_col(
    session, col_name1, col_name2, as_query, enforce_ordering
):
    expected_query_count = 3 if enforce_ordering else 2
    with SqlCounter(query_count=expected_query_count):
        # create table
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        Utils.create_table(
            session, table_name, f"{col_name1} int, {col_name2} text", is_temporary=True
        )

        col_label1 = extract_pandas_label_from_snowflake_quoted_identifier(
            quote_name(col_name1)
        )
        col_label2 = extract_pandas_label_from_snowflake_quoted_identifier(
            quote_name(col_name2)
        )

        # create snowpark pandas dataframe
        df = read_snowflake_and_verify_snapshot_creation_if_any(
            session, table_name, as_query, False, enforce_ordering, index_col=col_label1
        )

        pdf = df.to_pandas()
        assert pdf.index.name == col_label1
        assert len(pdf.columns) == 1
        assert pdf.columns[0] == col_label2


@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_index_col_multiindex(session, as_query, enforce_ordering):
    expected_query_count = 4 if enforce_ordering else 3
    with SqlCounter(query_count=expected_query_count):
        # create table
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        Utils.create_table(
            session,
            table_name,
            "col1 text, col2 text, col3 text, col4 text",
            is_temporary=True,
        )
        session.sql(f"insert into {table_name} values ('A', 'B', 'C', 'D')").collect()

        # create snowpark pandas dataframe
        df = read_snowflake_and_verify_snapshot_creation_if_any(
            session,
            table_name,
            as_query,
            False,
            enforce_ordering,
            index_col=["COL1", "COL2", "COL3"],
        )

        assert_index_equal(
            df.index,
            pd.MultiIndex.from_tuples(
                [("A", "B", "C")], names=["COL1", "COL2", "COL3"]
            ),
        )


@pytest.mark.parametrize(
    # non_existing_index_col latter doesn't exist in the table because we require
    # it equals to extract_pandas_label_from_snowflake_quoted_identifier(quote_name(col_name))
    # in read_snowflake
    "col_name, non_existing_index_col",
    (
        ("col", "test"),
        ("col", "col"),
        ("COL", "col"),
        ('"col"', "COL"),
        ('"COL"', "col"),
    ),
)
@pytest.mark.parametrize("index_col_or_columns", [True, False])
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_non_existing(
    session, col_name, non_existing_index_col, index_col_or_columns, enforce_ordering
):
    expected_query_count = 2 if enforce_ordering else 1
    with SqlCounter(query_count=expected_query_count):
        # create table
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        Utils.create_table(session, table_name, f"{col_name} int", is_temporary=True)
        with pytest.raises(
            KeyError,
            match="is not in existing snowflake columns",
        ):
            if index_col_or_columns:
                pd.read_snowflake(
                    table_name,
                    index_col=non_existing_index_col,
                    enforce_ordering=enforce_ordering,
                )
            else:
                pd.read_snowflake(
                    table_name,
                    columns=[non_existing_index_col],
                    enforce_ordering=enforce_ordering,
                )


@pytest.mark.parametrize("col_name", VALID_SNOWFLAKE_COLUMN_NAMES)
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_columns(session, col_name, enforce_ordering):
    expected_query_count = 3 if enforce_ordering else 2
    with SqlCounter(query_count=expected_query_count):
        # create table
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        Utils.create_table(
            session, table_name, f"{col_name} int, s text", is_temporary=True
        )

        col_label = extract_pandas_label_from_snowflake_quoted_identifier(
            quote_name(col_name)
        )

        # create snowpark pandas dataframe
        df = pd.read_snowflake(
            table_name, columns=[col_label], enforce_ordering=enforce_ordering
        )

        pdf = df.to_pandas()
        assert pdf.columns[0] == col_label


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_both_index_col_columns(session, enforce_ordering):
    expected_query_count = 3 if enforce_ordering else 2
    with SqlCounter(query_count=expected_query_count):
        # create table
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        Utils.create_table(
            session,
            table_name,
            "X int, Y int, Z int",
            is_temporary=True,
        )

        df = pd.read_snowflake(
            table_name, index_col="X", columns=["Y"], enforce_ordering=enforce_ordering
        )
        pdf = df.to_pandas()
        assert pdf.index.name == "X"
        assert pdf.columns[0] == "Y"


@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_duplicate_columns(session, enforce_ordering):
    expected_query_count = 7 if enforce_ordering else 3
    with SqlCounter(
        query_count=expected_query_count,
        high_count_expected=True,
        high_count_reason="Each read creates counts a single row to get an estimated upper bound for hybrid execution",
    ):
        # create table
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        Utils.create_table(session, table_name, '"X" int, Y int', is_temporary=True)

        df = pd.read_snowflake(
            table_name, columns=["X", "X"], enforce_ordering=enforce_ordering
        )
        assert df.columns.tolist() == ["X", "X"]

        df = pd.read_snowflake(
            table_name, index_col=["X", "X"], enforce_ordering=enforce_ordering
        )
        assert df.index.names == ["X", "X"]

        df = pd.read_snowflake(
            table_name,
            index_col="X",
            columns=["X", "Y"],
            enforce_ordering=enforce_ordering,
        )
        assert df.index.names == ["X"]
        assert df.columns.tolist() == ["X", "Y"]

        df = pd.read_snowflake(
            table_name,
            index_col=["X", "Y"],
            columns=["X", "Y"],
            enforce_ordering=enforce_ordering,
        )
        assert df.index.names == ["X", "Y"]
        assert df.columns.tolist() == ["X", "Y"]


@pytest.mark.parametrize("enforce_ordering", [True, False])
@sql_count_checker(query_count=0)
def test_read_snowflake_table_not_exist_negative(session, enforce_ordering) -> None:
    table_name = "non_exist_table_error"

    expected_error = (
        SnowparkPandasException if enforce_ordering else SnowparkSQLException
    )
    with pytest.raises(expected_error) as ex:
        pd.read_snowflake(table_name, enforce_ordering=enforce_ordering)

    if enforce_ordering:
        assert (
            ex.value.error_code == SnowparkPandasErrorCode.GENERAL_SQL_EXCEPTION.value
        )


@pytest.mark.parametrize("enforce_ordering", [True, False])
@sql_count_checker(query_count=1)
def test_read_snowflake_column_not_list_raises(session, enforce_ordering) -> None:
    # create table
    table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
    col_name = "TEST"
    Utils.create_table(
        session, table_name, f"{col_name} int, s text", is_temporary=True
    )

    with pytest.raises(ValueError, match="columns must be provided as list"):
        pd.read_snowflake(
            table_name, columns=col_name, enforce_ordering=enforce_ordering
        )


@pytest.mark.modin_sp_precommit
@pytest.mark.parametrize(
    "table_type",
    [
        "",
        "temporary",
        "transient",
        "view",
        "SECURE VIEW",
        "TEMP VIEW",
    ],
)
@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_with_views(
    setup_use_scoped_object,
    session,
    test_table_name,
    table_type,
    caplog,
    as_query,
    enforce_ordering,
) -> None:
    # create a temporary test table
    expected_query_count = 6 if enforce_ordering else 4
    original_table_type = "temporary"
    if table_type in ["", "temporary", "transient"]:
        original_table_type = table_type
        expected_query_count = 3 if enforce_ordering else 2
    elif table_type == "MATERIALIZED VIEW":
        original_table_type = ""
    with SqlCounter(query_count=expected_query_count):
        create_table_with_type(
            session, test_table_name, "col1 int, s text", table_type=original_table_type
        )
        session.sql(f"insert into {test_table_name} values (1, 'ok')").collect()
        # create a view out of the temporary test table
        table_name = test_table_name
        view_name = None
        try:
            verify_materialization = False
            if table_type in ["view", "SECURE VIEW", "TEMP VIEW"]:
                view_name = Utils.random_name_for_temp_object(TempObjectType.VIEW)
                session.sql(
                    f"create or replace {table_type} {view_name} (col1, s) as select * from {test_table_name}"
                ).collect()
                table_name = view_name
                verify_materialization = True
            caplog.clear()
            with caplog.at_level(logging.WARNING):
                df = read_snowflake_and_verify_snapshot_creation_if_any(
                    session,
                    table_name,
                    as_query,
                    verify_materialization,
                    enforce_ordering,
                )
            assert df.columns.tolist() == ["COL1", "S"]
            if enforce_ordering:
                failing_reason = (
                    "SQL compilation error: Cannot clone from a view object"
                )
                materialize_log = f"Data from source table/view '{table_name}' is being copied into a new temporary table"
                if table_type in ["view", "SECURE VIEW", "TEMP VIEW"]:
                    # verify temporary table is materialized for view, secure view and temp view
                    assert materialize_log in caplog.text
                    assert failing_reason in caplog.text
                else:
                    # verify no temporary table is materialized for regular table
                    assert not (materialize_log in caplog.text)
        finally:
            if view_name:
                Utils.drop_view(session, view_name)


@pytest.mark.modin_sp_precommit
@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_row_access_policy_table(
    setup_use_scoped_object,
    session,
    test_table_name,
    as_query,
    enforce_ordering,
) -> None:
    Utils.create_table(session, test_table_name, "col1 int, s text", is_temporary=True)
    session.sql(f"insert into {test_table_name} values (1, 'ok')").collect()
    # create row access policy that there is no access to the row
    session.sql(
        "create or replace row access policy no_access_policy as (c1 int) returns boolean -> False"
    ).collect()
    # add the row access policy to the table on column col1
    session.sql(
        f"alter table {test_table_name} add row access policy no_access_policy on (col1)"
    ).collect()

    expected_query_count = 3 if enforce_ordering else 1
    with SqlCounter(query_count=expected_query_count):
        df = read_snowflake_and_verify_snapshot_creation_if_any(
            session, test_table_name, as_query, True, enforce_ordering
        )

        assert df.columns.tolist() == ["COL1", "S"]
        # We fetch the row size using metadata, which we have access to, but
        # we cannot actually query the rows themselves due to access control
        # restrictions. This results in a disagreement about the row counts.
        assert len(df) == 1
        assert len(df.to_pandas()) == 0


@pytest.mark.parametrize(
    "input_data, snowflake_type_string, logical_dtype, actual_dtype",
    [
        ([1.1, 2.2, 3.3], "decimal(3, 1)", np.dtype("float64"), np.dtype("float64")),
        ([1.1, 2.2, 3.3], "decimal(38, 1)", np.dtype("float64"), np.dtype("float64")),
        ([1.0, 2.0, 3.0], "decimal(3, 0)", np.dtype("int64"), np.dtype("int8")),
        ([1.0, 2.0, 3.0], "decimal(38, 0)", np.dtype("int64"), np.dtype("int8")),
        (
            [1 << 20, 2 << 20, 3 << 20],
            "decimal(15, 0)",
            np.dtype("int64"),
            np.dtype("int32"),
        ),
        # The example below is questionable. Arguably to_pandas should be returning objects with decimals.
        (
            [1 << 65, 2 << 65, 3 << 65],
            "decimal(38, 0)",
            np.dtype("int64"),
            np.dtype("float64"),
        ),
    ],
)
@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_decimal(
    input_data,
    snowflake_type_string,
    logical_dtype,
    actual_dtype,
    session,
    test_table_name,
    as_query,
    enforce_ordering,
) -> None:
    expected_query_count = 5 if enforce_ordering else 4
    with SqlCounter(query_count=expected_query_count):
        colname = "D"
        values_string = ",".join(f"({i})" for i in input_data)
        Utils.create_table(
            session, test_table_name, f"d {snowflake_type_string}", is_temporary=True
        )
        session.sql(f"insert into {test_table_name} values {values_string}").collect()
        # create row access policy that there is no access to the row
        df = read_snowflake_and_verify_snapshot_creation_if_any(
            session, test_table_name, as_query, False, enforce_ordering
        )

        assert_series_equal(
            df.dtypes, native_pd.Series([logical_dtype], index=[colname])
        )
        pandas_df = df.to_pandas()
        assert_series_equal(
            pandas_df.dtypes, native_pd.Series([actual_dtype], index=[colname])
        )
        assert_frame_equal(df, pandas_df, check_dtype=False)


@pytest.mark.parametrize(
    "as_query", [True, False], ids=["read_with_select_*", "read_with_table_name"]
)
@pytest.mark.parametrize("enforce_ordering", [True, False])
def test_read_snowflake_with_table_in_different_db(
    setup_use_scoped_object, session, caplog, as_query, enforce_ordering
) -> None:
    expected_query_count = 9 if enforce_ordering else 8
    with SqlCounter(
        query_count=expected_query_count,
        high_count_expected=True,
        high_count_reason="Expected high count temp table",
    ):
        db_name = f"testdb_snowpandas_{Utils.random_alphanumeric_str(4)}"
        schema_name = f"testschema_snowpandas_{Utils.random_alphanumeric_str(4)}"
        table_name = Utils.random_name_for_temp_object(TempObjectType.TABLE)
        origin_db = session.get_current_database()
        origin_schema = session.get_current_schema()
        try:
            # create a different database and schema for testing compare with the
            # current database and schema used in current session.
            session.sql(f"create database {db_name}").collect()
            session.sql(f"create schema {db_name}.{schema_name}").collect()

            # create table
            Utils.create_table(
                session,
                f"{db_name}.{schema_name}.{table_name}",
                '"X" int, Y int',
                is_temporary=True,
            )

            caplog.clear()
            with caplog.at_level(logging.DEBUG):
                df = read_snowflake_and_verify_snapshot_creation_if_any(
                    session, table_name, as_query, False, enforce_ordering
                )
            # verify no temporary table is materialized for regular table
            assert not ("Materialize temporary table" in caplog.text)
            assert df.columns.tolist() == ["X", "Y"]
        finally:
            # drop the created temp object
            Utils.drop_table(session, f"{db_name}.{schema_name}.{table_name}")
            Utils.drop_schema(session, f"{db_name}.{schema_name}")
            Utils.drop_database(session, db_name)
            # recover the origin db and schema
            session.use_database(origin_db)
            session.use_schema(origin_schema)
