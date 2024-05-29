#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

"""
File containing top-level APIs defined in Snowpark pandas but not the Modin API layer
under the `pd` namespace, such as `pd.read_snowflake`.
"""
import inspect
from typing import Any, Iterable, Literal, Optional, Union

from pandas._typing import IndexLabel

from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark.modin.pandas import DataFrame, Series
from snowflake.snowpark.modin.pandas.api.extensions import register_pd_accessor
from snowflake.snowpark.modin.plugin._internal.telemetry import (
    snowpark_pandas_telemetry_standalone_function_decorator,
)


def _snowpark_pandas_obj_check(obj: Union[DataFrame, Series]):
    if not isinstance(obj, (DataFrame, Series)):
        raise TypeError("obj must be a Snowpark pandas DataFrame or Series")


@register_pd_accessor("read_snowflake")
@snowpark_pandas_telemetry_standalone_function_decorator
def read_snowflake(
    name_or_query: Union[str, Iterable[str]],
    index_col: Union[str, list[str], None] = None,
    columns: Optional[list[str]] = None,
) -> DataFrame:
    """
    Read a Snowflake table or SQL Query to a Snowpark pandas DataFrame.

    Args:
        name_or_query: A table name or fully-qualified object identifier or a SQL SELECT Query. It follows the same syntax in
            https://docs.snowflake.com/developer-guide/snowpark/reference/python/api/snowflake.snowpark.Session.table.html
        index_col: A column name or a list of column names to use as index.
        columns: A list of column names to select from the table. If not specified, select all columns.

    See also:
        - :func:`to_snowflake <snowflake.snowpark.modin.pandas.io.to_snowflake>`

    Notes:
        Transformations applied to the returned Snowpark pandas Dataframe do not affect the underlying Snowflake table
        (or object). Use
        - :func:`snowflake.snowpark.modin.pandas.to_snowpark <snowflake.snowpark.modin.pandas.io.to_snowflake>`
        to write the Snowpark pandas DataFrame back to a Snowpark table.

        This API supports table names, SELECT queries (including those that use CTEs), CTEs with anonymous stored procedures
        and CALL queries, and is read only. To interact with Snowflake objects, e.g., listing tables, deleting tables or appending columns use the
        `Snowflake Python Connector <https://docs.snowflake.com/en/developer-guide/python-connector/python-connector>`_, or Snowpark's
        Session object which can be retrieved via `pd.session`.

        Snowpark pandas provides the same consistency and isolation guarantees for `read_snowflake` as if local files were read. Depending on the type of source, `pd.read_snowflake` will do one of the following
        at the time of calling `pd.read_snowflake`:

            * For a table referenced by `name_or_query` the base table is snapshotted and the snapshot is used to back the resulting DataFrame.

            * For SELECT queries of the form `SELECT * FROM {table_name}` the base table is snapshotted as though it were referenced directly from `pd.read_snowflake`,
              and the snapshot will be used to back the resulting DataFrame as above.

            * In the following cases, a temporary table is created and snapshotted, and the snapshot of the temporary table is used to back the resulting
              DataFrame.

                * For VIEWs, SECURE VIEWs, and TEMPORARY VIEWs, a temporary table is created as a materialized copy of the view at
                  the time of calling `pd.read_snowflake` whether `pd.read_snowflake` is called as `pd.read_snowflake("SELECT * FROM {view_name}")` or
                  `pd.read_snowflake(view_name)`.

                * For more complex SELECT queries, including those with ORDER BY's or CTEs, the query is evaluated, and a temporary
                  table is created with the result at the time of calling `pd.read_snowflake`.

                * For CTEs with anonymous stored procedures and CALL queries, the procedure is evaluated at the time of calling `pd.read_snowflake`,
                  and a temporary table is created with the result.

        Any changes to the base table(s) or view(s) of the queries (whether the query is a SELECT query or a CTE with an anonymous stored procedure) that
        happen after calling `pd.read_snowflake` will not be reflected in the DataFrame object returned by `pd.read_snowflake`.

    Examples:

        Let's create a Snowflake table using SQL first for demonstrating the behavior of
        ``index_col`` and ``columns``:

        >>> session = pd.session
        >>> table_name = "RESULT"
        >>> create_result = session.sql(f"CREATE TEMP TABLE {table_name} (A int, B int, C int)").collect()
        >>> insert_result = session.sql(f"INSERT INTO {table_name} VALUES(1, 2, 3)").collect()
        >>> session.table(table_name).show()
        -------------------
        |"A"  |"B"  |"C"  |
        -------------------
        |1    |2    |3    |
        -------------------
        <BLANKLINE>

        - When ``index_col`` and ``columns`` are both not specified, a Snowpark pandas DataFrame
          will have a default index from 0 to n-1, where n is the number of rows in the table,
          and have all columns in the Snowflake table as data columns.

          >>> import snowflake.snowpark.modin.pandas as pd
          >>> pd.read_snowflake(table_name)   # doctest: +NORMALIZE_WHITESPACE
             A  B  C
          0  1  2  3

        - When ``index_col`` is specified and ``columns`` is not specified, ``index_col``
          will be used as index columns in Snowpark pandas DataFrame and rest of columns in the
          Snowflake table will be data columns. Note that duplication is allowed and
          duplicate pandas labels are maintained.

          >>> pd.read_snowflake(table_name, index_col="A")   # doctest: +NORMALIZE_WHITESPACE
             B  C
          A
          1  2  3

          >>> pd.read_snowflake(table_name, index_col=["A", "B"])   # doctest: +NORMALIZE_WHITESPACE
               C
          A B
          1 2  3

          >>> pd.read_snowflake(table_name, index_col=["A", "A", "B"])  # doctest: +NORMALIZE_WHITESPACE
                 C
          A A B
          1 1 2  3

        - When ``index_col`` is not specified and ``columns`` is specified, a Snowpark pandas DataFrame
          will have a default index from 0 to n-1 and ``columns`` as data columns.

          >>> pd.read_snowflake(table_name, columns=["A"])  # doctest: +NORMALIZE_WHITESPACE
             A
          0  1

          >>> pd.read_snowflake(table_name, columns=["A", "B"])  # doctest: +NORMALIZE_WHITESPACE
             A  B
          0  1  2

          >>> pd.read_snowflake(table_name, columns=["A", "A", "B"])  # doctest: +NORMALIZE_WHITESPACE
             A  A  B
          0  1  1  2

        - When ``index_col`` and ``columns`` are specified, ``index_col``
          will be used as index columns and ``columns`` will be used as data columns.
          ``index_col`` doesn't need to be a part of ``columns``.

          >>> pd.read_snowflake(table_name, index_col=["A"], columns=["B", "C"])  # doctest: +NORMALIZE_WHITESPACE
             B  C
          A
          1  2  3

          >>> pd.read_snowflake(table_name, index_col=["A", "B"], columns=["A", "B"])  # doctest: +NORMALIZE_WHITESPACE
               A  B
          A B
          1 2  1  2

        Examples of `pd.read_snowflake` using SQL queries:

        >>> session = pd.session
        >>> table_name = "RESULT"
        >>> create_result = session.sql(f"CREATE OR REPLACE TEMP TABLE {table_name} (A int, B int, C int)").collect()
        >>> insert_result = session.sql(f"INSERT INTO {table_name} VALUES(1, 2, 3),(-1, -2, -3)").collect()
        >>> session.table(table_name).show()
        -------------------
        |"A"  |"B"  |"C"  |
        -------------------
        |1    |2    |3    |
        |-1   |-2   |-3   |
        -------------------
        <BLANKLINE>

        - When ``index_col`` is not specified, a Snowpark pandas DataFrame
          will have a default index from 0 to n-1, where n is the number of rows in the table.

          >>> import snowflake.snowpark.modin.pandas as pd
          >>> pd.read_snowflake(f"SELECT * FROM {table_name}")   # doctest: +NORMALIZE_WHITESPACE
             A  B  C
          0  1  2  3
          1 -1 -2 -3

        - When ``index_col`` is specified, it
          will be used as index columns in Snowpark pandas DataFrame and rest of columns in the
          Snowflake table will be data columns. Note that duplication is allowed and
          duplicate pandas labels are maintained.

          >>> pd.read_snowflake(f"SELECT * FROM {table_name}", index_col="A")   # doctest: +NORMALIZE_WHITESPACE
              B  C
          A
           1  2  3
          -1 -2 -3

          >>> pd.read_snowflake(f"SELECT * FROM {table_name}", index_col=["A", "B"])   # doctest: +NORMALIZE_WHITESPACE
                 C
          A  B
           1  2  3
          -1 -2 -3

          >>> pd.read_snowflake(f"SELECT * FROM {table_name}", index_col=["A", "A", "B"])  # doctest: +NORMALIZE_WHITESPACE
                    C
          A  A  B
           1  1  2  3
          -1 -1 -2 -3

        - More complex queries can also be passed in.

          >>> pd.read_snowflake(f"SELECT * FROM {table_name} WHERE A > 0")  # doctest: +NORMALIZE_WHITESPACE
             A  B  C
          0  1  2  3

        - SQL comments can also be included, and will be ignored.

          >>> pd.read_snowflake(f"-- SQL Comment 1\\nSELECT * FROM {table_name} WHERE A > 0")
             A  B  C
          0  1  2  3

          >>> pd.read_snowflake(f'''-- SQL Comment 1
          ... -- SQL Comment 2
          ... SELECT * FROM {table_name} WHERE A > 0
          ... -- SQL Comment 3''')
             A  B  C
          0  1  2  3

        - Note that in the next example, `sort_values` is called to impose an ordering on the DataFrame.

          >>> # Compute all Fibonacci numbers less than 100.
          ... pd.read_snowflake(f'''WITH RECURSIVE current_f (current_val, previous_val) AS
          ... (
          ...   SELECT 0, 1
          ...   UNION ALL
          ...   SELECT current_val + previous_val, current_val FROM current_f
          ...   WHERE current_val + previous_val < 100
          ... )
          ... SELECT current_val FROM current_f''').sort_values("CURRENT_VAL").reset_index(drop=True)
              CURRENT_VAL
          0             0
          1             1
          2             1
          3             2
          4             3
          5             5
          6             8
          7            13
          8            21
          9            34
          10           55
          11           89

          >>> pd.read_snowflake(f'''WITH T1 AS (SELECT SQUARE(A) AS A2, SQUARE(B) AS B2, SQUARE(C) AS C2 FROM {table_name}),
          ... T2 AS (SELECT SQUARE(A2) AS A4, SQUARE(B2) AS B4, SQUARE(C2) AS C4 FROM T1),
          ... T3 AS (SELECT * FROM T1 UNION ALL SELECT * FROM T2)
          ... SELECT * FROM T3''')  # doctest: +NORMALIZE_WHITESPACE
              A2    B2    C2
          0  1.0   4.0   9.0
          1  1.0   4.0   9.0
          2  1.0  16.0  81.0
          3  1.0  16.0  81.0

        - Anonymous Stored Procedures (using CTEs) may also be used (although special care must be taken with respect to indentation of the code block,
          since the entire string encapsulated by the `$$` will be passed directly to a Python interpreter. In the example below, the lines within
          the function are indented, but not the import statement or function definition). The output schema must be specified when defining
          an anonymous stored procedure.

          >>> pd.read_snowflake('''WITH filter_rows AS PROCEDURE (table_name VARCHAR, column_to_filter VARCHAR, value NUMBER)
          ... RETURNS TABLE(A NUMBER, B NUMBER, C NUMBER)
          ... LANGUAGE PYTHON
          ... RUNTIME_VERSION = '3.8'
          ... PACKAGES = ('snowflake-snowpark-python')
          ... HANDLER = 'filter_rows'
          ... AS $$from snowflake.snowpark.functions import col
          ... def filter_rows(session, table_name, column_to_filter, value):
          ...   df = session.table(table_name)
          ...   return df.filter(col(column_to_filter) == value)$$
          ... ''' + f"CALL filter_rows('{table_name}', 'A', 1)")
             A  B  C
          0  1  2  3

        - An example using an anonymous stored procedure defined in Scala.

          >>> pd.read_snowflake('''
          ... WITH filter_rows AS PROCEDURE (table_name VARCHAR, column_to_filter VARCHAR, value NUMBER)
          ... Returns TABLE(A NUMBER, B NUMBER, C NUMBER)
          ... LANGUAGE SCALA
          ... RUNTIME_VERSION = '2.12'
          ... PACKAGES = ('com.snowflake:snowpark:latest')
          ... HANDLER = 'Filter.filterRows'
          ... AS $$
          ... import com.snowflake.snowpark.functions._
          ... import com.snowflake.snowpark._
          ...
          ... object Filter {
          ...   def filterRows(session: Session, tableName: String, column_to_filter: String, value: Int): DataFrame = {
          ...       val table = session.table(tableName)
          ...       val filteredRows = table.filter(col(column_to_filter) === value)
          ...       return filteredRows
          ...   }
          ... }
          ... $$
          ... ''' + f"CALL filter_rows('{table_name}', 'A', -1)")
             A  B  C
          0 -1 -2 -3

        - An example using a stored procedure defined via SQL using Snowpark's Session object.

          >>> from snowflake.snowpark.functions import sproc
          >>> from snowflake.snowpark.types import IntegerType, StructField, StructType, StringType
          >>> from snowflake.snowpark.functions import col
          >>> _ = session.sql("create or replace temp stage mystage").collect()
          >>> session.add_packages('snowflake-snowpark-python')
          >>> @sproc(return_type=StructType([StructField("A", IntegerType()), StructField("B", IntegerType()), StructField("C", IntegerType()), StructField("D", IntegerType())]), input_types=[StringType(), StringType(), IntegerType()], is_permanent=True, name="multiply_col_by_value", stage_location="mystage")
          ... def select_sp(session_, tableName, col_to_multiply, value):
          ...     df = session_.table(table_name)
          ...     return df.select('*', (col(col_to_multiply)*value).as_("D"))

          >>> pd.read_snowflake(f"CALL multiply_col_by_value('{table_name}', 'A', 2)")
             A  B  C  D
          0  1  2  3  2
          1 -1 -2 -3 -2

          >>> session.sql("DROP PROCEDURE multiply_col_by_value(VARCHAR, VARCHAR, NUMBER)").collect()
          [Row(status='MULTIPLY_COL_BY_VALUE successfully dropped.')]

    Note:
        The names/labels used for the parameters of the Snowpark pandas IO functions such as index_col, columns are normalized
        Snowflake Identifiers (The Snowflake stored and resolved Identifiers). The Normalized Snowflake Identifiers
        are also used as default pandas label after constructing a Snowpark pandas DataFrame out of the Snowflake
        table or Snowpark DataFrame. Following are the rules about how Normalized Snowflake Identifiers are generated:

            - When the column identifier in Snowflake/Snowpark DataFrame is an unquoted object identifier,
              it is stored and resolved as uppercase characters (e.g. `id` is stored and resolved as `ID`),
              the valid input is an uppercase string. For example, for the column identifier ``A`` or ``a``, the
              stored and resolved identifier is ``A``, and the valid input for the parameters can only be ``A``,
              and the corresponding pandas label in Snowpark pandas DataFrame is ``A``. ``a`` and ``"A"`` are both invalid.

            - When the column identifier in Snowflake/Snowpark DataFrame is a quoted object identifier, the case
              of the identifier is preserved when storing and resolving the identifier
              (e.g. `"id"` is stored and resolved as `id`), the valid input is case-sensitive string.
              For example, for the column identifier ``"a"``, the valid input for the parameter can only be
              ``a``, and the corresponding pandas label in Snowpark pandas DataFrame is ``a``.
              ``"a"`` is invalid. For the column identifier ``"A"``, the valid input for the parameter can only be
              ``A``, and the corresponding pandas label in Snowpark pandas DataFrame is ``A``, and``"A"`` is invalid.

        See `Snowflake Identifier Requirements <https://docs.snowflake.com/en/sql-reference/identifiers-syntax>`_ for
        more details about Snowflake Identifiers.

        To see what are the Normalized Snowflake Identifiers for columns of a Snowpark DataFrame, you can call
        dataframe.show() to see all column names, which is the Normalized identifier.

        To see what are the Normalized Snowflake Identifiers for columns of a Snowflake table, you can call SQL query
        `SELECT * FROM TABLE` or `DESCRIBE TABLE` to see the column names.
    """
    _, _, _, f_locals = inspect.getargvalues(inspect.currentframe())
    # mangle_dupe_cols has no effect starting in pandas 1.5. Exclude it from
    # kwargs so pandas doesn't spuriously warn people not to use it.
    f_locals.pop("mangle_dupe_cols", None)

    from snowflake.snowpark.modin.core.execution.dispatching.factories.dispatcher import (
        FactoryDispatcher,
    )

    return DataFrame(
        query_compiler=FactoryDispatcher.read_snowflake(
            name_or_query, index_col=index_col, columns=columns
        )
    )


@register_pd_accessor("to_snowflake")
@snowpark_pandas_telemetry_standalone_function_decorator
def to_snowflake(
    obj: Union[DataFrame, Series],
    name: Union[str, Iterable[str]],
    if_exists: Optional[Literal["fail", "replace", "append"]] = "fail",
    index: bool = True,
    index_label: Optional[IndexLabel] = None,
    table_type: Literal["", "temp", "temporary", "transient"] = "",
) -> None:
    """
    Save the Snowpark pandas DataFrame or Series as a Snowflake table.

    Args:
        obj: Either a Snowpark pandas DataFrame or Series
        name:
            Name of the SQL table or fully-qualified object identifier
        if_exists:
            How to behave if table already exists. default 'fail'
            - fail: Raise ValueError.
            - replace: Drop the table before inserting new values.
            - append: Insert new values to the existing table. The order of insertion is not guaranteed.
        index: default True
            If true, save DataFrame index columns as table columns.
        index_label:
            Column label for index column(s). If None is given (default) and index is True,
            then the index names are used. A sequence should be given if the DataFrame uses MultiIndex.
        table_type:
            The table type of table to be created. The supported values are: ``temp``, ``temporary``,
            and ``transient``. An empty string means to create a permanent table. Learn more about table
            types `here <https://docs.snowflake.com/en/user-guide/tables-temp-transient.html>`_.

    See also:
        - :func:`DataFrame.to_snowflake <snowflake.snowpark.modin.pandas.DataFrame.to_snowflake>`
        - :func:`Series.to_snowflake <snowflake.snowpark.modin.pandas.Series.to_snowflake>`
        - :func:`read_snowflake <snowflake.snowpark.modin.pandas.io.read_snowflake>`
    """
    _snowpark_pandas_obj_check(obj)

    return obj._query_compiler.to_snowflake(
        name, if_exists, index, index_label, table_type
    )


@register_pd_accessor("to_snowpark")
@snowpark_pandas_telemetry_standalone_function_decorator
def to_snowpark(
    obj: Union[DataFrame, Series],
    index: bool = True,
    index_label: Optional[IndexLabel] = None,
) -> SnowparkDataFrame:
    """
    Convert the Snowpark pandas DataFrame or Series to a Snowpark DataFrame.
    Note that once converted to a Snowpark DataFrame, no ordering information will be preserved. You can call
    reset_index to generate a default index column that is the same as the row position before the call to_snowpark.

    Args:
        obj: The object to be converted to Snowpark DataFrame. It must be either a Snowpark pandas DataFrame or Series
        index: bool, default True.
            Whether to keep the index columns in the result Snowpark DataFrame. If True, the index columns
            will be the first set of columns. Otherwise, no index column will be included in the final Snowpark
            DataFrame.
        index_label: IndexLabel, default None.
            Column label(s) to use for the index column(s). If None is given (default) and index is True,
            then the original index column labels are used. A sequence should be given if the DataFrame uses
            MultiIndex, and the length of the given sequence should be the same as the number of index columns.

    Returns:
        :class:`~snowflake.snowpark.dataframe.DataFrame`
            A Snowpark DataFrame contains the index columns if index=True and all data columns of the Snowpark pandas
            DataFrame. The identifier for the Snowpark DataFrame will be the normalized quoted identifier with
            the same name as the pandas label.

    Raises:
         ValueError if duplicated labels occur among the index and data columns.
         ValueError if the label used for a index or data column is None.

    See also:
        - :func:`Snowpark.DataFrame.to_snowpark_pandas <snowflake.snowpark.DataFrame.to_snowpark_pandas>`
        - :func:`DataFrame.to_snowpark <snowflake.snowpark.modin.pandas.DataFrame.to_snowpark>`
        - :func:`Series.to_snowpark <snowflake.snowpark.modin.pandas.Series.to_snowpark>`

    Note:
        The labels of the Snowpark pandas DataFrame or index_label provided will be used as Normalized Snowflake
        Identifiers of the Snowpark DataFrame.
        For details about Normalized Snowflake Identifiers, please refer to the Note in :func:`~snowflake.snowpark.modin.pandas.io.read_snowflake`

    Examples::

        >>> df = pd.DataFrame({'Animal': ['Falcon', 'Falcon',
        ...                               'Parrot', 'Parrot'],
        ...                    'Max Speed': [380., 370., 24., 26.]})
        >>> df
           Animal  Max Speed
        0  Falcon      380.0
        1  Falcon      370.0
        2  Parrot       24.0
        3  Parrot       26.0
        >>> snowpark_df = pd.to_snowpark(df, index_label='Order')
        >>> snowpark_df.order_by('"Max Speed"').show()
        ------------------------------------
        |"Order"  |"Animal"  |"Max Speed"  |
        ------------------------------------
        |2        |Parrot    |24.0         |
        |3        |Parrot    |26.0         |
        |1        |Falcon    |370.0        |
        |0        |Falcon    |380.0        |
        ------------------------------------
        <BLANKLINE>
        >>> snowpark_df = pd.to_snowpark(df, index=False)
        >>> snowpark_df.order_by('"Max Speed"').show()
        --------------------------
        |"Animal"  |"Max Speed"  |
        --------------------------
        |Parrot    |24.0         |
        |Parrot    |26.0         |
        |Falcon    |370.0        |
        |Falcon    |380.0        |
        --------------------------
        <BLANKLINE>
        >>> df = pd.DataFrame({'Animal': ['Falcon', 'Falcon',
        ...                               'Parrot', 'Parrot'],
        ...                    'Max Speed': [380., 370., 24., 26.]}, index=pd.Index([3, 5, 6, 7], name="id"))
        >>> df      # doctest: +NORMALIZE_WHITESPACE
            Animal  Max Speed
        id
        3  Falcon      380.0
        5  Falcon      370.0
        6  Parrot       24.0
        7  Parrot       26.0
        >>> snowpark_df = pd.to_snowpark(df)
        >>> snowpark_df.order_by('"id"').show()
        ---------------------------------
        |"id"  |"Animal"  |"Max Speed"  |
        ---------------------------------
        |3     |Falcon    |380.0        |
        |5     |Falcon    |370.0        |
        |6     |Parrot    |24.0         |
        |7     |Parrot    |26.0         |
        ---------------------------------
        <BLANKLINE>

        MultiIndex usage

        >>> df = pd.DataFrame({'Animal': ['Falcon', 'Falcon',
        ...                               'Parrot', 'Parrot'],
        ...                    'Max Speed': [380., 370., 24., 26.]},
        ...                    index=pd.MultiIndex.from_tuples([('bar', 'one'), ('foo', 'one'), ('bar', 'two'), ('foo', 'three')], names=['first', 'second']))
        >>> df      # doctest: +NORMALIZE_WHITESPACE
                        Animal  Max Speed
        first second
        bar   one     Falcon      380.0
        foo   one     Falcon      370.0
        bar   two     Parrot       24.0
        foo   three   Parrot       26.0
        >>> snowpark_df = pd.to_snowpark(df, index=True, index_label=['A', 'B'])
        >>> snowpark_df.order_by('"A"', '"B"').show()
        ----------------------------------------
        |"A"  |"B"    |"Animal"  |"Max Speed"  |
        ----------------------------------------
        |bar  |one    |Falcon    |380.0        |
        |bar  |two    |Parrot    |24.0         |
        |foo  |one    |Falcon    |370.0        |
        |foo  |three  |Parrot    |26.0         |
        ----------------------------------------
        <BLANKLINE>
        >>> snowpark_df = pd.to_snowpark(df, index=False)
        >>> snowpark_df.order_by('"Max Speed"').show()
        --------------------------
        |"Animal"  |"Max Speed"  |
        --------------------------
        |Parrot    |24.0         |
        |Parrot    |26.0         |
        |Falcon    |370.0        |
        |Falcon    |380.0        |
        --------------------------
        <BLANKLINE>
        >>> snowpark_df = pd.to_snowpark(df["Animal"], index=False)
        >>> snowpark_df.order_by('"Animal"').show()
        ------------
        |"Animal"  |
        ------------
        |Falcon    |
        |Falcon    |
        |Parrot    |
        |Parrot    |
        ------------
        <BLANKLINE>
    """
    _snowpark_pandas_obj_check(obj)

    return obj._query_compiler.to_snowpark(index, index_label)


@register_pd_accessor("to_pandas")
@snowpark_pandas_telemetry_standalone_function_decorator
def to_pandas(
    obj: Union[DataFrame, Series],
    *,
    statement_params: Optional[dict[str, str]] = None,
    **kwargs: Any,
) -> Union[DataFrame, Series]:
    """
    Convert Snowpark pandas DataFrame or Series to pandas DataFrame or Series

    Args:
        obj: The object to be converted to native pandas. It must be either a Snowpark pandas DataFrame or Series
        statement_params: Dictionary of statement level parameters to be set while executing this action.

    Returns:
        pandas DataFrame or Series

    See also:
        - :func:`DataFrame.to_pandas <snowflake.snowpark.modin.pandas.DataFrame.to_pandas>`
        - :func:`Series.to_pandas <snowflake.snowpark.modin.pandas.Series.to_pandas>`

    Examples:

        >>> df = pd.DataFrame({'Animal': ['Falcon', 'Falcon',
        ...                               'Parrot', 'Parrot'],
        ...                    'Max Speed': [380., 370., 24., 26.]})
        >>> pd.to_pandas(df)
           Animal  Max Speed
        0  Falcon      380.0
        1  Falcon      370.0
        2  Parrot       24.0
        3  Parrot       26.0

        >>> pd.to_pandas(df['Animal'])
        0    Falcon
        1    Falcon
        2    Parrot
        3    Parrot
        Name: Animal, dtype: object
    """
    _snowpark_pandas_obj_check(obj)
    return obj.to_pandas(statement_params=statement_params, *kwargs)
