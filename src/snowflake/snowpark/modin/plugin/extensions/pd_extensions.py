#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
File containing top-level APIs defined in Snowpark pandas but not the Modin API layer
under the `pd` namespace, such as `pd.read_snowflake`.
"""
from functools import wraps
from typing import Any, Iterable, List, Literal, Optional, Union

from modin.pandas import DataFrame, Series
from modin.pandas.api.extensions import (
    register_pd_accessor as _register_pd_accessor,
)

from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark.async_job import AsyncJob
from snowflake.snowpark.row import Row
from .general_overrides import register_pd_accessor as register_snowflake_accessor
from pandas._typing import IndexLabel
import pandas as native_pd
from snowflake.snowpark import DataFrame as SnowparkDataFrame
from snowflake.snowpark.modin.plugin.extensions.datetime_index import (  # noqa: F401
    DatetimeIndex,
)
from modin.utils import _inherit_docstrings
from snowflake.snowpark.modin.plugin.extensions.index import Index  # noqa: F401
from snowflake.snowpark.modin.plugin.extensions.timedelta_index import (  # noqa: F401
    TimedeltaIndex,
)
import modin.pandas as pd
from modin.config import context as config_context
from pandas.util._decorators import doc
from snowflake.snowpark.modin.plugin.utils.warning_message import (
    materialization_warning,
)


register_snowflake_accessor("Index")(Index)
register_snowflake_accessor("DatetimeIndex")(DatetimeIndex)
register_snowflake_accessor("TimedeltaIndex")(TimedeltaIndex)


def _snowpark_pandas_obj_check(obj: Union[DataFrame, Series]):
    if not isinstance(obj, (DataFrame, Series)):
        raise TypeError("obj must be a Snowpark pandas DataFrame or Series")


def _check_obj_and_set_backend_to_snowflake(
    obj: Any,
) -> Union[Series, DataFrame]:
    """
    Check if the object is a Snowpark pandas object and set the backend to Snowflake.

    Args:
        obj: The object to be checked and moved to Snowflake backend.

    Returns:
        The Series or DataFrame on the Snowflake backend.
    """
    _snowpark_pandas_obj_check(obj)
    return obj.set_backend("Snowflake") if obj.get_backend() != "Snowflake" else obj


# Use a template string so that we can share it between the read_snowflake
# functions on the Snowflake and Pandas backends. We can't use the exact same
# docstring because each doctest creates and inserts to a temp table, and also
# creates, uses, and drops a stored procedure. Using a common table name or a
# common stored procedure name would cause conflicts between doctests. Note
# that we escape curly braces in this docstring by doubling them.
_READ_SNOWFLAKE_DOC = """
    Read a Snowflake table or SQL Query to a Snowpark pandas DataFrame.

    Args:
        name_or_query:
            A table name or fully-qualified object identifier or a SQL SELECT Query. It follows the same syntax in
            https://docs.snowflake.com/developer-guide/snowpark/reference/python/api/snowflake.snowpark.Session.table.html
        index_col:
            A column name or a list of column names to use as index.
        columns:
            A list of column names to select from the table. If not specified, select all columns.
        enforce_ordering:
            If False, Snowpark pandas will provide relaxed consistency and ordering guarantees for the returned
            DataFrame object. Otherwise, strict consistency and ordering guarantees are provided. See the Notes
            section for more details.

    See also:
        - :func:`to_snowflake <modin.pandas.to_snowflake>`

    Notes:
        Transformations applied to the returned Snowpark pandas Dataframe do not affect the underlying Snowflake table
        (or object). Use
        - :func:`modin.pandas.to_snowpark <modin.pandas.to_snowpark>`
        to write the Snowpark pandas DataFrame back to a Snowpark table.

        This API supports table names, SELECT queries (including those that use CTEs), CTEs with anonymous stored procedures
        and calling stored procedures using CALL, and is read only. To interact with Snowflake objects, e.g., listing tables, deleting tables or appending columns use the
        `Snowflake Python Connector <https://docs.snowflake.com/en/developer-guide/python-connector/python-connector>`_, or Snowpark's
        Session object which can be retrieved via `pd.session`.

        Snowpark pandas provides two modes of consistency and ordering semantics.

        * When `enforce_ordering` is set to False, Snowpark pandas provides relaxed consistency and ordering guarantees. In particular, the returned DataFrame object will be
          directly based on the source given by `name_or_query`. Consistency and isolation guarantees are relaxed in this case because any changes that happen to the source will be reflected in the
          DataFrame object returned by `pd.read_snowflake`.

          Ordering guarantees will also be relaxed in the sense that each time an operation is run on the returned DataFrame object, the underlying ordering of rows maybe
          different. For example, calling `df.head(5)` two consecutive times can result in a different set of 5 rows each time and with different ordering.
          Some order-sensitive operations (such as `df.items`, `df.iterrows`, or `df.itertuples`) may not behave as expected.

          With this mode, it is still possible to switch to strict ordering guarantees by explicitly calling `df.sort_values()` and providing a custom sort key. This will
          ensure that future operations will consistently experience the same sort order, but the consistency guarantees will remain relaxed.

          Note that when `name_or_query` is a query with an ORDER BY clause, this will only guarantee that the immediate results of the input query are sorted. But it still gives no guarantees
          on the order of the final results (after applying a sequence of pandas operations to those initial results).

        * When `enforce_ordering` is set to True, Snowpark pandas provides the same consistency and ordering guarantees for `read_snowflake` as if local files were read.
          For example, calling `df.head(5)` two consecutive times is guaranteed to result in the exact same set of 5 rows each time and with the same ordering.
          Depending on the type of source, `pd.read_snowflake` will do one of the following
          at the time of calling `pd.read_snowflake`:

            * For a table referenced by `name_or_query` the base table is snapshotted and the snapshot is used to back the resulting DataFrame.

            * For SELECT queries of the form `SELECT * FROM $TABLE_NAME` the base table is snapshotted as though it were referenced directly from `pd.read_snowflake`,
              and the snapshot will be used to back the resulting DataFrame as above.

            * In the following cases, a temporary table is created and snapshotted, and the snapshot of the temporary table is used to back the resulting
              DataFrame.

                * For VIEWs, SECURE VIEWs, and TEMPORARY VIEWs, a temporary table is created as a materialized copy of the view at
                  the time of calling `pd.read_snowflake` whether `pd.read_snowflake` is called as `pd.read_snowflake("SELECT * FROM $TABLE_NAME")` or
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
        >>> table_name = "{table_name}"
        >>> create_result = session.sql(f"CREATE TEMP TABLE {{table_name}} (A int, B int, C int)").collect()
        >>> insert_result = session.sql(f"INSERT INTO {{table_name}} VALUES(1, 2, 3)").collect()
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

          >>> import modin.pandas as pd
          >>> import snowflake.snowpark.modin.plugin
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
        >>> create_result = session.sql(f"CREATE OR REPLACE TEMP TABLE {{table_name}} (A int, B int, C int)").collect()
        >>> insert_result = session.sql(f"INSERT INTO {{table_name}} VALUES(1, 2, 3),(-1, -2, -3)").collect()
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

          >>> import modin.pandas as pd
          >>> import snowflake.snowpark.modin.plugin
          >>> pd.read_snowflake(f"SELECT * FROM {{table_name}}")   # doctest: +NORMALIZE_WHITESPACE
             A  B  C
          0  1  2  3
          1 -1 -2 -3

        - When ``index_col`` is specified, it
          will be used as index columns in Snowpark pandas DataFrame and rest of columns in the
          Snowflake table will be data columns. Note that duplication is allowed and
          duplicate pandas labels are maintained.

          >>> pd.read_snowflake(f"SELECT * FROM {{table_name}}", index_col="A")   # doctest: +NORMALIZE_WHITESPACE
              B  C
          A
           1  2  3
          -1 -2 -3

          >>> pd.read_snowflake(f"SELECT * FROM {{table_name}}", index_col=["A", "B"])   # doctest: +NORMALIZE_WHITESPACE
                 C
          A  B
           1  2  3
          -1 -2 -3

          >>> pd.read_snowflake(f"SELECT * FROM {{table_name}}", index_col=["A", "A", "B"])  # doctest: +NORMALIZE_WHITESPACE
                    C
          A  A  B
           1  1  2  3
          -1 -1 -2 -3

        - More complex queries can also be passed in.

          >>> pd.read_snowflake(f"SELECT * FROM {{table_name}} WHERE A > 0")  # doctest: +NORMALIZE_WHITESPACE
             A  B  C
          0  1  2  3

        - SQL comments can also be included, and will be ignored.

          >>> pd.read_snowflake(f"-- SQL Comment 1\\nSELECT * FROM {{table_name}} WHERE A > 0")
             A  B  C
          0  1  2  3

          >>> pd.read_snowflake(f'''-- SQL Comment 1
          ... -- SQL Comment 2
          ... SELECT * FROM {{table_name}}
          ... -- SQL Comment 3
          ... WHERE A > 0''')
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

          >>> pd.read_snowflake(f'''WITH T1 AS (SELECT SQUARE(A) AS A2, SQUARE(B) AS B2, SQUARE(C) AS C2 FROM {{table_name}}),
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
          an anonymous stored procedure. Currently CALL statements are only supported when `enforce_ordering=True`.

          >>> pd.read_snowflake('''WITH filter_rows AS PROCEDURE (table_name VARCHAR, column_to_filter VARCHAR, value NUMBER)
          ... RETURNS TABLE(A NUMBER, B NUMBER, C NUMBER)
          ... LANGUAGE PYTHON
          ... RUNTIME_VERSION = '3.9'
          ... PACKAGES = ('snowflake-snowpark-python')
          ... HANDLER = 'filter_rows'
          ... AS $$from snowflake.snowpark.functions import col
          ... def filter_rows(session, table_name, column_to_filter, value):
          ...   df = session.table(table_name)
          ...   return df.filter(col(column_to_filter) == value)$$
          ... ''' + f"CALL filter_rows('{{table_name}}', 'A', 1)", enforce_ordering=True)
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
          ... object Filter {{
          ...   def filterRows(session: Session, tableName: String, column_to_filter: String, value: Int): DataFrame = {{
          ...       val table = session.table(tableName)
          ...       val filteredRows = table.filter(col(column_to_filter) === value)
          ...       return filteredRows
          ...   }}
          ... }}
          ... $$
          ... ''' + f"CALL filter_rows('{{table_name}}', 'A', -1)", enforce_ordering=True)
             A  B  C
          0 -1 -2 -3

        - An example using a stored procedure defined via SQL using Snowpark's Session object.

          >>> from snowflake.snowpark.functions import sproc
          >>> from snowflake.snowpark.types import IntegerType, StructField, StructType, StringType
          >>> from snowflake.snowpark.functions import col
          >>> _ = session.sql("create or replace temp stage mystage").collect()
          >>> session.add_packages('snowflake-snowpark-python')
          >>> @sproc(return_type=StructType([StructField("A", IntegerType()), StructField("B", IntegerType()), StructField("C", IntegerType()), StructField("D", IntegerType())]), input_types=[StringType(), StringType(), IntegerType()], is_permanent=True, name="{stored_procedure_name}", stage_location="mystage")
          ... def select_sp(session_, tableName, col_to_multiply, value):
          ...     df = session_.table(table_name)
          ...     return df.select('*', (col(col_to_multiply)*value).as_("D"))

          >>> pd.read_snowflake(f"CALL {stored_procedure_name}('{{table_name}}', 'A', 2)", enforce_ordering=True)
             A  B  C  D
          0  1  2  3  2
          1 -1 -2 -3 -2

          >>> session.sql("DROP PROCEDURE {stored_procedure_name}(VARCHAR, VARCHAR, NUMBER)").collect()
          [Row(status='{stored_procedure_name} successfully dropped.')]

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

_TO_SNOWFLAKE_DOC = """
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
        - :func:`DataFrame.to_snowflake <modin.pandas.DataFrame.to_snowflake>`
        - :func:`Series.to_snowflake <modin.pandas.Series.to_snowflake>`
        - :func:`read_snowflake <modin.pandas.read_snowflake>`
"""


@register_snowflake_accessor("read_snowflake")
@doc(
    _READ_SNOWFLAKE_DOC,
    table_name="RESULT_0",
    stored_procedure_name="MULTIPLY_COL_BY_VALUE_0",
)
def read_snowflake(
    name_or_query: Union[str, Iterable[str]],
    index_col: Union[str, list[str], None] = None,
    columns: Optional[list[str]] = None,
    enforce_ordering: bool = False,
) -> DataFrame:
    from modin.core.execution.dispatching.factories.dispatcher import FactoryDispatcher

    return DataFrame(
        query_compiler=FactoryDispatcher.get_factory()._read_snowflake(
            name_or_query,
            index_col=index_col,
            columns=columns,
            enforce_ordering=enforce_ordering,
        )
    )


@_register_pd_accessor("read_snowflake", backend="Pandas")
@_inherit_docstrings(read_snowflake)
@doc(
    _READ_SNOWFLAKE_DOC,
    table_name="RESULT_1",
    stored_procedure_name="MULTIPLY_COL_BY_VALUE_1",
)
def _read_snowflake_pandas_backend(
    name_or_query, index_col=None, columns=None, enforce_ordering=False
) -> pd.DataFrame:
    with config_context(Backend="Snowflake"):
        df = pd.read_snowflake(
            name_or_query,
            index_col=index_col,
            columns=columns,
            enforce_ordering=enforce_ordering,
        )
    return df.set_backend("Pandas")


@_register_pd_accessor("read_snowflake", backend="Ray")
@doc(
    _READ_SNOWFLAKE_DOC,
    table_name="RESULT_2",
    stored_procedure_name="MULTIPLY_COL_BY_VALUE_2",
)
def _read_snowflake_ray_backend(
    name_or_query, index_col=None, columns=None, enforce_ordering=False
) -> pd.DataFrame:
    with config_context(Backend="Snowflake"):
        df = pd.read_snowflake(
            name_or_query,
            index_col=index_col,
            columns=columns,
            enforce_ordering=enforce_ordering,
        )
    return df.set_backend("Ray")


def to_snowflake(
    obj: Union[DataFrame, Series],
    name: Union[str, Iterable[str]],
    if_exists: Optional[Literal["fail", "replace", "append"]] = "fail",
    index: bool = True,
    index_label: Optional[IndexLabel] = None,
    table_type: Literal["", "temp", "temporary", "transient"] = "",
) -> None:
    _snowpark_pandas_obj_check(obj)
    return obj.to_snowflake(
        name=name,
        if_exists=if_exists,
        index=index,
        index_label=index_label,
        table_type=table_type,
    )


_register_pd_accessor(name="to_snowflake", backend="Snowflake")(to_snowflake)
_register_pd_accessor(name="to_snowflake", backend="Ray")(to_snowflake)
_register_pd_accessor(name="to_snowflake", backend="Pandas")(to_snowflake)


@_register_pd_accessor("to_snowpark")
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
        - :func:`DataFrame.to_snowpark <modin.pandas.DataFrame.to_snowpark>`
        - :func:`Series.to_snowpark <modin.pandas.Series.to_snowpark>`

    Note:
        The labels of the Snowpark pandas DataFrame or index_label provided will be used as Normalized Snowflake
        Identifiers of the Snowpark DataFrame.
        For details about Normalized Snowflake Identifiers, please refer to the Note in :func:`~modin.pandas.read_snowflake`

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
    return _check_obj_and_set_backend_to_snowflake(obj)._query_compiler.to_snowpark(
        index, index_label
    )


@register_snowflake_accessor("to_pandas")
@materialization_warning
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
        - :func:`DataFrame.to_pandas <modin.pandas.DataFrame.to_pandas>`
        - :func:`Series.to_pandas <modin.pandas.Series.to_pandas>`

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


@register_snowflake_accessor("to_view")
def to_view(
    obj: Union[DataFrame, Series],
    name: Union[str, Iterable[str]],
    *,
    comment: Optional[str] = None,
    index: bool = False,
    index_label: Optional[IndexLabel] = None,
) -> List[Row]:
    """
    Creates a view that captures the computation expressed by the given DataFrame or Series.

    For ``name``, you can include the database and schema name (i.e. specify a
    fully-qualified name). If no database name or schema name are specified, the
    view will be created in the current database or schema.

    ``name`` must be a valid `Snowflake identifier <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_.

    Args:
        obj: The object to create the view from. It must be either a Snowpark pandas DataFrame or Series
        name: The name of the view to create or replace. Can be a list of strings
            that specifies the database name, schema name, and view name.
        comment: Adds a comment for the created view. See
            `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_.
        index: default False
            If true, save DataFrame index columns in view columns.
        index_label:
            Column label for index column(s). If None is given (default) and index is True,
            then the index names are used. A sequence should be given if the DataFrame uses MultiIndex.
    """
    _snowpark_pandas_obj_check(obj)
    return obj.to_view(
        name=name,
        comment=comment,
        index=index,
        index_label=index_label,
    )


@register_snowflake_accessor("to_dynamic_table")
def to_dynamic_table(
    obj: Union[DataFrame, Series],
    name: Union[str, Iterable[str]],
    *,
    warehouse: str,
    lag: str,
    comment: Optional[str] = None,
    mode: str = "overwrite",
    refresh_mode: Optional[str] = None,
    initialize: Optional[str] = None,
    clustering_keys: Optional[Iterable[ColumnOrName]] = None,
    is_transient: bool = False,
    data_retention_time: Optional[int] = None,
    max_data_extension_time: Optional[int] = None,
    iceberg_config: Optional[dict] = None,
    index: bool = False,
    index_label: Optional[IndexLabel] = None,
) -> List[Row]:
    """
    Creates a dynamic table that captures the computation expressed by the given DataFrame or Series.

    For ``name``, you can include the database and schema name (i.e. specify a
    fully-qualified name). If no database name or schema name are specified, the
    dynamic table will be created in the current database or schema.

    ``name`` must be a valid `Snowflake identifier <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_.

    Args:
        obj: The object to create the dynamic table from. It must be either a Snowpark pandas DataFrame or Series
        name: The name of the dynamic table to create or replace. Can be a list of strings
            that specifies the database name, schema name, and view name.
        warehouse: The name of the warehouse used to refresh the dynamic table.
        lag: specifies the target data freshness
        comment: Adds a comment for the created table. See
            `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_.
        mode: Specifies the behavior of create dynamic table. Allowed values are:
            - "overwrite" (default): Overwrite the table by dropping the old table.
            - "errorifexists": Throw and exception if the table already exists.
            - "ignore": Ignore the operation if table already exists.
        refresh_mode: Specifies the refresh mode of the dynamic table. The value can be "AUTO",
            "FULL", or "INCREMENTAL".
        initialize: Specifies the behavior of initial refresh. The value can be "ON_CREATE" or
            "ON_SCHEDULE".
        clustering_keys: Specifies one or more columns or column expressions in the table as the clustering key.
            See `Clustering Keys & Clustered Tables <https://docs.snowflake.com/en/user-guide/tables-clustering-keys>`_
            for more details.
        is_transient: A boolean value that specifies whether the dynamic table is transient.
        data_retention_time: Specifies the retention period for the dynamic table in days so that
            Time Travel actions can be performed on historical data in the dynamic table.
        max_data_extension_time: Specifies the maximum number of days for which Snowflake can extend
            the data retention period of the dynamic table to prevent streams on the dynamic table
            from becoming stale.
        iceberg_config: A dictionary that can contain the following iceberg configuration values:

            - external_volume: specifies the identifier for the external volume where
                the Iceberg table stores its metadata files and data in Parquet format.
            - catalog: specifies either Snowflake or a catalog integration to use for this table.
            - base_location: the base directory that snowflake can write iceberg metadata and files to.
            - catalog_sync: optionally sets the catalog integration configured for Polaris Catalog.
            - storage_serialization_policy: specifies the storage serialization policy for the table.
        index: default False
            If true, save DataFrame index columns as table columns.
        index_label:
            Column label for index column(s). If None is given (default) and index is True,
            then the index names are used. A sequence should be given if the DataFrame uses MultiIndex.


    Note:
        See `understanding dynamic table refresh <https://docs.snowflake.com/en/user-guide/dynamic-tables-refresh>`_.
        for more details on refresh mode.
    """
    _snowpark_pandas_obj_check(obj)
    return obj.to_dynamic_table(
        name=name,
        warehouse=warehouse,
        lag=lag,
        comment=comment,
        mode=mode,
        refresh_mode=refresh_mode,
        initialize=initialize,
        clustering_keys=clustering_keys,
        is_transient=is_transient,
        data_retention_time=data_retention_time,
        max_data_extension_time=max_data_extension_time,
        iceberg_config=iceberg_config,
        index=index,
        index_label=index_label,
    )


@_register_pd_accessor("to_iceberg")
def to_iceberg(
    obj: Union[DataFrame, Series],
    table_name: Union[str, Iterable[str]],
    *,
    iceberg_config: dict,
    mode: Optional[str] = None,
    column_order: str = "index",
    clustering_keys: Optional[Iterable[ColumnOrName]] = None,
    block: bool = True,
    comment: Optional[str] = None,
    enable_schema_evolution: Optional[bool] = None,
    data_retention_time: Optional[int] = None,
    max_data_extension_time: Optional[int] = None,
    change_tracking: Optional[bool] = None,
    copy_grants: bool = False,
    index: bool = True,
    index_label: Optional[IndexLabel] = None,
) -> Optional[AsyncJob]:
    """
    Writes the given DataFrame or Series data to the specified iceberg table in a Snowflake database.

    Args:
        obj: The object to create the iceberg table from. It must be either a Snowpark pandas DataFrame or Series.
        table_name: A string or list of strings representing table name.
            If input is a string, it represents the table name; if input is of type iterable of strings,
            it represents the fully-qualified object identifier (database name, schema name, and table name).
        iceberg_config: A dictionary that can contain the following iceberg configuration values:

            * external_volume: specifies the identifier for the external volume where
                the Iceberg table stores its metadata files and data in Parquet format

            * catalog: specifies either Snowflake or a catalog integration to use for this table

            * base_location: the base directory that snowflake can write iceberg metadata and files to

            * catalog_sync: optionally sets the catalog integration configured for Polaris Catalog

            * storage_serialization_policy: specifies the storage serialization policy for the table
        mode: One of the following values. When it's ``None`` or not provided,
            the save mode set by :meth:`mode` is used.

            "append": Append data of this DataFrame to the existing table. Creates a table if it does not exist.

            "overwrite": Overwrite the existing table by dropping old table.

            "truncate": Overwrite the existing table by truncating old table.

            "errorifexists": Throw an exception if the table already exists.

            "ignore": Ignore this operation if the table already exists.

        column_order: When ``mode`` is "append", data will be inserted into the target table by matching column sequence or column name. Default is "index". When ``mode`` is not "append", the ``column_order`` makes no difference.

            "index": Data will be inserted into the target table by column sequence.
            "name": Data will be inserted into the target table by matching column names. If the target table has more columns than the source DataFrame, use this one.

        clustering_keys: Specifies one or more columns or column expressions in the table as the clustering key.
            See `Clustering Keys & Clustered Tables <https://docs.snowflake.com/en/user-guide/tables-clustering-keys#defining-a-clustering-key-for-a-table>`_
            for more details.
        block: A bool value indicating whether this function will wait until the result is available.
            When it is ``False``, this function executes the underlying queries of the dataframe
            asynchronously and returns an :class:`AsyncJob`.
        comment: Adds a comment for the created table. See
            `COMMENT <https://docs.snowflake.com/en/sql-reference/sql/comment>`_. This argument is ignored if a
            table already exists and save mode is ``append`` or ``truncate``.
        enable_schema_evolution: Enables or disables automatic changes to the table schema from data loaded into the table from source files. Setting
            to ``True`` enables automatic schema evolution and setting to ``False`` disables it. If not set, the default behavior is used.
        data_retention_time: Specifies the retention period for the table in days so that Time Travel actions (SELECT, CLONE, UNDROP) can be performed
            on historical data in the table.
        max_data_extension_time: Specifies the maximum number of days for which Snowflake can extend the data retention period for the table to prevent
            streams on the table from becoming stale.
        change_tracking: Specifies whether to enable change tracking for the table. If not set, the default behavior is used.
        copy_grants: When true, retain the access privileges from the original table when a new table is created with "overwrite" mode.
        index: default True
            If true, save DataFrame index columns as table columns.
        index_label:
            Column label for index column(s). If None is given (default) and index is True,
            then the index names are used. A sequence should be given if the DataFrame uses MultiIndex.


    Example::

        Saving DataFrame to an Iceberg table. Note that the external_volume, catalog, and base_location should have been setup externally.
        See `Create your first Iceberg table <https://docs.snowflake.com/en/user-guide/tutorials/create-your-first-iceberg-table>`_ for more information on creating iceberg resources.

        >>> df = session.create_dataframe([[1,2],[3,4]], schema=["a", "b"])
        >>> iceberg_config = {
        ...     "external_volume": "example_volume",
        ...     "catalog": "example_catalog",
        ...     "base_location": "/iceberg_root",
        ...     "storage_serialization_policy": "OPTIMIZED",
        ... }
        >>> pd.to_iceberg(df.to_snowpark_pandas(), "my_table", iceberg_config=iceberg_config, mode="overwrite") # doctest: +SKIP
    """
    return _check_obj_and_set_backend_to_snowflake(obj)._query_compiler.to_iceberg(
        table_name=table_name,
        iceberg_config=iceberg_config,
        mode=mode,
        column_order=column_order,
        clustering_keys=clustering_keys,
        block=block,
        comment=comment,
        enable_schema_evolution=enable_schema_evolution,
        data_retention_time=data_retention_time,
        max_data_extension_time=max_data_extension_time,
        change_tracking=change_tracking,
        copy_grants=copy_grants,
        index=index,
        index_label=index_label,
    )


def _make_unimplemented_extension(name: str, to_wrap: callable):
    """
    Make an extension for an unimplemented function.

    Args:
        name: The name of the function.
        to_wrap: The function to wrap.

    Returns:
        A function that raises NotImplementedError.
    """

    @wraps(to_wrap)
    def _unimplemented_extension(obj, *args, **kwargs):
        _snowpark_pandas_obj_check(obj)
        # Let the object take care of raising the NotImplementedError.
        return getattr(obj, name)(*args, **kwargs)

    return _unimplemented_extension


for function in (to_dynamic_table, to_view):
    _register_pd_accessor(name=function.__name__, backend=None)(
        _make_unimplemented_extension(name=function.__name__, to_wrap=function)
    )


@register_snowflake_accessor("explain_switch")
def explain_switch(simple=True) -> Union[native_pd.DataFrame, None]:
    """
    Shows a log of all backend switching decisions made by Snowpark pandas.

    Display information where Snowpark pandas considered switching execution
    backends. There can be multiple switch decisions per line of code. The output
    of this method may change in the future and this is to be used for debugging
    purposes.

    "source" is the user code where the switch point occurred, if it is available.
    "api" is the top level api call which initiated the switch point.
    "mode" is either "merge" for decisions involving multiple DataFrames or "auto"
    for decisions involving a single DataFrame.
    "decision" is the decision on which engine to use for that switch point.

    Args:
        simple: bool, default True
            If False, this will display the raw hybrid switch point information
            including costing calculations and dataset size estimates.

    Returns:
        pandas.DataFrame:
            A native pandas DataFrame containing the log of backend switches.

    Examples:
        >>> from modin.config import context as config_context
        >>> with config_context(AutoSwitchBackend=True):
        ...     df = pd.DataFrame({'Animal': ['Falcon', 'Falcon',
        ...         'Parrot', 'Parrot'],
        ...         'Max Speed': [380., 370., 24., 26.]})
        >>> pd.explain_switch()  # doctest: +NORMALIZE_WHITESPACE
                                          decision
        source    api                mode
        <unknown> DataFrame.__init__ auto
                                     auto   Pandas

    """
    if simple:
        return explain_switch_simple()
    return explain_switch_complex()


def explain_switch_simple() -> Union[native_pd.DataFrame, None]:
    from snowflake.snowpark.modin.plugin._internal.telemetry import (
        get_hybrid_switch_log,
    )

    stats = get_hybrid_switch_log()
    if len(stats) <= 0:
        stats["source"] = []
        stats["mode"] = []
        stats["decision"] = []
        stats["api"] = []
    else:
        stats["decision"] = stats.groupby(["group", "mode"]).ffill()["decision"]
        stats["api"] = stats.groupby(["source", "group", "mode"]).ffill()["api"]
        stats["mode"] = stats.groupby(["source", "group"]).ffill()["mode"]
    stats = (
        stats[
            [
                "source",
                "mode",
                "decision",
                "api",
            ]
        ]
        .fillna("")
        .drop_duplicates()
        .set_index(["source", "api", "mode"])
    )
    return stats


def explain_switch_complex() -> Union[native_pd.DataFrame, None]:
    from snowflake.snowpark.modin.plugin._internal.telemetry import (
        get_hybrid_switch_log,
    )

    return get_hybrid_switch_log()
