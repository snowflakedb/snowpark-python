#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
import re
from collections import Counter
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union

import snowflake.snowpark
from snowflake.connector.options import pandas
from snowflake.snowpark._internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark._internal.analyzer.lateral import Lateral as SPLateral
from snowflake.snowpark._internal.analyzer.limit import Limit as SPLimit
from snowflake.snowpark._internal.analyzer.snowflake_plan import CopyIntoNode
from snowflake.snowpark._internal.analyzer.sp_identifiers import TableIdentifier
from snowflake.snowpark._internal.analyzer.sp_views import (
    CreateViewCommand as SPCreateViewCommand,
    LocalTempView as SPLocalTempView,
    PersistedView as SPPersistedView,
    ViewType as SPViewType,
)
from snowflake.snowpark._internal.analyzer.table_function import (
    TableFunctionJoin as SPTableFunctionJoin,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.plans.logical.basic_logical_operators import (
    Except as SPExcept,
    Intersect as SPIntersect,
    Join as SPJoin,
    Sort as SPSort,
    Union as SPUnion,
)
from snowflake.snowpark._internal.plans.logical.hints import JoinHint as SPJoinHint
from snowflake.snowpark._internal.plans.logical.logical_plan import (
    Filter as SPFilter,
    Project as SPProject,
    Sample as SPSample,
)
from snowflake.snowpark._internal.sp_expressions import (
    Ascending as SPAscending,
    Attribute as SPAttribute,
    Descending as SPDescending,
    Expression as SPExpression,
    FlattenFunction,
    Literal as SPLiteral,
    NamedExpression as SPNamedExpression,
    SortOrder as SPSortOrder,
    Star as SPStar,
    TableFunctionExpression as SPTableFunctionExpression,
)
from snowflake.snowpark._internal.sp_types.sp_join_types import (
    Cross as SPCrossJoin,
    JoinType as SPJoinType,
    LeftAnti as SPLeftAnti,
    LeftSemi as SPLeftSemi,
    NaturalJoin as SPNaturalJoin,
    UsingJoin as SPUsingJoin,
)
from snowflake.snowpark._internal.sp_types.types_package import (
    ColumnOrName,
    LiteralType,
)
from snowflake.snowpark._internal.utils import Utils
from snowflake.snowpark.column import Column
from snowflake.snowpark.dataframe_na_functions import DataFrameNaFunctions
from snowflake.snowpark.dataframe_stat_functions import DataFrameStatFunctions
from snowflake.snowpark.dataframe_writer import DataFrameWriter
from snowflake.snowpark.exceptions import (
    SnowparkClientException,
    SnowparkDataframeException,
)
from snowflake.snowpark.functions import (
    _create_table_function_expression,
    _to_col_if_str,
    col,
    row_number,
)
from snowflake.snowpark.row import Row
from snowflake.snowpark.types import StructType


class DataFrame:
    """Represents a lazily-evaluated relational dataset that contains a collection
    of :class:`Row` objects with columns defined by a schema (column name and type).

    A DataFrame is considered lazy because it encapsulates the computation or query
    required to produce a relational dataset. The computation is not performed until
    you call a method that performs an action (e.g. :func:`collect`).

    **Creating a DataFrame**

    You can create a DataFrame in a number of different ways, as shown in the examples
    below.

    Example 1
        Creating a DataFrame by reading a table in Snowflake::

            df_prices = session.table("itemsdb.publicschema.prices")

    Example 2
        Creating a DataFrame by reading files from a stage::

            df_catalog = session.read.csv("@stage/some_dir")


    Example 3
        Creating a DataFrame by specifying a sequence or a range::

            df = session.createDataFrame([(1, "one"), (2, "two")])
            df = session.range(1, 10, 2)


    Example 4
        Create a new DataFrame by applying transformations to other existing DataFrames::

            df_merged_data = df_catalog.join(df_prices, df_catalog["itemId"] == df_prices["ID"])


    **Performing operations on a DataFrame**

    Broadly, the operations on DataFrame can be divided into two types:

    - **Transformations** produce a new DataFrame from one or more existing DataFrames. Note that tranformations are lazy and don't cause the DataFrame to be evaluated. If the API does not provide a method to express the SQL that you want to use, you can use :func:`functions.sqlExpr` as a workaround.
    - **Actions** cause the DataFrame to be evaluated. When you call a method that performs an action, Snowpark sends the SQL query for the DataFrame to the server for evaluation.

    **Transforming a DataFrame**

    The following examples demonstrate how you can transform a DataFrame.

    Example 5
        Using the :func:`select()` method to select the columns that should be in the
        DataFrame (similar to adding a ``SELECT`` clause)::

            # Return a new DataFrame containing the ID and amount columns of the prices table.
            # This is equivalent to: SELECT ID, AMOUNT FROM PRICES;
            df_price_ids_and_amounts = df_prices.select(col("ID"), col("amount"))

    Example 6
        Using the :func:`Column.as_` method to rename a column in a DataFrame (similar
        to using ``SELECT col AS alias``)::

            # Return a new DataFrame containing the ID column of the prices table as a column named
            # itemId. This is equivalent to: SELECT ID AS itemId FROM PRICES;
            df_price_item_ids = df_prices.select(col("ID").as_("itemId"))

    Example 7
        Using the :func:`filter` method to filter data (similar to adding a ``WHERE`` clause)::

            # Return a new DataFrame containing the row from the prices table with the ID 1.
            # This is equivalent to:
            # SELECT FROM PRICES WHERE ID = 1;
            df_price1 = df_prices.filter((col("ID") === 1))

    Example 8
        Using the :func:`sort()` method to specify the sort order of the data (similar to adding an ``ORDER BY`` clause)::

            # Return a new DataFrame for the prices table with the rows sorted by ID.
            # This is equivalent to: SELECT FROM PRICES ORDER BY ID;
            df_sorted_prices = df_prices.sort(col("ID"))

    Example 9
        Using the :func:`groupBy()` method to return a
        :class:`RelationalGroupedDataFrame` that you can use to group and aggregate
        results (similar to adding a ``GROUP BY`` clause).

        :class:`RelationalGroupedDataFrame` provides methods for aggregating results, including:

        - :func:`RelationalGroupedDataFrame.avg()` (equivalent to AVG(column))
        - :func:`RelationalGroupedDataFrame.count()` (equivalent to COUNT())
        - :func:`RelationalGroupedDataFrame.max()` (equivalent to MAX(column))
        - :func:`RelationalGroupedDataFrame.median()` (equivalent to MEDIAN(column))
        - :func:`RelationalGroupedDataFrame.min()` (equivalent to MIN(column))
        - :func:`RelationalGroupedDataFrame.sum()` (equivalent to SUM(column))

        ::

            # Return a new DataFrame for the prices table that computes the sum of the prices by
            # category. This is equivalent to:
            #  SELECT CATEGORY, SUM(AMOUNT) FROM PRICES GROUP BY CATEGORY
            df_total_price_per_category = df_prices.groupBy(col("category")).sum(col("amount"))

    **Performing an action on a DataFrame**

    The following examples demonstrate how you can perform an action on a DataFrame.

    Example 10
        Performing a query and returning an array of Rows::

            results = df_prices.collect()

    Example 11
        Performing a query and print the results::

            df_prices.show()
    """

    __NUM_PREFIX_DIGITS = 4
    __get_unaliased_regex = re.compile(
        f"""._[a-zA-Z0-9]{{{__NUM_PREFIX_DIGITS}}}_(.*)"""
    )

    def __init__(self, session=None, plan=None):
        self.session = session
        self.__plan = session._analyzer.resolve(plan)

        # Use this to simulate scala's lazy val
        self.__placeholder_schema = None
        self.__placeholder_output = None
        self._stat = DataFrameStatFunctions(self)
        self.approxQuantile = self._stat.approxQuantile
        self.corr = self._stat.corr
        self.cov = self._stat.cov
        self.crosstab = self._stat.crosstab
        self.sampleBy = self._stat.sampleBy

        self._na = DataFrameNaFunctions(self)
        self.dropna = self._na.drop
        self.fillna = self._na.fill
        self.replace = self._na.replace

        self._reader = None  # type: Optional[snowflake.snowpark.DataFrameReader]

    @staticmethod
    def get_unaliased(col_name: str) -> List[str]:
        unaliased = list()
        c = col_name
        while True:
            match = DataFrame.__get_unaliased_regex.match(c)
            if match:
                c = match.group(1)
                unaliased.append(c)
            else:
                break

        return unaliased

    @staticmethod
    def __generate_prefix(prefix: str) -> str:
        return f"{prefix}_{Utils.generate_random_alphanumeric(DataFrame.__NUM_PREFIX_DIGITS)}_"

    @property
    def stat(self) -> DataFrameStatFunctions:
        return self._stat

    def collect(self) -> List["Row"]:
        """Executes the query representing this DataFrame and returns the result as a
        list of :class:`Row` objects.
        """
        return self._collect_with_tag()

    def _collect_with_tag(self) -> List["Row"]:
        return self.session._conn.execute(
            self.__plan,
            _statement_params={"QUERY_TAG": Utils.create_statement_query_tag(3)}
            if not self.session.query_tag
            else None,
        )

    def clone(self) -> "DataFrame":
        """Returns a clone of this :class:`DataFrame`."""
        return DataFrame(self.session, self.__plan.clone())

    def toPandas(self, **kwargs) -> "pandas.DataFrame":
        """
        Returns the contents of this DataFrame as a `Pandas DataFrame <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html>`__.

        Note:
            1. This method is only available if Pandas is installed and available.

            2. If you use :func:`Session.sql` with this method, the input query of :func:`Session.sql` can only be a SELECT statement.
        """
        if not self.session.query_tag:
            kwargs["_statement_params"] = {
                "QUERY_TAG": Utils.create_statement_query_tag(2)
            }
        result = self.session._conn.execute(self.__plan, to_pandas=True, **kwargs)

        # if the returned result is not a pandas dataframe, raise Exception
        # this might happen when calling this method with non-select commands
        # e.g., session.sql("create ...").toPandas()
        if not isinstance(result, pandas.DataFrame):
            raise SnowparkClientExceptionMessages.SERVER_FAILED_FETCH_PANDAS(
                "toPandas() did not return a Pandas DataFrame. "
                "If you use session.sql(...).toPandas(), the input query can only be a "
                "SELECT statement. Or you can use session.sql(...).collect() to get a "
                "list of Row objects for a non-SELECT statement, then convert it to a "
                "Pandas DataFrame."
            )

        return result

    def toDF(self, *names: Union[str, List[str], Tuple[str, ...]]) -> "DataFrame":
        """
        Creates a new DataFrame containing columns with the specified names.

        The number of column names that you pass in must match the number of columns in the existing
        DataFrame.

        Examples::

            df = session.range(1, 10, 2).toDF("col1")
            df = session.range(1, 10, 2).toDF(["col1"])

        Args:
            names: list of new column names
        """
        col_names = Utils.parse_positional_args_to_list(*names)
        if not all(isinstance(n, str) for n in col_names):
            raise TypeError(
                f"Invalid input type in toDF(), expected str or a list of strs."
            )

        if len(self.__output()) != len(col_names):
            raise ValueError(
                f"The number of columns doesn't match. "
                f"Old column names ({len(self.__output())}): "
                f"{','.join(attr.name for attr in self.__output())}. "
                f"New column names ({len(col_names)}): {','.join(col_names)}."
            )

        new_cols = []
        for attr, name in zip(self.__output(), col_names):
            new_cols.append(Column(attr).alias(name))
        return self.select(new_cols)

    def __getitem__(self, item):
        if isinstance(item, str):
            return self.col(item)
        elif isinstance(item, Column):
            return self.filter(item)
        elif isinstance(item, (list, tuple)):
            return self.select(item)
        elif isinstance(item, int):
            return self.__getitem__(self.columns[item])
        else:
            raise TypeError(f"Unexpected item type: {type(item)}")

    def __getattr__(self, name):
        if AnalyzerPackage.quote_name(name) not in self.columns:
            raise AttributeError(
                f"{self.__class__.__name__} object has no attribute {name}"
            )
        return self.col(name)

    @property
    def columns(self) -> List[str]:
        """Returns all column names as a list."""
        # Does not exist in scala snowpark.
        return [attr.name for attr in self.__output()]

    def col(self, col_name: str) -> Column:
        """Returns a reference to a column in the DataFrame."""
        if col_name == "*":
            return Column(SPStar(self.__plan.output()))
        else:
            return Column(self.__resolve(col_name))

    def select(
        self,
        *cols: Union[ColumnOrName, List[ColumnOrName], Tuple[ColumnOrName, ...]],
    ) -> "DataFrame":
        """Returns a new DataFrame with the specified Column expressions as output
        (similar to SELECT in SQL). Only the Columns specified as arguments will be
        present in the resulting DataFrame.

        You can use any :class:`Column` expression or strings for named columns.

        Example 1::

            df_selected = df.select(col("col1"), substring(col("col2"), 0, 10),
                                    df["col3"] + df["col4"])

        Example 2::

            df_selected = df.select("col1", "col2", "col3")

        Example 3::

            df_selected = df.select(["col1", "col2", "col3"])

        Args:
            *cols: A :class:`Column`, :class:`str`, or a list of those.
        """
        exprs = Utils.parse_positional_args_to_list(*cols)
        if not exprs:
            raise ValueError("The input of select() cannot be empty")

        names = []
        for e in exprs:
            if isinstance(e, Column):
                names.append(e._named())
            elif isinstance(e, str):
                names.append(Column(e)._named())
            else:
                raise TypeError(
                    "The input of select() must be Column, column name, or a list of them"
                )

        return self.__with_plan(SPProject(names, self.__plan))

    def drop(
        self,
        *cols: Union[ColumnOrName, List[ColumnOrName], Tuple[ColumnOrName, ...]],
    ) -> "DataFrame":
        """Returns a new DataFrame that excludes the columns with the specified names
        from the output.

        This is functionally equivalent to calling :func:`select()` and passing in all
        columns except the ones to exclude. This is a no-op if schema does not contain
        the given column name(s).

        Args:
            *cols: the columns to exclude, as :class:`str`, :class:`Column` or a list
                of those.

        Raises:
            :class:`SnowparkClientException`: if the resulting :class:`DataFrame`
                contains no output columns.
        """
        # an empty list should be accept, as dropping nothing
        if not cols:
            raise ValueError("The input of drop() cannot be empty")
        exprs = Utils.parse_positional_args_to_list(*cols)

        names = []
        for c in exprs:
            if isinstance(c, str):
                names.append(c)
            elif isinstance(c, Column) and isinstance(c.expression, SPAttribute):
                names.append(
                    self.__plan.expr_to_alias.get(
                        c.expression.expr_id, c.expression.name
                    )
                )
            elif isinstance(c, Column) and isinstance(c.expression, SPNamedExpression):
                names.append(c.expression.name)
            else:
                raise SnowparkClientExceptionMessages.DF_CANNOT_DROP_COLUMN_NAME(str(c))

        normalized_names = {AnalyzerPackage.quote_name(n) for n in names}
        existing_names = [attr.name for attr in self.__output()]
        keep_col_names = [c for c in existing_names if c not in normalized_names]
        if not keep_col_names:
            raise SnowparkClientExceptionMessages.DF_CANNOT_DROP_ALL_COLUMNS()
        else:
            return self.select(list(keep_col_names))

    def filter(self, expr: Column) -> "DataFrame":
        """Filters rows based on the specified conditional expression (similar to WHERE
        in SQL).

        Examples::

            df_filtered = df.filter(col("A") > 1 & col("B") < 100)

            # The following two result in the same SQL query:
            prices_df.filter(col("price") > 100)
            prices_df.where(col("price") > 100)

        Args:
            expr: a :class:`Column` expression.
        """
        if not isinstance(expr, Column):
            raise TypeError(
                f"The input type of filter() must be Column. Got: {type(expr)}"
            )

        return self.__with_plan(SPFilter(expr.expression, self.__plan))

    where = filter

    def sort(
        self,
        *cols: Union[str, Column, List[ColumnOrName], Tuple[ColumnOrName, ...]],
        ascending: Optional[Union[bool, int, List[Union[bool, int]]]] = None,
    ) -> "DataFrame":
        """Sorts a DataFrame by the specified expressions (similar to ORDER BY in SQL).

        Examples::

            from snowflake.snowpark.functions import col
            df_sorted = df.sort(col("A"), col("B").asc())
            df_sorted = df.sort(col("a"), ascending=False)
            # The values from the list overwrite the column ordering.
            sorted_rows = df.sort(["a", col("b").desc()], ascending=[1, 1]).collect()

        Args:
            *cols: A column name as :class:`str` or :class:`Column`, or a list of
             columns to sort by.
            ascending: A :class:`bool` or a list of :class:`bool` for sorting the
             DataFrame, where ``True`` sorts a column in ascending order and ``False``
             sorts a column in descending order . If you specify a list of multiple
             sort orders, the length of the list must equal the number of columns.
        """
        if not cols:
            raise ValueError("sort() needs at least one sort expression.")
        exprs = self.__convert_cols_to_exprs("sort()", *cols)
        if not exprs:
            raise ValueError("sort() needs at least one sort expression.")
        orders = []
        if ascending is not None:
            if isinstance(ascending, (list, tuple)):
                orders = [SPAscending() if asc else SPDescending() for asc in ascending]
            elif isinstance(ascending, (bool, int)):
                orders = [SPAscending() if ascending else SPDescending()]
            else:
                raise TypeError(
                    "ascending can only be boolean or list,"
                    " but got {}".format(str(type(ascending)))
                )
            if len(exprs) != len(orders):
                raise ValueError(
                    "The length of col ({}) should be same with"
                    " the length of ascending ({}).".format(len(exprs), len(orders))
                )

        sort_exprs = []
        for idx in range(len(exprs)):
            expr = exprs[idx]
            # orders will overwrite current orders in expression (but will not overwrite null ordering)
            # if no order is provided, use ascending order
            if isinstance(exprs[idx], SPSortOrder):
                sort_exprs.append(
                    SPSortOrder(expr.child, orders[idx], expr.null_ordering)
                    if orders
                    else expr
                )
            else:
                sort_exprs.append(
                    SPSortOrder(expr, orders[idx] if orders else SPAscending())
                )

        return self.__with_plan(SPSort(sort_exprs, True, self.__plan))

    def agg(
        self,
        exprs: Union[
            Column, Tuple[str, str], List[Column], List[Tuple[str, str]], Dict[str, str]
        ],
    ) -> "DataFrame":
        """Aggregate the data in the DataFrame. Use this method if you don't need to
        group the data (:func:`groupBy`).

        For the input value, pass in a list of expressions that apply aggregation
        functions to columns (functions that are defined in the
        :mod:`snowflake.snowpark.functions` module).

        Alternatively, pass in a list of pairs, or a dictionary with key-value pairs,
        that specify the column names and aggregation functions. For each pair:

            - Set the key of the key-value pair to the name of the column to aggregate.
            - Set the value of the key-value pair to the name of the aggregation function to use on that column.

        Examples::

            from snowflake.snowpark.functions import col, stddev, stddev_pop
            df.agg(stddev(col("a")))
            df.agg([stddev(col("a")), stddev_pop(col("a"))])

            df.agg([("length", "min"), ("width", "max")])
            df.agg({"customers": "count", "amount": "sum"})
        """
        grouping_exprs = None
        if isinstance(exprs, Column):
            grouping_exprs = [exprs]
        elif isinstance(exprs, (list, tuple)):
            # the first if-statement also handles the case of empty list
            if all(isinstance(e, Column) for e in exprs):
                grouping_exprs = [e for e in exprs]
            elif all(
                isinstance(e, (list, tuple))
                and len(e) == 2
                and isinstance(e[0], str)
                and isinstance(e[1], str)
                for e in exprs
            ):
                grouping_exprs = [(self.col(e[0]), e[1]) for e in exprs]
            # case for just a single pair passed as input
            elif len(exprs) == 2:
                if isinstance(exprs[0], str) and isinstance(exprs[1], str):
                    grouping_exprs = [(self.col(exprs[0]), exprs[1])]
            else:
                raise TypeError(
                    "Lists passed to DataFrame.agg() should only contain Column-objects, or pairs of strings."
                )
        elif isinstance(exprs, dict):
            grouping_exprs = []
            for k, v in exprs.items():
                if not (isinstance(k, str) and isinstance(v, str)):
                    raise TypeError(
                        f"Dictionary passed to DataFrame.agg() should contain only strings: got key-value pair with types {type(k), type(v)}"
                    )
                grouping_exprs.append((self.col(k), v))

        if grouping_exprs is None:
            raise TypeError(f"Invalid type passed to agg(): {type(exprs)}")

        return self.groupBy().agg(grouping_exprs)

    def rollup(
        self,
        *cols: Union[ColumnOrName, List[ColumnOrName], Tuple[ColumnOrName, ...]],
    ) -> "snowflake.snowpark.RelationalGroupedDataFrame":
        """Performs a SQL
        `GROUP BY ROLLUP <https://docs.snowflake.com/en/sql-reference/constructs/group-by-rollup.html>`_.
        on the DataFrame.

        Args:
            cols: The columns to group by rollup.
        """
        rollup_exprs = self.__convert_cols_to_exprs("rollup()", *cols)
        return snowflake.snowpark.RelationalGroupedDataFrame(
            self,
            rollup_exprs,
            snowflake.snowpark.relational_grouped_dataframe._RollupType(),
        )

    def groupBy(
        self,
        *cols: Union[ColumnOrName, List[ColumnOrName], Tuple[ColumnOrName, ...]],
    ) -> "snowflake.snowpark.RelationalGroupedDataFrame":
        """Groups rows by the columns specified by expressions (similar to GROUP BY in
        SQL).

        This method returns a :class:`RelationalGroupedDataFrame` that you can use to
        perform aggregations on each group of data.

        Args:
            *cols: The columns to group by.

        Valid inputs are:

            - Empty input
            - One or multiple :class:`Column` object(s) or column name(s) (:class:`str`)
            - A list of :class:`Column` objects or column names (:class:`str`)

        """
        grouping_exprs = self.__convert_cols_to_exprs("groupBy()", *cols)
        return snowflake.snowpark.RelationalGroupedDataFrame(
            self,
            grouping_exprs,
            snowflake.snowpark.relational_grouped_dataframe._GroupByType(),
        )

    def groupByGroupingSets(
        self,
        *grouping_sets: Union[
            "snowflake.snowpark.GroupingSets",
            List["snowflake.snowpark.GroupingSets"],
            Tuple["snowflake.snowpark.GroupingSets", ...],
        ],
    ) -> "snowflake.snowpark.RelationalGroupedDataFrame":
        """Performs a SQL
        `GROUP BY GROUPING SETS <https://docs.snowflake.com/en/sql-reference/constructs/group-by-grouping-sets.html>`_.
        on the DataFrame.

        GROUP BY GROUPING SETS is an extension of the GROUP BY clause
        that allows computing multiple GROUP BY clauses in a single statement.
        The group set is a set of dimension columns.

        GROUP BY GROUPING SETS is equivalent to the UNION of two or
        more GROUP BY operations in the same result set.


        Examples::

            df.groupByGroupingSets(GroupingSets([col("a")])).count().collect()  # is equivalent to
            df.groupByGroupingSets(GroupingSets(col("a"))).count().collect()  # is equivalent to
            df.groupBy("a").count().collect()

            df.groupByGroupingSets(GroupingSets([col("a")], [col("b")])).count().collect()  # is equivalent to
            df.groupBy("a").count().unionAll(df.groupBy("b").count()).collect()

            df.groupByGroupingSets(GroupingSets([col("a"), col("b")], [col("c")])).count().collect()  # is equivalent to
            df.groupBy("a", "b").count().unionAll(df.groupBy("c").count()).collect()

        Args:
            grouping_sets: The list of :class:`GroupingSets` to group by.
        """
        return snowflake.snowpark.RelationalGroupedDataFrame(
            self,
            [
                gs.to_expression
                for gs in Utils.parse_positional_args_to_list(*grouping_sets)
            ],
            snowflake.snowpark.relational_grouped_dataframe._GroupByType(),
        )

    def cube(
        self,
        *cols: Union[ColumnOrName, List[ColumnOrName], Tuple[ColumnOrName, ...]],
    ) -> "snowflake.snowpark.RelationalGroupedDataFrame":
        """Performs a SQL
        `GROUP BY CUBE <https://docs.snowflake.com/en/sql-reference/constructs/group-by-cube.html>`_.
        on the DataFrame.

        Args:
            cols: The columns to group by cube.
        """
        cube_exprs = self.__convert_cols_to_exprs("cube()", *cols)
        return snowflake.snowpark.RelationalGroupedDataFrame(
            self,
            cube_exprs,
            snowflake.snowpark.relational_grouped_dataframe._CubeType(),
        )

    def distinct(self) -> "DataFrame":
        """Returns a new DataFrame that contains only the rows with distinct values
        from the current DataFrame.

        This is equivalent to performing a SELECT DISTINCT in SQL.
        """
        return self.groupBy(
            [self.col(AnalyzerPackage.quote_name(f.name)) for f in self.schema.fields]
        ).agg([])

    def dropDuplicates(self, *subset: Union[str, Iterable[str]]) -> "DataFrame":
        """Creates a new DataFrame by removing duplicated rows on given subset of columns.

        If no subset of columns is specified, this function is the same as the :meth:`distinct` function.
        The result is non-deterministic when removing duplicated rows from the subset of columns but not all columns.

        For example, if we have a DataFrame ``df``, which has columns ("a", "b", "c") and contains three rows ``(1, 1, 1), (1, 1, 2), (1, 2, 3)``,
        the result of ``df.dropDuplicates("a", "b")`` can be either
        ``(1, 1, 1), (1, 2, 3)``
        or
        ``(1, 1, 2), (1, 2, 3)``

        Args:
            subset: The column names on which duplicates are dropped.

        :meth:`dropDuplicates` is an alias of :meth:`drop_duplicates`.
        """
        if not subset:
            return self.distinct()
        subset = Utils.parse_positional_args_to_list(*subset)

        filter_cols = [self.col(x) for x in subset]
        output_cols = [self.col(col_name) for col_name in self.columns]
        rownum = row_number().over(
            snowflake.snowpark.Window.partitionBy(*filter_cols).orderBy(*filter_cols)
        )
        rownum_name = Utils.generate_random_alphanumeric(10)
        return (
            self.select(*output_cols, rownum.as_(rownum_name))
            .where(col(rownum_name) == 1)
            .select(output_cols)
        )

    drop_duplicates = dropDuplicates

    def pivot(
        self,
        pivot_col: ColumnOrName,
        values: Union[List[LiteralType], Tuple[LiteralType, ...]],
    ) -> "snowflake.snowpark.RelationalGroupedDataFrame":
        """Rotates this DataFrame by turning the unique values from one column in the input
        expression into multiple columns and aggregating results where required on any
        remaining column values.

        Only one aggregate is supported with pivot.

        Example::

            df_pivoted = df.pivot("col_1", [1,2,3]).agg(sum(col("col_2")))

        Args:
            pivot_col: The column or name of the column to use.
            values: A list of values in the column.
        """
        pc = self.__convert_cols_to_exprs("pivot()", pivot_col)
        value_exprs = [
            v.expression if isinstance(v, Column) else SPLiteral(v) for v in values
        ]
        return snowflake.snowpark.RelationalGroupedDataFrame(
            self,
            [],
            snowflake.snowpark.relational_grouped_dataframe._PivotType(
                pc[0], value_exprs
            ),
        )

    def limit(self, n: int) -> "DataFrame":
        """Returns a new DataFrame that contains at most ``n`` rows from the current
        DataFrame (similar to LIMIT in SQL).

        Note that this is a transformation method and not an action method.

        Args:
            n: Number of rows to return.
        """
        return self.__with_plan(SPLimit(SPLiteral(n), self.__plan))

    def union(self, other: "DataFrame") -> "DataFrame":
        """Returns a new DataFrame that contains all the rows in the current DataFrame
        and another DataFrame (``other``), excluding any duplicate rows. Both input
        DataFrames must contain the same number of columns.

        Example::

             df1_and_2 = df1.union(df2)

        Args:
            other: the other :class:`DataFrame` that contains the rows to include.
        """
        return self.__with_plan(
            SPUnion(self.__plan, other._DataFrame__plan, is_all=False)
        )

    def unionAll(self, other: "DataFrame") -> "DataFrame":
        """Returns a new DataFrame that contains all the rows in the current DataFrame
        and another DataFrame (``other``), including any duplicate rows. Both input
        DataFrames must contain the same number of columns.

        Example::

             df1_and_2 = df1.unionAll(df2)

        Args:
            other: the other :class:`DataFrame` that contains the rows to include.
        """
        return self.__with_plan(
            SPUnion(self.__plan, other._DataFrame__plan, is_all=True)
        )

    def unionByName(self, other: "DataFrame") -> "DataFrame":
        """Returns a new DataFrame that contains all the rows in the current DataFrame
        and another DataFrame (``other``), excluding any duplicate rows.

        This method matches the columns in the two DataFrames by their names, not by
        their positions. The columns in the other DataFrame are rearranged to match
        the order of columns in the current DataFrame.

        Example::

             df1_and_2 = df1.unionByName(df2)

        Args:
            other: the other :class:`DataFrame` that contains the rows to include.
        """
        return self.__union_by_name_internal(other, is_all=False)

    def unionAllByName(self, other: "DataFrame") -> "DataFrame":
        """Returns a new DataFrame that contains all the rows in the current DataFrame
        and another DataFrame (``other``), including any duplicate rows.

        This method matches the columns in the two DataFrames by their names, not by
        their positions. The columns in the other DataFrame are rearranged to match
        the order of columns in the current DataFrame.

        Example::

             df1_and_2 = df1.unionAllByName(df2)

        Args:
            other: the other :class:`DataFrame` that contains the rows to include.
        """
        return self.__union_by_name_internal(other, is_all=True)

    def __union_by_name_internal(
        self, other: "DataFrame", is_all: bool = False
    ) -> "DataFrame":
        left_output_attrs = self.__output()
        right_output_attrs = other.__output()
        right_output_attr_by_name = {rattr.name: rattr for rattr in right_output_attrs}

        try:
            right_project_list = [
                right_output_attr_by_name[lattr.name] for lattr in left_output_attrs
            ]
        except KeyError:
            missing_lattrs = [
                lattr.name
                for lattr in left_output_attrs
                if lattr.name not in right_output_attr_by_name
            ]
            raise SnowparkClientExceptionMessages.DF_CANNOT_RESOLVE_COLUMN_NAME_AMONG(
                ", ".join(missing_lattrs),
                ", ".join(list(right_output_attr_by_name.keys())),
            )

        not_found_attrs = [
            rattr for rattr in right_output_attrs if rattr not in right_project_list
        ]

        right_child = self.__with_plan(
            SPProject(right_project_list + not_found_attrs, other.__plan)
        )

        return self.__with_plan(SPUnion(self.__plan, right_child.__plan, is_all))

    def intersect(self, other: "DataFrame") -> "DataFrame":
        """Returns a new DataFrame that contains the intersection of rows from the
        current DataFrame and another DataFrame (``other``). Duplicate rows are
        eliminated.

        Example::

            df_intersection_of_1_and_2 = df1.intersect(df2)

        Args:
            other: the other :class:`DataFrame` that contains the rows to use for the
                intersection.
        """
        return self.__with_plan(SPIntersect(self.__plan, other._DataFrame__plan))

    def except_(self, other: "DataFrame") -> "DataFrame":
        """Returns a new DataFrame that contains all the rows from the current DataFrame
        except for the rows that also appear in the ``other`` DataFrame. Duplicate rows are eliminated.

        Example::

            df1_except_df2 = df1.except_(df2)

        Args:
            other: The :class:`DataFrame` that contains the rows to exclude.
        """
        return self.__with_plan(SPExcept(self.__plan, other._DataFrame__plan))

    def naturalJoin(
        self, right: "DataFrame", join_type: Optional[str] = None
    ) -> "DataFrame":
        """Performs a natural join of the specified type (``joinType``) with the
        current DataFrame and another DataFrame (``right``).

        Examples::

            df_natural_join = df.naturalJoin(df2)
            df_natural_join = df.naturalJoin(df2, "left")

        Args:
            right: the other :class:`DataFrame` to join
            join_type: The type of join (e.g. "right", "outer", etc.). The default value is "inner".
        """
        join_type = join_type if join_type else "inner"
        return self.__with_plan(
            SPJoin(
                self.__plan,
                right._DataFrame__plan,
                SPNaturalJoin(SPJoinType.from_string(join_type)),
                None,
                SPJoinHint.none(),
            )
        )

    def join(
        self,
        right: "DataFrame",
        using_columns: Optional[Union[ColumnOrName, List[ColumnOrName]]] = None,
        join_type: Optional[str] = None,
    ) -> "DataFrame":
        """Performs a join of the specified type (``join_type``) with the current
        DataFrame and another DataFrame (``right``) on a list of columns
        (``using_columns``).

        The method assumes that the columns in ``using_columns`` have the same meaning
        in the left and right DataFrames.

        Examples::

            df_left_join = df1.join(df2, "a", "left")
            df_outer_join = df.join(df2, ["a","b"], "outer")

        Args:
            right: The other :class:`Dataframe` to join.
            using_columns: A list of names of the columns, or the column objects, to
                use for the join.
            join_type: The type of join (e.g. "right", "outer", etc.).
        """
        if isinstance(right, DataFrame):
            if self is right or self.__plan is right._DataFrame__plan:
                raise SnowparkClientExceptionMessages.DF_SELF_JOIN_NOT_SUPPORTED()

            if isinstance(join_type, SPCrossJoin) or (
                isinstance(join_type, str)
                and join_type.strip().lower().replace("_", "").startswith("cross")
            ):
                if using_columns:
                    raise Exception("Cross joins cannot take columns as input.")

            sp_join_type = (
                SPJoinType.from_string("inner")
                if not join_type
                else SPJoinType.from_string(join_type)
            )

            # Parse using_columns arg
            if using_columns is None:
                using_columns = []
            elif isinstance(using_columns, str):
                using_columns = [using_columns]
            elif isinstance(using_columns, Column):
                using_columns = using_columns
            elif not isinstance(using_columns, list):
                raise TypeError(
                    f"Invalid input type for join column: {type(using_columns)}"
                )

            return self.__join_dataframes(right, using_columns, sp_join_type)

        raise TypeError("Invalid type for join. Must be Dataframe")

    def joinTableFunction(
        self,
        func_name: Union[str, List[str]],
        *func_arguments: ColumnOrName,
        **func_named_arguments: ColumnOrName,
    ) -> "DataFrame":
        """Lateral joins the current DataFrame with the output of the specified table function.

        References: `Snowflake SQL functions <https://docs.snowflake.com/en/sql-reference/functions-table.html>`_.

        Example::

            df = session.sql("select 'James' as name, 'address1 address2 address3' as addresses")
            name_address_list = df.joinTableFunction("split_to_table", df["addresses"], lit(" ")).collect()

        Args:

            func_name: The SQL function name.
            func_arguments: The positional arguments for the SQL function.
            func_named_arguments: The named arguments for the SQL function, if it accepts named arguments.

        Returns:
            A new :class:`DataFrame` that has the columns carried from this :class:`DataFrame`, plus new columns and rows from the lateral join with the table function.

        See Also:
            - :meth:`Session.table_function`, which creates a new :class:`DataFrame` by using the SQL table function.

        """
        func_expr = _create_table_function_expression(
            func_name, *func_arguments, **func_named_arguments
        )
        return DataFrame(self.session, SPTableFunctionJoin(self.__plan, func_expr))

    def crossJoin(self, right: "DataFrame") -> "DataFrame":
        """Performs a cross join, which returns the Cartesian product of the current
        :class:`DataFrame` and another :class:`DataFrame` (``right``).

        If the current and ``right`` DataFrames have columns with the same name, and
        you need to refer to one of these columns in the returned DataFrame, use the
        :func:`col` function on the current or ``right`` DataFrame to disambiguate
        references to these columns.

        Example::

            df_cross = this.crossJoin(right)
            project = df_cross.select([this(["common_col"]), right(["common_col"])])

        Args:
            right: the right :class:`DataFrame` to join.
        """
        return self.__join_dataframes_internal(
            right, SPJoinType.from_string("cross"), None
        )

    def __join_dataframes(
        self,
        right: "DataFrame",
        using_columns: Union[Column, List[str]],
        join_type: SPJoinType,
    ) -> "DataFrame":
        if isinstance(using_columns, Column):
            return self.__join_dataframes_internal(
                right, join_type, join_exprs=using_columns
            )

        if isinstance(join_type, (SPLeftSemi, SPLeftAnti)):
            # Create a Column with expression 'true AND <expr> AND <expr> .."
            join_cond = Column(SPLiteral(True))
            for c in using_columns:
                quoted = AnalyzerPackage.quote_name(c)
                join_cond = join_cond & (self.col(quoted) == right.col(quoted))
            return self.__join_dataframes_internal(right, join_type, join_cond)
        else:
            lhs, rhs = self.__disambiguate(self, right, join_type, using_columns)
            return self.__with_plan(
                SPJoin(
                    lhs._DataFrame__plan,
                    rhs._DataFrame__plan,
                    SPUsingJoin(join_type, using_columns),
                    None,
                    SPJoinHint.none(),
                )
            )

    def __join_dataframes_internal(
        self, right: "DataFrame", join_type: SPJoinType, join_exprs: Optional[Column]
    ) -> "DataFrame":
        (lhs, rhs) = self.__disambiguate(self, right, join_type, [])
        expression = join_exprs.expression if join_exprs else None
        return self.__with_plan(
            SPJoin(
                lhs._DataFrame__plan,
                rhs._DataFrame__plan,
                join_type,
                expression,
                SPJoinHint.none(),
            )
        )

    # TODO complete function. Requires TableFunction
    def __join_dataframe_table_function(self, table_function, columns) -> "DataFrame":
        pass

    def withColumn(self, col_name: str, col: Column) -> "DataFrame":
        """
        Returns a DataFrame with an additional column with the specified name
        ``col_name``. The column is computed by using the specified expression ``col``.

        If a column with the same name already exists in the DataFrame, that column is
        replaced by the new column.

        This example adds a new column named ``mean_price`` that contains the mean of
        the existing ``price`` column in the DataFrame::

            df_with_mean_price_col = df.withColumn("mean_price", mean(col("price")))

        Args:
            col_name: The name of the column to add or replace.
            col: The :class:`Column` to add or replace.
        """
        return self.withColumns([col_name], [col])

    def withColumns(self, col_names: List[str], values: List[Column]) -> "DataFrame":
        """Returns a DataFrame with additional columns with the specified names
        ``col_names``. The columns are computed by using the specified expressions
        ``values``.

        If columns with the same names already exist in the DataFrame, those columns
        are removed and appended at the end by new columns.

        This example adds new columns named ``mean_price`` and ``avg_price`` that
        contain the mean and average of the existing ``price`` column::

            df_with_added_columns = df.withColumns(["mean_price", "avg_price"],
                                                   [mean(col("price")),
                                                   avg(col("price"))])

        Args:
            col_names: A list of the names of the columns to add or replace.
            values: A list of the :class:`Column` objects to
                    add or replace.
        """
        if len(col_names) != len(values):
            raise ValueError(
                f"The size of column names ({len(col_names)}) is not equal to the size of columns ({len(values)})"
            )

        # Get a list of the new columns and their dedupped values
        qualified_names = [AnalyzerPackage.quote_name(n) for n in col_names]
        new_column_names = set(qualified_names)

        if len(col_names) != len(new_column_names):
            raise ValueError(
                "The same column name is used multiple times in the col_names parameter."
            )

        new_cols = [col.as_(name) for name, col in zip(qualified_names, values)]

        # Get a list of existing column names that are not being replaced
        old_cols = [
            Column(field)
            for field in self.__output()
            if field.name not in new_column_names
        ]

        # Put it all together
        return self.select([*old_cols, *new_cols])

    def count(self) -> int:
        """Executes the query representing this DataFrame and returns the number of
        rows in the result (similar to the COUNT function in SQL).
        """
        return self.agg(("*", "count"))._collect_with_tag()[0][0]

    @property
    def write(self) -> DataFrameWriter:
        """Returns a new :class:`DataFrameWriter` object that you can use to write the data in the :class:`DataFrame` to
        a Snowflake database.

        Example::

            df.write.mode("overwrite").saveAsTable("table1")
        """

        return DataFrameWriter(self)

    def copy_into_table(
        self,
        table_name: Union[str, Iterable[str]],
        *,
        files: Optional[Iterable[str]] = None,
        pattern: Optional[str] = None,
        validation_mode: Optional[str] = None,
        target_columns: Optional[Iterable[str]] = None,
        transformations: Optional[Iterable[Union[Column, str]]] = None,
        format_type_options: Optional[Dict[str, Any]] = None,
        **copy_options: Any,
    ) -> List[Row]:
        """Executes a `COPY INTO <table> <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html>`__ command to load data from files in a stage location into a specified table.

        It returns the load result described in `OUTPUT section of the COPY INTO <table> command <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#output>`__.
        The returned result also depends on the value of ``validation_mode``.

        It's slightly different from the ``COPY INTO`` command in that this method will automatically create a table if the table doesn't exist and the input files are CSV files whereas the ``COPY INTO <table>`` doesn't.

        To call this method, this DataFrame must be created from a :class:`DataFrameReader`.

        Example::

            # user_schema is used to read from CSV files. For other files it's not needed.
            user_schema = StructType(StructField("A", StringType()), StructField("A_LEN", IntegerType())
            stage_location = "@somestage/somefiles.csv"
            # Use the DataFrameReader (session.read below) to read from CSV files.
            df = session.read.schema(user_schema).csv(stage_location)
            # specify transformations and target column names. It's optional for the "copy into" command
            transformations = [col("$1"), length(col("$1"))]
            target_column_names = ["A", "A_LEN"]
            # Use format type options and copy options
            csv_file_format_options = {"skip_header": 2}
            df.copy_into_table("T", target_column_names, transformations, format_type_options=csv_file_format_options, force=True)

        The arguments of this function match the optional parameters of the `COPY INTO <table> <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html#optional-parameters>`__.

        Args:
            table_name: A string or list of strings that specify the table name or fully-qualified object identifier
                (database name, schema name, and table name).
            files: Specific files to load from the stage location.
            pattern: The regular expression that is used to match file names of the stage location.
            validation_mode: A ``str`` that instructs the ``COPY INTO <table>`` command to validate the data files instead of loading them into the specified table.
                Values can be "RETURN_n_ROWS", "RETURN_ERRORS", or "RETURN_ALL_ERRORS". Refer to the above mentioned ``COPY INTO <table>`` command optional parameters for more details.
            target_columns: Name of the columns in the table where the data should be saved.
            transformations: A list of column transformations.
            format_type_options: A dict that contains the ``formatTypeOptions`` of the ``COPY INTO <table>`` command.
            copy_options: The kwargs that is used to specify the ``copyOptions`` of the ``COPY INTO <table>`` command.
        """
        if not self._reader or not self._reader._file_path:
            raise SnowparkDataframeException(
                "To copy into a table, the DataFrame must be created from a DataFrameReader and specify a file path."
            )
        target_columns = tuple(target_columns) if target_columns else None
        transformations = tuple(transformations) if transformations else None
        if (
            target_columns
            and transformations
            and len(target_columns) != len(transformations)
        ):
            raise ValueError(
                f"Number of column names provided to copy into does not match the number of transformations provided. Number of column names: {len(target_columns)}, number of transformations: {len(transformations)}"
            )

        full_table_name = (
            table_name if isinstance(table_name, str) else ".".join(table_name)
        )
        Utils.validate_object_name(full_table_name)
        pattern = pattern or self._reader._cur_options.get("pattern")
        format_type_options = format_type_options or self._reader._cur_options.get(
            "format_type_options"
        )
        target_columns = target_columns or self._reader._cur_options.get(
            "target_columns"
        )
        transformations = transformations or self._reader._cur_options.get(
            "transformations"
        )
        transformations = (
            [_to_col_if_str(column, "copy_into_table") for column in transformations]
            if transformations
            else None
        )
        copy_options = copy_options or self._reader._cur_options.get("copy_options")
        validation_mode = validation_mode or self._reader._cur_options.get(
            "validation_mode"
        )
        normalized_column_names = (
            [AnalyzerPackage.quote_name(col_name) for col_name in target_columns]
            if target_columns
            else None
        )
        transformation_exps = (
            [
                column.expression if isinstance(column, Column) else column
                for column in transformations
            ]
            if transformations
            else None
        )
        return DataFrame(
            self.session,
            CopyIntoNode(
                full_table_name,
                file_path=self._reader._file_path,
                files=files,
                file_format=self._reader._file_type,
                pattern=pattern,
                column_names=normalized_column_names,
                transformations=transformation_exps,
                copy_options=copy_options,
                format_type_options=format_type_options,
                validation_mode=validation_mode,
                user_schema=self._reader._user_schema,
                cur_options=self._reader._cur_options,
            ),
        )._collect_with_tag()

    def show(self, n: int = 10, max_width: int = 50) -> None:
        """Evaluates this DataFrame and prints out the first ``n`` rows with the
        specified maximum number of characters per column.

        Args:
            n: The number of rows to print out.
            max_width: The maximum number of characters to print out for each column.
                If the number of characters exceeds the maximum, the method prints out
                an ellipsis (...) at the end of the column.
        """
        print(
            self.__show_string(
                n,
                max_width,
                _statement_params={"QUERY_TAG": Utils.create_statement_query_tag(2)}
                if not self.session.query_tag
                else None,
            )
        )

    def flatten(
        self,
        input: ColumnOrName,
        path: Optional[str] = None,
        outer: bool = False,
        recursive: bool = False,
        mode: str = "BOTH",
    ) -> "DataFrame":
        """Flattens (explodes) compound values into multiple rows.

        It creates a new ``DataFrame`` from this ``DataFrame``, carries the existing columns to the new ``DataFrame``,
        and adds the following columns to it:

            - SEQ
            - KEY
            - PATH
            - INDEX
            - VALUE
            - THIS

        References: `Snowflake SQL function FLATTEN <https://docs.snowflake.com/en/sql-reference/functions/flatten.html>`_.

        If this ``DataFrame`` also has columns with the names above, you can disambiguate the columns by renaming them.

        Example::

            table1 = session.sql("select parse_json(value) as value from values('[1,2]') as T(value)")
            flattened = table1.flatten(table1["value"])
            flattened.select(table1["value"], flattened["value"].as_("newValue")).show()

        Args:
            input: The name of a column or a :class:`Column` instance that will be unseated into rows.
                The column data must be of Snowflake data type VARIANT, OBJECT, or ARRAY.
            path: The path to the element within a VARIANT data structure which needs to be flattened.
                The outermost element is to be flattened if path is empty or ``None``.
            outer: If ``False``, any input rows that cannot be expanded, either because they cannot be accessed in the ``path``
                or because they have zero fields or entries, are completely omitted from the output.
                Otherwise, exactly one row is generated for zero-row expansions
                (with NULL in the KEY, INDEX, and VALUE columns).
            recursive: If ``False``, only the element referenced by ``path`` is expanded.
                Otherwise, the expansion is performed for all sub-elements recursively.
            mode: Specifies which types should be flattened "OBJECT", "ARRAY", or "BOTH".

        Returns:
            A new :class:`DataFrame` that has the columns carried from this :class:`DataFrame`, the flattened new columns and new rows.

        See Also:
            - :meth:`Session.flatten`, which creates a new :class:`DataFrame` by flattening compound values into multiple rows.
        """
        mode = mode.upper()
        if mode not in ("OBJECT", "ARRAY", "BOTH"):
            raise ValueError("mode must be one of ('OBJECT', 'ARRAY', 'BOTH')")

        if isinstance(input, str):
            input = self.col(input)
        return self._lateral(
            FlattenFunction(input.expression, path, outer, recursive, mode)
        )

    def _lateral(self, table_function: SPTableFunctionExpression) -> "DataFrame":
        result_columns = [
            attr.name
            for attr in self.session._analyzer.resolve(
                SPLateral(self.__plan, table_function)
            ).attributes()
        ]
        common_col_names = [k for k, v in Counter(result_columns).items() if v > 1]
        if len(common_col_names) == 0:
            return DataFrame(self.session, SPLateral(self.__plan, table_function))
        prefix = DataFrame.__generate_prefix("a")
        child = self.select(
            [
                self.__alias_if_needed(self, attr.name, prefix, common_col_names)
                for attr in self.__output()
            ]
        )
        return DataFrame(self.session, SPLateral(child.__plan, table_function))

    def __show_string(self, n: int = 10, max_width: int = 50, **kwargs) -> str:
        query = self.__plan.queries[-1].sql.strip().lower()

        if query.startswith("select"):
            result, meta = self.session._conn.get_result_and_metadata(
                self.limit(n)._DataFrame__plan, **kwargs
            )
        else:
            res, meta = self.session._conn.get_result_and_metadata(
                self.__plan, **kwargs
            )
            result = res[:n]

        # The query has been executed
        col_count = len(meta)
        col_width = []
        header = []
        for field in meta:
            name = field.name
            col_width.append(len(name))
            header.append(name)

        body = []
        for row in result:
            lines = []
            for i, v in enumerate(row):
                texts = str(v).split("\n") if v is not None else ["NULL"]
                for t in texts:
                    col_width[i] = max(len(t), col_width[i])
                    col_width[i] = min(max_width, col_width[i])
                lines.append(texts)

            # max line number in this row
            line_count = max(len(li) for li in lines)
            res = []
            for line_number in range(line_count):
                new_line = []
                for colIndex in range(len(lines)):
                    n = (
                        lines[colIndex][line_number]
                        if len(lines[colIndex]) > line_number
                        else ""
                    )
                    new_line.append(n)
                res.append(new_line)
            body.extend(res)

        # Add 2 more spaces in each column
        col_width = [w + 2 for w in col_width]

        total_width = sum(col_width) + col_count + 1
        line = "-" * total_width + "\n"

        def row_to_string(row: List[str]) -> str:
            tokens = []
            for segment, size in zip(row, col_width):
                if len(segment) > max_width:
                    # if truncated, add ... to the end
                    formatted = (segment[: max_width - 3] + "...").ljust(size, " ")
                else:
                    formatted = segment.ljust(size, " ")
                tokens.append(formatted)
            return f"|{'|'.join(tok for tok in tokens)}|\n"

        return (
            line
            + row_to_string(header)
            + line
            + "".join(row_to_string(b) for b in body)
            + line
        )

    def createOrReplaceView(
        self, name: Union[str, List[str], Tuple[str, ...]]
    ) -> List[Row]:
        """Creates a view that captures the computation expressed by this DataFrame.

        For ``name``, you can include the database and schema name (i.e. specify a
        fully-qualified name). If no database name or schema name are specified, the
        view will be created in the current database or schema.

        ``name`` must be a valid `Snowflake identifier <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_.

        Args:
            name: The name of the view to create or replace. Can be a list of strings
                that specifies the database name, schema name, and view name.
        """
        if isinstance(name, str):
            formatted_name = name
        elif isinstance(name, (list, tuple)) and all(isinstance(n, str) for n in name):
            formatted_name = ".".join(name)
        else:
            raise TypeError(
                f"The input of createOrReplaceView() can only a str or list of strs."
            )

        return self.__do_create_or_replace_view(
            formatted_name,
            SPPersistedView(),
            _statement_params={"QUERY_TAG": Utils.create_statement_query_tag(2)}
            if not self.session.query_tag
            else None,
        )

    def createOrReplaceTempView(
        self, name: Union[str, List[str], Tuple[str, ...]]
    ) -> List[Row]:
        """Creates a temporary view that returns the same results as this DataFrame.

        You can use the view in subsequent SQL queries and statements during the
        current session. The temporary view is only available in the session in which
        it is created.

        For ``name``, you can include the database and schema name (i.e. specify a
        fully-qualified name). If no database name or schema name are specified, the
        view will be created in the current database or schema.

        ``name`` must be a valid `Snowflake identifier <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_.

        Args:
            name: The name of the view to create or replace. Can be a list of strings
                that specifies the database name, schema name, and view name.
        """
        if isinstance(name, str):
            formatted_name = name
        elif isinstance(name, (list, tuple)) and all(isinstance(n, str) for n in name):
            formatted_name = ".".join(name)
        else:
            raise TypeError(
                f"The input of createOrReplaceTempView() can only a str or list of strs."
            )

        return self.__do_create_or_replace_view(
            formatted_name,
            SPLocalTempView(),
            _statement_params={"QUERY_TAG": Utils.create_statement_query_tag(2)}
            if not self.session.query_tag
            else None,
        )

    def __do_create_or_replace_view(
        self, view_name: str, view_type: SPViewType, **kwargs
    ):
        Utils.validate_object_name(view_name)
        name = TableIdentifier(view_name)
        cmd = SPCreateViewCommand(
            name=name,
            user_specified_columns=[],
            comment=None,
            properties={},
            original_text="",
            child=self.__plan,
            allow_existing=False,
            replace=True,
            view_type=view_type,
        )

        return self.session._conn.execute(self.session._analyzer.resolve(cmd), **kwargs)

    def first(self, n: Optional[int] = None) -> Union[Optional[Row], List[Row]]:
        """Executes the query representing this DataFrame and returns the first ``n``
        rows of the results.

        Returns:
             A list of the first ``n`` :class:`Row` objects if ``n`` is not ``None``. If ``n`` is negative or
             larger than the number of rows in the result, returns all rows in the
             results. ``n`` is ``None``, it returns the first :class:`Row` of
             results, or ``None`` if it does not exist.
        """
        if n is None:
            result = self.limit(1)._collect_with_tag()
            return result[0] if result else None
        elif not isinstance(n, int):
            raise ValueError(f"Invalid type of argument passed to first(): {type(n)}")
        elif n < 0:
            return self._collect_with_tag()
        else:
            return self.limit(n)._collect_with_tag()

    take = first

    def sample(
        self, frac: Optional[float] = None, n: Optional[int] = None
    ) -> "DataFrame":
        """Samples rows based on either the number of rows to be returned or a
        percentage of rows to be returned.

        Args:
            frac: the percentage of rows to be sampled.
            n: the number of rows to sample in the range of 0 to 1,000,000 (inclusive).
        Returns:
            a :class:`DataFrame` containing the sample of rows.
        """
        if frac is None and n is None:
            raise ValueError(
                "probability_fraction and row_count cannot both be None. "
                "One of those values must be defined"
            )
        if frac is not None and (frac < 0.0 or frac > 1.0):
            raise ValueError(
                f"probability_fraction value {frac} "
                f"is out of range (0 <= probability_fraction <= 1)"
            )
        if n is not None and n < 0:
            raise ValueError(f"row_count value {n} must be greater than 0")

        return self.__with_plan(
            SPSample(self.__plan, probability_fraction=frac, row_count=n)
        )

    @property
    def na(self) -> DataFrameNaFunctions:
        """
        Returns a :class:`DataFrameNaFunctions` object that provides functions for
        handling missing values in the DataFrame.
        """
        return self._na

    def withColumnRenamed(self, existing: ColumnOrName, new: str) -> "DataFrame":
        """Returns a DataFrame with the specified column ``existing`` renamed as ``new``.

        Example::

            # This example renames the column `A` as `NEW_A` in the DataFrame.
            df = session.sql("select 1 as A, 2 as B")
            df_renamed = df.withColumnRenamed(col("A"), "NEW_A")

        Args:
            existing: The old column instance or column name to be renamed.
            new: The new column name.

        :meth:`withColumnRenamed` is an alias of :meth:`rename`.
        """
        new_quoted_name = AnalyzerPackage.quote_name(new)
        if isinstance(existing, str):
            old_name = AnalyzerPackage.quote_name(existing)
        elif isinstance(existing, Column):
            if isinstance(existing.expression, SPAttribute):
                att = existing.expression
                old_name = self.__plan.expr_to_alias.get(att.expr_id, att.name)
            elif isinstance(existing.expression, SPNamedExpression):
                old_name = existing.expression.name
            else:
                raise ValueError(
                    f"Unable to rename column {existing} because it doesn't exist."
                )
        else:
            raise TypeError("'exisitng' must be a column name or Column object.")

        to_be_renamed = [x for x in self.columns if x.upper() == old_name.upper()]
        if not to_be_renamed:
            raise ValueError(
                f'Unable to rename column "{existing}" because it doesn\'t exist.'
            )
        elif len(to_be_renamed) > 1:
            raise SnowparkClientExceptionMessages.DF_CANNOT_RENAME_COLUMN_BECAUSE_MULTIPLE_EXIST(
                old_name, new_quoted_name, len(to_be_renamed)
            )
        new_columns = [
            Column(att).as_(new_quoted_name) if old_name == att.name else Column(att)
            for att in self.__output()
        ]
        return self.select(new_columns)

    rename = withColumnRenamed

    # Utils
    def __resolve(self, col_name: str) -> SPNamedExpression:
        normalized_col_name = AnalyzerPackage.quote_name(col_name)
        cols = list(
            filter(lambda attr: attr.name == normalized_col_name, self.__output())
        )
        if len(cols) == 1:
            return cols[0].with_name(normalized_col_name)
        else:
            raise SnowparkClientExceptionMessages.DF_CANNOT_RESOLVE_COLUMN_NAME(
                col_name
            )

    @staticmethod
    def __alias_if_needed(
        df: "DataFrame", c: str, prefix: str, common_col_names: List[str]
    ):
        col = df.col(c)
        unquoted = c.strip('"')
        if c in common_col_names:
            return col.alias(f'"{prefix}{unquoted}"')
        else:
            return col.alias(f'"{unquoted}"')

    def __disambiguate(
        self,
        lhs: "DataFrame",
        rhs: "DataFrame",
        join_type: SPJoinType,
        using_columns: List[str],
    ):
        # Normalize the using columns.
        normalized_using_columns = {
            AnalyzerPackage.quote_name(c) for c in using_columns
        }
        #  Check if the LHS and RHS have columns in common. If they don't just return them as-is. If
        #  they do have columns in common, alias the common columns with randomly generated l_
        #  and r_ prefixes for the left and right sides respectively.
        #  We assume the column names from the schema are normalized and quoted.
        lhs_names = [attr.name for attr in lhs.__output()]
        rhs_names = [attr.name for attr in rhs.__output()]
        common_col_names = [
            n
            for n in lhs_names
            if n in set(rhs_names) and n not in normalized_using_columns
        ]

        lhs_prefix = self.__generate_prefix("l")
        rhs_prefix = self.__generate_prefix("r")

        lhs_remapped = lhs.select(
            [
                self.__alias_if_needed(
                    lhs,
                    name,
                    lhs_prefix,
                    []
                    if isinstance(join_type, (SPLeftSemi, SPLeftAnti))
                    else common_col_names,
                )
                for name in lhs_names
            ]
        )

        rhs_remapped = rhs.select(
            [
                self.__alias_if_needed(rhs, name, rhs_prefix, common_col_names)
                for name in rhs_names
            ]
        )
        return lhs_remapped, rhs_remapped

    def __output(self) -> List[SPAttribute]:
        if not self.__placeholder_output:
            self.__placeholder_output = self.__plan.output()
        return self.__placeholder_output

    @property
    def schema(self) -> StructType:
        """The definition of the columns in this DataFrame (the "relational schema" for
        the DataFrame).
        """
        if not self.__placeholder_schema:
            self.__placeholder_schema = StructType._from_attributes(
                self.__plan.attributes()
            )
        return self.__placeholder_schema

    def __with_plan(self, plan):
        return DataFrame(self.session, plan)

    def __convert_cols_to_exprs(
        self,
        calling_method: str,
        *cols: Union[ColumnOrName, List[ColumnOrName], Tuple[ColumnOrName]],
    ) -> List["SPExpression"]:
        """Convert a string or a Column, or a list of string and Column objects to expression(s)."""

        def convert(col: ColumnOrName):
            if isinstance(col, str):
                return self.__resolve(col)
            elif isinstance(col, Column):
                return col.expression
            else:
                raise TypeError(
                    "{} only accepts str and Column objects, or a list containing str and"
                    " Column objects".format(calling_method)
                )

        exprs = [convert(col) for col in Utils.parse_positional_args_to_list(*cols)]
        return exprs

    def sqls(self) -> List[str]:
        plan = self.__plan  # type: SnowflakePlan
        return [x.sql for x in plan.queries]

    def post_action_sqls(self) -> List[str]:
        plan = self.__plan  # type: SnowflakePlan
        return plan.post_actions
