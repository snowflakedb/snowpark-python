#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import enum
import string
from random import choice
from typing import List, Optional, Tuple, Union

from snowflake.snowpark.column import Column
from snowflake.snowpark.dataframe_writer import DataFrameWriter
from snowflake.snowpark.internal.analyzer.analyzer_package import AnalyzerPackage
from snowflake.snowpark.internal.analyzer.limit import Limit as SPLimit
from snowflake.snowpark.internal.analyzer.sp_identifiers import TableIdentifier
from snowflake.snowpark.internal.analyzer.sp_views import (
    CreateViewCommand as SPCreateViewCommand,
    LocalTempView as SPLocalTempView,
    PersistedView as SPPersistedView,
    ViewType as SPViewType,
)
from snowflake.snowpark.internal.sp_expressions import (
    Ascending as SPAscending,
    Attribute as SPAttribute,
    Descending as SPDescending,
    Expression as SPExpression,
    Literal as SPLiteral,
    NamedExpression as SPNamedExpression,
    SortOrder as SPSortOrder,
    Star as SPStar,
)
from snowflake.snowpark.internal.utils import Utils
from snowflake.snowpark.plans.logical.basic_logical_operators import (
    Except as SPExcept,
    Intersect as SPIntersect,
    Join as SPJoin,
    Sort as SPSort,
    Union as SPUnion,
)
from snowflake.snowpark.plans.logical.hints import JoinHint as SPJoinHint
from snowflake.snowpark.plans.logical.logical_plan import (
    Filter as SPFilter,
    Project as SPProject,
    Sample as SPSample,
)
from snowflake.snowpark.row import Row
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.types.sf_types import StructType
from snowflake.snowpark.types.sp_data_types import LongType as SPLongType
from snowflake.snowpark.types.sp_join_types import (
    Cross as SPCrossJoin,
    JoinType as SPJoinType,
    LeftAnti as SPLeftAnti,
    LeftSemi as SPLeftSemi,
    NaturalJoin as SPNaturalJoin,
    UsingJoin as SPUsingJoin,
)


class DataFrame:
    """Represents a lazily-evaluated relational dataset that contains a collection
    of :class:`Row` objects with columns defined by a schema (column name and type).

    A DataFrame is considered lazy because it encapsulates the computation or query
    required to produce a relational dataset. The computation is not performed until
    you call a method that performs an action (e.g. :func:`collect`).

    .. rubric:: Creating a DataFrame

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


    .. rubric:: Performing operations on a DataFrame

    Broadly, the operations on DataFrame can be divided into two types:

    - **Transformations** produce a new DataFrame from one or more existing DataFrames. Note that tranformations are lazy and don't cause the DataFrame to be evaluated. If the API does not provide a method to express the SQL that you want to use, you can use :func:`functions.sqlExpr` as a workaround.
    - **Actions** cause the DataFrame to be evaluated. When you call a method that performs an action, Snowpark sends the SQL query for the DataFrame to the server for evaluation.

    .. rubric:: Transforming a DataFrame

    The following examples demonstrate how you can transform a DataFrame.

    Example 5
        Using the :func:`select()` method to select the columns that should be in the
        DataFrame (similar to adding a `SELECT` clause)::

            # Return a new DataFrame containing the ID and amount columns of the prices table.
            # This is equivalent to: SELECT ID, AMOUNT FROM PRICES;
            df_price_ids_and_amounts = df_prices.select(col("ID"), col("amount"))

    Example 6
        Using the :func:`Column.as_` method to rename a column in a DataFrame (similar
        to using `SELECT col AS alias`)::

            # Return a new DataFrame containing the ID column of the prices table as a column named
            # itemId. This is equivalent to: SELECT ID AS itemId FROM PRICES;
            df_price_item_ids = df_prices.select(col("ID").as_("itemId"))

    Example 7
        Using the :func:`filter` method to filter data (similar to adding a `WHERE` clause)::

            # Return a new DataFrame containing the row from the prices table with the ID 1.
            # This is equivalent to:
            # SELECT FROM PRICES WHERE ID = 1;
            df_price1 = df_prices.filter((col("ID") === 1))

    Example 8
        Using the :func:`sort()` method to specify the sort order of the data (similar to adding an `ORDER BY` clause)::

            # Return a new DataFrame for the prices table with the rows sorted by ID.
            # This is equivalent to: SELECT FROM PRICES ORDER BY ID;
            df_sorted_prices = df_prices.sort(col("ID"))

    Example 9
        Using the :func:`groupBy()` method to return a
        :class:`RelationalGroupedDataFrame` that you can use to group and aggregate
        results (similar to adding a `GROUP BY` clause).

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

    .. rubric:: Performing an action on a DataFrame

    The following examples demonstrate how you can perform an action on a DataFrame.

    Example 10
        Performing a query and returning an array of Rows::

            results = df_prices.collect()

    Example 11
        Performing a query and print the results::

            df_prices.show()
    """

    __NUM_PREFIX_DIGITS = 4

    def __init__(self, session=None, plan=None):
        self.session = session
        self.__plan = session.analyzer.resolve(plan)

        # Use this to simulate scala's lazy val
        self.__placeholder_schema = None
        self.__placeholder_output = None

    @staticmethod
    def __generate_prefix(prefix: str) -> str:
        alphanumeric = string.ascii_lowercase + string.digits
        return f"{prefix}_{''.join(choice(alphanumeric) for _ in range(DataFrame.__NUM_PREFIX_DIGITS))}_"

    def collect(self) -> List["Row"]:
        """Executes the query representing this DataFrame and returns the result as a
        list of :class:`Row` objects.

        Returns:
            :class:`DataFrame`
        """
        return self.session.conn.execute(self.__plan)

    def clone(self) -> "DataFrame":
        """Returns a clone of this :class:`DataFrame`.

        Returns:
            :class:`DataFrame`
        """
        return DataFrame(self.session, self.__plan.clone())

    def toPandas(self, **kwargs):
        """Returns the contents of this DataFrame as Pandas DataFrame.

        This method is only available if Pandas is installed and available.

        Returns:
            :class:`pandas.DataFrame`
        """
        return self.session.conn.execute(self.__plan, to_pandas=True, **kwargs)

    def toDF(self, *names: Union[str, List[str]]) -> "DataFrame":
        """
        Creates a new DataFrame containing columns with the specified names.

        The number of column names that you pass in must match the number of columns in the existing
        DataFrame.

        Examples::

            df = session.range(1, 10, 2).toDF("col1")
            df = session.range(1, 10, 2).toDF(["col1"])

        Args:
            names: list of new column names

        Returns:
            :class:`DataFrame`
        """
        col_names = Utils.parse_positional_args_to_list(*names)
        if not all(type(n) == str for n in col_names):
            raise TypeError(f"Invalid input type in toDF(), expected str or list[str].")

        assert len(self.__output()) == len(col_names), (
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
        if type(item) == str:
            return self.col(item)
        elif isinstance(item, Column):
            return self.filter(item)
        elif type(item) in [list, tuple]:
            return self.select(item)
        elif type(item) == int:
            return self.__getitem__(self.columns[item])
        else:
            raise TypeError(f"unexpected item type: {type(item)}")

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

    def col(self, col_name: str) -> "Column":
        """Returns a reference to a column in the DataFrame.

        Returns:
            :class:`Column`
        """
        if col_name == "*":
            return Column(SPStar(self.__plan.output()))
        else:
            return Column(self.__resolve(col_name))

    def select(
        self,
        *cols: Union[str, Column, List[Union[str, Column]], Tuple[Union[str, Column]]],
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

        Returns:
             :class:`DataFrame`
        """
        exprs = Utils.parse_positional_args_to_list(*cols)
        if not exprs:
            raise TypeError("select() input cannot be empty")

        if not all(type(e) in [Column, str] for e in exprs):
            raise TypeError("select() input must be Column, str, or list")

        names = [e.named() if type(e) == Column else Column(e).named() for e in exprs]
        return self.__with_plan(SPProject(names, self.__plan))

    def drop(
        self,
        *cols: Union[str, Column, List[Union[str, Column]], Tuple[Union[str, Column]]],
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
        if not cols:
            raise TypeError("drop() input cannot be empty")
        exprs = Utils.parse_positional_args_to_list(*cols)

        names = []
        for c in exprs:
            if type(c) is str:
                names.append(c)
            elif type(c) is Column and isinstance(c.expression, SPNamedExpression):
                names.append(c.expression.name)
            else:
                raise SnowparkClientException(
                    f"Could not drop column {str(c)}. Can only drop columns by name."
                )

        normalized = {AnalyzerPackage.quote_name(n) for n in names}
        existing = [attr.name for attr in self.__output()]
        keep_col_names = [c for c in existing if c not in normalized]
        if not keep_col_names:
            raise SnowparkClientException("Cannot drop all columns")
        else:
            return self.select(list(keep_col_names))

    def filter(self, expr: Column) -> "DataFrame":
        """Filters rows based on the specified conditional expression (similar to WHERE
        in SQL).

        Example::

            df_filtered = df.filter(col("A") > 1 && col("B") < 100)

        Args:
            expr: a :class:`Column` expression.

        Returns:
            a filtered :class:`DataFrame`
        """
        if type(expr) != Column:
            raise TypeError(
                f"DataFrame.filter() input type must be Column. Got: {type(expr)}"
            )

        return self.__with_plan(SPFilter(expr.expression, self.__plan))

    def where(self, expr: Column) -> "DataFrame":
        """Filters rows based on the specified conditional expression (similar to WHERE
        in SQL). This is equivalent to calling :func:`filter()`.

        Examples::

            # The following two result in the same SQL query:
            prices_df.filter(col("price") > 100)
            prices_df.where(col("price") > 100)

        Args:
            expr: a :class:`Column` expression.

        Returns:
            a filtered :class:`DataFrame`
        """
        return self.filter(expr)

    def sort(
        self,
        *cols: Union[str, Column, List[Union[str, Column]], Tuple[Union[str, Column]]],
        ascending: Union[
            bool, int, List[Union[bool, int]], Tuple[Union[bool, int]]
        ] = None,
    ) -> "DataFrame":
        """Sorts a DataFrame by the specified expressions (similar to ORDER BY in SQL).

        Example::

            df_sorted = df.sort(col("A"), col(B).asc)

        Args:
            *cols: list of Column or column names to sort by.
            ascending: boolean or list of boolean (default True). Sort ascending vs.
                descending. Specify list for multiple sort orders. If a list is
                specified, length of the list must equal length of the cols.

        Returns:
            a sorted :class:`DataFrame`
        """
        if not cols:
            raise ValueError("sort() needs at least one sort expression.")
        exprs = self.__convert_cols_to_exprs("sort()", *cols)
        if not exprs:
            raise ValueError("sort() needs at least one sort expression.")
        orders = []
        if ascending is not None:
            if type(ascending) in [list, tuple]:
                orders = [SPAscending() if asc else SPDescending() for asc in ascending]
            elif type(ascending) in [bool, int]:
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
            if type(exprs[idx]) == SPSortOrder:
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
        self, exprs: Union[str, Column, Tuple[str, str], List[Union[str, Column]]]
    ) -> "DataFrame":
        """Aggregate the data in the DataFrame. Use this method if you don't need to
        group the data (:func:`groupBy`).

        For the input value, pass in a list of expressions that apply aggregation
        functions to columns (functions that are defined in the
        :mod:`snowflake.snowpark.functions` module).

        Alternatively, pass in a list of pairs that specify the column names and
        aggregation functions. For each pair in the list:

            - Set the first pair-value to the name of the column to aggregate.
            - Set the second pair-value to the name of the aggregation function to use on that column.

        Returns:
            :class:`DataFrame`
        """
        grouping_exprs = None
        if type(exprs) == str:
            grouping_exprs = [self.col(exprs)]
        elif type(exprs) == Column:
            grouping_exprs = [exprs]
        elif type(exprs) == tuple:
            if len(exprs) == 2:
                grouping_exprs = [(self.col(exprs[0]), exprs[1])]
        elif type(exprs) == list:
            if all(type(e) == str for e in exprs):
                grouping_exprs = [self.col(e) for e in exprs]
            if all(type(e) == Column for e in exprs):
                grouping_exprs = [e for e in exprs]
            if all(
                type(e) in [list, tuple]
                and len(e) == 2
                and type(e[0]) == type(e[1]) == str
                for e in exprs
            ):
                grouping_exprs = [(self.col(e[0]), e[1]) for e in exprs]

        if grouping_exprs is None:
            raise TypeError(f"Invalid type passed to agg(): {type(exprs)}")

        return self.groupBy().agg(grouping_exprs)

    def groupBy(
        self,
        *cols: Union[str, Column, List[Union[str, Column]], Tuple[Union[str, Column]]],
    ):
        """Groups rows by the columns specified by expressions (similar to GROUP BY in
        SQL).

        This method returns a :class:`RelationalGroupedDataFrame` that you can use to
        perform aggregations on each group of data.

        Valid inputs are:

            - Empty input
            - One or multiple Column object(s) or column name(s) (str)
            - A list of Column objects or column names (str)

        Returns:
            :class:`RelationalGroupedDataFrame`
        """
        # TODO fix dependency cycle
        from snowflake.snowpark.relational_grouped_dataframe import (
            GroupByType,
            RelationalGroupedDataFrame,
        )

        grouping_exprs = self.__convert_cols_to_exprs("groupBy()", *cols)
        return RelationalGroupedDataFrame(self, grouping_exprs, GroupByType())

    def distinct(self) -> "DataFrame":
        """Returns a new DataFrame that contains only the rows with distinct values
        from the current DataFrame.

        This is equivalent to performing a SELECT DISTINCT in SQL.

        Returns:
            :class:`DataFrame`
        """
        return self.groupBy(
            [self.col(AnalyzerPackage.quote_name(f.name)) for f in self.schema.fields]
        ).agg([])

    def limit(self, n: int) -> "DataFrame":
        """Returns a new DataFrame that contains at most ``n`` rows from the current
        DataFrame (similar to LIMIT in SQL).

        Note that this is a transformation method and not an action method.

        Args:
            n: Number of rows to return

        Returns:
            :class:`DataFrame`
        """
        return self.__with_plan(SPLimit(SPLiteral(n, SPLongType()), self.__plan))

    def union(self, other: "DataFrame") -> "DataFrame":
        """Returns a new DataFrame that contains all the rows in the current DataFrame
        and another DataFrame (``other``). Both input DataFrames must contain the same
        number of columns.

        This is same as the :func:`unionAll` method.

        Example::

             df1_and_2 = df1.union(df2)

        Args:
            other: the other :class:`DataFrame` that contains the rows to include.

        Returns:
            :class:`DataFrame`
        """
        return self.__with_plan(
            SPUnion(self.__plan, other._DataFrame__plan, is_all=True)
        )

    def unionAll(self, other: "DataFrame") -> "DataFrame":
        """Returns a new DataFrame that contains all the rows in the current DataFrame
        and another DataFrame (``other``). Both input DataFrames must contain the same
        number of columns.

        This is same as the :func:`union` method.

        Example::

             df1_and_2 = df1.unionAll(df2)

        Args:
            other: the other :class:`DataFrame` that contains the rows to include.

        Returns:
            :class:`DataFrame`
        """
        return self.union(other)

    def intersect(self, other: "DataFrame") -> "DataFrame":
        """Returns a new DataFrame that contains the intersection of rows from the
        current DataFrame and another DataFrame (`other`). Duplicate rows are
        eliminated.

        Example::

            df_intersection_of_1_and_2 = df1.intersect(df2)

        Args:
            other: the other :class:`DataFrame` that contains the rows to use for the
                intersection.

        Returns:
            :class:`DataFrame`
        """
        return self.__with_plan(SPIntersect(self.__plan, other._DataFrame__plan))

    def except_(self, other: "DataFrame") -> "DataFrame":
        """Returns a new DataFrame that contains all the rows from the current DataFrame
        except for the rows that also appear in `other` DataFrame. Duplicate rows are eliminated.

        Example::

            df1_except_df2 = df1.except_(df2)

        Args:
            other: The :class:`DataFrame` that contains the rows to exclude.

        Returns:
            :class:`DataFrame`
        """
        return self.__with_plan(SPExcept(self.__plan, other._DataFrame__plan))

    def naturalJoin(self, right: "DataFrame", join_type: str = None) -> "DataFrame":
        """Performs a natural join of the specified type (``joinType``) with the
        current DataFrame and another DataFrame (``right``).

        Examples::

            df_natural_join = df.naturalJoin(df2)
            df_natural_join = df.naturalJoin(df2, "left")

        Args:
            right: the other :class:`DataFrame` to join
            join_type: The type of join (e.g. "right", "outer", etc.).

        Returns:
             :class:`DataFrame`
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

    def join(self, right, using_columns=None, join_type=None) -> "DataFrame":
        """Performs a join of the specified type (``join_type``) with the current
        DataFrame and another DataFrame (``right``) on a list of columns
        (``using_columns``).

        The method assumes that the columns in ``usingColumns`` have the same meaning
        in the left and right DataFrames.

        Examples::

            df_left_join = df1.join(df2, "a", "left")
            df_outer_join = df.join(df2, ["a","b"], "outer")

        Args:
            right: The other :class:`Dataframe` to join.
            using_columns: A list of names of the columns, or the column objects, to
                use for the join
            join_type: The type of join (e.g. "right", "outer", etc.).

        Returns:
            :class:`DataFrame`
        """
        if isinstance(right, DataFrame):
            if self is right or self.__plan is right._DataFrame__plan:
                raise SnowparkClientException(
                    "Joining a DataFrame to itself can lead to incorrect results due to ambiguity of column references. Instead, join this DataFrame to a clone() of itself."
                )

            if type(join_type) == SPCrossJoin or (
                type(join_type) == str
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

        # TODO handle case where right is a TableFunction
        # if isinstance(right, TableFunction):
        #    return self.__join_dataframe_table_function(other, using_columns)
        raise TypeError("Invalid type for join. Must be Dataframe")

    def crossJoin(self, right: "DataFrame") -> "DataFrame":
        """Performs a cross join, which returns the cartesian product of the current
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

        Returns:
            :class:`DataFrame`
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
        if type(using_columns) == Column:
            return self.__join_dataframes_internal(
                right, join_type, join_exprs=using_columns
            )

        if type(join_type) in [SPLeftSemi, SPLeftAnti]:
            # Create a Column with expression 'true AND <expr> AND <expr> .."
            join_cond = Column(SPLiteral.create(True))
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

    def withColumn(self, col_name: str, col: "Column") -> "DataFrame":
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
            col: The :class:`~snowflake.snowpark.column.Column` to add or replace.

        Returns:
             :class:`~snowflake.snowpark.dataframe.DataFrame`
        """
        return self.withColumns([col_name], [col])

    def withColumns(self, col_names: List[str], cols: List[Column]) -> "DataFrame":
        """Returns a DataFrame with additional columns with the specified names
        ``col_names``. The columns are computed by using the specified expressions
        ``cols``.

        If columns with the same names already exist in the DataFrame, those columns
        are replaced by the new columns.

        This example adds new columns named ``mean_price`` and ``avg_price`` that
        contain the mean and average of the existing ``price`` column::

            df_with_added_columns = df.withColumns(["mean_price", "avg_price"],
                                                   [mean(col("price")),
                                                   avg(col("price"))])

        Args:
            col_names: A list of the names of the columns to add or replace.
            cols: A list of the :class:`~snowflake.snowpark.column.Column` objects to
                    add or replace.

        Returns:
            :class:`~snowflake.snowpark.dataframe.DataFrame`
        """
        if len(col_names) != len(cols):
            raise ValueError(
                f"The size of column names: {len(col_names)} is not equal to the size of columns: {len(cols)}"
            )

        column_map = {AnalyzerPackage.quote_name(n): c for n, c in zip(col_names, cols)}
        # Get a list of the columns that we are replacing or that already exist in the current
        # dataframe plan with the new and updated column names.
        replaced_and_existing_columns = []
        output_names = []
        for field in self.__output():
            output_names.append(field.name)
            if field.name in column_map:
                # Replacing column
                col_name = field.name
                col = column_map[field.name]
                column_to_append = (
                    col.as_(col_name)
                    if (type(col) == Column and type(col_name) == str)
                    else Column(field)
                )
                replaced_and_existing_columns.append(column_to_append)
            else:
                # Keeping existing column
                replaced_and_existing_columns.append(Column(field))

        # Adding in new columns that aren't part of this dataframe
        new_columns = [
            col.as_(col_name)
            for col_name, col in column_map.items()
            if col_name not in output_names
        ]

        return self.select([*replaced_and_existing_columns, *new_columns])

    def count(self) -> int:
        """Executes the query representing this DataFrame and returns the number of
        rows in the result (similar to the COUNT function in SQL).

        Returns:
            the number of rows.
        """
        return self.agg(("*", "count")).collect()[0].get_int(0)

    @property
    def write(self) -> DataFrameWriter:
        return DataFrameWriter(self)

    def show(self, n: int = 10, max_width: int = 50):
        """Evaluates this DataFrame and prints out the first ``n`` rows with the
        specified maximum number of characters per column.

        Args:
            n: The number of rows to print out.
            max_width: The maximum number of characters to print out for each column.
                If the number of characters exceeds the maximum, the method prints out
                an ellipsis (...) at the end of the column.
        """
        print(self.__show_string(n, max_width))

    def __show_string(self, n: int = 10, max_width: int = 50) -> str:
        query = self.__plan.queries[-1].sql.strip().lower()

        if query.startswith("select"):
            result, meta = self.session.conn.get_result_and_metadata(
                self.limit(n)._DataFrame__plan
            )
        else:
            res, meta = self.session.conn.get_result_and_metadata(self.__plan)
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

    def createOrReplaceView(self, name: Union[str, List[str]]):
        """Creates a view that captures the computation expressed by this DataFrame.

        For ``name``, you can include the database and schema name (i.e. specify a
        fully-qualified name). If no database name or schema name are specified, the
        view will be created in the current database or schema.

        ``name`` must be a valid `Snowflake identifier <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_.

        Args:
            name: The name of the view to create or replace. Can be a list of strings
                that specifies the database name, schema name, and view name.
        """
        if type(name) == str:
            formatted_name = name
        elif isinstance(name, (list, tuple)):
            if not all(type(i) == str for i in name):
                raise ValueError(
                    f"createOrReplaceView takes as input a string or list of strings."
                )
            formatted_name = ".".join(name)
        else:
            raise ValueError(
                f"createOrReplaceView takes as input a string or list of strings."
            )

        return self.__do_create_or_replace_view(formatted_name, SPPersistedView())

    def createOrReplaceTempView(self, name: Union[str, List[str]]):
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
        if type(name) == str:
            formatted_name = name
        elif isinstance(name, (list, tuple)):
            if not all(type(i) == str for i in name):
                raise ValueError(
                    f"createOrReplaceTempView() takes as input a string or list of strings."
                )
            formatted_name = ".".join(name)
        else:
            raise ValueError(
                f"createOrReplaceTempView() takes as input a string or list of strings."
            )

        return self.__do_create_or_replace_view(formatted_name, SPLocalTempView())

    def __do_create_or_replace_view(self, view_name: str, view_type: SPViewType):
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

        return self.session.conn.execute(self.session.analyzer.resolve(cmd))

    def first(self, n: Optional[int] = None):
        """Executes the query representing this DataFrame and returns the first ``n``
        rows of the results.

        Returns:
             A list of the first ``n`` :class:`Row` objects. If ``n`` is negative or
             larger than the number of rows in the result, returns all rows in the
             results. If no input is given, it returns the first :class:`Row` of
             results, or ``None`` if it does not exist.
        """
        if n is None:
            result = self.limit(1).collect()
            return result[0] if result else None
        elif not type(n) == int:
            raise ValueError(f"Invalid type of argument passed to first(): {type(n)}")
        elif n < 0:
            return self.collect()
        else:
            return self.limit(n).collect()

    def sample(self, frac: Optional[float] = None, n: Optional[int] = None):
        """Samples rows based on either the number of rows to be returned or a
        percentage or rows to be returned.

        Args:
            frac: the percentage or rows to be sampled.
            n: the number of rows to sample in the range of 0 to 1,000,00.
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

    # Utils
    def __resolve(self, col_name: str) -> SPNamedExpression:
        normalized_col_name = AnalyzerPackage.quote_name(col_name)
        cols = list(
            filter(lambda attr: attr.name == normalized_col_name, self.__output())
        )
        if len(cols) == 1:
            return cols[0].with_name(normalized_col_name)
        else:
            raise SnowparkClientException(f"Cannot resolve column name {col_name}")

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
                    if type(join_type) in [SPLeftSemi, SPLeftAnti]
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
        """The definition of the columns in this DataFrame (the "relations schema" for
        the DataFrame).
        """
        if not self.__placeholder_schema:
            self.__placeholder_schema = StructType.from_attributes(
                self.__plan.attributes()
            )
        return self.__placeholder_schema

    def __with_plan(self, plan):
        return DataFrame(self.session, plan)

    def __convert_cols_to_exprs(
        self,
        calling_method: str,
        *cols: Union[str, Column, List[Union[str, Column]], Tuple[Union[str, Column]]],
    ) -> List["SPExpression"]:
        """Convert a string or a Column, or a list of string and Column objects to expression(s)."""

        def convert(col: Union[str, Column]):
            if type(col) == str:
                return self.__resolve(col)
            elif type(col) == Column:
                return col.expression
            else:
                raise TypeError(
                    "{} only accepts str and Column objects, or a list containing str and"
                    " Column objects".format(calling_method)
                )

        exprs = [convert(col) for col in Utils.parse_positional_args_to_list(*cols)]
        return exprs
