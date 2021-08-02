#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import string
from random import choice
from typing import List, Optional, Tuple, Union

from snowflake.snowpark.column import Column
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
    ResolvedStar as SPResolvedStar,
    SortOrder as SPSortOrder,
)
from snowflake.snowpark.internal.utils import Utils
from snowflake.snowpark.plans.logical.basic_logical_operators import (
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

    def collect(self):
        return self.session.conn.execute(self.__plan)

    def clone(self) -> "DataFrame":
        return DataFrame(self.session, self.__plan.clone())

    def toPandas(self, **kwargs):
        """Returns the contents of this DataFrame as Pandas pandas.DataFrame.

        This is only available if Pandas is installed and available."""
        return self.session.conn.execute(self.__plan, to_pandas=True, **kwargs)

    # TODO
    def cache_result(self):
        raise Exception("Not implemented. df.cache_result()")

    # TODO
    def explain(self):
        raise Exception("Not implemented. df.explain()")

    def toDF(self, *names: Union[str, List[str]]) -> "DataFrame":
        """
        Creates a new DataFrame containing columns with the specified names.

        The number of column names that you pass in must match the number of columns in the existing
        DataFrame.
        :param names: list of new column names
        :return: a Dataframe
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
        # TODO revisit, do we want to uppercase the name, or should the user do that?
        if AnalyzerPackage.quote_name(name) not in self.columns:
            raise AttributeError(
                f"{self.__class__.__name__} object has no attribute {name}"
            )
        return self.col(name)

    @property
    def columns(self) -> List[str]:
        """Returns all column names as a list"""
        # Does not exist in scala snowpark.
        return [attr.name for attr in self.__output()]

    def col(self, col_name: str) -> "Column":
        if col_name == "*":
            return Column(SPResolvedStar(self.__plan.output()))
        else:
            return Column(self.__resolve(col_name))

    def select(
        self,
        *cols: Union[str, Column, List[Union[str, Column]], Tuple[Union[str, Column]]],
    ) -> "DataFrame":
        exprs = Utils.parse_positional_args_to_list(*cols)
        if not exprs:
            raise TypeError("select() input cannot be empty")

        if not all(type(e) in [Column, str] for e in exprs):
            raise TypeError("select() input must be Column, str, or list")

        names = [e.named() if type(e) == Column else Column(e).named() for e in exprs]
        return self.__with_plan(SPProject(names, self.__plan))

    # TODO complete. requires plan.output
    def drop(
        self,
        *cols: Union[str, Column, List[Union[str, Column]], Tuple[Union[str, Column]]],
    ) -> "DataFrame":
        """Returns a new DataFrame that drops the specified column. This is a no-op if schema
        does not contain the given column name(s)."""
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

    # TODO
    def filter(self, expr: Union[str, Column]) -> "DataFrame":
        if type(expr) == str:
            column = Column(expr)
            return self.__with_plan(SPFilter(column.expression, self.__plan))
        if type(expr) == Column:
            return self.__with_plan(SPFilter(expr.expression, self.__plan))

    def sample(self, frac: Optional[float] = None, n: Optional[int] = None):
        """Samples rows based on either the number of rows to be returned or a
        percentage or rows to be returned"""
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

    def where(self, expr: Union[str, Column]) -> "DataFrame":
        """Filters rows based on given condition. This is equivalent to calling [[filter]]."""
        return self.filter(expr)

    def sort(
        self,
        *cols: Union[str, Column, List[Union[str, Column]], Tuple[Union[str, Column]]],
        ascending: Union[
            bool, int, List[Union[bool, int]], Tuple[Union[bool, int]]
        ] = None,
    ) -> "DataFrame":
        """Sorts a DataFrame by the specified expressions (similar to ORDER BY in SQL)."""
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
        """Aggregate the data in the DataFrame. Use this method if you don't need to group the
        data (`groupBy`).

        For the input value, pass in a list of expressions that apply aggregation functions to
         columns (functions that are defined in the [[functions]] file).

        Alternatively, pass in a list of pairs that specify the column names and
        aggregation functions. For each pair in the list:
        - Set the first pair-value to the name of the column to aggregate.
        - Set the second pair-value to the name of the aggregation function to use on that column.

        :return DataFrame
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
        """Groups rows by the columns specified by expressions (similar to GROUP BY in SQL).

        This method returns a [[RelationalGroupedDataFrame]] that you can use to perform
        aggregations on each group of data.

        Valid inputs are:
        - Empty input
        - One or multiple Column object(s) or column name(s) (str)
        - A list of Column objects or column names (str)

        :return: RelationalGroupedDataFrame
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

        This is equivalent to performing a SELECT DISTINCT in SQL."""
        return self.groupBy(
            [self.col(AnalyzerPackage.quote_name(f.name)) for f in self.schema.fields]
        ).agg([])

    def limit(self, n: int) -> "DataFrame":
        """Returns a new DataFrame that contains at most ''n'' rows from the current
        DataFrame (similar to LIMIT in SQL).

        Note that this is a transformation method and not an action method."""
        return self.__with_plan(SPLimit(SPLiteral(n, SPLongType()), self.__plan))

    def union(self, other: "DataFrame") -> "DataFrame":
        """Returns a new DataFrame that contains all the rows in the current DataFrame
        and another DataFrame (`other`). Both input DataFrames must contain the same
        number of columns.

        This is same as the [[unionAll]] method.
        For example: `df1and2 = df1.union(df2)`
        """
        return self.__with_plan(
            SPUnion(self.__plan, other._DataFrame__plan, is_all=True)
        )

    def unionAll(self, other: "DataFrame") -> "DataFrame":
        """Returns a new DataFrame that contains all the rows in the current DataFrame
        and another DataFrame (`other`). Both input DataFrames must contain the same
        number of columns.

        This is same as the [[union]] method.
        For example: `df1and2 = df1.union(df2)`
        """
        return self.union(other)

    def intersect(self, other: "DataFrame") -> "DataFrame":
        """Returns a new DataFrame that contains the intersection of rows from the
        current DataFrame and another DataFrame (`other`). Duplicate rows are
        eliminated.

        For example: `df_intersection_of_1_and_2 = df1.intersect(df2)
        """
        return self.__with_plan(
            SPIntersect(self.__plan, other._DataFrame__plan, is_all=False)
        )

    def naturalJoin(self, right: "DataFrame", join_type: str = None) -> "DataFrame":
        """Performs a natural join of the specified type (`joinType`) with the current DataFrame and
        another DataFrame (`right`).

        For example: dfNaturalJoin = df.naturalJoin(df2, "left")
        :return A DataFrame
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
        """Performs a join of the specified type (`join_type`) with the current DataFrame and
        another DataFrame (`right`) on a list of columns (`using_columns`).

        The method assumes that the columns in `usingColumns` have the same meaning in the left and
        right DataFrames.

        For example:
            dfLeftJoin = df1.join(df2, "a", "left")
            dfOuterJoin = df.join(df2, ["a","b"], "outer")

        :param right: The other Dataframe to join.
        :param using_columns: A list of names of the columns, or the column objects, to use for the
        join.
        :param join_type: The type of join (e.g. "right", "outer", etc.).
        :return: a DataFrame
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
        """Performs a cross join, which returns the cartesian product of the current DataFrame and
        another DataFrame (`right`).

        If the current and `right` DataFrames have columns with the same name, and you need to refer
        to one of these columns in the returned DataFrame, use the [[coll]] function
        on the current or `right` DataFrame to disambiguate references to these columns.

        For example:
        df_cross = this.crossJoin(right)
        project = df.df_cross.select([this("common_col"), right("common_col")])

        :param right The right Dataframe to join.
        :return a Dataframe
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

    def count(self) -> int:
        return self.agg(("*", "count")).collect()[0].get_int(0)

    def show(self, n: int = 10, max_width: int = 50):
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
        """Executes the query representing this DataFrame and returns the first n rows
        of the results.

        Returns the first row of results if no input is given. If that row does not
        exist, it returns `None`.
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
