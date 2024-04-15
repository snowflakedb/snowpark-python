#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union

from snowflake.connector.options import pandas
from snowflake.snowpark import functions
from snowflake.snowpark._internal.analyzer.expression import (
    Expression,
    Literal,
    NamedExpression,
    ScalarSubquery,
    SnowflakeUDF,
    UnresolvedAttribute,
)
from snowflake.snowpark._internal.analyzer.grouping_set import (
    Cube,
    GroupingSetsExpression,
    Rollup,
)
from snowflake.snowpark._internal.analyzer.unary_expression import (
    Alias,
    UnresolvedAlias,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import Aggregate, Pivot
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import relational_group_df_api_usage
from snowflake.snowpark._internal.type_utils import ColumnOrName, LiteralType
from snowflake.snowpark._internal.utils import (
    parse_positional_args_to_list,
    prepare_pivot_arguments,
)
from snowflake.snowpark.column import Column
from snowflake.snowpark.dataframe import DataFrame
from snowflake.snowpark.types import StructType


def _alias(expr: Expression) -> NamedExpression:
    if isinstance(expr, UnresolvedAttribute):
        return UnresolvedAlias(expr)
    elif isinstance(expr, (NamedExpression, SnowflakeUDF)):
        return expr
    else:
        return Alias(expr, expr.sql.upper().replace('"', ""))


def _expr_to_func(expr: str, input_expr: Expression) -> Expression:
    lowered = expr.lower()
    if lowered in ["avg", "average", "mean"]:
        return functions.avg(Column(input_expr))._expression
    elif lowered in ["stddev", "std"]:
        return functions.stddev(Column(input_expr))._expression
    elif lowered in ["count", "size"]:
        return functions.count(Column(input_expr))._expression
    else:
        return functions.function(expr)(input_expr)._expression


def _str_to_expr(expr: str) -> Callable:
    return lambda input_expr: _expr_to_func(expr, input_expr)


class _GroupType:
    def to_string(self) -> str:
        return self.__class__.__name__[1:-4]


class _GroupByType(_GroupType):
    pass


class _CubeType(_GroupType):
    pass


class _RollupType(_GroupType):
    pass


class _PivotType(_GroupType):
    def __init__(
        self,
        pivot_col: Expression,
        values: Optional[Union[List[Expression], ScalarSubquery]],
        default_on_null: Optional[Expression],
    ) -> None:
        self.pivot_col = pivot_col
        self.values = values
        self.default_on_null = default_on_null


class GroupingSets:
    """Creates a :class:`GroupingSets` object from a list of column/expression sets that you pass
    to :meth:`DataFrame.group_by_grouping_sets`. See :meth:`DataFrame.group_by_grouping_sets` for
    examples of how to use this class with a :class:`DataFrame`. See
    `GROUP BY GROUPING SETS <https://docs.snowflake.com/en/sql-reference/constructs/group-by-grouping-sets.html>`_
    for its counterpart in SQL (several examples are shown below).

    =============================================================  ==================================
    Python interface                                               SQL interface
    =============================================================  ==================================
    ``GroupingSets([col("a")], [col("b")])``                       ``GROUPING SETS ((a), (b))``
    ``GroupingSets([col("a") , col("b")], [col("c"), col("d")])``  ``GROUPING SETS ((a, b), (c, d))``
    ``GroupingSets([col("a"), col("b")])``                         ``GROUPING SETS ((a, b))``
    ``GroupingSets(col("a"), col("b"))``                           ``GROUPING SETS ((a, b))``
    =============================================================  ==================================
    """

    def __init__(self, *sets: Union[Column, List[Column]]) -> None:
        prepared_sets = parse_positional_args_to_list(*sets)
        prepared_sets = (
            prepared_sets if isinstance(prepared_sets[0], list) else [prepared_sets]
        )
        self._to_expression = GroupingSetsExpression(
            [[c._expression for c in s] for s in prepared_sets]
        )


class RelationalGroupedDataFrame:
    """Represents an underlying DataFrame with rows that are grouped by common values.
    Can be used to define aggregations on these grouped DataFrames.

    See also:
        :meth:`snowflake.snowpark.DataFrame.agg`.
    """

    def __init__(
        self, df: DataFrame, grouping_exprs: List[Expression], group_type: _GroupType
    ) -> None:
        self._df = df
        self._grouping_exprs = grouping_exprs
        self._group_type = group_type
        self._df_api_call = None

    def _to_df(self, agg_exprs: List[Expression]) -> DataFrame:
        aliased_agg = []
        for grouping_expr in self._grouping_exprs:
            if isinstance(grouping_expr, GroupingSetsExpression):
                # avoid doing list(set(grouping_expr.args)) because it will change the order
                gr_used = set()
                gr_uniq = [
                    a
                    for arg in grouping_expr.args
                    for a in arg
                    if a not in gr_used and (gr_used.add(a) or True)
                ]
                aliased_agg.extend(gr_uniq)
            else:
                aliased_agg.append(grouping_expr)

        aliased_agg.extend(agg_exprs)

        # Avoid doing aliased_agg = [self.alias(a) for a in list(set(aliased_agg))],
        # to keep order
        used = set()
        unique = [a for a in aliased_agg if a not in used and (used.add(a) or True)]
        aliased_agg = [_alias(a) for a in unique]

        if isinstance(self._group_type, _GroupByType):
            group_plan = Aggregate(
                self._grouping_exprs,
                aliased_agg,
                self._df._select_statement or self._df._plan,
            )
        elif isinstance(self._group_type, _RollupType):
            group_plan = Aggregate(
                [Rollup(self._grouping_exprs)],
                aliased_agg,
                self._df._select_statement or self._df._plan,
            )
        elif isinstance(self._group_type, _CubeType):
            group_plan = Aggregate(
                [Cube(self._grouping_exprs)],
                aliased_agg,
                self._df._select_statement or self._df._plan,
            )
        elif isinstance(self._group_type, _PivotType):
            if len(agg_exprs) != 1:
                raise SnowparkClientExceptionMessages.DF_PIVOT_ONLY_SUPPORT_ONE_AGG_EXPR()
            group_plan = Pivot(
                self._grouping_exprs,
                self._group_type.pivot_col,
                self._group_type.values,
                agg_exprs,
                self._group_type.default_on_null,
                self._df._select_statement or self._df._plan,
            )
        else:  # pragma: no cover
            raise TypeError(f"Wrong group by type {self._group_type}")

        if self._df._select_statement:
            group_plan = self._df._session._analyzer.create_select_statement(
                from_=self._df._session._analyzer.create_select_snowflake_plan(
                    group_plan, analyzer=self._df._session._analyzer
                ),
                analyzer=self._df._session._analyzer,
            )

        return DataFrame(self._df._session, group_plan)

    @relational_group_df_api_usage
    def agg(
        self, *exprs: Union[Column, Tuple[ColumnOrName, str], Dict[str, str]]
    ) -> DataFrame:
        """Returns a :class:`DataFrame` with computed aggregates. See examples in :meth:`DataFrame.group_by`.

        Args:
            exprs: A variable length arguments list where every element is

                - A Column object
                - A tuple where the first element is a column object or a column name and the second element is the name of the aggregate function
                - A list of the above

                or a ``dict`` maps column names to aggregate function names.

        Note:
            The name of the aggregate function to compute must be a valid Snowflake `aggregate function
            <https://docs.snowflake.com/en/sql-reference/functions-aggregation.html>`_.

        See also:
            - :meth:`DataFrame.agg`
            - :meth:`DataFrame.group_by`
        """

        def is_valid_tuple_for_agg(e: Union[list, tuple]) -> bool:
            return (
                len(e) == 2
                and isinstance(e[0], (Column, str))
                and isinstance(e[1], str)
            )

        exprs = parse_positional_args_to_list(*exprs)
        # special case for single list or tuple
        if is_valid_tuple_for_agg(exprs):
            exprs = [exprs]

        agg_exprs = []
        if len(exprs) > 0 and isinstance(exprs[0], dict):
            for k, v in exprs[0].items():
                if not (isinstance(k, str) and isinstance(v, str)):
                    raise TypeError(
                        "Dictionary passed to DataFrame.agg() or RelationalGroupedDataFrame.agg() "
                        f"should contain only strings: got key-value pair with types {type(k), type(v)}"
                    )
                agg_exprs.append(_str_to_expr(v)(Column(k)._expression))
        else:
            for e in exprs:
                if isinstance(e, Column):
                    agg_exprs.append(e._expression)
                elif isinstance(e, (list, tuple)) and is_valid_tuple_for_agg(e):
                    col_expr = (
                        e[0]._expression
                        if isinstance(e[0], Column)
                        else Column(e[0])._expression
                    )
                    agg_exprs.append(_str_to_expr(e[1])(col_expr))
                else:
                    raise TypeError(
                        "List passed to DataFrame.agg() or RelationalGroupedDataFrame.agg() should "
                        "contain only Column objects, or pairs of Column object (or column name) and strings."
                    )

        return self._to_df(agg_exprs)

    def apply_in_pandas(
        self,
        func: Callable,
        output_schema: StructType,
        **kwargs,
    ) -> DataFrame:
        """Maps each grouped dataframe in to a pandas.DataFrame, applies the given function on
        data of each grouped dataframe, and returns a pandas.DataFrame. Internally, a vectorized
        UDTF with input ``func`` argument as the ``end_partition`` is registered and called. Additional
        ``kwargs`` are accepted to specify arguments to register the UDTF. Group by clause used must be
        column reference, not a general expression.

        Requires ``pandas`` to be installed in the execution environment and declared as a dependency by either
        specifying the keyword argument `packages=["pandas]` in this call or calling :meth:`~snowflake.snowpark.Session.add_packages` beforehand.

        Args:
            func: A Python native function that accepts a single input argument - a ``pandas.DataFrame``
                object and returns a ``pandas.Dataframe``. It is used as input to ``end_partition`` in
                a vectorized UDTF.
            output_schema: A :class:`~snowflake.snowpark.types.StructType` instance that represents the
                table function's output columns.
            input_names: A list of strings that represents the table function's input column names. Optional,
                if unspecified, default column names will be ARG1, ARG2, etc.
            kwargs: Additional arguments to register the vectorized UDTF. See
                :meth:`~snowflake.snowpark.udtf.UDTFRegistration.register` for all options.

        Examples::
            Call ``apply_in_pandas`` using temporary UDTF:

                >>> import pandas as pd
                >>> from snowflake.snowpark.types import StructType, StructField, StringType, FloatType
                >>> def convert(pandas_df):
                ...     return pandas_df.assign(TEMP_F = lambda x: x.TEMP_C * 9 / 5 + 32)
                >>> df = session.createDataFrame([('SF', 21.0), ('SF', 17.5), ('SF', 24.0), ('NY', 30.9), ('NY', 33.6)],
                ...         schema=['location', 'temp_c'])
                >>> df.group_by("location").apply_in_pandas(convert,
                ...     output_schema=StructType([StructField("location", StringType()),
                ...                               StructField("temp_c", FloatType()),
                ...                               StructField("temp_f", FloatType())])).order_by("temp_c").show()
                ---------------------------------------------
                |"LOCATION"  |"TEMP_C"  |"TEMP_F"           |
                ---------------------------------------------
                |SF          |17.5      |63.5               |
                |SF          |21.0      |69.8               |
                |SF          |24.0      |75.2               |
                |NY          |30.9      |87.61999999999999  |
                |NY          |33.6      |92.48              |
                ---------------------------------------------
                <BLANKLINE>

            Call ``apply_in_pandas`` using permanent UDTF with replacing original UDTF:

                >>> from snowflake.snowpark.types import IntegerType, DoubleType
                >>> _ = session.sql("create or replace temp stage mystage").collect()
                >>> def group_sum(pdf):
                ...     return pd.DataFrame([(pdf.GRADE.iloc[0], pdf.DIVISION.iloc[0], pdf.VALUE.sum(), )])
                ...
                >>> df = session.createDataFrame([('A', 2, 11.0), ('A', 2, 13.9), ('B', 5, 5.0), ('B', 2, 12.1)],
                ...                              schema=["grade", "division", "value"])
                >>> df.group_by([df.grade, df.division] ).applyInPandas(
                ...     group_sum,
                ...     output_schema=StructType([StructField("grade", StringType()),
                ...                                        StructField("division", IntegerType()),
                ...                                        StructField("sum", DoubleType())]),
                ...                is_permanent=True, stage_location="@mystage", name="group_sum_in_pandas", replace=True
                ...            ).order_by("sum").show()
                --------------------------------
                |"GRADE"  |"DIVISION"  |"SUM"  |
                --------------------------------
                |B        |5           |5.0    |
                |B        |2           |12.1   |
                |A        |2           |24.9   |
                --------------------------------
                <BLANKLINE>

        See Also:
            - :class:`~snowflake.snowpark.udtf.UDTFRegistration`
            - :func:`~snowflake.snowpark.functions.pandas_udtf`
        """

        class _ApplyInPandas:
            def end_partition(self, pdf: pandas.DataFrame) -> pandas.DataFrame:
                return func(pdf)

        # for vectorized UDTF
        _ApplyInPandas.end_partition._sf_vectorized_input = pandas.DataFrame

        # The assumption here is that we send all columns of the dataframe in the apply_in_pandas
        # function so the inferred input types are the types of each column in the dataframe.
        kwargs["input_types"] = kwargs.get(
            "input_types", [field.datatype for field in self._df.schema.fields]
        )

        kwargs["input_names"] = kwargs.get(
            "input_names", [field.name for field in self._df.schema.fields]
        )

        _apply_in_pandas_udtf = self._df._session.udtf.register(
            _ApplyInPandas,
            output_schema=output_schema,
            **kwargs,
        )
        partition_by = [functions.col(expr) for expr in self._grouping_exprs]

        return self._df.select(
            _apply_in_pandas_udtf(*self._df.columns).over(partition_by=partition_by)
        )

    applyInPandas = apply_in_pandas

    def pivot(
        self,
        pivot_col: ColumnOrName,
        values: Optional[Union[Iterable[LiteralType], DataFrame]] = None,
        default_on_null: Optional[LiteralType] = None,
    ) -> "RelationalGroupedDataFrame":
        """Rotates this DataFrame by turning unique values from one column in the input
        expression into multiple columns and aggregating results where required on any
        remaining column values.

        Only one aggregate is supported with pivot.

        Args:
            pivot_col: The column or name of the column to use.
            values: A list of values in the column,
                or dynamic based on the DataFrame query,
                or None (default) will use all values of the pivot column.
            default_on_null: Expression to replace empty result values.

        Example::

            >>> create_result = session.sql('''create or replace temp table monthly_sales(empid int, team text, amount int, month text)
            ... as select * from values
            ... (1, 'A', 10000, 'JAN'),
            ... (1, 'B', 400, 'JAN'),
            ... (2, 'A', 4500, 'JAN'),
            ... (2, 'A', 35000, 'JAN'),
            ... (1, 'B', 5000, 'FEB'),
            ... (1, 'A', 3000, 'FEB'),
            ... (2, 'B', 200, 'FEB') ''').collect()
            >>> df = session.table("monthly_sales")
            >>> df.group_by("empid").pivot("month", ['JAN', 'FEB']).sum("amount").sort(df["empid"]).show()
            -------------------------------
            |"EMPID"  |"'JAN'"  |"'FEB'"  |
            -------------------------------
            |1        |10400    |8000     |
            |2        |39500    |200      |
            -------------------------------
            <BLANKLINE>

            >>> df.group_by(["empid", "team"]).pivot("month", ['JAN', 'FEB']).sum("amount").sort("empid", "team").show()
            ----------------------------------------
            |"EMPID"  |"TEAM"  |"'JAN'"  |"'FEB'"  |
            ----------------------------------------
            |1        |A       |10000    |3000     |
            |1        |B       |400      |5000     |
            |2        |A       |39500    |NULL     |
            |2        |B       |NULL     |200      |
            ----------------------------------------
            <BLANKLINE>

            >>> df = session.table("monthly_sales")
            >>> df.group_by(["empid", "team"]).pivot("month").sum("amount").sort("empid", "team").show()
            ----------------------------------------
            |"EMPID"  |"TEAM"  |"'FEB'"  |"'JAN'"  |
            ----------------------------------------
            |1        |A       |3000     |10000    |
            |1        |B       |5000     |400      |
            |2        |A       |NULL     |39500    |
            |2        |B       |200      |NULL     |
            ----------------------------------------
            <BLANKLINE>

            >>> from snowflake.snowpark.functions import col
            >>> subquery_df = session.table("monthly_sales").select("month").filter(col("month") == "JAN")
            >>> df = session.table("monthly_sales")
            >>> df.group_by(["empid", "team"]).pivot("month", values=subquery_df, default_on_null=999).sum("amount").sort("empid", "team").show()
            ------------------------------
            |"EMPID"  |"TEAM"  |"'JAN'"  |
            ------------------------------
            |1        |A       |10000    |
            |1        |B       |400      |
            |2        |A       |39500    |
            |2        |B       |999      |
            ------------------------------
            <BLANKLINE>
        """
        self._df, pc, pivot_values, default_on_null = prepare_pivot_arguments(
            self._df,
            "RelationalGroupedDataFrame.pivot",
            pivot_col,
            values,
            default_on_null,
        )

        self._group_type = _PivotType(pc[0], pivot_values, default_on_null)
        return self

    @relational_group_df_api_usage
    def avg(self, *cols: ColumnOrName) -> DataFrame:
        """Return the average for the specified numeric columns."""
        return self._non_empty_argument_function("avg", *cols)

    mean = avg

    @relational_group_df_api_usage
    def sum(self, *cols: ColumnOrName) -> DataFrame:
        """Return the sum for the specified numeric columns."""
        return self._non_empty_argument_function("sum", *cols)

    @relational_group_df_api_usage
    def median(self, *cols: ColumnOrName) -> DataFrame:
        """Return the median for the specified numeric columns."""
        return self._non_empty_argument_function("median", *cols)

    @relational_group_df_api_usage
    def min(self, *cols: ColumnOrName) -> DataFrame:
        """Return the min for the specified numeric columns."""
        return self._non_empty_argument_function("min", *cols)

    @relational_group_df_api_usage
    def max(self, *cols: ColumnOrName) -> DataFrame:
        """Return the max for the specified numeric columns."""
        return self._non_empty_argument_function("max", *cols)

    @relational_group_df_api_usage
    def count(self) -> DataFrame:
        """Return the number of rows for each group."""
        return self._to_df(
            [
                Alias(
                    functions.builtin("count")(Literal(1))._expression,
                    "count",
                )
            ]
        )

    def function(self, agg_name: str) -> Callable:
        """Computes the builtin aggregate ``agg_name`` over the specified columns. Use
        this function to invoke any aggregates not explicitly listed in this class.
        See examples in :meth:`DataFrame.group_by`.
        """
        return lambda *cols: self._function(agg_name, *cols)

    builtin = function

    def _function(self, agg_name: str, *cols: ColumnOrName) -> DataFrame:
        agg_exprs = []
        for c in cols:
            c_expr = Column(c)._expression if isinstance(c, str) else c._expression
            expr = functions.builtin(agg_name)(c_expr)._expression
            agg_exprs.append(expr)
        return self._to_df(agg_exprs)

    def _non_empty_argument_function(
        self, func_name: str, *cols: ColumnOrName
    ) -> DataFrame:
        if not cols:
            raise ValueError(
                f"You must pass a list of one or more Columns to function: {func_name}"
            )
        else:
            return self.builtin(func_name)(*cols)
