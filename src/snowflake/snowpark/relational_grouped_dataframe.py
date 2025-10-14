#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
from typing import Callable, Dict, Iterable, List, Optional, Tuple, Union
import inspect

from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
import snowflake.snowpark.context as context
from snowflake.connector.options import pandas
from snowflake.snowpark._internal.analyzer.analyzer_utils import unquote_if_quoted
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
from snowflake.snowpark._internal.ast.utils import (
    build_expr_from_python_val,
    build_expr_from_snowpark_column_or_col_name,
    build_proto_from_callable,
    build_proto_from_struct_type,
    debug_check_missing_ast,
    with_src_position,
)
from snowflake.snowpark._internal.telemetry import relational_group_df_api_usage
from snowflake.snowpark._internal.type_utils import ColumnOrName, LiteralType
from snowflake.snowpark._internal.utils import (
    check_agg_exprs,
    experimental,
    is_valid_tuple_for_agg,
    parse_positional_args_to_list,
    parse_positional_args_to_list_variadic,
    prepare_pivot_arguments,
    publicapi,
    generate_random_alphanumeric,
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


def _expr_to_func(expr: str, input_expr: Expression, _emit_ast: bool) -> Expression:
    lowered = expr.lower()

    def create_column(input_expr):
        return Column(input_expr, _emit_ast=_emit_ast)

    if lowered in ["avg", "average", "mean"]:
        return functions.avg(create_column(input_expr))._expression
    elif lowered in ["stddev", "std"]:
        return functions.stddev(create_column(input_expr))._expression
    elif lowered in ["count", "size"]:
        return functions.count(create_column(input_expr))._expression
    else:
        return functions._call_function(
            expr, input_expr, _emit_ast=_emit_ast
        )._expression


def _str_to_expr(expr: str, _emit_ast: bool) -> Callable:
    return lambda input_expr: _expr_to_func(expr, input_expr, _emit_ast)


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

    @publicapi
    def __init__(
        self, *sets: Union[Column, List[Column]], _emit_ast: bool = True
    ) -> None:
        self._ast = None
        if _emit_ast:
            self._ast = proto.Expr()
            grouping_sets_ast = with_src_position(self._ast.grouping_sets)
            (
                set_list,
                grouping_sets_ast.sets.variadic,
            ) = parse_positional_args_to_list_variadic(*sets)
            for s in set_list:
                build_expr_from_python_val(grouping_sets_ast.sets.args.add(), s)

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
        self,
        df: DataFrame,
        grouping_exprs: List[Expression],
        group_type: _GroupType,
        _ast_stmt: Optional[proto.Bind] = None,
    ) -> None:
        self._dataframe = df
        self._grouping_exprs = grouping_exprs
        self._group_type = group_type
        self._df_api_call = None
        self._ast_id = _ast_stmt.uid if _ast_stmt is not None else None

    def _to_df(
        self,
        agg_exprs: List[Expression],
        _ast_stmt: Optional[proto.Bind] = None,
        _emit_ast: bool = False,
        **kwargs,
    ) -> DataFrame:
        exclude_grouping_columns = kwargs.get("exclude_grouping_columns", False)
        aliased_agg = []

        if not exclude_grouping_columns:
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
        # Aliases cannot be used in grouping statements, but the child expresion can
        unaliased_grouping = [
            expr.child if isinstance(expr, Alias) else expr
            for expr in self._grouping_exprs
        ]

        if isinstance(self._group_type, _GroupByType):
            group_plan = Aggregate(
                unaliased_grouping,
                aliased_agg,
                self._dataframe._select_statement or self._dataframe._plan,
            )
        elif isinstance(self._group_type, _RollupType):
            group_plan = Aggregate(
                [Rollup(unaliased_grouping)],
                aliased_agg,
                self._dataframe._select_statement or self._dataframe._plan,
            )
        elif isinstance(self._group_type, _CubeType):
            group_plan = Aggregate(
                [Cube(unaliased_grouping)],
                aliased_agg,
                self._dataframe._select_statement or self._dataframe._plan,
            )
        elif isinstance(self._group_type, _PivotType):
            if len(agg_exprs) != 1 and len(unaliased_grouping) == 0:
                raise SnowparkClientExceptionMessages.DF_PIVOT_ONLY_SUPPORT_ONE_AGG_EXPR()
            group_plan = Pivot(
                unaliased_grouping,
                self._group_type.pivot_col,
                self._group_type.values,
                agg_exprs,
                self._group_type.default_on_null,
                self._dataframe._select_statement or self._dataframe._plan,
            )
        else:  # pragma: no cover
            raise TypeError(f"Wrong group by type {self._group_type}")

        if self._dataframe._select_statement:
            group_plan = self._dataframe._session._analyzer.create_select_statement(
                from_=self._dataframe._session._analyzer.create_select_snowflake_plan(
                    group_plan, analyzer=self._dataframe._session._analyzer
                ),
                analyzer=self._dataframe._session._analyzer,
            )

        return DataFrame(
            self._dataframe._session,
            group_plan,
            _ast_stmt=_ast_stmt,
            _emit_ast=_emit_ast,
        )

    @relational_group_df_api_usage
    @publicapi
    def agg(
        self,
        *exprs: Union[Column, Tuple[ColumnOrName, str], Dict[str, str]],
        _ast_stmt: Optional[proto.Bind] = None,
        _emit_ast: bool = True,
        **kwargs,
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
        exclude_grouping_columns = kwargs.get("exclude_grouping_columns", False)

        exprs, is_variadic = parse_positional_args_to_list_variadic(*exprs)

        # special case for single list or tuple
        if is_valid_tuple_for_agg(exprs):
            exprs = [exprs]

        check_agg_exprs(exprs)

        # AST.
        stmt = None
        if _emit_ast:
            if _ast_stmt is None:
                stmt = self._dataframe._session._ast_batch.bind()
                ast = with_src_position(
                    stmt.expr.relational_grouped_dataframe_agg, stmt
                )
                self._set_ast_ref(ast.grouped_df)
                ast.exprs.variadic = is_variadic
                for e in exprs:
                    build_expr_from_python_val(ast.exprs.args.add(), e)
            else:
                stmt = _ast_stmt

        agg_exprs = []
        if len(exprs) > 0 and isinstance(exprs[0], dict):
            for k, v in exprs[0].items():
                agg_exprs.append(_str_to_expr(v, _emit_ast)(Column(k)._expression))
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
                    agg_exprs.append(_str_to_expr(e[1], _emit_ast)(col_expr))

        df = self._to_df(
            agg_exprs,
            exclude_grouping_columns=exclude_grouping_columns,
            _emit_ast=False,
        )
        # if no grouping exprs, there is already a LIMIT 1 in the query
        # see aggregate_statement in analyzer_utils.py
        df._ops_after_agg = set() if self._grouping_exprs else {"limit"}

        if _emit_ast:
            df._ast_id = stmt.uid
        return df

    @relational_group_df_api_usage
    @publicapi
    def apply_in_pandas(
        self,
        func: Callable,
        output_schema: StructType,
        _emit_ast: bool = True,
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

        partition_by = [Column(expr, _emit_ast=False) for expr in self._grouping_exprs]

        # this is the case where this is being called from spark
        # this is not handling nested column access, it is assuming that the access in the function is not nested
        original_columns: List[str] | None = None
        key_columns: List[str] = []

        working_dataframe = self._dataframe
        working_columns: List[Union[str, Column]] = self._dataframe.columns
        working_columns_count = len(working_columns)
        parameters_count = None

        if context._is_snowpark_connect_compatible_mode:
            if self._dataframe._column_map is not None:
                original_columns = [
                    column.spark_name for column in self._dataframe._column_map.columns
                ]

            parameters_count = len(inspect.signature(func).parameters)

            # the strategy here is to create a new dataframe with all the columns including
            # the non-NamedExpression columns which have no name, and perform the apply_in_pandas on the new dataframe
            for col in partition_by:
                column_name = col.get_name()
                unquoted_name = unquote_if_quoted(column_name) if column_name else None
                if not column_name:
                    # expression is not a NamedExpression, so it has no name
                    # example: df.group_by("id", ceil(df.v / 2))
                    aliased_name = (
                        f"sas_col_alias_{generate_random_alphanumeric(5)}".upper()
                    )
                    working_columns.append(col.alias(aliased_name))
                    key_columns.append(aliased_name)
                elif isinstance(col._expression, (Alias, UnresolvedAlias)):
                    # expression is an Alias or UnresolvedAlias, which inherits from NamedExpression
                    # however, underlying it could be aliasing non-NamedExpression
                    # example: df.group_by("id", ceil(df.v / 2).alias('newcol'))
                    # thus we always add the working column to the new dataframe
                    working_columns.append(col)
                    key_columns.append(unquoted_name)
                else:
                    key_columns.append(unquoted_name)

            partition_by = (
                key_columns  # override the partition_by with the new key columns
            )
            working_dataframe = self._dataframe.select(
                *working_columns
            )  # select to evaluate the expressions
            working_columns_count = len(working_columns)

        # This class is defined as a local class, which is pickled and executed remotely.
        # As a result, pytest-cov is unable to track its code coverage.
        class _ApplyInPandas:
            def end_partition(
                self, pdf: pandas.DataFrame
            ) -> pandas.DataFrame:  # pragma: no cover
                # -- start of spark mode code
                if parameters_count == 2:
                    import numpy as np

                    key_list = [pdf[key].iloc[0] for key in key_columns]
                    numpy_array = np.array(key_list)
                    keys = tuple(numpy_array)
                if original_columns is not None:
                    # drop the extra aliased columns that were appended to the working dataframe
                    # this won't affect non-spark logic as original_columns is only set in spark mode
                    to_drop_columns = working_columns_count - len(original_columns)
                    if to_drop_columns > 0:
                        pdf = pdf.drop(pdf.columns[-to_drop_columns:], axis=1)
                    pdf.columns = original_columns
                if parameters_count == 2:
                    return func(keys, pdf)
                # -- end of spark mode code
                return func(pdf)

        # for vectorized UDTF
        _ApplyInPandas.end_partition._sf_vectorized_input = pandas.DataFrame

        # The assumption here is that we send all columns of the dataframe in the apply_in_pandas
        # function so the inferred input types are the types of each column in the dataframe.
        kwargs["input_types"] = kwargs.get(
            "input_types", [field.datatype for field in working_dataframe.schema.fields]
        )

        kwargs["input_names"] = kwargs.get(
            "input_names", [field.name for field in working_dataframe.schema.fields]
        )

        _apply_in_pandas_udtf = working_dataframe._session.udtf.register(
            _ApplyInPandas,
            output_schema=output_schema,
            _emit_ast=_emit_ast,
            **kwargs,
        )

        df = working_dataframe.select(
            _apply_in_pandas_udtf(*working_dataframe.columns).over(
                partition_by=partition_by, _emit_ast=False
            ),
            _emit_ast=False,
        )
        # if no grouping exprs, there is already a LIMIT 1 in the query
        # see aggregate_statement in analyzer_utils.py
        df._ops_after_agg = set() if self._grouping_exprs else {"limit"}

        if _emit_ast:
            stmt = working_dataframe._session._ast_batch.bind()
            ast = with_src_position(
                stmt.expr.relational_grouped_dataframe_apply_in_pandas, stmt
            )
            self._set_ast_ref(ast.grouped_df)
            build_proto_from_callable(
                ast.func, func, working_dataframe._session._ast_batch
            )
            build_proto_from_struct_type(output_schema, ast.output_schema)
            for k, v in kwargs.items():
                entry = ast.kwargs.add()
                entry._1 = k
                build_expr_from_python_val(entry._2, v)
            df._ast_id = stmt.uid

        return df

    applyInPandas = apply_in_pandas

    @publicapi
    def pivot(
        self,
        pivot_col: ColumnOrName,
        values: Optional[Union[Iterable[LiteralType], DataFrame]] = None,
        default_on_null: Optional[LiteralType] = None,
        _emit_ast: bool = True,
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

        (
            self._dataframe,
            pc,
            pivot_values,
            pivot_default_on_null,
        ) = prepare_pivot_arguments(
            self._dataframe,
            "RelationalGroupedDataFrame.pivot",
            pivot_col,
            values,
            default_on_null,
        )

        self._group_type = _PivotType(pc[0], pivot_values, pivot_default_on_null)

        # special case: This is an internal state modifying operation.
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.relational_grouped_dataframe_pivot, stmt)
            if default_on_null is not None:
                build_expr_from_python_val(ast.default_on_null, default_on_null)
            build_expr_from_snowpark_column_or_col_name(ast.pivot_col, pivot_col)
            build_expr_from_python_val(ast.values, values)
            self._set_ast_ref(ast.grouped_df)

            # Update self's id.
            self._ast_id = stmt.uid

        return self

    @relational_group_df_api_usage
    @publicapi
    def avg(self, *cols: ColumnOrName, _emit_ast: bool = True, **kwargs) -> DataFrame:
        """Return the average for the specified numeric columns.

        Args:
            cols: The columns to calculate average for.
        """
        exclude_grouping_columns = kwargs.get("exclude_grouping_columns", False)
        return self._non_empty_argument_function(
            "avg",
            *cols,
            exclude_grouping_columns=exclude_grouping_columns,
            _emit_ast=_emit_ast,
        )

    mean = avg

    @relational_group_df_api_usage
    @publicapi
    def sum(self, *cols: ColumnOrName, _emit_ast: bool = True, **kwargs) -> DataFrame:
        """Return the sum for the specified numeric columns.

        Args:
            cols: The columns to calculate sum for.
        """
        exclude_grouping_columns = kwargs.get("exclude_grouping_columns", False)
        return self._non_empty_argument_function(
            "sum",
            *cols,
            exclude_grouping_columns=exclude_grouping_columns,
            _emit_ast=_emit_ast,
        )

    @relational_group_df_api_usage
    @publicapi
    def median(
        self, *cols: ColumnOrName, _emit_ast: bool = True, **kwargs
    ) -> DataFrame:
        """Return the median for the specified numeric columns.

        Args:
            cols: The columns to calculate median for.
        """
        exclude_grouping_columns = kwargs.get("exclude_grouping_columns", False)
        return self._non_empty_argument_function(
            "median",
            *cols,
            exclude_grouping_columns=exclude_grouping_columns,
            _emit_ast=_emit_ast,
        )

    @relational_group_df_api_usage
    @publicapi
    def min(self, *cols: ColumnOrName, _emit_ast: bool = True, **kwargs) -> DataFrame:
        """Return the min for the specified numeric columns.

        Args:
            cols: The columns to calculate min for.
        """
        exclude_grouping_columns = kwargs.get("exclude_grouping_columns", False)
        return self._non_empty_argument_function(
            "min",
            *cols,
            exclude_grouping_columns=exclude_grouping_columns,
            _emit_ast=_emit_ast,
        )

    @relational_group_df_api_usage
    @publicapi
    def max(self, *cols: ColumnOrName, _emit_ast: bool = True, **kwargs) -> DataFrame:
        """Return the max for the specified numeric columns.

        Args:
            cols: The columns to calculate max for.
        """
        exclude_grouping_columns = kwargs.get("exclude_grouping_columns", False)
        return self._non_empty_argument_function(
            "max",
            *cols,
            exclude_grouping_columns=exclude_grouping_columns,
            _emit_ast=_emit_ast,
        )

    @relational_group_df_api_usage
    @publicapi
    def count(self, _emit_ast: bool = True, **kwargs) -> DataFrame:
        """Return the number of rows for each group."""
        exclude_grouping_columns = kwargs.get("exclude_grouping_columns", False)
        df = self._to_df(
            [
                Alias(
                    functions._call_function(
                        "count", Literal(1), _emit_ast=False
                    )._expression,
                    "count",
                )
            ],
            exclude_grouping_columns=exclude_grouping_columns,
            _emit_ast=False,
        )
        # if no grouping exprs, there is already a LIMIT 1 in the query
        # see aggregate_statement in analyzer_utils.py
        df._ops_after_agg = set() if self._grouping_exprs else {"limit"}

        # TODO: count seems similar to mean, min, .... Can we unify implementation here?
        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(
                stmt.expr.relational_grouped_dataframe_builtin, stmt
            )
            self._set_ast_ref(ast.grouped_df)
            ast.agg_name = "count"
            df._ast_id = stmt.uid

        return df

    @publicapi
    def function(self, agg_name: str, _emit_ast: bool = True, **kwargs) -> Callable:
        """Computes the builtin aggregate ``agg_name`` over the specified columns. Use
        this function to invoke any aggregates not explicitly listed in this class.
        See examples in :meth:`DataFrame.group_by`.

        Args:
            agg_name: The name of the aggregate function.
        """
        exclude_grouping_columns = kwargs.get("exclude_grouping_columns", False)
        return lambda *cols: self._function(
            agg_name,
            *cols,
            exclude_grouping_columns=exclude_grouping_columns,
            _emit_ast=_emit_ast,
        )

    builtin = function

    @publicapi
    def _function(
        self, agg_name: str, *cols: ColumnOrName, _emit_ast: bool = True, **kwargs
    ) -> DataFrame:
        exclude_grouping_columns = kwargs.get("exclude_grouping_columns", False)
        agg_exprs = []
        for c in cols:
            c_expr = Column(c)._expression if isinstance(c, str) else c._expression
            expr = functions._call_function(
                agg_name, c_expr, _emit_ast=False
            )._expression
            agg_exprs.append(expr)

        df = self._to_df(agg_exprs, exclude_grouping_columns=exclude_grouping_columns)
        # if no grouping exprs, there is already a LIMIT 1 in the query
        # see aggregate_statement in analyzer_utils.py
        df._ops_after_agg = set() if self._grouping_exprs else {"limit"}

        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(
                stmt.expr.relational_grouped_dataframe_builtin, stmt
            )
            self._set_ast_ref(ast.grouped_df)
            ast.agg_name = agg_name
            exprs, is_variadic = parse_positional_args_to_list_variadic(*cols)
            ast.cols.variadic = is_variadic
            for e in exprs:
                build_expr_from_python_val(ast.cols.args.add(), e)

            df._ast_id = stmt.uid

        return df

    @relational_group_df_api_usage
    @experimental(version="1.39.0")
    @publicapi
    def ai_agg(
        self,
        expr: ColumnOrName,
        task_description: str,
        _emit_ast: bool = True,
        **kwargs,
    ) -> DataFrame:
        """Aggregate a column of text data using a natural language task description.

        This method reduces a column of text by performing a natural language aggregation
        as described in the task description for each group. For instance, it can summarize
        large datasets or extract specific insights per group.

        Args:
            expr: The column (Column object or column name as string) containing the text data
                on which the aggregation operation is to be performed.
            task_description: A plain English string that describes the aggregation task, such as
                "Summarize the product reviews for a blog post targeting consumers" or
                "Identify the most positive review and translate it into French and Polish, one word only".

        Returns:
            A DataFrame with one row per group containing the aggregated result.

        Example::

            >>> df = session.create_dataframe([
            ...     ["electronics", "Excellent product, highly recommend!"],
            ...     ["electronics", "Great quality and fast shipping"],
            ...     ["clothing", "Perfect fit and great material"],
            ...     ["clothing", "Poor quality, very disappointed"],
            ... ], schema=["category", "review"])
            >>> summary_df = df.group_by("category").ai_agg(
            ...     expr="review",
            ...     task_description="Summarize these product reviews for a blog post targeting consumers"
            ... )
            >>> summary_df.count()
            2

        Note:
            For optimal performance, follow these guidelines:

                - Use plain English text for the task description.

                - Describe the text provided in the task description. For example, instead of a task
                  description like "summarize", use "Summarize the phone call transcripts".

                - Describe the intended use case. For example, instead of "find the best review",
                  use "Find the most positive and well-written restaurant review to highlight on
                  the restaurant website".

                - Consider breaking the task description into multiple steps.
        """
        exclude_grouping_columns = kwargs.get("exclude_grouping_columns", False)

        # Convert expr to Column expression
        expr_col = (
            Column(expr)._expression if isinstance(expr, str) else expr._expression
        )

        # Create the ai_agg expression
        agg_expr = functions.ai_agg(
            Column(expr_col, _emit_ast=False),
            functions.lit(task_description, _emit_ast=False),
            _emit_ast=False,
        )._expression

        df = self._to_df(
            [agg_expr],
            exclude_grouping_columns=exclude_grouping_columns,
            _emit_ast=False,
        )
        # if no grouping exprs, there is already a LIMIT 1 in the query
        # see aggregate_statement in analyzer_utils.py
        df._ops_after_agg = set() if self._grouping_exprs else {"limit"}

        if _emit_ast:
            stmt = self._dataframe._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.relational_grouped_dataframe_ai_agg, stmt)
            # Reference the grouped dataframe
            self._set_ast_ref(ast.grouped_df)
            # Set arguments
            build_expr_from_python_val(ast.expr, expr)
            ast.task_description = task_description
            df._ast_id = stmt.uid

        return df

    @publicapi
    def _non_empty_argument_function(
        self, func_name: str, *cols: ColumnOrName, _emit_ast: bool = True, **kwargs
    ) -> DataFrame:
        exclude_grouping_columns = kwargs.get("exclude_grouping_columns", False)
        if not cols:
            raise ValueError(
                f"You must pass a list of one or more Columns to function: {func_name}"
            )
        else:
            return self.builtin(
                func_name,
                exclude_grouping_columns=exclude_grouping_columns,
                _emit_ast=_emit_ast,
            )(*cols)

    def _set_ast_ref(self, expr_builder: proto.Expr) -> None:
        """
        Given a field builder expression of the AST type Expr, points the builder to reference this RelationalGroupedDataFrame.
        """
        debug_check_missing_ast(self._ast_id, self._dataframe._session, self._dataframe)
        expr_builder.relational_grouped_dataframe_ref.id = self._ast_id
