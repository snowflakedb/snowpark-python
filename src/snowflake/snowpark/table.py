#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import sys
from logging import getLogger
from typing import Dict, List, NamedTuple, Optional, Union, overload

import snowflake.snowpark
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
from snowflake.snowpark._internal.analyzer.binary_plan_node import create_join_type
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import SnowflakeTable
from snowflake.snowpark._internal.analyzer.table_merge_expression import (
    DeleteMergeExpression,
    InsertMergeExpression,
    TableDelete,
    TableMerge,
    TableUpdate,
    UpdateMergeExpression,
)
from snowflake.snowpark._internal.analyzer.unary_plan_node import Sample
from snowflake.snowpark._internal.ast.utils import (
    build_expr_from_dict_str_str,
    build_expr_from_snowpark_column,
    build_expr_from_snowpark_column_or_python_val,
    with_src_position,
    DATAFRAME_AST_PARAMETER,
    build_table_name,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import add_api_call, set_api_call_source
from snowflake.snowpark._internal.type_utils import ColumnOrLiteral
from snowflake.snowpark._internal.utils import publicapi
from snowflake.snowpark.column import Column
from snowflake.snowpark.dataframe import DataFrame, _disambiguate
from snowflake.snowpark.row import Row

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable

_logger = getLogger(__name__)


class UpdateResult(NamedTuple):
    """Result of updating rows in a :class:`Table`."""

    rows_updated: int  #: The number of rows modified.
    multi_joined_rows_updated: Optional[
        int
    ] = None  #: The number of multi-joined rows modified. ``None`` if ERROR_ON_NONDETERMINISTIC_UPDATE is enabled.


class DeleteResult(NamedTuple):
    """Result of deleting rows in a :class:`Table`."""

    rows_deleted: int  #: The number of rows deleted.


class MergeResult(NamedTuple):
    """Result of merging a :class:`DataFrame` into a :class:`Table`."""

    rows_inserted: int  #: The number of rows inserted.
    rows_updated: int  #: The number of rows updated.
    rows_deleted: int  #: The number of rows deleted.


class WhenMatchedClause:
    """
    A matched clause for the :meth:`Table.merge` action. It matches all
    remaining rows in the target :class:`Table` that satisfy ``join_expr``
    while also satisfying ``condition``, if it is provided. You can use
    :func:`functions.when_matched` to instantiate this class.

    Args:
        condition: An optional :class:`Column` object representing the
            specified condition. For example, ``col("a") == 1``.
    """

    def __init__(
        self, condition: Optional[Column] = None, _emit_ast: bool = True
    ) -> None:
        self._condition_expr = condition._expression if condition is not None else None
        self._condition = condition
        self._clause = None

    def update(self, assignments: Dict[str, ColumnOrLiteral]) -> "WhenMatchedClause":
        """
        Defines an update action for the matched clause and
        returns an updated :class:`WhenMatchedClause` with the new
        update action added.

        Args:
            assignments: A list of values or a ``dict`` that associates
                the names of columns with the values that should be updated.
                The value of ``assignments`` can either be a literal value or
                a :class:`Column` object.

        Example::

            >>> # Adds a matched clause where a row in source is matched
            >>> # if its key is equal to the key of any row in target.
            >>> # For all such rows, update its value to the value of the
            >>> # corresponding row in source.
            >>> from snowflake.snowpark.functions import when_matched, lit
            >>> target_df = session.create_dataframe([(10, "old"), (10, "too_old"), (11, "old")], schema=["key", "value"])
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> target = session.table("my_table")
            >>> source = session.create_dataframe([(10, "new")], schema=["key", "value"])
            >>> target.merge(source, (target["key"] == source["key"]) & (target["value"] == lit("too_old")), [when_matched().update({"value": source["value"]})])
            MergeResult(rows_inserted=0, rows_updated=1, rows_deleted=0)
            >>> target.sort("key", "value").collect() # the value in the table is updated
            [Row(KEY=10, VALUE='new'), Row(KEY=10, VALUE='old'), Row(KEY=11, VALUE='old')]

        Note:
            An exception will be raised if this method or :meth:`WhenMatchedClause.delete`
            is called more than once on the same :class:`WhenMatchedClause` object.
        """
        if self._clause:
            raise SnowparkClientExceptionMessages.MERGE_TABLE_ACTION_ALREADY_SPECIFIED(
                "update"
                if isinstance(self._clause, UpdateMergeExpression)
                else "delete",
                "WhenMatchedClause",
            )
        self._clause = UpdateMergeExpression(self._condition_expr, assignments)
        return self

    def delete(self):
        """
        Defines a delete action for the matched clause and
        returns an updated :class:`WhenMatchedClause` with the new
        delete action added.

        Example::

            >>> # Adds a matched clause where a row in source is matched
            >>> # if its key is equal to the key of any row in target.
            >>> # For all such rows, delete them.
            >>> from snowflake.snowpark.functions import when_matched
            >>> target_df = session.create_dataframe([(10, "old"), (10, "too_old"), (11, "old")], schema=["key", "value"])
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> target = session.table("my_table")
            >>> source = session.create_dataframe([(10, "new")], schema=["key", "value"])
            >>> target.merge(source, target["key"] == source["key"], [when_matched().delete()])
            MergeResult(rows_inserted=0, rows_updated=0, rows_deleted=2)
            >>> target.collect() # the rows are deleted
            [Row(KEY=11, VALUE='old')]

        Note:
            An exception will be raised if this method or :meth:`WhenMatchedClause.update`
            is called more than once on the same :class:`WhenMatchedClause` object.
        """
        if self._clause:
            raise SnowparkClientExceptionMessages.MERGE_TABLE_ACTION_ALREADY_SPECIFIED(
                "update"
                if isinstance(self._clause, UpdateMergeExpression)
                else "delete",
                "WhenMatchedClause",
            )
        self._clause = DeleteMergeExpression(self._condition_expr)
        return self


class WhenNotMatchedClause:
    """
    A not-matched clause for the :meth:`Table.merge` action. It matches all
    remaining rows in the target :class:`Table` that do not satisfy ``join_expr``
    but satisfy ``condition``, if it is provided. You can use
    :func:`functions.when_not_matched` to instantiate this class.

    Args:
        condition: An optional :class:`Column` object representing the
            specified condition.
    """

    def __init__(
        self, condition: Optional[Column] = None, _emit_ast: bool = True
    ) -> None:
        self._condition_expr = condition._expression if condition is not None else None
        self._condition = condition
        self._clause = None

    def insert(
        self, assignments: Union[Iterable[ColumnOrLiteral], Dict[str, ColumnOrLiteral]]
    ) -> "WhenNotMatchedClause":
        """
        Defines an insert action for the not-matched clause and
        returns an updated :class:`WhenNotMatchedClause` with the new
        insert action added.

        Args:
            assignments: A list of values or a ``dict`` that associates
                the names of columns with the values that should be inserted.
                The value of ``assignments`` can either be a literal value or
                a :class:`Column` object.

        Examples::

            >>> # Adds a not-matched clause where a row in source is not matched
            >>> # if its key does not equal the key of any row in target.
            >>> # For all such rows, insert a row into target whose ley and value
            >>> # are assigned to the key and value of the not matched row.
            >>> from snowflake.snowpark.functions import when_not_matched
            >>> from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType
            >>> schema = StructType([StructField("key", IntegerType()), StructField("value", StringType())])
            >>> target_df = session.create_dataframe([(10, "old"), (10, "too_old"), (11, "old")], schema=schema)
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> target = session.table("my_table")

            >>> source = session.create_dataframe([(12, "new")], schema=schema)
            >>> target.merge(source, target["key"] == source["key"], [when_not_matched().insert([source["key"], source["value"]])])
            MergeResult(rows_inserted=1, rows_updated=0, rows_deleted=0)
            >>> target.sort("key", "value").collect() # the rows are inserted
            [Row(KEY=10, VALUE='old'), Row(KEY=10, VALUE='too_old'), Row(KEY=11, VALUE='old'), Row(KEY=12, VALUE='new')]

            >>> # For all such rows, insert a row into target whose key is
            >>> # assigned to the key of the not matched row.
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> target.merge(source, target["key"] == source["key"], [when_not_matched().insert({"key": source["key"]})])
            MergeResult(rows_inserted=1, rows_updated=0, rows_deleted=0)
            >>> target.sort("key", "value").collect() # the rows are inserted
            [Row(KEY=10, VALUE='old'), Row(KEY=10, VALUE='too_old'), Row(KEY=11, VALUE='old'), Row(KEY=12, VALUE=None)]

        Note:
            An exception will be raised if this method is called more than once
            on the same :class:`WhenNotMatchedClause` object.
        """
        if self._clause:
            raise SnowparkClientExceptionMessages.MERGE_TABLE_ACTION_ALREADY_SPECIFIED(
                "insert", "WhenNotMatchedClause"
            )
        if isinstance(assignments, dict):
            keys = list(assignments.keys())
            values = list(assignments.values())
        else:
            keys = []
            values = list(assignments)
        self._clause = InsertMergeExpression(self._condition_expr, keys, values)
        return self


def _get_update_result(rows: List[Row]) -> UpdateResult:
    if len(rows[0]) == 2:
        return UpdateResult(int(rows[0][0]), int(rows[0][1]))
    return UpdateResult(int(rows[0][0]))


def _get_delete_result(rows: List[Row]) -> DeleteResult:
    return DeleteResult(int(rows[0][0]))


def _get_merge_result(
    rows: List[Row], inserted: bool, updated: bool, deleted: bool
) -> MergeResult:
    idx = 0
    rows_inserted, rows_updated, rows_deleted = 0, 0, 0
    if inserted:
        rows_inserted = int(rows[0][idx])
        idx += 1
    if updated:
        rows_updated = int(rows[0][idx])
        idx += 1
    if deleted:
        rows_deleted = int(rows[0][idx])
    return MergeResult(rows_inserted, rows_updated, rows_deleted)


class Table(DataFrame):
    """
    Represents a lazily-evaluated Table. It extends :class:`DataFrame` so all
    :class:`DataFrame` operations can be applied to it.

    You can create a :class:`Table` object by calling :meth:`Session.table`
    with the name of the table in Snowflake. See examples in :meth:`Session.table`.
    """

    @publicapi
    def __init__(
        self,
        table_name: str,
        session: Optional["snowflake.snowpark.session.Session"] = None,
        is_temp_table_for_cleanup: bool = False,
        _ast_stmt: Optional[proto.Bind] = None,
        _emit_ast: bool = True,
    ) -> None:
        if _ast_stmt is None and session is not None and _emit_ast:
            _ast_stmt = session._ast_batch.bind()
            ast = with_src_position(_ast_stmt.expr.table, _ast_stmt)
            build_table_name(ast.name, table_name)
            ast.variant.table_init = True
            ast.is_temp_table_for_cleanup = is_temp_table_for_cleanup

        snowflake_table_plan = SnowflakeTable(
            table_name,
            session=session,
            is_temp_table_for_cleanup=is_temp_table_for_cleanup,
        )
        if session.sql_simplifier_enabled:
            plan = session._analyzer.create_select_statement(
                from_=session._analyzer.create_selectable_entity(
                    snowflake_table_plan, analyzer=session._analyzer
                ),
                analyzer=session._analyzer,
            )
        else:
            plan = snowflake_table_plan
        super().__init__(session, plan, _ast_stmt=_ast_stmt, _emit_ast=_emit_ast)
        self.is_cached: bool = self.is_cached  #: Whether the table is cached.
        self.table_name: str = table_name  #: The table name
        self._is_temp_table_for_cleanup = is_temp_table_for_cleanup

        # By default, the set the initial API call to say 'Table.__init__' since
        # people could instantiate a table directly. This value is overwritten when
        # created from Session object
        set_api_call_source(self, "Table.__init__")

    def _copy_without_ast(self):
        return Table(
            self.table_name,
            session=self._session,
            is_temp_table_for_cleanup=self._is_temp_table_for_cleanup,
            _emit_ast=False,
        )

    def __copy__(self) -> "Table":
        return Table(
            self.table_name,
            session=self._session,
            is_temp_table_for_cleanup=self._is_temp_table_for_cleanup,
            _emit_ast=self._session.ast_enabled,
        )

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.drop_table()

    @publicapi
    def sample(
        self,
        frac: Optional[float] = None,
        n: Optional[int] = None,
        *,
        seed: Optional[int] = None,
        sampling_method: Optional[str] = None,
        _emit_ast: bool = True,
    ) -> "DataFrame":
        """Samples rows based on either the number of rows to be returned or a percentage of rows to be returned.

        Sampling with a seed is not supported on views or subqueries. This method works on tables so it supports ``seed``.
        This is the main difference between :meth:`DataFrame.sample` and this method.

        Args:
            frac: The percentage of rows to be sampled.
            n: The fixed number of rows to sample in the range of 0 to 1,000,000 (inclusive). Either ``frac`` or ``n`` should be provided.
            seed: Specifies a seed value to make the sampling deterministic. Can be any integer between 0 and 2147483647 inclusive.
                Default value is ``None``.
            sampling_method: Specifies the sampling method to use:
                - "BERNOULLI" (or "ROW"): Includes each row with a probability of p/100. Similar to flipping a weighted coin for each row.
                - "SYSTEM" (or "BLOCK"): Includes each block of rows with a probability of p/100. Similar to flipping a weighted coin for each block of rows. This method does not support fixed-size sampling.
                Default is ``None``. Then the Snowflake database will use "ROW" by default.

        Note:
            - SYSTEM | BLOCK sampling is often faster than BERNOULLI | ROW sampling.
            - Sampling without a seed is often faster than sampling with a seed.
            - Fixed-size sampling can be slower than equivalent fraction-based sampling because fixed-size sampling prevents some query optimization.
            - Fixed-size sampling doesn't work with SYSTEM | BLOCK sampling.

        """
        DataFrame._validate_sample_input(frac, n)
        if sampling_method and sampling_method.upper() not in (
            "BERNOULLI",
            "ROW",
            "SYSTEM",
            "BLOCK",
        ):
            raise ValueError(
                f"'sampling_method' value {sampling_method} must be None or one of 'BERNOULLI', 'ROW', 'SYSTEM', or 'BLOCK'."
            )

        from snowflake.snowpark.mock._connection import MockServerConnection

        stmt = None
        if _emit_ast:
            # AST.
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.table_sample, stmt)
            if frac:
                ast.probability_fraction.value = frac
            if n:
                ast.num.value = n
            if seed:
                ast.seed.value = seed
            if sampling_method:
                ast.sampling_method.value = sampling_method
            self._set_ast_ref(ast.df)

        if isinstance(self._session._conn, MockServerConnection):
            if sampling_method in ("SYSTEM", "BLOCK"):
                _logger.warning(
                    "[Local Testing] SYSTEM/BLOCK sampling is not supported for Local Testing, falling back to ROW sampling"
                )

            sample_plan = Sample(
                self._plan, probability_fraction=frac, row_count=n, seed=seed
            )
            return self._with_plan(
                self._session._analyzer.create_select_statement(
                    from_=self._session._analyzer.create_select_snowflake_plan(
                        sample_plan, analyzer=self._session._analyzer
                    ),
                    analyzer=self._session._analyzer,
                ),
                _ast_stmt=stmt,
            )

        # The analyzer will generate a sql with subquery. So we build the sql directly without using the analyzer.
        sampling_method_text = sampling_method or ""
        frac_or_rowcount_text = str(frac * 100.0) if frac is not None else f"{n} ROWS"
        seed_text = f" SEED ({seed})" if seed is not None else ""
        sql_text = f"SELECT * FROM {self.table_name} SAMPLE {sampling_method_text} ({frac_or_rowcount_text}) {seed_text}"
        new_df = self._session.sql(sql_text, _ast_stmt=stmt)
        if self._session.reduce_describe_query_enabled:
            new_df._plan._metadata = self._plan._metadata
        return new_df

    @overload
    @publicapi
    def update(
        self,
        assignments: Dict[str, ColumnOrLiteral],
        condition: Optional[Column] = None,
        source: Optional[DataFrame] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        _emit_ast: bool = True,
    ) -> UpdateResult:
        ...  # pragma: no cover

    @overload
    @publicapi
    def update(
        self,
        assignments: Dict[str, ColumnOrLiteral],
        condition: Optional[Column] = None,
        source: Optional[DataFrame] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.AsyncJob":
        ...  # pragma: no cover

    @publicapi
    def update(
        self,
        assignments: Dict[str, ColumnOrLiteral],
        condition: Optional[Column] = None,
        source: Optional[DataFrame] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        _emit_ast: bool = True,
    ) -> Union[UpdateResult, "snowflake.snowpark.AsyncJob"]:
        """
        Updates rows in the Table with specified ``assignments`` and returns a
        :class:`UpdateResult`, representing the number of rows modified and the
        number of multi-joined rows modified.

        Args:
            assignments: A ``dict`` that associates the names of columns with the
                values that should be updated. The value of ``assignments`` can
                either be a literal value or a :class:`Column` object.
            condition: An optional :class:`Column` object representing the
                specified condition. It must be provided if ``source`` is provided.
            source: An optional :class:`DataFrame` that is included in ``condition``.
                It can also be another :class:`Table`.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.

        Examples::

            >>> target_df = session.create_dataframe([(1, 1),(1, 2),(2, 1),(2, 2),(3, 1),(3, 2)], schema=["a", "b"])
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> t = session.table("my_table")

            >>> # update all rows in column "b" to 0 and all rows in column "a"
            >>> # to the summation of column "a" and column "b"
            >>> t.update({"b": 0, "a": t.a + t.b})
            UpdateResult(rows_updated=6, multi_joined_rows_updated=0)
            >>> t.sort("a", "b").collect()
            [Row(A=2, B=0), Row(A=3, B=0), Row(A=3, B=0), Row(A=4, B=0), Row(A=4, B=0), Row(A=5, B=0)]

            >>> # update all rows in column "b" to 0 where column "a" has value 1
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> t.update({"b": 0}, t["a"] == 1)
            UpdateResult(rows_updated=2, multi_joined_rows_updated=0)
            >>> t.sort("a", "b").collect()
            [Row(A=1, B=0), Row(A=1, B=0), Row(A=2, B=1), Row(A=2, B=2), Row(A=3, B=1), Row(A=3, B=2)]

            >>> # update all rows in column "b" to 0 where column "a" in this
            >>> # table is equal to column "a" in another dataframe
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> source_df = session.create_dataframe([1, 2, 3, 4], schema=["a"])
            >>> t.update({"b": 0}, t["a"] == source_df.a, source_df)
            UpdateResult(rows_updated=6, multi_joined_rows_updated=0)
            >>> t.sort("a", "b").collect()
            [Row(A=1, B=0), Row(A=1, B=0), Row(A=2, B=0), Row(A=2, B=0), Row(A=3, B=0), Row(A=3, B=0)]
        """
        if source:
            assert (
                condition is not None
            ), "condition should also be provided if source is provided"

        kwargs = {}
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.table_update, stmt)
            self._set_ast_ref(ast.df)
            if assignments is not None:
                for k, v in assignments.items():
                    t = ast.assignments.add()
                    t._1 = k
                    build_expr_from_snowpark_column_or_python_val(t._2, v)
            if condition is not None:
                build_expr_from_snowpark_column(ast.condition, condition)
            if source is not None:
                source._set_ast_ref(ast.source)
            if statement_params is not None:
                build_expr_from_dict_str_str(ast.statement_params, statement_params)
            ast.block = block

            self._session._ast_batch.eval(stmt)

            # Flush AST and encode it as part of the query.
            _, kwargs[DATAFRAME_AST_PARAMETER] = self._session._ast_batch.flush(stmt)

        new_df = self._with_plan(
            TableUpdate(
                self.table_name,
                {
                    Column(k)._expression: Column._to_expr(v)
                    for k, v in assignments.items()
                },
                condition._expression if condition is not None else None,
                _disambiguate(self, source, create_join_type("left"), [])[1]._plan
                if source
                else None,
            ),
            _ast_stmt=stmt,
        )

        add_api_call(new_df, "Table.update")
        result = new_df._internal_collect_with_tag(
            statement_params=statement_params,
            block=block,
            data_type=snowflake.snowpark.async_job._AsyncResultType.UPDATE,
            **kwargs,
        )
        return _get_update_result(result) if block else result

    @overload
    @publicapi
    def delete(
        self,
        condition: Optional[Column] = None,
        source: Optional[DataFrame] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        _emit_ast: bool = True,
    ) -> DeleteResult:
        ...  # pragma: no cover

    @overload
    @publicapi
    def delete(
        self,
        condition: Optional[Column] = None,
        source: Optional[DataFrame] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.AsyncJob":
        ...  # pragma: no cover

    @publicapi
    def delete(
        self,
        condition: Optional[Column] = None,
        source: Optional[DataFrame] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        _emit_ast: bool = True,
    ) -> Union[DeleteResult, "snowflake.snowpark.AsyncJob"]:
        """
        Deletes rows in a Table and returns a :class:`DeleteResult`,
        representing the number of rows deleted.

        Args:
            condition: An optional :class:`Column` object representing the
                specified condition. It must be provided if ``source`` is provided.
            source: An optional :class:`DataFrame` that is included in ``condition``.
                It can also be another :class:`Table`.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.

        Examples::

            >>> target_df = session.create_dataframe([(1, 1),(1, 2),(2, 1),(2, 2),(3, 1),(3, 2)], schema=["a", "b"])
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> t = session.table("my_table")

            >>> # delete all rows in a table
            >>> t.delete()
            DeleteResult(rows_deleted=6)
            >>> t.collect()
            []

            >>> # delete all rows where column "a" has value 1
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> t.delete(t["a"] == 1)
            DeleteResult(rows_deleted=2)
            >>> t.sort("a", "b").collect()
            [Row(A=2, B=1), Row(A=2, B=2), Row(A=3, B=1), Row(A=3, B=2)]

            >>> # delete all rows in this table where column "a" in this
            >>> # table is equal to column "a" in another dataframe
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> source_df = session.create_dataframe([2, 3, 4, 5], schema=["a"])
            >>> t.delete(t["a"] == source_df.a, source_df)
            DeleteResult(rows_deleted=4)
            >>> t.sort("a", "b").collect()
            [Row(A=1, B=1), Row(A=1, B=2)]
        """
        if source:
            assert (
                condition is not None
            ), "condition should also be provided if source is provided"

        kwargs = {}
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.table_delete, stmt)
            self._set_ast_ref(ast.df)
            if condition is not None:
                build_expr_from_snowpark_column(ast.condition, condition)
            if source is not None:
                source._set_ast_ref(ast.source)
            if statement_params is not None:
                build_expr_from_dict_str_str(ast.statement_params, statement_params)
            ast.block = block

            self._session._ast_batch.eval(stmt)

            # Flush AST and encode it as part of the query.
            _, kwargs[DATAFRAME_AST_PARAMETER] = self._session._ast_batch.flush(stmt)

        new_df = self._with_plan(
            TableDelete(
                self.table_name,
                condition._expression if condition is not None else None,
                _disambiguate(self, source, create_join_type("left"), [])[1]._plan
                if source
                else None,
            ),
            _ast_stmt=stmt,
        )
        add_api_call(new_df, "Table.delete")
        result = new_df._internal_collect_with_tag(
            statement_params=statement_params,
            block=block,
            data_type=snowflake.snowpark.async_job._AsyncResultType.DELETE,
            **kwargs,
        )
        return _get_delete_result(result) if block else result

    @overload
    @publicapi
    def merge(
        self,
        source: DataFrame,
        join_expr: Column,
        clauses: Iterable[Union[WhenMatchedClause, WhenNotMatchedClause]],
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        _emit_ast: bool = True,
    ) -> MergeResult:
        ...  # pragma: no cover

    @overload
    @publicapi
    def merge(
        self,
        source: DataFrame,
        join_expr: Column,
        clauses: Iterable[Union[WhenMatchedClause, WhenNotMatchedClause]],
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.AsyncJob":
        ...  # pragma: no cover

    @publicapi
    def merge(
        self,
        source: DataFrame,
        join_expr: Column,
        clauses: Iterable[Union[WhenMatchedClause, WhenNotMatchedClause]],
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
        _emit_ast: bool = True,
    ) -> Union[MergeResult, "snowflake.snowpark.AsyncJob"]:
        """
        Merges this :class:`Table` with :class:`DataFrame` source on the specified
        join expression and a list of matched or not-matched clauses, and returns
        a :class:`MergeResult`, representing the number of rows inserted,
        updated and deleted by this merge action.
        See `MERGE <https://docs.snowflake.com/en/sql-reference/sql/merge.html#merge>`_
        for details.

        Args:
            source: A :class:`DataFrame` to join with this :class:`Table`.
                It can also be another :class:`Table`.
            join_expr: A :class:`Column` object representing the expression on which
                to join this :class:`Table` and ``source``.
            clauses: A list of matched or not-matched clauses specifying the actions
                to perform when the values from this :class:`Table` and ``source``
                match or not match on ``join_expr``. These actions can only be instances
                of :class:`WhenMatchedClause` and :class:`WhenNotMatchedClause`, and will
                be performed sequentially in this list.
            statement_params: Dictionary of statement level parameters to be set while executing this action.
            block: A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.

        Example::

            >>> from snowflake.snowpark.functions import when_matched, when_not_matched
            >>> from snowflake.snowpark.types import IntegerType, StringType, StructField, StructType
            >>> schema = StructType([StructField("key", IntegerType()), StructField("value", StringType())])
            >>> target_df = session.create_dataframe([(10, "old"), (10, "too_old"), (11, "old")], schema=schema)
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> target = session.table("my_table")
            >>> source = session.create_dataframe([(10, "new"), (12, "new"), (13, "old")], schema=schema)
            >>> target.merge(source, (target["key"] == source["key"]) & (target["value"] == "too_old"),
            ...              [when_matched().update({"value": source["value"]}), when_not_matched().insert({"key": source["key"]})])
            MergeResult(rows_inserted=2, rows_updated=1, rows_deleted=0)
            >>> target.sort("key", "value").collect()
            [Row(KEY=10, VALUE='new'), Row(KEY=10, VALUE='old'), Row(KEY=11, VALUE='old'), Row(KEY=12, VALUE=None), Row(KEY=13, VALUE=None)]
        """
        inserted, updated, deleted = False, False, False
        merge_exprs = []
        for c in clauses:
            if isinstance(c, WhenMatchedClause):
                if isinstance(c._clause, UpdateMergeExpression):
                    updated = True
                else:
                    deleted = True
            elif isinstance(c, WhenNotMatchedClause):
                inserted = True
            else:
                raise TypeError(
                    "clauses only accepts WhenMatchedClause or WhenNotMatchedClause instances"
                )
            merge_exprs.append(c._clause)

        kwargs = {}
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.table_merge, stmt)
            self._set_ast_ref(ast.df)
            source._set_ast_ref(ast.source)
            build_expr_from_snowpark_column_or_python_val(ast.join_expr, join_expr)

            for value in clauses:
                if value is not None and value._clause is not None:
                    matched_clause = ast.clauses.add()
                    if isinstance(value, WhenMatchedClause):
                        if isinstance(value._clause, UpdateMergeExpression):
                            matched_clause.merge_update_when_matched_clause.Clear()
                            assignments = value._clause._assignments
                            if assignments is not None:
                                for k, v in assignments.items():
                                    t = (
                                        matched_clause.merge_update_when_matched_clause.update_assignments.add()
                                    )
                                    build_expr_from_snowpark_column_or_python_val(
                                        t._1, k
                                    )
                                    build_expr_from_snowpark_column_or_python_val(
                                        t._2, v
                                    )
                            if value._condition is not None:
                                build_expr_from_snowpark_column_or_python_val(
                                    matched_clause.merge_update_when_matched_clause.condition,
                                    value._condition,
                                )
                        elif isinstance(value._clause, DeleteMergeExpression):
                            matched_clause.merge_delete_when_matched_clause.Clear()
                            if value._condition is not None:
                                build_expr_from_snowpark_column_or_python_val(
                                    # build_expr_from_snowpark_column_or_python_val(
                                    matched_clause.merge_delete_when_matched_clause.condition,
                                    value._condition,
                                )
                    elif isinstance(value, WhenNotMatchedClause):
                        if isinstance(value._clause, InsertMergeExpression):
                            matched_clause.merge_insert_when_not_matched_clause.Clear()
                            if value._clause._keys is not None:
                                for k in value._clause._keys:
                                    t = (
                                        matched_clause.merge_insert_when_not_matched_clause.insert_keys.add()
                                    )
                                    build_expr_from_snowpark_column_or_python_val(t, k)
                            if value._clause._values is not None:
                                for v in value._clause._values:
                                    t = (
                                        matched_clause.merge_insert_when_not_matched_clause.insert_values.add()
                                    )
                                    build_expr_from_snowpark_column_or_python_val(t, v)
                            if value._condition is not None:
                                build_expr_from_snowpark_column_or_python_val(
                                    matched_clause.merge_insert_when_not_matched_clause.condition,
                                    value._condition,
                                )
                    else:
                        raise TypeError(
                            f"{type(value)} is not a valid type for merge clause AST."
                        )

            if statement_params is not None:
                build_expr_from_dict_str_str(ast.statement_params, statement_params)
            ast.block = block

            self._session._ast_batch.eval(stmt)

            # Flush AST and encode it as part of the query.
            _, kwargs[DATAFRAME_AST_PARAMETER] = self._session._ast_batch.flush(stmt)

        new_df = self._with_plan(
            TableMerge(
                self.table_name,
                _disambiguate(self, source, create_join_type("left"), [])[1]._plan,
                join_expr._expression,
                merge_exprs,
            ),
            _ast_stmt=stmt,
        )
        add_api_call(new_df, "Table.update")
        result = new_df._internal_collect_with_tag(
            statement_params=statement_params,
            block=block,
            data_type=snowflake.snowpark.async_job._AsyncResultType.MERGE,
            **kwargs,
        )

        from snowflake.snowpark.mock._connection import MockServerConnection

        # Note that local testing mode is non-blocking regardless of block specified.
        if not block and not isinstance(self._session._conn, MockServerConnection):
            result._inserted = inserted
            result._updated = updated
            result._deleted = deleted
        return (
            _get_merge_result(
                result,
                inserted=inserted,
                updated=updated,
                deleted=deleted,
            )
            if block
            else result
        )

    @publicapi
    def drop_table(self, _emit_ast: bool = True) -> None:
        """Drops the table from the Snowflake database.

        Note that subsequent operations such as :meth:`DataFrame.select`, :meth:`DataFrame.collect` on this ``Table`` instance and the derived DataFrame will raise errors because the underlying
        table in the Snowflake database no longer exists.
        """

        from snowflake.snowpark.mock._connection import MockServerConnection

        kwargs = {}
        stmt = None
        if _emit_ast:
            stmt = self._session._ast_batch.bind()
            ast = with_src_position(stmt.expr.table_drop_table, stmt)
            self._set_ast_ref(ast.df)
            self._session._ast_batch.eval(stmt)

            # Flush AST and encode it as part of the query.
            _, kwargs[DATAFRAME_AST_PARAMETER] = self._session._ast_batch.flush(stmt)

        if isinstance(self._session._conn, MockServerConnection):
            # only mock connection has entity_registry
            self._session._conn.entity_registry.drop_table(
                self.table_name,
                **kwargs,
            )
        else:
            self._session.sql(
                f"drop table {self.table_name}",
                _ast_stmt=stmt,
            )._internal_collect_with_tag_no_telemetry(**kwargs)
