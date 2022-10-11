#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import Dict, Iterable, List, NamedTuple, Optional, Union, overload

import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.binary_plan_node import create_join_type
from snowflake.snowpark._internal.analyzer.select_statement import (
    SelectableEntity,
    SelectStatement,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import UnresolvedRelation
from snowflake.snowpark._internal.analyzer.table_merge_expression import (
    DeleteMergeExpression,
    InsertMergeExpression,
    TableDelete,
    TableMerge,
    TableUpdate,
    UpdateMergeExpression,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.telemetry import add_api_call, set_api_call_source
from snowflake.snowpark._internal.type_utils import ColumnOrLiteral
from snowflake.snowpark._internal.utils import warning
from snowflake.snowpark.column import Column
from snowflake.snowpark.dataframe import DataFrame, _disambiguate
from snowflake.snowpark.row import Row


class UpdateResult(NamedTuple):
    """Result of updating rows in a :class:`Table`."""

    rows_updated: int  #: The number of rows modified.
    multi_joined_rows_updated: int  #: The number of multi-joined rows modified.


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
            specified condition.
    """

    def __init__(self, condition: Optional[Column] = None) -> None:
        self._condition_expr = condition._expression if condition is not None else None
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
            >>> from snowflake.snowpark.functions import when_matched
            >>> target_df = session.create_dataframe([(10, "old"), (10, "too_old"), (11, "old")], schema=["key", "value"])
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> target = session.table("my_table")
            >>> source = session.create_dataframe([(10, "new")], schema=["key", "value"])
            >>> target.merge(source, target["key"] == source["key"], [when_matched().update({"value": source["value"]})])
            MergeResult(rows_inserted=0, rows_updated=2, rows_deleted=0)
            >>> target.collect() # the value in the table is updated
            [Row(KEY=10, VALUE='new'), Row(KEY=10, VALUE='new'), Row(KEY=11, VALUE='old')]

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
        self._clause = UpdateMergeExpression(
            self._condition_expr,
            {Column(k)._expression: Column._to_expr(v) for k, v in assignments.items()},
        )
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

    def __init__(self, condition: Optional[Column] = None) -> None:
        self._condition_expr = condition._expression if condition is not None else None
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
            >>> target_df = session.create_dataframe([(10, "old"), (10, "too_old"), (11, "old")], schema=["key", "value"])
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> target = session.table("my_table")
            >>> source = session.create_dataframe([(12, "new")], schema=["key", "value"])
            >>> target.merge(source, target["key"] == source["key"], [when_not_matched().insert([source["key"], source["value"]])])
            MergeResult(rows_inserted=1, rows_updated=0, rows_deleted=0)
            >>> target.collect() # the rows are inserted
            [Row(KEY=12, VALUE='new'), Row(KEY=10, VALUE='old'), Row(KEY=10, VALUE='too_old'), Row(KEY=11, VALUE='old')]

            >>> # For all such rows, insert a row into target whose key is
            >>> # assigned to the key of the not matched row.
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> target.merge(source, target["key"] == source["key"], [when_not_matched().insert({"key": source["key"]})])
            MergeResult(rows_inserted=1, rows_updated=0, rows_deleted=0)
            >>> target.collect() # the rows are inserted
            [Row(KEY=12, VALUE=None), Row(KEY=10, VALUE='old'), Row(KEY=10, VALUE='too_old'), Row(KEY=11, VALUE='old')]

        Note:
            An exception will be raised if this method is called more than once
            on the same :class:`WhenNotMatchedClause` object.
        """
        if self._clause:
            raise SnowparkClientExceptionMessages.MERGE_TABLE_ACTION_ALREADY_SPECIFIED(
                "insert", "WhenNotMatchedClause"
            )
        if isinstance(assignments, dict):
            keys = [Column(k)._expression for k in assignments.keys()]
            values = [Column._to_expr(v) for v in assignments.values()]
        else:
            keys = []
            values = [Column._to_expr(v) for v in assignments]
        self._clause = InsertMergeExpression(self._condition_expr, keys, values)
        return self


def _get_update_result(rows: List[Row]) -> UpdateResult:
    return UpdateResult(int(rows[0][0]), int(rows[0][1]))


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

    def __init__(
        self,
        table_name: str,
        session: Optional["snowflake.snowpark.session.Session"] = None,
    ) -> None:
        super().__init__(
            session, session._analyzer.resolve(UnresolvedRelation(table_name))
        )
        self.table_name: str = table_name  #: The table name

        if self._session.sql_simplifier_enabled:
            self._select_statement = SelectStatement(
                from_=SelectableEntity(table_name, analyzer=session._analyzer),
                analyzer=session._analyzer,
            )
        # By default, the set the initial API call to say 'Table.__init__' since
        # people could instantiate a table directly. This value is overwritten when
        # created from Session object
        set_api_call_source(self, "Table.__init__")

    def __copy__(self) -> "Table":
        return Table(self.table_name, self._session)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.drop_table()

    def sample(
        self,
        frac: Optional[float] = None,
        n: Optional[int] = None,
        *,
        seed: Optional[float] = None,
        sampling_method: Optional[str] = None,
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
        if sampling_method is None and seed is None:
            return super().sample(frac=frac, n=n)
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

        # The analyzer will generate a sql with subquery. So we build the sql directly without using the analyzer.
        sampling_method_text = sampling_method or ""
        frac_or_rowcount_text = str(frac * 100.0) if frac is not None else f"{n} ROWS"
        seed_text = f" SEED ({seed})" if seed is not None else ""
        sql_text = f"SELECT * FROM {self.table_name} SAMPLE {sampling_method_text} ({frac_or_rowcount_text}) {seed_text}"
        return self._session.sql(sql_text)

    @overload
    def update(
        self,
        assignments: Dict[str, ColumnOrLiteral],
        condition: Optional[Column] = None,
        source: Optional[DataFrame] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
    ) -> UpdateResult:
        ...

    @overload
    def update(
        self,
        assignments: Dict[str, ColumnOrLiteral],
        condition: Optional[Column] = None,
        source: Optional[DataFrame] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
    ) -> "snowflake.snowpark.AsyncJob":
        ...

    def update(
        self,
        assignments: Dict[str, ColumnOrLiteral],
        condition: Optional[Column] = None,
        source: Optional[DataFrame] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
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
            block: (Experimental) A bool value indicating whether this function will wait until the result is available.
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
            >>> t.collect()
            [Row(A=2, B=0), Row(A=3, B=0), Row(A=3, B=0), Row(A=4, B=0), Row(A=4, B=0), Row(A=5, B=0)]

            >>> # update all rows in column "b" to 0 where column "a" has value 1
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> t.update({"b": 0}, t["a"] == 1)
            UpdateResult(rows_updated=2, multi_joined_rows_updated=0)
            >>> t.collect()
            [Row(A=1, B=0), Row(A=1, B=0), Row(A=2, B=1), Row(A=2, B=2), Row(A=3, B=1), Row(A=3, B=2)]

            >>> # update all rows in column "b" to 0 where column "a" in this
            >>> # table is equal to column "a" in another dataframe
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> source_df = session.create_dataframe([1, 2, 3, 4], schema=["a"])
            >>> t.update({"b": 0}, t["a"] == source_df.a, source_df)
            UpdateResult(rows_updated=6, multi_joined_rows_updated=0)
            >>> t.collect()
            [Row(A=1, B=0), Row(A=1, B=0), Row(A=2, B=0), Row(A=2, B=0), Row(A=3, B=0), Row(A=3, B=0)]
        """
        if not block:
            warning(
                "update.block",
                "block argument is experimental. Do not use it in production.",
            )

        if source:
            assert (
                condition is not None
            ), "condition should also be provided if source is provided"

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
            )
        )
        add_api_call(new_df, "Table.update")
        result = new_df._internal_collect_with_tag(
            statement_params=statement_params,
            block=block,
            data_type=snowflake.snowpark.async_job._AsyncResultType.UPDATE,
        )
        return _get_update_result(result) if block else result

    @overload
    def delete(
        self,
        condition: Optional[Column] = None,
        source: Optional[DataFrame] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
    ) -> DeleteResult:
        ...

    @overload
    def delete(
        self,
        condition: Optional[Column] = None,
        source: Optional[DataFrame] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
    ) -> "snowflake.snowpark.AsyncJob":
        ...

    def delete(
        self,
        condition: Optional[Column] = None,
        source: Optional[DataFrame] = None,
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
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
            block: (Experimental) A bool value indicating whether this function will wait until the result is available.
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
            >>> t.collect()
            [Row(A=2, B=1), Row(A=2, B=2), Row(A=3, B=1), Row(A=3, B=2)]

            >>> # delete all rows in this table where column "a" in this
            >>> # table is equal to column "a" in another dataframe
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> source_df = session.create_dataframe([2, 3, 4, 5], schema=["a"])
            >>> t.delete(t["a"] == source_df.a, source_df)
            DeleteResult(rows_deleted=4)
            >>> t.collect()
            [Row(A=1, B=1), Row(A=1, B=2)]
        """
        if not block:
            warning(
                "delete.block",
                "block argument is experimental. Do not use it in production.",
            )

        if source:
            assert (
                condition is not None
            ), "condition should also be provided if source is provided"

        new_df = self._with_plan(
            TableDelete(
                self.table_name,
                condition._expression if condition is not None else None,
                _disambiguate(self, source, create_join_type("left"), [])[1]._plan
                if source
                else None,
            )
        )
        add_api_call(new_df, "Table.delete")
        result = new_df._internal_collect_with_tag(
            statement_params=statement_params,
            block=block,
            data_type=snowflake.snowpark.async_job._AsyncResultType.DELETE,
        )
        return _get_delete_result(result) if block else result

    @overload
    def merge(
        self,
        source: DataFrame,
        join_expr: Column,
        clauses: Iterable[Union[WhenMatchedClause, WhenNotMatchedClause]],
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
    ) -> MergeResult:
        ...

    @overload
    def merge(
        self,
        source: DataFrame,
        join_expr: Column,
        clauses: Iterable[Union[WhenMatchedClause, WhenNotMatchedClause]],
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = False,
    ) -> "snowflake.snowpark.AsyncJob":
        ...

    def merge(
        self,
        source: DataFrame,
        join_expr: Column,
        clauses: Iterable[Union[WhenMatchedClause, WhenNotMatchedClause]],
        *,
        statement_params: Optional[Dict[str, str]] = None,
        block: bool = True,
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
            block: (Experimental) A bool value indicating whether this function will wait until the result is available.
                When it is ``False``, this function executes the underlying queries of the dataframe
                asynchronously and returns an :class:`AsyncJob`.

        Example::

            >>> from snowflake.snowpark.functions import when_matched, when_not_matched
            >>> target_df = session.create_dataframe([(10, "old"), (10, "too_old"), (11, "old")], schema=["key", "value"])
            >>> target_df.write.save_as_table("my_table", mode="overwrite", table_type="temporary")
            >>> target = session.table("my_table")
            >>> source = session.create_dataframe([(10, "new"), (12, "new"), (13, "old")], schema=["key", "value"])
            >>> target.merge(source, target["key"] == source["key"],
            ...              [when_matched().update({"value": source["value"]}), when_not_matched().insert({"key": source["key"]})])
            MergeResult(rows_inserted=2, rows_updated=2, rows_deleted=0)
            >>> target.collect()
            [Row(KEY=13, VALUE=None), Row(KEY=12, VALUE=None), Row(KEY=10, VALUE='new'), Row(KEY=10, VALUE='new'), Row(KEY=11, VALUE='old')]
        """
        if not block:
            warning(
                "merge.block",
                "block argument is experimental. Do not use it in production.",
            )

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

        new_df = self._with_plan(
            TableMerge(
                self.table_name,
                _disambiguate(self, source, create_join_type("left"), [])[1]._plan,
                join_expr._expression,
                merge_exprs,
            )
        )
        add_api_call(new_df, "Table.update")
        result = new_df._internal_collect_with_tag(
            statement_params=statement_params,
            block=block,
            data_type=snowflake.snowpark.async_job._AsyncResultType.MERGE,
        )
        if not block:
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

    def drop_table(self) -> None:
        """Drops the table from the Snowflake database.

        Note that subsequent operations such as :meth:`DataFrame.select`, :meth:`DataFrame.collect` on this ``Table`` instance and the derived DataFrame will raise errors because the underlying
        table in the Snowflake database no longer exists.
        """
        self._session.sql(
            f"drop table {self.table_name}"
        )._internal_collect_with_tag_no_telemetry()
