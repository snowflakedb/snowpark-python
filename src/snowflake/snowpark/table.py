#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import Dict, Iterable, List, NamedTuple, Optional, Union

import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    TableDelete,
    TableMerge,
    TableUpdate,
)
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.plans.logical.logical_plan import UnresolvedRelation
from snowflake.snowpark._internal.sp_expressions import (
    DeleteMergeExpression,
    InsertMergeExpression,
    UpdateMergeExpression,
)
from snowflake.snowpark._internal.sp_types.sp_join_types import JoinType as SPJoinType
from snowflake.snowpark._internal.sp_types.types_package import ColumnOrLiteral
from snowflake.snowpark.column import Column
from snowflake.snowpark.dataframe import DataFrame
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

    def __init__(self, condition: Optional[Column] = None):
        self.condition_expr = condition.expression if condition else None
        self.clause = None

    def update(
        self, assignments: Dict[str, Union[ColumnOrLiteral]]
    ) -> "WhenMatchedClause":
        """
        Defines an update action for the matched clause and
        returns an updated :class:`WhenMatchedClause` with the new
        update action added.

        Args:
            assignments: A list of values or a ``dict`` that associates
                the names of columns with the values that should be updated.
                The value of ``assignments`` can either be a literal value or
                a :class:`Column` object.

        Examples::

            # Adds a matched clause where a row in source is matched
            # if its id is equal to the id of any row in target.
            # For all such rows, update its value to the value of the
            # corresponding row in source.
            from snowflake.snowpark.functions import when_matched
            target.merge(source, target["id"] == source["id"], [when_matched().update({"value": source("value")})])

        Note:
            An exception will be raised if this method or :meth:`WhenMatchedClause.delete`
            is called more than once on the same :class:`WhenMatchedClause` object.
        """
        if self.clause:
            raise SnowparkClientExceptionMessages.MERGE_TABLE_ACTION_ALREADY_SPECIFIED(
                "update"
                if isinstance(self.clause, UpdateMergeExpression)
                else "delete",
                "WhenMatchedClause",
            )
        self.clause = UpdateMergeExpression(
            self.condition_expr,
            {Column(k).expression: Column._to_expr(v) for k, v in assignments.items()},
        )
        return self

    def delete(self):
        """
        Defines a delete action for the matched clause and
        returns an updated :class:`WhenMatchedClause` with the new
        delete action added.

        Example::

            # Adds a matched clause where a row in source is matched
            # if its id is equal to the id of any row in target.
            # For all such rows, update its value to the value of the
            # corresponding row in source.
            from snowflake.snowpark.functions import when_matched
            target.merge(source, target["id"] == source["id"], [when_matched().delete()])

        Note:
            An exception will be raised if this method or :meth:`WhenMatchedClause.update`
            is called more than once on the same :class:`WhenMatchedClause` object.
        """
        if self.clause:
            raise SnowparkClientExceptionMessages.MERGE_TABLE_ACTION_ALREADY_SPECIFIED(
                "update"
                if isinstance(self.clause, UpdateMergeExpression)
                else "delete",
                "WhenMatchedClause",
            )
        self.clause = DeleteMergeExpression(self.condition_expr)
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

    def __init__(self, condition: Optional[Column] = None):
        self.condition_expr = condition.expression if condition else None
        self.clause = None

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

            # Adds a not-matched clause where a row in source is not matched
            # if its id does not equal the id of any row in target.
            # For all such rows, insert a row into target whose id and value
            # are assigned to the id and value of the not matched row.
            from snowflake.snowpark.functions import when_not_matched
            target.merge(source, target["id"] == source["id"], [when_not_matched().insert([source("id"), source("value")])])

            # For all such rows, insert a row into target whose id is
            # assigned to the id of the not matched row.
            target.merge(source, target["id"] == source["id"], [when_not_matched().insert({"id": source("id")})])

        Note:
            An exception will be raised if this method is called more than once
            on the same :class:`WhenNotMatchedClause` object.
        """
        if self.clause:
            raise SnowparkClientExceptionMessages.MERGE_TABLE_ACTION_ALREADY_SPECIFIED(
                "insert", "WhenNotMatchedClause"
            )
        if isinstance(assignments, dict):
            keys = [Column(k).expression for k in assignments.keys()]
            values = [Column._to_expr(v) for v in assignments.values()]
        else:
            keys = []
            values = [Column._to_expr(v) for v in assignments]
        self.clause = InsertMergeExpression(self.condition_expr, keys, values)
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
    with the name of the table in Snowflake.

    Example::

        # create a Table object with a table name in Snowflake
        df_prices = session.table("itemsdb.publicschema.prices")
    """

    def __init__(
        self, table_name: str, session: Optional["snowflake.snowpark.Session"] = None
    ):
        super().__init__(
            session, session._analyzer.resolve(UnresolvedRelation(table_name))
        )
        self.table_name = table_name

    def clone(self) -> "Table":
        """Returns a clone of this :class:`Table`."""
        return Table(self.table_name, self.session)

    def update(
        self,
        # TODO SNOW-526251: also accept Column as a key when Column is hashable
        assignments: Dict[str, ColumnOrLiteral],
        condition: Optional[Column] = None,
        source: Optional[DataFrame] = None,
    ) -> UpdateResult:
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

        Examples::

            t = session.table("mytable")
            # update all rows in column "b" to 0 and all rows in column "a"
            # to the summation of column "a" and column "b"
            t.update({"b": 0, "a": t.a + t.b})
            # update all rows in column "b" to 0 where column "a" has value 1
            t.update({"b": 0}, t["a"] == 1)
            # update all rows in column "b" to 0 where column "a" in this
            # table is equal to column "a" in another dataframe
            df = sessions.createDataFrame([1, 2, 3, 4], schema=["a"])
            t.update({"b": 0}, t["a"] == df["a"], df)
        """
        if source:
            assert condition, "condition should also be provided if source is provided"

        new_df = self._with_plan(
            TableUpdate(
                self.table_name,
                {
                    Column(k).expression: Column._to_expr(v)
                    for k, v in assignments.items()
                },
                condition.expression if condition else None,
                DataFrame._disambiguate(
                    self, source, SPJoinType.from_string("left"), []
                )[1]._plan
                if source
                else None,
            )
        )
        return _get_update_result(new_df.collect())

    def delete(
        self,
        condition: Optional[Column] = None,
        source: Optional[DataFrame] = None,
    ) -> DeleteResult:
        """
        Deletes rows in a Table and returns a :class:`DeleteResult`,
        representing the number of rows deleted.

        Args:
            condition: An optional :class:`Column` object representing the
                specified condition. It must be provided if ``source`` is provided.
            source: An optional :class:`DataFrame` that is included in ``condition``.
                It can also be another :class:`Table`.

        Examples::

            t = session.table("mytable")
            # delete all rows in a table
            t.delete()
            # delete all rows where column "a" has value 1
            t.delete(t["a"] == 1)
            # delete all rows in this table where column "a" in this
            # table is equal to column "a" in another dataframe
            df = sessions.createDataFrame([1, 2, 3, 4], schema=["a"])
            t.delete(t["a"] == df["a"], df)
        """
        if source:
            assert condition, "condition should also be provided if source is provided"

        new_df = self._with_plan(
            TableDelete(
                self.table_name,
                condition.expression if condition else None,
                DataFrame._disambiguate(
                    self, source, SPJoinType.from_string("left"), []
                )[1]._plan
                if source
                else None,
            )
        )
        return _get_delete_result(new_df.collect())

    def merge(
        self,
        source: DataFrame,
        join_expr: Column,
        clauses: Iterable[Union[WhenMatchedClause, WhenNotMatchedClause]],
    ) -> MergeResult:
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

        Example::

            from snowflake.snowpark.functions import when_matched, when_not_matched
            target.merge(source, target["id"] == source["id"],
                        [when_matched().update({"value": source["value"]})
                         when_not_matched().insert({"id": source["id"]})])
        """
        inserted, updated, deleted = False, False, False
        merge_exprs = []
        for c in clauses:
            if isinstance(c, WhenMatchedClause):
                if isinstance(c.clause, UpdateMergeExpression):
                    updated = True
                else:
                    deleted = True
            elif isinstance(c, WhenNotMatchedClause):
                inserted = True
            else:
                raise TypeError(
                    "clauses only accepts WhenMatchedClause or WhenNotMatchedClause instances"
                )
            merge_exprs.append(c.clause)

        new_df = self._with_plan(
            TableMerge(
                self.table_name,
                DataFrame._disambiguate(
                    self, source, SPJoinType.from_string("left"), []
                )[1]._plan,
                join_expr.expression,
                merge_exprs,
            )
        )
        return _get_merge_result(
            new_df.collect(), inserted=inserted, updated=updated, deleted=deleted
        )
