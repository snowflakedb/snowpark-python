#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import Dict, List, NamedTuple, Optional, Union

import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.snowflake_plan import (
    TableDelete,
    TableUpdate,
)
from snowflake.snowpark._internal.plans.logical.logical_plan import UnresolvedRelation
from snowflake.snowpark._internal.sp_types.sp_join_types import JoinType as SPJoinType
from snowflake.snowpark._internal.sp_types.types_package import LiteralType
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


def _get_update_result(rows: List[Row]) -> UpdateResult:
    return UpdateResult(int(rows[0][0]), int(rows[0][1]))


def _get_delete_result(rows: List[Row]) -> DeleteResult:
    return DeleteResult(int(rows[0][0]))


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

    def update(
        self,
        # TODO SNOW-526251: also accept Column as a key when Column is hashable
        assignments: Dict[str, Union[Column, LiteralType]],
        condition: Optional[Column] = None,
        source_data: Optional[DataFrame] = None,
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
                specified condition. It must be provided if ``source_data`` is provided.
            source_data: An optional :class:`DataFrame` that is included in ``condition``.
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
        if source_data:
            assert (
                condition
            ), "condition should also be provided if source_data is provided"

        assignments_expr = {}
        for k, v in assignments.items():
            k_expr = Column(k).expression
            v_expr = v.expression if isinstance(v, Column) else Column._to_expr(v)
            assignments_expr[k_expr] = v_expr
        new_df = self._with_plan(
            TableUpdate(
                self.table_name,
                assignments_expr,
                condition.expression if condition else None,
                DataFrame._disambiguate(
                    self, source_data, SPJoinType.from_string("left"), []
                )[1]._plan
                if source_data
                else None,
            )
        )
        return _get_update_result(new_df.collect())

    def delete(
        self,
        condition: Optional[Column] = None,
        source_data: Optional[DataFrame] = None,
    ) -> DeleteResult:
        """
        Deletes rows in a Table and returns a :class:`DeleteResult`,
        representing the number of rows deleted.

        Args:
            condition: An optional :class:`Column` object representing the
                specified condition. It must be provided if ``source_data`` is provided.
            source_data: An optional :class:`DataFrame` that is included in ``condition``.
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
        if source_data:
            assert (
                condition
            ), "condition should also be provided if source_data is provided"

        new_df = self._with_plan(
            TableDelete(
                self.table_name,
                condition.expression if condition else None,
                DataFrame._disambiguate(
                    self, source_data, SPJoinType.from_string("left"), []
                )[1]._plan
                if source_data
                else None,
            )
        )
        return _get_delete_result(new_df.collect())
