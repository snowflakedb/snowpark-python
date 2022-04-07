#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
"""Window frames in Snowpark."""
from __future__ import annotations

import sys
from typing import List, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.analyzer.expression import Expression, Literal
from snowflake.snowpark._internal.analyzer.sort_expression import Ascending, SortOrder
from snowflake.snowpark._internal.analyzer.window_expression import (
    CurrentRow,
    RangeFrame,
    RowFrame,
    SpecifiedWindowFrame,
    UnboundedFollowing,
    UnboundedPreceding,
    UnspecifiedFrame,
    WindowExpression,
    WindowFrame,
    WindowSpecDefinition,
)
from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark._internal.utils import Utils


class Window:
    """
    Contains functions to form :class:`WindowSpec`. See
    `Snowflake Window functions <https://docs.snowflake.com/en/sql-reference/functions-analytic.html#window-functions>`_ for reference.

    Examples::

        from snowflake.snowpark.functions import col, avg
        window1 = Window.partition_by("value").order_by("key").rows_between(Window.currentRow, 2)
        window2 = Window.order_by(col("key").desc()).range_between(Window.UNBOUNDED_PRECEDING, Window.UNBOUNDED_FOLLOWING)
        df.select("key", "value", avg("value").over(window1), avg("value").over(window2)
    """

    #: Returns a value representing unbounded preceding.
    UNBOUNDED_PRECEDING: int = -sys.maxsize
    unboundedPreceding = UNBOUNDED_PRECEDING

    #: Returns a value representing unbounded following.
    UNBOUNDED_FOLLOWING: int = sys.maxsize
    unboundedFollowing = UNBOUNDED_FOLLOWING

    #: Returns a value representing current row.
    currentRow: int = 0

    @staticmethod
    def partition_by(
        *cols: (ColumnOrName | list[ColumnOrName] | tuple[ColumnOrName, ...])
    ) -> WindowSpec:
        """
        Returns a :class:`WindowSpec` object with partition by clause.

        Args:
            cols: A column, as :class:`str`, :class:`~snowflake.snowpark.column.Column`
                or a list of those.
        """
        return Window._spec().partition_by(*cols)

    @staticmethod
    def order_by(
        *cols: (ColumnOrName | list[ColumnOrName] | tuple[ColumnOrName, ...])
    ) -> WindowSpec:
        """
        Returns a :class:`WindowSpec` object with order by clause.

        Args:
            cols: A column, as :class:`str`, :class:`~snowflake.snowpark.column.Column`
                or a list of those.
        """
        return Window._spec().order_by(*cols)

    @staticmethod
    def rows_between(start: int, end: int) -> WindowSpec:
        """
        Returns a :class:`WindowSpec` object with the row frame clause.

        Args:
            start: The relative position from the current row as a boundary start (inclusive).
                The frame is unbounded if this is :attr:`Window.UNBOUNDED_PRECEDING`, or any
                value less than or equal to -9223372036854775807 (``-sys.maxsize``).
            end: The relative position from the current row as a boundary end (inclusive).
                The frame is unbounded if this is :attr:`Window.UNBOUNDED_FOLLOWING`, or any
                value greater than or equal to 9223372036854775807 (``sys.maxsize``).

        Note:
            You can use :attr:`Window.UNBOUNDED_PRECEDING`, :attr:`Window.UNBOUNDED_FOLLOWING`,
            and :attr:`Window.currentRow` to specify ``start`` and ``end``, instead of using
            integral values directly.
        """
        return Window._spec().rows_between(start, end)

    @staticmethod
    def range_between(start: int, end: int) -> WindowSpec:
        """
        Returns a :class:`WindowSpec` object with the range frame clause.

        Args:
            start: The relative position from the current row as a boundary start (inclusive).
                The frame is unbounded if this is :attr:`Window.UNBOUNDED_PRECEDING`, or any
                value less than or equal to -9223372036854775807 (``-sys.maxsize``).
            end: The relative position from the current row as a boundary end (inclusive).
                The frame is unbounded if this is :attr:`Window.UNBOUNDED_FOLLOWING`, or any
                value greater than or equal to 9223372036854775807 (``sys.maxsize``).

        Note:
            You can use :attr:`Window.UNBOUNDED_PRECEDING`, :attr:`Window.UNBOUNDED_FOLLOWING`,
            and :attr:`Window.currentRow` to specify ``start`` and ``end``, instead of using
            integral values directly.
        """
        return Window._spec().range_between(start, end)

    @staticmethod
    def _spec() -> WindowSpec:
        return WindowSpec([], [], UnspecifiedFrame())

    orderBy = order_by
    partitionBy = partition_by
    rangeBetween = range_between
    rowsBetween = rows_between


class WindowSpec:
    """Represents a window frame clause."""

    def __init__(
        self,
        partition_spec: list[Expression],
        order_spec: list[SortOrder],
        frame: WindowFrame,
    ):
        self.partition_spec = partition_spec
        self.order_spec = order_spec
        self.frame = frame

    def partition_by(
        self, *cols: (ColumnOrName | list[ColumnOrName] | tuple[ColumnOrName, ...])
    ) -> WindowSpec:
        """
        Returns a new :class:`WindowSpec` object with the new partition by clause.

        See Also:
            - :func:`Window.partition_by`
        """
        exprs = Utils.parse_positional_args_to_list(*cols)
        partition_spec = [
            e.expression
            if isinstance(e, snowflake.snowpark.column.Column)
            else snowflake.snowpark.column.Column(e).expression
            for e in exprs
        ]

        return WindowSpec(partition_spec, self.order_spec, self.frame)

    def order_by(
        self, *cols: (ColumnOrName | list[ColumnOrName] | tuple[ColumnOrName, ...])
    ) -> WindowSpec:
        """
        Returns a new :class:`WindowSpec` object with the new order by clause.

        See Also:
            - :func:`Window.order_by`
        """
        exprs = Utils.parse_positional_args_to_list(*cols)
        order_spec = []
        for e in exprs:
            if isinstance(e, str):
                order_spec.append(
                    SortOrder(
                        snowflake.snowpark.column.Column(e).expression, Ascending()
                    )
                )
            elif isinstance(e, snowflake.snowpark.column.Column):
                if isinstance(e.expression, SortOrder):
                    order_spec.append(e.expression)
                elif isinstance(e.expression, Expression):
                    order_spec.append(SortOrder(e.expression, Ascending()))

        return WindowSpec(self.partition_spec, order_spec, self.frame)

    def rows_between(self, start: int, end: int) -> WindowSpec:
        """
        Returns a new :class:`WindowSpec` object with the new row frame clause.

        See Also:
            - :func:`Window.rows_between`
        """
        boundary_start, boundary_end = self._convert_boundary_to_expr(start, end)
        return WindowSpec(
            self.partition_spec,
            self.order_spec,
            SpecifiedWindowFrame(RowFrame(), boundary_start, boundary_end),
        )

    def range_between(self, start: int, end: int) -> WindowSpec:
        """
        Returns a new :class:`WindowSpec` object with the new range frame clause.

        See Also:
            - :func:`Window.range_between`
        """
        boundary_start, boundary_end = self._convert_boundary_to_expr(start, end)
        return WindowSpec(
            self.partition_spec,
            self.order_spec,
            SpecifiedWindowFrame(RangeFrame(), boundary_start, boundary_end),
        )

    def _convert_boundary_to_expr(
        self, start: int, end: int
    ) -> tuple[Expression, Expression]:
        if start == 0:
            boundary_start = CurrentRow()
        elif start <= Window.UNBOUNDED_PRECEDING:
            boundary_start = UnboundedPreceding()
        else:
            boundary_start = Literal(start)

        if end == 0:
            boundary_end = CurrentRow()
        elif end >= Window.UNBOUNDED_FOLLOWING:
            boundary_end = UnboundedFollowing()
        else:
            boundary_end = Literal(end)

        return boundary_start, boundary_end

    def _with_aggregate(
        self, aggregate: Expression
    ) -> snowflake.snowpark.column.Column:
        spec = WindowSpecDefinition(self.partition_spec, self.order_spec, self.frame)
        return snowflake.snowpark.column.Column(WindowExpression(aggregate, spec))

    orderBy = order_by
    partitionBy = partition_by
    rangeBetween = range_between
    rowsBetween = rows_between
