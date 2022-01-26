#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
"""Window frames in Snowpark."""
import sys
from typing import List, Tuple, Union

import snowflake.snowpark
from snowflake.snowpark._internal.sp_expressions import (
    Ascending as SPAscending,
    CurrentRow as SPCurrentRow,
    Expression as SPExpression,
    Literal as SPLiteral,
    RangeFrame as SPRangeFrame,
    RowFrame as SPRowFrame,
    SortOrder as SPSortOrder,
    SpecifiedWindowFrame as SPSpecifiedWindowFrame,
    UnboundedFollowing as SPUnboundedFollowing,
    UnboundedPreceding as SPUnboundedPreceding,
    UnspecifiedFrame as SPUnspecifiedFrame,
    WindowExpression as SPWindowExpression,
    WindowFrame as SPWindowFrame,
    WindowSpecDefinition as SPWindowSpecDefinition,
)
from snowflake.snowpark._internal.sp_types.types_package import ColumnOrName
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
        *cols: Union[
            ColumnOrName,
            List[ColumnOrName],
            Tuple[ColumnOrName, ...],
        ]
    ) -> "WindowSpec":
        """
        Returns a :class:`WindowSpec` object with partition by clause.

        Args:
            cols: A column, as :class:`str`, :class:`~snowflake.snowpark.column.Column`
                or a list of those.
        """
        return Window._spec().partition_by(*cols)

    @staticmethod
    def order_by(
        *cols: Union[
            ColumnOrName,
            List[ColumnOrName],
            Tuple[ColumnOrName, ...],
        ]
    ) -> "WindowSpec":
        """
        Returns a :class:`WindowSpec` object with order by clause.

        Args:
            cols: A column, as :class:`str`, :class:`~snowflake.snowpark.column.Column`
                or a list of those.
        """
        return Window._spec().order_by(*cols)

    @staticmethod
    def rows_between(start: int, end: int) -> "WindowSpec":
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
    def range_between(start: int, end: int) -> "WindowSpec":
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
    def _spec() -> "WindowSpec":
        return WindowSpec([], [], SPUnspecifiedFrame())

    orderBy = order_by
    partitionBy = partition_by
    rangeBetween = range_between
    rowsBetween = rows_between


class WindowSpec:
    """Represents a window frame clause."""

    def __init__(
        self,
        partition_spec: List[SPExpression],
        order_spec: List[SPSortOrder],
        frame: SPWindowFrame,
    ):
        self.partition_spec = partition_spec
        self.order_spec = order_spec
        self.frame = frame

    def partition_by(
        self,
        *cols: Union[
            ColumnOrName,
            List[ColumnOrName],
            Tuple[ColumnOrName, ...],
        ]
    ) -> "WindowSpec":
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
        self,
        *cols: Union[
            ColumnOrName,
            List[ColumnOrName],
            Tuple[ColumnOrName, ...],
        ]
    ) -> "WindowSpec":
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
                    SPSortOrder(
                        snowflake.snowpark.column.Column(e).expression, SPAscending()
                    )
                )
            elif isinstance(e, snowflake.snowpark.column.Column):
                if isinstance(e.expression, SPSortOrder):
                    order_spec.append(e.expression)
                elif isinstance(e.expression, SPExpression):
                    order_spec.append(SPSortOrder(e.expression, SPAscending()))

        return WindowSpec(self.partition_spec, order_spec, self.frame)

    def rows_between(self, start: int, end: int) -> "WindowSpec":
        """
        Returns a new :class:`WindowSpec` object with the new row frame clause.

        See Also:
            - :func:`Window.rows_between`
        """
        boundary_start, boundary_end = self._convert_boundary_to_expr(start, end)
        return WindowSpec(
            self.partition_spec,
            self.order_spec,
            SPSpecifiedWindowFrame(SPRowFrame(), boundary_start, boundary_end),
        )

    def range_between(self, start: int, end: int) -> "WindowSpec":
        """
        Returns a new :class:`WindowSpec` object with the new range frame clause.

        See Also:
            - :func:`Window.range_between`
        """
        boundary_start, boundary_end = self._convert_boundary_to_expr(start, end)
        return WindowSpec(
            self.partition_spec,
            self.order_spec,
            SPSpecifiedWindowFrame(SPRangeFrame(), boundary_start, boundary_end),
        )

    def _convert_boundary_to_expr(
        self, start: int, end: int
    ) -> Tuple[SPExpression, SPExpression]:
        if start == 0:
            boundary_start = SPCurrentRow()
        elif start <= Window.UNBOUNDED_PRECEDING:
            boundary_start = SPUnboundedPreceding()
        else:
            boundary_start = SPLiteral(start)

        if end == 0:
            boundary_end = SPCurrentRow()
        elif end >= Window.UNBOUNDED_FOLLOWING:
            boundary_end = SPUnboundedFollowing()
        else:
            boundary_end = SPLiteral(end)

        return boundary_start, boundary_end

    def _with_aggregate(
        self, aggregate: SPExpression
    ) -> "snowflake.snowpark.column.Column":
        spec = SPWindowSpecDefinition(self.partition_spec, self.order_spec, self.frame)
        return snowflake.snowpark.column.Column(SPWindowExpression(aggregate, spec))

    orderBy = order_by
    partitionBy = partition_by
    rangeBetween = range_between
    rowsBetween = rows_between
