#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""Window frames in Snowpark."""
import sys
from enum import IntEnum
from typing import List, Optional, Tuple, Union

import snowflake.snowpark
import snowflake.snowpark._internal.proto.generated.ast_pb2 as proto
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
from snowflake.snowpark._internal.ast.utils import (
    build_expr_from_snowpark_column_or_python_val,
    with_src_position,
)
from snowflake.snowpark._internal.type_utils import ColumnOrName
from snowflake.snowpark._internal.utils import parse_positional_args_to_list, publicapi

# Python 3.8 needs to use typing.Iterable because collections.abc.Iterable is not subscriptable
# Python 3.9 can use both
# Python 3.10 needs to use collections.abc.Iterable because typing.Iterable is removed
if sys.version_info <= (3, 9):
    from typing import Iterable
else:
    from collections.abc import Iterable


def _convert_boundary_to_expr(
    start: Union[int, "snowflake.snowpark.Column"],
    end: Union[int, "snowflake.snowpark.Column"],
) -> Tuple[Expression, Expression]:
    if isinstance(start, int):
        if start == 0:
            boundary_start = CurrentRow()
        elif start <= Window.UNBOUNDED_PRECEDING:
            boundary_start = UnboundedPreceding()
        else:
            boundary_start = Literal(int(start))
    elif isinstance(start, snowflake.snowpark.Column):
        boundary_start = start._expression
    else:
        raise ValueError("start must be an integer or a Column")

    if isinstance(end, int):
        if end == 0:
            boundary_end = CurrentRow()
        elif end >= Window.UNBOUNDED_FOLLOWING:
            boundary_end = UnboundedFollowing()
        else:
            boundary_end = Literal(int(end))
    elif isinstance(end, snowflake.snowpark.Column):
        boundary_end = end._expression
    else:
        raise ValueError("end must be an integer or a Column")

    return boundary_start, boundary_end


class WindowRelativePosition(IntEnum):
    UNBOUNDED_PRECEDING = -sys.maxsize
    UNBOUNDED_FOLLOWING = sys.maxsize
    CURRENT_ROW = 0


class Window:
    """
    Contains functions to form :class:`WindowSpec`. See
    `Snowflake Window functions <https://docs.snowflake.com/en/sql-reference/functions-analytic.html#window-functions>`_ for reference.

    Examples::

        >>> from snowflake.snowpark.functions import col, avg
        >>> window1 = Window.partition_by("value").order_by("key").rows_between(Window.CURRENT_ROW, 2)
        >>> window2 = Window.order_by(col("key").desc()).range_between(Window.UNBOUNDED_PRECEDING, Window.UNBOUNDED_FOLLOWING)
        >>> df = session.create_dataframe([(1, "1"), (2, "2"), (1, "3"), (2, "4")], schema=["key", "value"])
        >>> df.select(avg("value").over(window1).as_("window1"), avg("value").over(window2).as_("window2")).sort("window1").collect()
        [Row(WINDOW1=1.0, WINDOW2=2.5), Row(WINDOW1=2.0, WINDOW2=2.5), Row(WINDOW1=3.0, WINDOW2=2.5), Row(WINDOW1=4.0, WINDOW2=2.5)]
    """

    #: Returns a value representing unbounded preceding.
    UNBOUNDED_PRECEDING: int = WindowRelativePosition.UNBOUNDED_PRECEDING
    unboundedPreceding: int = UNBOUNDED_PRECEDING

    #: Returns a value representing unbounded following.
    UNBOUNDED_FOLLOWING: int = WindowRelativePosition.UNBOUNDED_FOLLOWING
    unboundedFollowing: int = UNBOUNDED_FOLLOWING

    #: Returns a value representing current row.
    CURRENT_ROW: int = WindowRelativePosition.CURRENT_ROW
    currentRow: int = CURRENT_ROW

    @staticmethod
    @publicapi
    def partition_by(
        *cols: Union[
            ColumnOrName,
            Iterable[ColumnOrName],
        ],
        _emit_ast: bool = True,
    ) -> "WindowSpec":
        """
        Returns a :class:`WindowSpec` object with partition by clause.

        Args:
            cols: A column, as :class:`str`, :class:`~snowflake.snowpark.column.Column`
                or a list of those.
        """
        return Window._spec().partition_by(*cols, _emit_ast=_emit_ast)

    @staticmethod
    @publicapi
    def order_by(
        *cols: Union[
            ColumnOrName,
            Iterable[ColumnOrName],
        ],
        _emit_ast: bool = True,
    ) -> "WindowSpec":
        """
        Returns a :class:`WindowSpec` object with order by clause.

        Args:
            cols: A column, as :class:`str`, :class:`~snowflake.snowpark.column.Column`
                or a list of those.
        """
        return Window._spec().order_by(*cols, _emit_ast=_emit_ast)

    @staticmethod
    @publicapi
    def rows_between(
        start: Union[int, WindowRelativePosition],
        end: Union[int, WindowRelativePosition],
        _emit_ast: bool = True,
    ) -> "WindowSpec":
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
            and :attr:`Window.CURRENT_ROW` to specify ``start`` and ``end``, instead of using
            integral values directly.
        """
        return Window._spec().rows_between(start, end, _emit_ast=_emit_ast)

    @staticmethod
    @publicapi
    def range_between(
        start: Union[int, "snowflake.snowpark.Column"],
        end: Union[int, "snowflake.snowpark.Column"],
        _emit_ast: bool = True,
    ) -> "WindowSpec":
        """
        Returns a :class:`WindowSpec` object with the range frame clause.
        ``start`` and ``end`` can be

            - an integer representing the relative position from the current row, or

            - :attr:`Window.UNBOUNDED_PRECEDING`, :attr:`Window.UNBOUNDED_FOLLOWING`
              and :attr:`Window.CURRENT_ROW`, which represent unbounded preceding,
              unbounded following and current row respectively, or

            - a :class:`~snowflake.snowpark.column.Column` object created by
              :func:`~snowflake.snowpark.functions.make_interval` to use
              `Interval constants <https://docs.snowflake.com/en/sql-reference/data-types-datetime#interval-constants>`_.
              Interval constants can only be used with this function when the order by column is TIMESTAMP or DATE type
              See more details how to use interval constants in
              `RANGE BETWEEN <https://docs.snowflake.com/sql-reference/functions-analytic#label-range-between-syntax-desc>`_
              clause. However, you cannot mix the numeric values and interval constants in the same range frame clause.

        Args:
            start: The relative position from the current row as a boundary start (inclusive).
                The frame is unbounded if this is :attr:`Window.UNBOUNDED_PRECEDING`, or any
                value less than or equal to -9223372036854775807 (``-sys.maxsize``).
            end: The relative position from the current row as a boundary end (inclusive).
                The frame is unbounded if this is :attr:`Window.UNBOUNDED_FOLLOWING`, or any
                value greater than or equal to 9223372036854775807 (``sys.maxsize``).

        Example 1
            Use numeric values to specify the range frame:

            >>> from snowflake.snowpark.functions import col, count, make_interval
            >>>
            >>> df = session.range(5)
            >>> window = Window.order_by("id").range_between(-1, Window.CURRENT_ROW)
            >>> df.select(col("id"), count("id").over(window).as_("count")).show()
            ------------------
            |"ID"  |"COUNT"  |
            ------------------
            |0     |1        |
            |1     |2        |
            |2     |2        |
            |3     |2        |
            |4     |2        |
            ------------------
            <BLANKLINE>

        Example 2
            Use interval constants to specify the range frame:

            >>> import datetime
            >>> from snowflake.snowpark.types import StructType, StructField, TimestampType, TimestampTimeZone
            >>>
            >>> df = session.create_dataframe(
            ...    [
            ...        datetime.datetime(2021, 12, 21, 9, 12, 56),
            ...        datetime.datetime(2021, 12, 21, 8, 12, 56),
            ...        datetime.datetime(2021, 12, 21, 7, 12, 56),
            ...        datetime.datetime(2021, 12, 21, 6, 12, 56),
            ...    ],
            ...    schema=StructType([StructField("a", TimestampType(TimestampTimeZone.NTZ))]),
            ... )
            >>> window = Window.order_by(col("a").desc()).range_between(-make_interval(hours=1), make_interval(hours=1))
            >>> df.select(col("a"), count("a").over(window).as_("count")).show()
            ---------------------------------
            |"A"                  |"COUNT"  |
            ---------------------------------
            |2021-12-21 09:12:56  |2        |
            |2021-12-21 08:12:56  |3        |
            |2021-12-21 07:12:56  |3        |
            |2021-12-21 06:12:56  |2        |
            ---------------------------------
            <BLANKLINE>

        """
        return Window._spec().range_between(start, end, _emit_ast=_emit_ast)

    @staticmethod
    def _spec() -> "WindowSpec":
        return WindowSpec([], [], UnspecifiedFrame())

    orderBy = order_by
    partitionBy = partition_by
    rangeBetween = range_between
    rowsBetween = rows_between


def _check_window_position_parameter(
    value: Union[int, WindowRelativePosition], name: str = ""
) -> None:
    from snowflake.snowpark import Column

    if not isinstance(value, int) and not isinstance(value, Column):
        raise ValueError(f"{name} must be an integer or a Column")


def _fill_window_spec_ast_with_relative_positions(
    ast: proto.WindowSpecExpr,
    start: Union[int, WindowRelativePosition],
    end: Union[int, WindowRelativePosition],
) -> None:
    """
    Helper function for AST generation to fill relative positions for window spec range-between, and rows-between. If value passed in for start/end is
    of type WindowRelativePosition encoding will preserve for e.g. Window.CURRENT_ROW the syntax.

    Args:
        ast: AST where to fill start/end.
        start: relative start position, integer or special enum value.
        end: relative end position, integer or special enum value.
    """

    if isinstance(start, WindowRelativePosition):
        if start == WindowRelativePosition.CURRENT_ROW:
            ast.start.window_relative_position__current_row = True
        if start == WindowRelativePosition.UNBOUNDED_FOLLOWING:
            ast.start.window_relative_position__unbounded_following = True
        if start == WindowRelativePosition.UNBOUNDED_PRECEDING:
            ast.start.window_relative_position__unbounded_preceding = True
    else:
        build_expr_from_snowpark_column_or_python_val(
            ast.start.window_relative_position__position.n, start
        )

    if isinstance(end, WindowRelativePosition):
        if end == WindowRelativePosition.CURRENT_ROW:
            ast.end.window_relative_position__current_row = True
        if end == WindowRelativePosition.UNBOUNDED_FOLLOWING:
            ast.end.window_relative_position__unbounded_following = True
        if end == WindowRelativePosition.UNBOUNDED_PRECEDING:
            ast.end.window_relative_position__unbounded_preceding = True
    else:
        build_expr_from_snowpark_column_or_python_val(
            ast.end.window_relative_position__position.n, end
        )


class WindowSpec:
    """Represents a window frame clause."""

    def __init__(
        self,
        partition_spec: List[Expression],
        order_spec: List[SortOrder],
        frame: WindowFrame,
        ast: Optional[proto.WindowSpecExpr] = None,
    ) -> None:
        self.partition_spec = partition_spec
        self.order_spec = order_spec
        self.frame = frame

        if ast is None:
            ast = proto.WindowSpecExpr()
            window_ast = with_src_position(ast.window_spec_empty)  # noqa: F841

        self._ast = ast

    @publicapi
    def partition_by(
        self,
        *cols: Union[
            ColumnOrName,
            Iterable[ColumnOrName],
        ],
        _emit_ast: bool = True,
    ) -> "WindowSpec":
        """
        Returns a new :class:`WindowSpec` object with the new partition by clause.

        See Also:
            - :func:`Window.partition_by`
        """
        exprs = parse_positional_args_to_list(*cols)
        partition_spec = [
            e._expression
            if isinstance(e, snowflake.snowpark.column.Column)
            else snowflake.snowpark.column.Column(e)._expression
            for e in exprs
        ]

        # AST.
        ast = None
        if _emit_ast:
            ast = proto.WindowSpecExpr()
            window_ast = with_src_position(ast.window_spec_partition_by)
            window_ast.wnd.CopyFrom(self._ast)
            for e in exprs:
                col_ast = window_ast.cols.add()
                col = (
                    e
                    if isinstance(e, snowflake.snowpark.column.Column)
                    else snowflake.snowpark.column.Column(e, _caller_name=None)
                )
                build_expr_from_snowpark_column_or_python_val(col_ast, col)

        return WindowSpec(partition_spec, self.order_spec, self.frame, ast=ast)

    @publicapi
    def order_by(
        self,
        *cols: Union[
            ColumnOrName,
            Iterable[ColumnOrName],
        ],
        _emit_ast: bool = True,
    ) -> "WindowSpec":
        """
        Returns a new :class:`WindowSpec` object with the new order by clause.

        See Also:
            - :func:`Window.order_by`
        """
        exprs = parse_positional_args_to_list(*cols)
        order_spec = []
        for e in exprs:
            if isinstance(e, str):
                order_spec.append(
                    SortOrder(
                        snowflake.snowpark.column.Column(e)._expression, Ascending()
                    )
                )
            elif isinstance(e, snowflake.snowpark.column.Column):
                if isinstance(e._expression, SortOrder):
                    order_spec.append(e._expression)
                elif isinstance(e._expression, Expression):
                    order_spec.append(SortOrder(e._expression, Ascending()))

        # AST.
        ast = None
        if _emit_ast:
            ast = proto.WindowSpecExpr()
            window_ast = with_src_position(ast.window_spec_order_by)
            window_ast.wnd.CopyFrom(self._ast)
            for e in exprs:
                col_ast = window_ast.cols.add()
                col = (
                    e
                    if isinstance(e, snowflake.snowpark.column.Column)
                    else snowflake.snowpark.column.Column(e, _caller_name=None)
                )
                build_expr_from_snowpark_column_or_python_val(col_ast, col)

        return WindowSpec(self.partition_spec, order_spec, self.frame, ast=ast)

    @publicapi
    def rows_between(
        self,
        start: Union[int, WindowRelativePosition],
        end: Union[int, WindowRelativePosition],
        _emit_ast: bool = True,
    ) -> "WindowSpec":
        """
        Returns a new :class:`WindowSpec` object with the new row frame clause.

        See Also:
            - :func:`Window.rows_between`
        """
        _check_window_position_parameter(start, "start")
        _check_window_position_parameter(end, "end")

        boundary_start, boundary_end = _convert_boundary_to_expr(start, end)

        # AST.
        ast = None
        if _emit_ast:
            ast = proto.WindowSpecExpr()
            window_ast = with_src_position(ast.window_spec_rows_between)
            _fill_window_spec_ast_with_relative_positions(window_ast, start, end)
            window_ast.wnd.CopyFrom(self._ast)

        return WindowSpec(
            self.partition_spec,
            self.order_spec,
            SpecifiedWindowFrame(RowFrame(), boundary_start, boundary_end),
            ast=ast,
        )

    @publicapi
    def range_between(
        self,
        start: Union[int, "snowflake.snowpark.Column"],
        end: Union[int, "snowflake.snowpark.Column"],
        _emit_ast: bool = True,
    ) -> "WindowSpec":
        """
        Returns a new :class:`WindowSpec` object with the new range frame clause.

        See Also:
            - :func:`Window.range_between`
        """

        _check_window_position_parameter(start, "start")
        _check_window_position_parameter(end, "end")

        # AST.
        ast = None
        if _emit_ast:
            ast = proto.WindowSpecExpr()
            window_ast = with_src_position(ast.window_spec_range_between)
            _fill_window_spec_ast_with_relative_positions(window_ast, start, end)
            window_ast.wnd.CopyFrom(self._ast)

        boundary_start, boundary_end = _convert_boundary_to_expr(start, end)

        return WindowSpec(
            self.partition_spec,
            self.order_spec,
            SpecifiedWindowFrame(RangeFrame(), boundary_start, boundary_end),
            ast=ast,
        )

    def _with_aggregate(
        self,
        aggregate: Expression,
        ast: Optional[proto.Expr] = None,
        _emit_ast: bool = True,
    ) -> "snowflake.snowpark.column.Column":
        spec = WindowSpecDefinition(self.partition_spec, self.order_spec, self.frame)
        return snowflake.snowpark.column.Column(
            WindowExpression(aggregate, spec), _ast=ast, _emit_ast=_emit_ast
        )

    orderBy = order_by
    partitionBy = partition_by
    rangeBetween = range_between
    rowsBetween = rows_between
