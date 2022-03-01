#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Apache Software Foundation as part of the
# Apache Spark project, under the Apache License, Version 2.0.
#
#  File containing the Expression definitions for ASTs (Spark).
import uuid
from typing import TYPE_CHECKING, Any, Dict, Iterable, List, Optional, Tuple

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan

import snowflake.snowpark._internal.analyzer.analyzer_package as analyzer_package
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.type_utils import (
    _VALID_PYTHON_TYPES_FOR_LITERAL_VALUE,
    _VALID_SNOWPARK_TYPES_FOR_LITERAL_VALUE,
    _infer_type,
)
from snowflake.snowpark.types import DataType


class Expression:
    """Consider removing attributes, and adding properties and methods.
    A subclass of Expression may have no child, one child, or multiple children.
    But the constructor accepts a single child. This might be refactored in the future.
    """

    def __init__(self, child: Optional["Expression"] = None):
        """
        Subclasses will override these attributes
        """
        self.child = child
        self.nullable = True
        self.children = [child] if child else None
        self.datatype: Optional[DataType] = None

    @property
    def pretty_name(self) -> str:
        """Returns a user-facing string representation of this expression's name.
        This should usually match the name of the function in SQL."""
        return self.__class__.__name__.upper()

    @property
    def sql(self) -> str:
        """TODO: analyzer_object.py's analyze() method doesn't use this method to create sql statement.
        The only place that uses Expression.sql() to generate sql statement
        is relational_grouped_dataframe.py's __toDF(). Re-consider whether we need to make the sql generation
        consistent among all different Expressions.
        """
        children_sql = (
            ", ".join([x.sql for x in self.children]) if self.children else ""
        )
        return f"{self.pretty_name}({children_sql})"

    def __repr__(self) -> str:
        return self.pretty_name


class NamedExpression(Expression):
    def __init__(self, child: Optional["Expression"] = None):
        super().__init__(child)
        self.name = None
        self.expr_id = uuid.uuid4()


class ScalarSubquery(Expression):
    def __init__(self, plan: "SnowflakePlan"):
        super().__init__()
        self.plan = plan


class MultipleExpression(Expression):
    def __init__(self, expressions: List[Expression]):
        super().__init__()
        self.expressions = expressions


class InExpression(Expression):
    def __init__(self, columns: Expression, values: List[Expression]):
        super().__init__()
        self.columns = columns
        self.values = values


class Star(Expression):
    def __init__(self, expressions: List[NamedExpression]):
        super().__init__()
        self.expressions = expressions


class UnaryExpression(Expression):
    def __init__(self, child: Expression):
        super().__init__(child)
        self.children = [child]
        self.datatype = (
            self.child.datatype if self.child and hasattr(self.child, "child") else None
        )

    def children(self) -> List[Expression]:
        return self.children


class BinaryExpression(Expression):
    sql_operator: str

    def __init__(self, left: Expression, right: Expression):
        super().__init__()
        self.left = left
        self.right = right
        self.children = [self.left, self.right]
        self.nullable = self.left.nullable or self.right.nullable

    def __repr__(self):
        return "{} {} {}".format(self.left, self.sql_operator, self.right)


# Grouping sets
class BaseGroupingSets(Expression):
    # https://github.com/apache/spark/blob/1dd0ca23f64acfc7a3dc697e19627a1b74012a2d/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/grouping.scala#L30
    pass


class Cube(BaseGroupingSets):
    def __init__(self, grouping_set_indexes):
        super().__init__()
        self.grouping_set_indexes = grouping_set_indexes
        self.children = self.grouping_set_indexes


class Rollup(BaseGroupingSets):
    def __init__(self, grouping_set_indexes):
        super().__init__()
        self.grouping_set_indexes = grouping_set_indexes
        self.children = self.grouping_set_indexes


class Alias(UnaryExpression, NamedExpression):
    def __init__(self, child: Expression, name: str):
        super().__init__(child)
        self.name = name


class Attribute(NamedExpression):
    def __init__(self, name: str, datatype: DataType, nullable: bool = True):
        super().__init__()
        self.name = name
        self.datatype = datatype
        self.nullable = nullable

    def with_name(self, new_name: str) -> "Attribute":
        if self.name == new_name:
            return self
        else:
            return Attribute(
                analyzer_package.AnalyzerPackage.quote_name(new_name),
                self.datatype,
                self.nullable,
            )

    @property
    def sql(self) -> str:
        return self.name

    def __str__(self):
        return self.name


class UnresolvedAttribute(NamedExpression):
    def __init__(self, name: str):
        super().__init__()
        self.name = name

    @property
    def sql(self) -> str:
        return self.name

    def __str__(self):
        return self.name

    def __eq__(self, other):
        return type(other) is type(self) and other.name == self.name

    def __hash__(self):
        return hash(self.name)


class UnresolvedAlias(UnaryExpression, NamedExpression):
    def __init__(self, child: Expression):
        super().__init__(child=child)
        self.name = child.sql


class Literal(Expression):
    def __init__(self, value: Any, datatype: Optional[DataType] = None):
        super().__init__()

        # check value
        if not isinstance(value, _VALID_PYTHON_TYPES_FOR_LITERAL_VALUE):
            raise SnowparkClientExceptionMessages.PLAN_CANNOT_CREATE_LITERAL(
                type(value)
            )
        self.value = value

        # check datatype
        if datatype:
            if not isinstance(datatype, _VALID_SNOWPARK_TYPES_FOR_LITERAL_VALUE):
                raise SnowparkClientExceptionMessages.PLAN_CANNOT_CREATE_LITERAL(
                    str(datatype)
                )
            self.datatype = datatype
        else:
            self.datatype = _infer_type(value)


class BinaryArithmeticExpression(BinaryExpression):
    pass


class EqualTo(BinaryArithmeticExpression):
    sql_operator = "="


class NotEqualTo(BinaryArithmeticExpression):
    sql_operator = "!="


class GreaterThan(BinaryArithmeticExpression):
    sql_operator = ">"


class LessThan(BinaryArithmeticExpression):
    sql_operator = "<"


class GreaterThanOrEqual(BinaryArithmeticExpression):
    sql_operator = ">="


class LessThanOrEqual(BinaryArithmeticExpression):
    sql_operator = "<="


class EqualNullSafe(BinaryExpression):
    sql_operator = "EQUAL_NULL"


# also inherits from Predicate, omitted
class And(BinaryArithmeticExpression):
    sql_operator = "AND"


class Or(BinaryArithmeticExpression):
    sql_operator = "OR"


class Add(BinaryArithmeticExpression):
    sql_operator = "+"


class Subtract(BinaryArithmeticExpression):
    sql_operator = "-"


class Multiply(BinaryArithmeticExpression):
    sql_operator = "*"


class Divide(BinaryArithmeticExpression):
    sql_operator = "/"


class Remainder(BinaryArithmeticExpression):
    sql_operator = "%"


class Pow(BinaryExpression):
    sql_operator = "POWER"


class ArraysOverlap(BinaryExpression):
    sql_operator = "ARRAYS_OVERLAP"


class ArrayIntersect(BinaryExpression):
    sql_operator = "ARRAY_INTERSECTION"


class BitwiseAnd(BinaryExpression):
    sql_operator = "BITAND"


class BitwiseOr(BinaryExpression):
    sql_operator = "BITOR"


class BitwiseXor(BinaryExpression):
    sql_operator = "BITXOR"


class UnaryMinus(UnaryExpression):
    def __init__(self, child: Expression):
        super().__init__(child)
        self.datatype = child.datatype
        self.nullable = child.nullable

    def __repr__(self):
        return "-{}".format(self.child)


class Not(UnaryExpression):
    def __repr__(self):
        return "~{}".format(self.child)


class IsNaN(UnaryExpression):
    def __init__(self, child: Expression):
        super().__init__(child)
        self.nullable = False


class IsNull(UnaryExpression):
    def __init__(self, child: Expression):
        super().__init__(child)
        self.nullable = False


class IsNotNull(UnaryExpression):
    def __init__(self, child: Expression):
        super().__init__(child)
        self.nullable = False


class Cast(UnaryExpression):
    def __init__(self, child: Expression, to: DataType, try_: bool = False):
        super().__init__(child)
        self.child = child
        self.to = to
        self.try_ = try_
        self.datatype = child.datatype
        self.nullable = child.nullable


class Like(Expression):
    def __init__(self, expr: Expression, pattern: Expression):
        super().__init__(expr)
        self.expr = expr
        self.pattern = pattern


class RegExp(Expression):
    def __init__(self, expr: Expression, pattern: Expression):
        super().__init__(expr)
        self.expr = expr
        self.pattern = pattern


class Collate(Expression):
    def __init__(self, expr: Expression, collation_spec: str):
        super().__init__(expr)
        self.expr = expr
        self.collation_spec = collation_spec


class SubfieldString(Expression):
    def __init__(self, expr: Expression, field: str):
        super().__init__(expr)
        self.expr = expr
        self.field = field


class SubfieldInt(Expression):
    def __init__(self, expr: Expression, field: int):
        super().__init__(expr)
        self.expr = expr
        self.field = field


class FunctionExpression(Expression):
    def __init__(self, name: str, arguments: List[Expression], is_distinct: bool):
        super().__init__()
        self.name = name
        self.children = arguments
        self.is_distinct = is_distinct

    @property
    def pretty_name(self) -> str:
        return self.name

    @property
    def sql(self) -> str:
        distinct = "DISTINCT " if self.is_distinct else ""
        return (
            f"{self.pretty_name}({distinct}{', '.join([c.sql for c in self.children])})"
        )


class TableFunctionExpression(Expression):
    def __init__(self):
        super().__init__()
        self.datatype = None


class FlattenFunction(TableFunctionExpression):
    def __init__(
        self, input: Expression, path: str, outer: bool, recursive: bool, mode: str
    ):
        super().__init__()
        self.input = input
        self.path = path
        self.outer = outer
        self.recursive = recursive
        self.mode = mode


class TableFunction(TableFunctionExpression):
    def __init__(self, func_name: str, args: Iterable[Expression]):
        super().__init__()
        self.func_name = func_name
        self.args = args


class NamedArgumentsTableFunction(TableFunctionExpression):
    def __init__(self, func_name: str, args: Dict[str, Expression]):
        super().__init__()
        self.func_name = func_name
        self.args = args


class GroupingSetsExpression(Expression):
    def __init__(self, args: List[List[Expression]]):
        super().__init__()
        self.args = args
        self.datatype = None


class WithinGroup(Expression):
    def __init__(self, expr: Expression, order_by_cols: List[Expression]):
        super().__init__(expr)
        self.expr = expr
        self.order_by_cols = order_by_cols
        self.datatype = expr.datatype


# Ordering
class NullOrdering:
    sql: str


class NullsFirst(NullOrdering):
    sql = "NULLS FIRST"


class NullsLast(NullOrdering):
    sql = "NULLS LAST"


class SortDirection:
    sql: str
    default_null_ordering: NullOrdering


class Ascending(SortDirection):
    sql = "ASC"
    default_null_ordering = NullsFirst


class Descending(SortDirection):
    sql = "DESC"
    default_null_ordering = NullsLast


class SortOrder(UnaryExpression):
    def __init__(
        self,
        child: Expression,
        direction: SortDirection,
        null_ordering: NullOrdering = None,
    ):
        super().__init__(child)
        self.direction = direction
        self.null_ordering = (
            null_ordering if null_ordering else direction.default_null_ordering
        )
        self.datatype = child.datatype
        self.nullable = child.nullable


# CaseWhen
class CaseWhen(Expression):
    def __init__(
        self,
        branches: List[Tuple[Expression, Expression]],
        else_value: Optional[Expression] = None,
    ):
        super().__init__()
        self.branches = branches
        self.else_value = else_value
        # nullable if any value is nullable
        self.nullable = any([value for _, value in branches]) or (
            else_value is not None and else_value.nullable
        )


class SnowflakeUDF(Expression):
    def __init__(
        self,
        udf_name: str,
        children: List[Expression],
        datatype: DataType,
        nullable: bool = True,
    ):
        super().__init__()
        self.udf_name = udf_name
        self.children = children
        self.datatype = datatype
        self.nullable = nullable


# Window Expressions
class SpecialFrameBoundary(Expression):
    def __init__(self):
        super().__init__()

    def sql(self) -> str:
        raise NotImplementedError


class UnboundedPreceding(SpecialFrameBoundary):
    def sql(self) -> str:
        return "UNBOUNDED PRECEDING"


class UnboundedFollowing(SpecialFrameBoundary):
    def sql(self) -> str:
        return "UNBOUNDED FOLLOWING"


class CurrentRow(SpecialFrameBoundary):
    def sql(self) -> str:
        return "CURRENT ROW"


class FrameType:
    sql = None


class RowFrame(FrameType):
    sql = "ROWS"


class RangeFrame(FrameType):
    sql = "RANGE"


class WindowFrame(Expression):
    def __init__(self):
        super().__init__()


class UnspecifiedFrame(WindowFrame):
    pass


class SpecifiedWindowFrame(WindowFrame):
    def __init__(self, frame_type: FrameType, lower: Expression, upper: Expression):
        super().__init__()
        self.frame_type = frame_type
        self.lower = lower
        self.upper = upper


class WindowSpecDefinition(Expression):
    def __init__(
        self,
        partition_spec: List[Expression],
        order_spec: List[SortOrder],
        frame_spec: WindowFrame,
    ):
        super().__init__()
        self.partition_spec = partition_spec
        self.order_spec = order_spec
        self.frame_spec = frame_spec


class WindowExpression(Expression):
    def __init__(self, window_function: Expression, window_spec: WindowSpecDefinition):
        super().__init__()
        self.window_function = window_function
        self.window_spec = window_spec


class RankRelatedFunctionExpression(Expression):
    sql: str

    def __init__(
        self, expr: Expression, offset: int, default: Expression, ignore_nulls: bool
    ):
        super().__init__()
        self.expr = expr
        self.offset = offset
        self.default = default
        self.ignore_nulls = ignore_nulls


class Lag(RankRelatedFunctionExpression):
    sql = "LAG"


class Lead(RankRelatedFunctionExpression):
    sql = "LEAD"


class MergeExpression(Expression):
    def __init__(self, condition: Optional[Expression]):
        super().__init__()
        self.condition = condition


class UpdateMergeExpression(MergeExpression):
    def __init__(
        self, condition: Optional[Expression], assignments: Dict[Expression, Expression]
    ):
        super().__init__(condition)
        self.assignments = assignments


class DeleteMergeExpression(MergeExpression):
    pass


class InsertMergeExpression(MergeExpression):
    def __init__(
        self,
        condition: Optional[Expression],
        keys: List[Expression],
        values: List[Expression],
    ):
        super().__init__(condition)
        self.keys = keys
        self.values = values


class ListAgg(Expression):
    def __init__(self, col: Expression, delimiter: str, is_distinct: bool):
        super().__init__()
        self.col = col
        self.delimiter = delimiter
        self.is_distinct = is_distinct
