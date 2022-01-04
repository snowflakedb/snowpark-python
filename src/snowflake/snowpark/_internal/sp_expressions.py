#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
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

from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.sp_types.sp_data_types import (
    DataType,
    DecimalType,
    DoubleType,
    IntegralType,
    LongType,
)
from snowflake.snowpark._internal.sp_types.types_package import (
    _VALID_PYTHON_TYPES_FOR_LITERAL_VALUE,
    _VALID_SNOWPARK_TYPES_FOR_LITERAL_VALUE,
    _infer_type,
)


class Expression:
    """Consider removing attributes, and adding properties and methods.
    A subclass of Expression may have no child, one child, or multiple children.
    But the constructor accepts a single child. This might be refactored in the future.
    """

    # https://github.com/apache/spark/blob/1dd0ca23f64acfc7a3dc697e19627a1b74012a2d/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Expression.scala#L86
    def __init__(self, child: Optional["Expression"] = None):
        """
        Subclasses will override these attributes
        """
        self.child = child
        self.nullable = True
        self.children = [child] if child else None
        self.datatype: Optional[DataType] = None

    def pretty_name(self) -> str:
        """Returns a user-facing string representation of this expression's name.
        This should usually match the name of the function in SQL."""
        return self.__class__.__name__.upper()

    def sql(self) -> str:
        """TODO: analyzer_object.py's analyze() method doesn't use this method to create sql statement.
        The only place that uses Expression.sql() to generate sql statement
        is relational_grouped_dataframe.py's __toDF(). Re-consider whether we need to make the sql generation
        consistent among all different Expressions.
        """
        children_sql = (
            ", ".join([x.sql() for x in self.children]) if self.children else ""
        )
        return f"{self.pretty_name()}({children_sql})"

    def __repr__(self) -> str:
        return self.pretty_name()


class NamedExpression:
    """In scala, `NamedExpression` is a trait that extends `Expression`. Python doesn't have trait.
    A Mixin is used to simulate it. They're still different though.
    """

    @property
    def name(self):
        return getattr(self, "_name", None)

    @property
    def expr_id(self):
        if not hasattr(self, "_expr_id"):
            self._expr_id = uuid.uuid4()
        return self._expr_id


class LeafExpression(Expression):
    pass


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


class Star(LeafExpression, NamedExpression):
    def __init__(self, expressions: List[NamedExpression]):
        super().__init__()
        self._name = "Star"
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


# ##### AggregateModes
class AggregateMode:
    pass


class Complete(AggregateMode):
    pass


# TODO complete AggregateModes
######


class AggregateExpression(Expression):
    # https://github.com/apache/spark/blob/1dd0ca23f64acfc7a3dc697e19627a1b74012a2d/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/interfaces.scala#L99
    def __init__(
        self,
        aggregate_function,
        mode: AggregateMode,
        is_distinct: bool,
        filter: Expression,
        result_id: uuid.UUID = None,
    ):
        super().__init__(aggregate_function)
        self.aggregate_function = aggregate_function
        self.mode = mode
        self.is_distinct = is_distinct
        self.filter = filter
        self.result_id = result_id if result_id else uuid.uuid4()

        # Original: self.children = aggregate_function +: filter.toSeq
        children = [aggregate_function]
        if filter:
            children.append(filter)
        self.children = children

        self.datatype = aggregate_function.datatype
        # TODO nullable needed?
        self.nullable = aggregate_function.nullable

    @property
    def name(self):
        return self.aggregate_function.name

    def __str__(self):
        return f"{self.aggregate_function.name}({', '.join((str(c) for c in self.aggregate_function.children))})"

    def sql(self) -> str:
        return self.children[0].sql() if self.children else ""


class TypedAggregateExpression(AggregateExpression):
    pass


class AggregateFunction(Expression):
    # https://github.com/apache/spark/blob/1dd0ca23f64acfc7a3dc697e19627a1b74012a2d/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/interfaces.scala#L207
    name: str

    def to_aggregate_expression(
        self, is_distinct=False, filter=None
    ) -> AggregateExpression:
        return AggregateExpression(self, Complete(), is_distinct, filter)


class DeclarativeAggregate(AggregateFunction):
    # https://github.com/apache/spark/blob/1dd0ca23f64acfc7a3dc697e19627a1b74012a2d/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/interfaces.scala#L394
    def __init__(self, child: Expression):
        super(DeclarativeAggregate, self).__init__(child)


class Count(DeclarativeAggregate):
    # https://github.com/apache/spark/blob/9af338cd685bce26abbc2dd4d077bde5068157b1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Count.scala
    name = "COUNT"

    def __init__(self, child: Expression):
        super().__init__(child)
        self.datatype = LongType()


class Max(DeclarativeAggregate):
    # https://github.com/apache/spark/blob/9af338cd685bce26abbc2dd4d077bde5068157b1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Max.scala
    name = "MAX"

    def __init__(self, child: Expression):
        super().__init__(child)
        self.datatype = child.datatype


class Min(DeclarativeAggregate):
    # https://github.com/apache/spark/blob/9af338cd685bce26abbc2dd4d077bde5068157b1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Max.scala
    name = "MIN"

    def __init__(self, child: Expression):
        super().__init__(child)
        self.datatype = child.datatype


class Avg(DeclarativeAggregate):
    # https://github.com/apache/spark/blob/9af338cd685bce26abbc2dd4d077bde5068157b1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala
    name = "AVG"

    def __init__(self, child: Expression):
        super().__init__(child)
        self.datatype = self.__get_type(child)

    @staticmethod
    def __get_type(child: Expression) -> DataType:
        if type(child) == DecimalType:
            return DecimalType(DecimalType.MAX_PRECISION, DecimalType.MAX_SCALE)
        else:
            return DoubleType()


class Sum(DeclarativeAggregate):
    name = "SUM"

    def __init__(self, child: Expression):
        super().__init__(child)
        self.datatype = self.__get_type(child)

    @staticmethod
    def __get_type(child: Expression) -> DataType:
        if type(child) == DecimalType:
            return DecimalType(DecimalType.MAX_PRECISION, DecimalType.MAX_SCALE)
        elif type(child) == IntegralType:
            return LongType()
        else:
            return DoubleType()


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


# Named Expressions
class Alias(UnaryExpression, NamedExpression):
    def __init__(self, child, name, expr_id=None):
        super().__init__(child)
        self._name = name
        self._expr_id = expr_id if expr_id else uuid.uuid4()


class Attribute(LeafExpression, NamedExpression):
    def __init__(self, name):
        super().__init__()
        self._name = name

    @classmethod
    def with_name(cls, name):
        return Attribute(name)

    def sql(self) -> str:
        return self.name


class UnresolvedAlias(UnaryExpression, NamedExpression):
    def __init__(self, child, alias_func):
        super().__init__(child=child)
        self.alias_func = alias_func
        self._name = alias_func


# Leaf Expressions
class Literal(LeafExpression):
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
    def __init__(self, child: Expression, to: DataType):
        super().__init__(child)
        self.child = child
        self.to = to
        self.datatype = child.datatype
        self.nullable = child.nullable


# Attributes
class AttributeReference(Attribute):
    def __init__(self, name: str, datatype, nullable: bool):
        super().__init__(name)
        self.datatype = datatype
        self.nullable = nullable

    def with_name(self, new_name):
        if self.name == new_name:
            return self
        else:
            return AttributeReference(self.name, self.datatype, self.nullable)

    def __str__(self):
        return self.name

    def __repr__(self):
        return self.__str__()


class UnresolvedAttribute(Attribute):
    def __init__(self, name_parts):
        super().__init__(name_parts if type(name_parts) == str else name_parts[-1])
        self.name_parts = [name_parts] if type(name_parts) == str else name_parts

    # @property
    # def expr_id(self) -> None:
    #    raise Exception("UnresolvedException - expr_id")

    @classmethod
    def quoted(cls, name):
        # TODO revisit
        return cls(name)

    @classmethod
    def quoted_string(cls, name):
        # TODO revisit
        return cls(UnresolvedAttribute.parse_attribute_name(name))

    @staticmethod
    def parse_attribute_name(name):
        # TODO
        return name

    def __str__(self):
        return ".".join(self.name_parts)

    def __eq__(self, other):
        return type(other) is type(self) and str(self) == str(other)

    def __hash__(self):
        return hash(str(self))

    def sql(self):
        return self.__str__()


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

    def pretty_name(self) -> str:
        return self.name

    def sql(self) -> str:
        distinct = "DISTINCT " if self.is_distinct else ""
        return f"{self.pretty_name()}({distinct}{', '.join([c.sql() for c in self.children])})"


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
