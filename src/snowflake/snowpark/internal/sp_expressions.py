#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Apache Software Foundation as part of the
# Apache Spark project, under the Apache License, Version 2.0.
#
#  File containing the Expression definitions for ASTs (Spark).
import uuid
from typing import List, Optional, Tuple

from snowflake.snowpark.internal.analyzer.datatype_mapper import DataTypeMapper
from snowflake.snowpark.types.sp_data_types import (
    DataType,
    DecimalType,
    DoubleType,
    IntegralType,
    LongType,
    NullType,
)
from snowflake.snowpark.types.types_package import _infer_type


class Expression:
    # https://github.com/apache/spark/blob/1dd0ca23f64acfc7a3dc697e19627a1b74012a2d/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/Expression.scala#L86
    nullable: bool = None
    datatype: DataType = None

    def pretty_name(self) -> str:
        """Returns a user-facing string representation of this expression's name.
        This should usually match the name of the function in SQL."""
        return self.__class__.__name__.upper()

    # TODO: SNOW-369125 set expression-related string/names
    def __repr__(self) -> str:
        return self.pretty_name()


class NamedExpression(Expression):
    name: str
    expr_id: uuid.UUID

    def __init__(self, name):
        self.name = name
        self.expr_id = uuid.uuid4()


class LeafExpression(Expression):
    pass


class Star(LeafExpression, NamedExpression):
    def __init__(self, expressions: List[NamedExpression]):
        super().__init__(name="Star")
        self.expressions = expressions


class UnaryExpression(Expression):
    child: Expression
    children: List["Expression"]

    def __init__(self, child: Expression):
        self.child = child
        self.children = [child]


class BinaryExpression(Expression):
    left: Expression
    right: Expression
    sql_operator: str

    def __init__(self, left: Expression, right: Expression):
        self.left = left
        self.right = right
        self.children = [self.left, self.right]
        self.nullable = self.left.nullable or self.right.nullable

    def __repr__(self):
        return "{} {} {}".format(self.left, self.sql_operator, self.right)


class UnresolvedFunction(Expression):
    def __init__(self, name, arguments, is_distinct=False):
        super().__init__()
        self.name = name
        self.children = arguments
        self.is_distinct = is_distinct

    def pretty_name(self) -> str:
        return self.name.strip('"')

    def to_string(self) -> str:
        return f"{self.name}({', '.join((c.to_string() for c in self.children))})"

    def __repr__(self):
        return self.to_string()


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
        super().__init__()
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
        # self.nullable = aggregate_function.nullable

    @property
    def name(self):
        return self.aggregate_function.name

    def to_string(self):
        return f"{self.aggregate_function.name}({', '.join((c.to_string() for c in self.aggregate_function.children))})"


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
    pass


class Count(DeclarativeAggregate):
    # https://github.com/apache/spark/blob/9af338cd685bce26abbc2dd4d077bde5068157b1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Count.scala
    name = "COUNT"

    def __init__(self, child: Expression):
        super().__init__()
        self.child = child
        self.children = [child]
        self.datatype = LongType()


class Max(DeclarativeAggregate):
    # https://github.com/apache/spark/blob/9af338cd685bce26abbc2dd4d077bde5068157b1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Max.scala
    name = "MAX"

    def __init__(self, child: Expression):
        super().__init__()
        self.child = child
        self.children = [child]
        self.datatype = child.datatype


class Min(DeclarativeAggregate):
    # https://github.com/apache/spark/blob/9af338cd685bce26abbc2dd4d077bde5068157b1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Max.scala
    name = "MIN"

    def __init__(self, child: Expression):
        super().__init__()
        self.child = child
        self.children = [child]
        self.datatype = child.datatype


class Avg(DeclarativeAggregate):
    # https://github.com/apache/spark/blob/9af338cd685bce26abbc2dd4d077bde5068157b1/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/aggregate/Average.scala
    name = "AVG"

    def __init__(self, child: Expression):
        super().__init__()
        self.child = child
        self.children = [child]
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
        super().__init__()
        self.child = child
        self.children = [child]
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
    def __init__(self, grouping_set_indexes, children=[]):
        super().__init__()
        self.grouping_set_indexes = grouping_set_indexes
        self.children = children


class Rollup(BaseGroupingSets):
    def __init__(self, grouping_set_indexes, children=[]):
        super().__init__()
        self.grouping_set_indexes = grouping_set_indexes
        self.children = children


# Named Expressions
class Alias(UnaryExpression, NamedExpression):
    def __init__(self, child, name, expr_id=None):
        UnaryExpression.__init__(self, child)
        NamedExpression.__init__(self, name)
        self.expr_id = expr_id if expr_id else uuid.uuid4()


class Attribute(LeafExpression, NamedExpression):
    def __init__(self, name):
        super().__init__(name=name)

    @classmethod
    def with_name(cls, name):
        return Attribute(name)


class UnresolvedAlias(UnaryExpression, NamedExpression):
    def __init__(self, child, alias_func):
        super().__init__(child=child)
        self.alias_func = alias_func


# Leaf Expressions
class Literal(LeafExpression):
    def __init__(self, value, datatype):
        super().__init__()
        self.value = value
        self.datatype = datatype

    @classmethod
    def create(cls, value):
        return cls(value, _infer_type(value))

    def to_string(self):
        return DataTypeMapper.to_sql_without_cast(self.value, self.datatype)

    def sql(self):
        return self.to_string()

    def __repr__(self):
        return self.to_string()


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

    def to_string(self):
        return self.name

    def __repr__(self):
        return self.to_string()


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

    def to_string(self):
        return ".".join(self.name_parts)

    def sql(self):
        return self.to_string()


class PrettyAttribute(Attribute):
    def __init__(self, name: str, datatype: Optional[DataType]):
        super().__init__(name=name)
        self.datatype = datatype

    @classmethod
    def this(cls, attribute: Attribute):
        if type(attribute) == AttributeReference:
            tpe = attribute.datatype
        elif type(attribute) == PrettyAttribute:
            tpe = attribute.datatype
        else:
            tpe = NullType()

        return cls(attribute.name, tpe)

    def to_string(self) -> str:
        return self.name

    def sql(self) -> str:
        return self.name


class Like(Expression):
    def __init__(self, expr: Expression, pattern: Expression):
        self.expr = expr
        self.pattern = pattern


class RegExp(Expression):
    def __init__(self, expr: Expression, pattern: Expression):
        self.expr = expr
        self.pattern = pattern


class Collate(Expression):
    def __init__(self, expr: Expression, collation_spec: str):
        self.expr = expr
        self.collation_spec = collation_spec


class SubfieldString(Expression):
    def __init__(self, expr: Expression, field: str):
        self.expr = expr
        self.field = field


class SubfieldInt(Expression):
    def __init__(self, expr: Expression, field: int):
        self.expr = expr
        self.field = field


class TableFunctionExpression(Expression):
    def __init__(self):
        self.datatype = None


class FlattenFunction(TableFunctionExpression):
    def __init__(
        self, input: Expression, path: str, outer: bool, recursive: bool, mode: str
    ):
        super.__init__()
        self.input = input
        self.path = path
        self.outer = outer
        self.recursive = recursive
        self.mode = mode


class TableFunction(TableFunctionExpression):
    def __init__(self, func_name: str, args: List[Expression]):
        super.__init__()
        self.func_name = func_name
        self.args = args


class NamedArgumentsTableFunction(TableFunctionExpression):
    def __init__(self, func_name: str, args: List[Expression]):
        super.__init__()
        self.func_name = func_name
        self.args = args


class GroupingSets(Expression):
    def __init__(self, args: List[Expression]):
        self.args = args
        self.datatype = None


class WithinGroup(Expression):
    def __init__(self, expr: Expression, order_by_cols: List[Expression]):
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
        self.udf_name = udf_name
        self.children = children
        self.datatype = datatype
        self.nullable = nullable
