import uuid
from typing import Any, Dict, Iterable, List, Optional, Tuple

from snowflake.snowpark._internal.sp_types.sp_data_types import DataType as DataType

class Expression:
    child: Any
    nullable: bool
    children: Any
    datatype: Any
    def __init__(self, child: Optional[Expression] = ...) -> None: ...
    def pretty_name(self) -> str: ...
    def sql(self) -> str: ...

class NamedExpression:
    @property
    def name(self): ...
    @property
    def expr_id(self): ...

class LeafExpression(Expression): ...

class Star(LeafExpression, NamedExpression):
    expressions: Any
    def __init__(self, expressions: List[NamedExpression]) -> None: ...

class UnaryExpression(Expression):
    datatype: Any
    def __init__(self, child: Expression) -> None: ...
    def children(self) -> List[Expression]: ...

class BinaryExpression(Expression):
    sql_operator: str
    left: Any
    right: Any
    children: Any
    nullable: Any
    def __init__(self, left: Expression, right: Expression) -> None: ...

class AggregateMode: ...
class Complete(AggregateMode): ...

class AggregateExpression(Expression):
    aggregate_function: Any
    mode: Any
    is_distinct: Any
    filter: Any
    result_id: Any
    children: Any
    datatype: Any
    nullable: Any
    def __init__(
        self,
        aggregate_function,
        mode: AggregateMode,
        is_distinct: bool,
        filter: Expression,
        result_id: uuid.UUID = ...,
    ) -> None: ...
    @property
    def name(self): ...
    def sql(self) -> str: ...

class TypedAggregateExpression(AggregateExpression): ...

class AggregateFunction(Expression):
    name: str
    def to_aggregate_expression(
        self, is_distinct: bool = ..., filter: Any | None = ...
    ) -> AggregateExpression: ...

class DeclarativeAggregate(AggregateFunction):
    def __init__(self, child: Expression) -> None: ...

class Count(DeclarativeAggregate):
    name: str
    datatype: Any
    def __init__(self, child: Expression) -> None: ...

class Max(DeclarativeAggregate):
    name: str
    datatype: Any
    def __init__(self, child: Expression) -> None: ...

class Min(DeclarativeAggregate):
    name: str
    datatype: Any
    def __init__(self, child: Expression) -> None: ...

class Avg(DeclarativeAggregate):
    name: str
    datatype: Any
    def __init__(self, child: Expression) -> None: ...

class Sum(DeclarativeAggregate):
    name: str
    datatype: Any
    def __init__(self, child: Expression) -> None: ...

class BaseGroupingSets(Expression): ...

class Cube(BaseGroupingSets):
    grouping_set_indexes: Any
    children: Any
    def __init__(self, grouping_set_indexes) -> None: ...

class Rollup(BaseGroupingSets):
    grouping_set_indexes: Any
    children: Any
    def __init__(self, grouping_set_indexes) -> None: ...

class Alias(UnaryExpression, NamedExpression):
    def __init__(self, child, name, expr_id: Any | None = ...) -> None: ...

class Attribute(LeafExpression, NamedExpression):
    def __init__(self, name) -> None: ...
    @classmethod
    def with_name(cls, name): ...
    def sql(self) -> str: ...

class UnresolvedAlias(UnaryExpression, NamedExpression):
    alias_func: Any
    def __init__(self, child, alias_func) -> None: ...

class Literal(LeafExpression):
    value: Any
    datatype: Any
    def __init__(self, value: Any, datatype: Optional[DataType] = ...) -> None: ...

class BinaryArithmeticExpression(BinaryExpression): ...

class EqualTo(BinaryArithmeticExpression):
    sql_operator: str

class NotEqualTo(BinaryArithmeticExpression):
    sql_operator: str

class GreaterThan(BinaryArithmeticExpression):
    sql_operator: str

class LessThan(BinaryArithmeticExpression):
    sql_operator: str

class GreaterThanOrEqual(BinaryArithmeticExpression):
    sql_operator: str

class LessThanOrEqual(BinaryArithmeticExpression):
    sql_operator: str

class EqualNullSafe(BinaryExpression):
    sql_operator: str

class And(BinaryArithmeticExpression):
    sql_operator: str

class Or(BinaryArithmeticExpression):
    sql_operator: str

class Add(BinaryArithmeticExpression):
    sql_operator: str

class Subtract(BinaryArithmeticExpression):
    sql_operator: str

class Multiply(BinaryArithmeticExpression):
    sql_operator: str

class Divide(BinaryArithmeticExpression):
    sql_operator: str

class Remainder(BinaryArithmeticExpression):
    sql_operator: str

class Pow(BinaryExpression):
    sql_operator: str

class ArraysOverlap(BinaryExpression):
    sql_operator: str

class ArrayIntersect(BinaryExpression):
    sql_operator: str

class BitwiseAnd(BinaryExpression):
    sql_operator: str

class BitwiseOr(BinaryExpression):
    sql_operator: str

class BitwiseXor(BinaryExpression):
    sql_operator: str

class UnaryMinus(UnaryExpression):
    datatype: Any
    nullable: Any
    def __init__(self, child: Expression) -> None: ...

class Not(UnaryExpression): ...

class IsNaN(UnaryExpression):
    nullable: bool
    def __init__(self, child: Expression) -> None: ...

class IsNull(UnaryExpression):
    nullable: bool
    def __init__(self, child: Expression) -> None: ...

class IsNotNull(UnaryExpression):
    nullable: bool
    def __init__(self, child: Expression) -> None: ...

class Cast(UnaryExpression):
    child: Any
    to: Any
    datatype: Any
    nullable: Any
    def __init__(self, child: Expression, to: DataType) -> None: ...

class AttributeReference(Attribute):
    datatype: Any
    nullable: Any
    def __init__(self, name: str, datatype, nullable: bool) -> None: ...
    def with_name(self, new_name): ...

class UnresolvedAttribute(Attribute):
    name_parts: Any
    def __init__(self, name_parts) -> None: ...
    @classmethod
    def quoted(cls, name): ...
    @classmethod
    def quoted_string(cls, name): ...
    @staticmethod
    def parse_attribute_name(name): ...
    def __eq__(self, other): ...
    def __hash__(self): ...
    def sql(self): ...

class Like(Expression):
    expr: Any
    pattern: Any
    def __init__(self, expr: Expression, pattern: Expression) -> None: ...

class RegExp(Expression):
    expr: Any
    pattern: Any
    def __init__(self, expr: Expression, pattern: Expression) -> None: ...

class Collate(Expression):
    expr: Any
    collation_spec: Any
    def __init__(self, expr: Expression, collation_spec: str) -> None: ...

class SubfieldString(Expression):
    expr: Any
    field: Any
    def __init__(self, expr: Expression, field: str) -> None: ...

class SubfieldInt(Expression):
    expr: Any
    field: Any
    def __init__(self, expr: Expression, field: int) -> None: ...

class FunctionExpression(Expression):
    name: Any
    children: Any
    is_distinct: Any
    def __init__(
        self, name: str, arguments: List[Expression], is_distinct: bool
    ) -> None: ...
    def pretty_name(self) -> str: ...
    def sql(self) -> str: ...

class TableFunctionExpression(Expression):
    datatype: Any
    def __init__(self) -> None: ...

class FlattenFunction(TableFunctionExpression):
    input: Any
    path: Any
    outer: Any
    recursive: Any
    mode: Any
    def __init__(
        self, input: Expression, path: str, outer: bool, recursive: bool, mode: str
    ) -> None: ...

class TableFunction(TableFunctionExpression):
    func_name: Any
    args: Any
    def __init__(self, func_name: str, args: Iterable[Expression]) -> None: ...

class NamedArgumentsTableFunction(TableFunctionExpression):
    func_name: Any
    args: Any
    def __init__(self, func_name: str, args: Dict[str, Expression]) -> None: ...

class GroupingSetsExpression(Expression):
    args: Any
    datatype: Any
    def __init__(self, args: List[List[Expression]]) -> None: ...

class WithinGroup(Expression):
    expr: Any
    order_by_cols: Any
    def __init__(self, expr: Expression, order_by_cols: List[Expression]) -> None: ...

class NullOrdering:
    sql: str

class NullsFirst(NullOrdering):
    sql: str

class NullsLast(NullOrdering):
    sql: str

class SortDirection:
    sql: str
    default_null_ordering: NullOrdering

class Ascending(SortDirection):
    sql: str
    default_null_ordering: Any

class Descending(SortDirection):
    sql: str
    default_null_ordering: Any

class SortOrder(UnaryExpression):
    direction: Any
    null_ordering: Any
    datatype: Any
    nullable: Any
    def __init__(
        self,
        child: Expression,
        direction: SortDirection,
        null_ordering: NullOrdering = ...,
    ) -> None: ...

class CaseWhen(Expression):
    branches: Any
    else_value: Any
    nullable: Any
    def __init__(
        self,
        branches: List[Tuple[Expression, Expression]],
        else_value: Optional[Expression] = ...,
    ) -> None: ...

class SnowflakeUDF(Expression):
    udf_name: Any
    children: Any
    datatype: Any
    nullable: Any
    def __init__(
        self,
        udf_name: str,
        children: List[Expression],
        datatype: DataType,
        nullable: bool = ...,
    ) -> None: ...

class SpecialFrameBoundary(Expression):
    def __init__(self) -> None: ...
    def sql(self) -> str: ...

class UnboundedPreceding(SpecialFrameBoundary):
    def sql(self) -> str: ...

class UnboundedFollowing(SpecialFrameBoundary):
    def sql(self) -> str: ...

class CurrentRow(SpecialFrameBoundary):
    def sql(self) -> str: ...

class FrameType:
    sql: Any

class RowFrame(FrameType):
    sql: str

class RangeFrame(FrameType):
    sql: str

class WindowFrame(Expression):
    def __init__(self) -> None: ...

class UnspecifiedFrame(WindowFrame): ...

class SpecifiedWindowFrame(WindowFrame):
    frame_type: Any
    lower: Any
    upper: Any
    def __init__(
        self, frame_type: FrameType, lower: Expression, upper: Expression
    ) -> None: ...

class WindowSpecDefinition(Expression):
    partition_spec: Any
    order_spec: Any
    frame_spec: Any
    def __init__(
        self,
        partition_spec: List[Expression],
        order_spec: List[SortOrder],
        frame_spec: WindowFrame,
    ) -> None: ...

class WindowExpression(Expression):
    window_function: Any
    window_spec: Any
    def __init__(
        self, window_function: Expression, window_spec: WindowSpecDefinition
    ) -> None: ...
