#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Apache Software Foundation as part of the
# Apache Spark project, under the Apache License, Version 2.0.
#
#  File containing the Expression definitions for ASTs (Spark).
import uuid
from typing import TYPE_CHECKING, Any, List, Optional, Tuple

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.snowflake_plan import SnowflakePlan

from functools import cached_property

import snowflake.snowpark._internal.analyzer.analyzer_package as analyzer_package
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.type_utils import (
    _VALID_PYTHON_TYPES_FOR_LITERAL_VALUE,
    _VALID_SNOWPARK_TYPES_FOR_LITERAL_VALUE,
    _infer_type,
)
from snowflake.snowpark.types import DataType, StructType


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


class NamedExpression:
    name: str = None

    @cached_property
    def expr_id(self) -> uuid.UUID:
        return uuid.uuid4()


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


class Attribute(Expression, NamedExpression):
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


class UnresolvedAttribute(Expression, NamedExpression):
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


class WithinGroup(Expression):
    def __init__(self, expr: Expression, order_by_cols: List[Expression]):
        super().__init__(expr)
        self.expr = expr
        self.order_by_cols = order_by_cols
        self.datatype = expr.datatype


class CaseWhen(Expression):
    def __init__(
        self,
        branches: List[Tuple[Expression, Expression]],
        else_value: Optional[Expression] = None,
    ):
        super().__init__()
        self.branches = branches
        self.else_value = else_value


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


class SnowflakeUDTF(Expression):
    def __init__(
        self,
        udtf_name: str,
        children: List[Expression],
        datatype: StructType,
        order_by_cols: List[Expression],
        partition_by_cols: List[Expression],
    ):
        super().__init__()
        self.udtf_name = udtf_name
        self.children = children
        self.datatype = datatype
        self.order_by_cols = order_by_cols
        self.partition_by_cols = partition_by_cols


class ListAgg(Expression):
    def __init__(self, col: Expression, delimiter: str, is_distinct: bool):
        super().__init__()
        self.col = col
        self.delimiter = delimiter
        self.is_distinct = is_distinct
