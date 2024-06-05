#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import copy
import uuid
from functools import cached_property
from typing import TYPE_CHECKING, AbstractSet, Any, List, Optional, Tuple

import snowflake.snowpark._internal.utils
from snowflake.snowpark._internal.analyzer.complexity_stat import (
    ComplexityStat,
    Counter,
)

if TYPE_CHECKING:
    from snowflake.snowpark._internal.analyzer.snowflake_plan import (
        SnowflakePlan,
    )  # pragma: no cover

from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages
from snowflake.snowpark._internal.type_utils import (
    VALID_PYTHON_TYPES_FOR_LITERAL_VALUE,
    VALID_SNOWPARK_TYPES_FOR_LITERAL_VALUE,
    infer_type,
)
from snowflake.snowpark.types import DataType

COLUMN_DEPENDENCY_DOLLAR = frozenset(
    "$"
)  # depend on any columns with expression `$n`. We don't flatten when seeing a $
COLUMN_DEPENDENCY_ALL = None  # depend on all columns including subquery's and same level columns when we can't infer the dependent columns
COLUMN_DEPENDENCY_EMPTY: AbstractSet[str] = frozenset()  # depend on no columns.


def derive_dependent_columns(
    *expressions: "Optional[Expression]",
) -> Optional[AbstractSet[str]]:
    result = set()
    for exp in expressions:
        if exp is not None:
            child_dependency = exp.dependent_column_names()
            if child_dependency == COLUMN_DEPENDENCY_DOLLAR:
                return COLUMN_DEPENDENCY_DOLLAR
            if child_dependency == COLUMN_DEPENDENCY_ALL:
                return COLUMN_DEPENDENCY_ALL
            assert child_dependency is not None
            result.update(child_dependency)
    return result


class Expression:
    """Consider removing attributes, and adding properties and methods.
    A subclass of Expression may have no child, one child, or multiple children.
    But the constructor accepts a single child. This might be refactored in the future.
    """

    def __init__(self, child: Optional["Expression"] = None) -> None:
        """
        Subclasses will override these attributes
        """
        self.child = child
        self.nullable = True
        self.children = [child] if child else None
        self.datatype: Optional[DataType] = None

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        # TODO: consider adding it to __init__ or use cached_property.
        return COLUMN_DEPENDENCY_EMPTY

    @property
    def pretty_name(self) -> str:
        """Returns a user-facing string representation of this expression's name.
        This should usually match the name of the function in SQL."""
        return self.__class__.__name__.upper()

    @property
    def sql(self) -> str:
        """The only place that uses Expression.sql() to generate sql statement
        is relational_grouped_dataframe.py's __toDF(). Re-consider whether we need to make the sql generation
        consistent among all different Expressions.
        """
        children_sql = (
            ", ".join([x.sql for x in self.children]) if self.children else ""
        )
        return f"{self.pretty_name}({children_sql})"

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        return Counter({})

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        children = self.children or []
        return sum(
            (child.cumulative_complexity_stat for child in children),
            self.individual_complexity_stat,
        )

    def __str__(self) -> str:
        return self.pretty_name


class NamedExpression:
    name: str
    _expr_id: Optional[uuid.UUID] = None

    @property
    def expr_id(self) -> uuid.UUID:
        if not self._expr_id:
            self._expr_id = uuid.uuid4()
        return self._expr_id

    def __copy__(self):
        new = copy.copy(super())
        new._expr_id = None  # type: ignore
        return new


class ScalarSubquery(Expression):
    def __init__(self, plan: "SnowflakePlan") -> None:
        super().__init__()
        self.plan = plan

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return COLUMN_DEPENDENCY_DOLLAR

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        return self.plan.cumulative_complexity_stat + self.individual_complexity_stat


class MultipleExpression(Expression):
    def __init__(self, expressions: List[Expression]) -> None:
        super().__init__()
        self.expressions = expressions

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(*self.expressions)

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        return (
            sum(
                (expr.cumulative_complexity_stat for expr in self.expressions),
                Counter({}),
            )
            + self.individual_complexity_stat
        )


class InExpression(Expression):
    def __init__(self, columns: Expression, values: List[Expression]) -> None:
        super().__init__()
        self.columns = columns
        self.values = values

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.columns, *self.values)

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        return Counter({ComplexityStat.IN.value: 1})

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        return (
            self.columns.cumulative_complexity_stat
            + self.individual_complexity_stat
            + sum(
                (expr.cumulative_complexity_stat for expr in self.values),
                Counter({}),
            )
        )


class Attribute(Expression, NamedExpression):
    def __init__(self, name: str, datatype: DataType, nullable: bool = True) -> None:
        super().__init__()
        self.name = name
        self.datatype: DataType = datatype
        self.nullable = nullable

    def with_name(self, new_name: str) -> "Attribute":
        if self.name == new_name:
            return self
        else:
            return Attribute(
                snowflake.snowpark._internal.utils.quote_name(new_name),
                self.datatype,
                self.nullable,
            )

    @property
    def sql(self) -> str:
        return self.name

    def __str__(self):
        return self.name

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return {self.name}

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        return Counter({ComplexityStat.COLUMN.value: 1})


class Star(Expression):
    def __init__(
        self, expressions: List[Attribute], df_alias: Optional[str] = None
    ) -> None:
        super().__init__()
        self.expressions = expressions
        self.df_alias = df_alias

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(*self.expressions)

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        if self.expressions:
            return Counter({})
        # if there are no expressions, we assign column value = 1 to Star
        return Counter({ComplexityStat.COLUMN.value: 1})

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        return self.individual_complexity_stat + sum(
            (child.individual_complexity_stat for child in self.expressions),
            Counter({}),
        )


class UnresolvedAttribute(Expression, NamedExpression):
    def __init__(
        self, name: str, is_sql_text: bool = False, df_alias: Optional[str] = None
    ) -> None:
        super().__init__()
        self.df_alias = df_alias
        self.name = name
        self.is_sql_text = is_sql_text
        if "$" in name:
            # $n refers to a column by index. We don't consider column index yet.
            # even though "$" isn't necessarily used to refer to a column by index. We're conservative here.
            self._dependent_column_names = COLUMN_DEPENDENCY_DOLLAR
        else:
            self._dependent_column_names = (
                COLUMN_DEPENDENCY_ALL if is_sql_text else {name}
            )

    @property
    def sql(self) -> str:
        return self.name

    def __str__(self):
        return self.name

    def __eq__(self, other):
        return type(other) is type(self) and other.name == self.name

    def __hash__(self):
        return hash(self.name)

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return self._dependent_column_names

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        return Counter({ComplexityStat.COLUMN.value: 1})


class Literal(Expression):
    def __init__(self, value: Any, datatype: Optional[DataType] = None) -> None:
        super().__init__()

        # check value
        if not isinstance(value, VALID_PYTHON_TYPES_FOR_LITERAL_VALUE):
            raise SnowparkClientExceptionMessages.PLAN_CANNOT_CREATE_LITERAL(
                type(value)
            )
        self.value = value

        self.datatype: DataType
        # check datatype
        if datatype:
            if not isinstance(datatype, VALID_SNOWPARK_TYPES_FOR_LITERAL_VALUE):
                raise SnowparkClientExceptionMessages.PLAN_CANNOT_CREATE_LITERAL(
                    str(datatype)
                )
            self.datatype = datatype
        else:
            self.datatype = infer_type(value)

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        return Counter({ComplexityStat.LITERAL.value: 1})


class Interval(Expression):
    def __init__(
        self,
        year: Optional[int] = None,
        quarter: Optional[int] = None,
        month: Optional[int] = None,
        week: Optional[int] = None,
        day: Optional[int] = None,
        hour: Optional[int] = None,
        minute: Optional[int] = None,
        second: Optional[int] = None,
        millisecond: Optional[int] = None,
        microsecond: Optional[int] = None,
        nanosecond: Optional[int] = None,
    ) -> None:
        super().__init__()
        self.values_dict = {}
        if year is not None:
            self.values_dict["YEAR"] = year
        if quarter is not None:
            self.values_dict["QUARTER"] = quarter
        if month is not None:
            self.values_dict["MONTH"] = month
        if week is not None:
            self.values_dict["WEEK"] = week
        if day is not None:
            self.values_dict["DAY"] = day
        if hour is not None:
            self.values_dict["HOUR"] = hour
        if minute is not None:
            self.values_dict["MINUTE"] = minute
        if second is not None:
            self.values_dict["SECOND"] = second
        if millisecond is not None:
            self.values_dict["MILLISECOND"] = millisecond
        if microsecond is not None:
            self.values_dict["MICROSECOND"] = microsecond
        if nanosecond is not None:
            self.values_dict["NANOSECOND"] = nanosecond

    @property
    def sql(self) -> str:
        return f"""INTERVAL '{",".join(f"{v} {k}" for k, v in self.values_dict.items())}'"""

    def __str__(self) -> str:
        return self.sql

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        return Counter(
            {
                ComplexityStat.LITERAL.value: 2 * len(self.values_dict),
                ComplexityStat.LOW_IMPACT.value: 1,
            }
        )


class Like(Expression):
    def __init__(self, expr: Expression, pattern: Expression) -> None:
        super().__init__(expr)
        self.expr = expr
        self.pattern = pattern

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.expr, self.pattern)

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        # expr LIKE pattern
        return Counter({ComplexityStat.LOW_IMPACT.value: 1})

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        return (
            self.expr.cumulative_complexity_stat
            + self.pattern.cumulative_complexity_stat
            + self.individual_complexity_stat
        )


class RegExp(Expression):
    def __init__(self, expr: Expression, pattern: Expression) -> None:
        super().__init__(expr)
        self.expr = expr
        self.pattern = pattern

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.expr, self.pattern)

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        # expr REG_EXP pattern
        return Counter({ComplexityStat.LOW_IMPACT.value: 1})

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        return (
            self.expr.cumulative_complexity_stat
            + self.pattern.cumulative_complexity_stat
            + self.individual_complexity_stat
        )


class Collate(Expression):
    def __init__(self, expr: Expression, collation_spec: str) -> None:
        super().__init__(expr)
        self.expr = expr
        self.collation_spec = collation_spec

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.expr)

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        # expr COLLATE collate_spec
        return Counter({ComplexityStat.LOW_IMPACT.value: 1})

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        return self.expr.cumulative_complexity_stat + self.individual_complexity_stat


class SubfieldString(Expression):
    def __init__(self, expr: Expression, field: str) -> None:
        super().__init__(expr)
        self.expr = expr
        self.field = field

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.expr)

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        # the literal corresponds to the contribution from self.field
        return Counter({ComplexityStat.LITERAL.value: 1})

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        # self.expr ( self.field )
        return self.expr.cumulative_complexity_stat + self.individual_complexity_stat


class SubfieldInt(Expression):
    def __init__(self, expr: Expression, field: int) -> None:
        super().__init__(expr)
        self.expr = expr
        self.field = field

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.expr)

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        # the literal corresponds to the contribution from self.field
        return Counter({ComplexityStat.LITERAL.value: 1})

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        # self.expr ( self.field )
        return self.expr.cumulative_complexity_stat + self.individual_complexity_stat


class FunctionExpression(Expression):
    def __init__(
        self,
        name: str,
        arguments: List[Expression],
        is_distinct: bool,
        api_call_source: Optional[str] = None,
        *,
        is_data_generator: bool = False,
    ) -> None:
        super().__init__()
        self.name = name
        self.children = arguments
        self.is_distinct = is_distinct
        self.api_call_source = api_call_source
        self.is_data_generator = is_data_generator

    @property
    def pretty_name(self) -> str:
        return self.name

    @property
    def sql(self) -> str:
        distinct = "DISTINCT " if self.is_distinct else ""
        return (
            f"{self.pretty_name}({distinct}{', '.join([c.sql for c in self.children])})"
        )

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(*self.children)

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        return Counter({ComplexityStat.FUNCTION.value: 1})


class WithinGroup(Expression):
    def __init__(self, expr: Expression, order_by_cols: List[Expression]) -> None:
        super().__init__(expr)
        self.expr = expr
        self.order_by_cols = order_by_cols
        self.datatype = expr.datatype

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.expr, *self.order_by_cols)

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        # expr WITHIN GROUP (ORDER BY cols)
        return Counter({ComplexityStat.ORDER_BY.value: 1})

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        return (
            sum(
                (col.cumulative_complexity_stat for col in self.order_by_cols),
                Counter({}),
            )
            + self.individual_complexity_stat
            + self.expr.cumulative_complexity_stat
        )


class CaseWhen(Expression):
    def __init__(
        self,
        branches: List[Tuple[Expression, Expression]],
        else_value: Optional[Expression] = None,
    ) -> None:
        super().__init__()
        self.branches = branches
        self.else_value = else_value

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        exps = []
        for exp_tuple in self.branches:
            exps.extend(exp_tuple)
        if self.else_value is not None:
            exps.append(self.else_value)
        return derive_dependent_columns(*exps)

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        return Counter({ComplexityStat.CASE_WHEN.value: 1})

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        estimate = self.individual_complexity_stat + sum(
            (
                condition.cumulative_complexity_stat + value.cumulative_complexity_stat
                for condition, value in self.branches
            ),
            Counter({}),
        )
        estimate += (
            self.else_value.cumulative_complexity_stat
            if self.else_value
            else Counter({})
        )
        return estimate


class SnowflakeUDF(Expression):
    def __init__(
        self,
        udf_name: str,
        children: List[Expression],
        datatype: DataType,
        nullable: bool = True,
        api_call_source: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.udf_name = udf_name
        self.children = children
        self.datatype = datatype
        self.nullable = nullable
        self.api_call_source = api_call_source

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(*self.children)

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        return Counter({ComplexityStat.FUNCTION.value: 1})

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        return sum(
            (expr.cumulative_complexity_stat for expr in self.children),
            self.individual_complexity_stat,
        )


class ListAgg(Expression):
    def __init__(self, col: Expression, delimiter: str, is_distinct: bool) -> None:
        super().__init__()
        self.col = col
        self.delimiter = delimiter
        self.is_distinct = is_distinct

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.col)

    @property
    def individual_complexity_stat(self) -> Counter[str]:
        return Counter({ComplexityStat.FUNCTION.value: 1})

    @cached_property
    def cumulative_complexity_stat(self) -> Counter[str]:
        return self.col.cumulative_complexity_stat + self.individual_complexity_stat
