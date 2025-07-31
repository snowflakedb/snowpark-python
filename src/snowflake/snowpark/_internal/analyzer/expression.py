#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import copy
import uuid
from typing import TYPE_CHECKING, AbstractSet, Any, Dict, List, Optional, Tuple

import snowflake.snowpark._internal.utils
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    sum_node_complexities,
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
    """
    Given set of expressions, derive the set of columns that the expressions dependents on.

    Note, the returned dependent columns is a set without duplication. For example, given expression
    concat(col1, upper(co1), upper(col2)), the result will be {col1, col2} even if col1 has
    occurred in the given expression twice.
    """
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


def derive_dependent_columns_with_duplication(
    *expressions: "Optional[Expression]",
) -> List[str]:
    """
    Given set of expressions, derive the list of columns that the expression dependents on.

    Note, the returned columns will have duplication if the column occurred more than once in
    the given expression. For example, concat(col1, upper(co1), upper(col2)) will have result
    [col1, col1, col2], where col1 occurred twice in the result.
    """
    result = []
    for exp in expressions:
        if exp is not None:
            result.extend(exp.dependent_column_names_with_duplication())
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
        self._cumulative_node_complexity: Optional[Dict[PlanNodeCategory, int]] = None
        self._ast = None

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        # TODO: consider adding it to __init__ or use cached_property.
        return COLUMN_DEPENDENCY_EMPTY

    def dependent_column_names_with_duplication(self) -> List[str]:
        return []

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

    def __str__(self) -> str:
        return self.pretty_name

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.OTHERS

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        """Returns the individual contribution of the expression node towards the overall
        compilation complexity of the generated sql.
        """
        return {self.plan_node_category: 1}

    @property
    def cumulative_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        """Returns the aggregate sum complexity statistic from the subtree rooted at this
        expression node. It is computed by adding all expression attributes of current nodes
        and cumulative complexity of all children nodes. To correctly maintain this statistic,
        override individual_node_complexity method for the derived Expression class.
        """
        if self._cumulative_node_complexity is None:
            children = self.children or []
            self._cumulative_node_complexity = sum_node_complexities(
                self.individual_node_complexity,
                *(child.cumulative_node_complexity for child in children),
            )
        return self._cumulative_node_complexity

    @cumulative_node_complexity.setter
    def cumulative_node_complexity(self, value: Dict[PlanNodeCategory, int]):
        self._cumulative_node_complexity = value


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

    def dependent_column_names_with_duplication(self) -> List[str]:
        return list(COLUMN_DEPENDENCY_DOLLAR)

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        return self.plan.cumulative_node_complexity


class MultipleExpression(Expression):
    def __init__(self, expressions: List[Expression]) -> None:
        super().__init__()
        self.expressions = expressions

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(*self.expressions)

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(*self.expressions)

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        return sum_node_complexities(
            *(expr.cumulative_node_complexity for expr in self.expressions),
        )


class InExpression(Expression):
    def __init__(self, columns: Expression, values: List[Expression]) -> None:
        super().__init__()
        self.columns = columns
        self.values = values

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.columns, *self.values)

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(self.columns, *self.values)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.IN

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        return sum_node_complexities(
            {self.plan_node_category: 1},
            self.columns.cumulative_node_complexity,
            *(expr.cumulative_node_complexity for expr in self.values),
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

    def dependent_column_names_with_duplication(self) -> List[str]:
        return [self.name]

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.COLUMN


class Star(Expression):
    def __init__(
        self, expressions: List[Attribute], df_alias: Optional[str] = None
    ) -> None:
        super().__init__()
        self.expressions = expressions
        self.df_alias = df_alias

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        # When the column is `df['*']`, `expressions` contains Attributes from all columns
        # When the column is `col('*')` or just '*' string, `expressions` is empty,
        # but its dependent columns should be all columns too
        return (
            derive_dependent_columns(*self.expressions)
            if self.expressions
            else COLUMN_DEPENDENCY_ALL
        )

    def dependent_column_names_with_duplication(self) -> List[str]:
        return (
            derive_dependent_columns_with_duplication(*self.expressions)
            if self.expressions
            else []  # we currently do not handle * dependency
        )

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        complexity = {} if self.expressions else {PlanNodeCategory.COLUMN: 1}

        return sum_node_complexities(
            complexity,
            *(expr.cumulative_node_complexity for expr in self.expressions),
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

    def dependent_column_names_with_duplication(self) -> List[str]:
        return (
            []
            if (self._dependent_column_names == COLUMN_DEPENDENCY_ALL)
            or (self._dependent_column_names is None)
            else list(self._dependent_column_names)
        )

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.COLUMN


class Literal(Expression):
    def __init__(self, value: Any, datatype: Optional[DataType] = None) -> None:
        super().__init__()

        # check value
        if not isinstance(value, VALID_PYTHON_TYPES_FOR_LITERAL_VALUE):
            raise SnowparkClientExceptionMessages.PLAN_CANNOT_CREATE_LITERAL(
                type(value)
            )
        self.value = value
        self.nullable = value is None

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
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.LITERAL


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
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.LOW_IMPACT


class Like(Expression):
    def __init__(self, expr: Expression, pattern: Expression) -> None:
        super().__init__(expr)
        self.expr = expr
        self.pattern = pattern

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.expr, self.pattern)

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(self.expr, self.pattern)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        # expr LIKE pattern
        return PlanNodeCategory.LOW_IMPACT

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        return sum_node_complexities(
            {self.plan_node_category: 1},
            self.expr.cumulative_node_complexity,
            self.pattern.cumulative_node_complexity,
        )


class RegExp(Expression):
    def __init__(
        self,
        expr: Expression,
        pattern: Expression,
        parameters: Optional[Expression] = None,
    ) -> None:
        super().__init__(expr)
        self.expr = expr
        self.pattern = pattern
        self.parameters = parameters

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.expr, self.pattern)

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(self.expr, self.pattern)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        # expr REG_EXP pattern
        return PlanNodeCategory.LOW_IMPACT

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        return sum_node_complexities(
            {self.plan_node_category: 1},
            self.expr.cumulative_node_complexity,
            self.pattern.cumulative_node_complexity,
        )


class Collate(Expression):
    def __init__(self, expr: Expression, collation_spec: str) -> None:
        super().__init__(expr)
        self.expr = expr
        self.collation_spec = collation_spec

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.expr)

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(self.expr)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        # expr COLLATE collate_spec
        return PlanNodeCategory.LOW_IMPACT

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        return sum_node_complexities(
            {self.plan_node_category: 1}, self.expr.cumulative_node_complexity
        )


class SubfieldString(Expression):
    def __init__(self, expr: Expression, field: str) -> None:
        super().__init__(expr)
        self.expr = expr
        self.field = field

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.expr)

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(self.expr)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        # the literal corresponds to the contribution from self.field
        return PlanNodeCategory.LITERAL

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # self.expr ( self.field )
        return sum_node_complexities(
            {self.plan_node_category: 1}, self.expr.cumulative_node_complexity
        )


class SubfieldInt(Expression):
    def __init__(self, expr: Expression, field: int) -> None:
        super().__init__(expr)
        self.expr = expr
        self.field = field

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.expr)

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(self.expr)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        # the literal corresponds to the contribution from self.field
        return PlanNodeCategory.LITERAL

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # self.expr ( self.field )
        return sum_node_complexities(
            {self.plan_node_category: 1}, self.expr.cumulative_node_complexity
        )


class ModelExpression(Expression):
    def __init__(
        self,
        model_name: str,
        version_or_alias_name: Optional[str],
        method_name: str,
        arguments: List[Expression],
    ) -> None:
        super().__init__()
        self.model_name = model_name
        self.version_or_alias_name = version_or_alias_name
        self.method_name = method_name
        self.children = arguments

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(*self.children)

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(*self.children)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.FUNCTION


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

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(*self.children)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.FUNCTION


class NamedFunctionExpression(Expression):
    def __init__(
        self,
        name: str,
        named_arguments: Dict[str, Expression],
        api_call_source: Optional[str] = None,
    ) -> None:
        super().__init__()
        self.name = name
        self.named_arguments = named_arguments
        self.children = list(named_arguments.values())
        self.api_call_source = api_call_source

    @property
    def pretty_name(self) -> str:
        return self.name

    @property
    def sql(self) -> str:
        return f"{self.pretty_name}({', '.join([f'{k} => {v.sql}' for k, v in self.named_arguments.items()])})"

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(*self.children)

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(*self.children)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.FUNCTION


class WithinGroup(Expression):
    def __init__(self, expr: Expression, order_by_cols: List[Expression]) -> None:
        super().__init__(expr)
        self.expr = expr
        self.order_by_cols = order_by_cols
        self.datatype = expr.datatype
        assert all(
            isinstance(order_by_col, Expression) for order_by_col in order_by_cols
        )

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.expr, *self.order_by_cols)

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(self.expr, *self.order_by_cols)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        # expr WITHIN GROUP (ORDER BY cols)
        return PlanNodeCategory.ORDER_BY

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        return sum_node_complexities(
            {self.plan_node_category: 1},
            self.expr.cumulative_node_complexity,
            *(col.cumulative_node_complexity for col in self.order_by_cols),
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

    @property
    def _child_expressions(self) -> List[Expression]:
        exps = []
        for exp_tuple in self.branches:
            exps.extend(exp_tuple)
        if self.else_value is not None:
            exps.append(self.else_value)

        return exps

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(*self._child_expressions)

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(*self._child_expressions)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.CASE_WHEN

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        complexity = sum_node_complexities(
            {self.plan_node_category: 1},
            *(
                sum_node_complexities(
                    condition.cumulative_node_complexity,
                    value.cumulative_node_complexity,
                )
                for condition, value in self.branches
            ),
        )
        complexity = (
            sum_node_complexities(
                complexity, self.else_value.cumulative_node_complexity
            )
            if self.else_value
            else complexity
        )
        return complexity


class SnowflakeUDF(Expression):
    def __init__(
        self,
        udf_name: str,
        children: List[Expression],
        datatype: DataType,
        nullable: bool = True,
        api_call_source: Optional[str] = None,
        is_aggregate_function: bool = False,
    ) -> None:
        super().__init__()
        self.udf_name = udf_name
        self.children = children
        self.datatype = datatype
        self.nullable = nullable
        self.api_call_source = api_call_source
        self.is_aggregate_function = is_aggregate_function

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(*self.children)

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(*self.children)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.FUNCTION


class ListAgg(Expression):
    def __init__(self, col: Expression, delimiter: str, is_distinct: bool) -> None:
        super().__init__()
        self.col = col
        self.delimiter = delimiter
        self.is_distinct = is_distinct

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(self.col)

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(self.col)

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.FUNCTION

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        return sum_node_complexities(
            {self.plan_node_category: 1}, self.col.cumulative_node_complexity
        )


class ColumnSum(Expression):
    def __init__(self, exprs: List[Expression]) -> None:
        super().__init__()
        self.exprs = exprs

    def dependent_column_names(self) -> Optional[AbstractSet[str]]:
        return derive_dependent_columns(*self.exprs)

    def dependent_column_names_with_duplication(self) -> List[str]:
        return derive_dependent_columns_with_duplication(*self.exprs)

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        return sum_node_complexities(
            *(expr.cumulative_node_complexity for expr in self.exprs)
        )
