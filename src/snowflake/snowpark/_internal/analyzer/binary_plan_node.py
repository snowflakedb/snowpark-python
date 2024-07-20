#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import Dict, List, Optional

from snowflake.snowpark._internal.analyzer.expression import Expression
from snowflake.snowpark._internal.analyzer.query_plan_analysis_utils import (
    PlanNodeCategory,
    sum_node_complexities,
)
from snowflake.snowpark._internal.analyzer.snowflake_plan_node import LogicalPlan
from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages

SUPPORTED_JOIN_TYPE_STR = [
    "inner",
    "outer",
    "full",
    "fullouter",
    "leftouter",
    "left",
    "rightouter",
    "right",
    "leftsemi",
    "semi",
    "leftanti",
    "anti",
    "cross",
    "asof",
]


def create_join_type(join_type: str) -> "JoinType":
    jt = join_type.strip().lower().replace("_", "")

    if jt == "inner":
        return Inner()

    if jt in ["outer", "full", "fullouter"]:
        return FullOuter()

    if jt in ["leftouter", "left"]:
        return LeftOuter()

    if jt in ["rightouter", "right"]:
        return RightOuter()

    if jt in ["leftsemi", "semi"]:
        return LeftSemi()

    if jt in ["leftanti", "anti"]:
        return LeftAnti()

    if jt == "cross":
        return Cross()

    if jt == "asof":
        return AsOf()

    raise SnowparkClientExceptionMessages.DF_JOIN_INVALID_JOIN_TYPE(
        join_type, ", ".join(SUPPORTED_JOIN_TYPE_STR)
    )


class BinaryNode(LogicalPlan):
    sql: str

    def __init__(self, left: LogicalPlan, right: LogicalPlan) -> None:
        super().__init__()
        self.left = left
        self.right = right
        self.children = [self.left, self.right]


class SetOperation(BinaryNode):
    @property
    def plan_node_category(self) -> PlanNodeCategory:
        # (left) operator (right)
        return PlanNodeCategory.SET_OPERATION


class Except(SetOperation):
    sql = "EXCEPT"


class Intersect(SetOperation):
    sql = "INTERSECT"


class Union(SetOperation):
    def __init__(self, left: LogicalPlan, right: LogicalPlan, is_all: bool) -> None:
        super().__init__(left, right)
        self.is_all = is_all

    @property
    def sql(self) -> str:
        return f"UNION{' ALL' if self.is_all else ''}"


class JoinType:
    sql: str


class InnerLike(JoinType):
    pass


class Inner(InnerLike):
    sql = "INNER"


class Cross(InnerLike):
    sql = "CROSS"


class LeftOuter(JoinType):
    sql = "LEFT OUTER"


class RightOuter(JoinType):
    sql = "RIGHT OUTER"


class FullOuter(JoinType):
    sql = "FULL OUTER"


class LeftSemi(JoinType):
    sql = "LEFT SEMI"


class LeftAnti(JoinType):
    sql = "LEFT ANTI"


class AsOf(JoinType):
    sql = "ASOF"


class NaturalJoin(JoinType):
    def __init__(self, tpe: JoinType) -> None:
        if not isinstance(
            tpe,
            (
                Inner,
                LeftOuter,
                RightOuter,
                FullOuter,
            ),
        ):
            raise SnowparkClientExceptionMessages.DF_JOIN_INVALID_NATURAL_JOIN_TYPE(
                tpe.__class__.__name__
            )
        self.sql = "NATURAL " + tpe.sql
        self.tpe = tpe


class UsingJoin(JoinType):
    def __init__(self, tpe: JoinType, using_columns: List[str]) -> None:
        if not isinstance(
            tpe,
            (
                Inner,
                LeftOuter,
                LeftSemi,
                RightOuter,
                FullOuter,
                LeftAnti,
                AsOf,
            ),
        ):
            raise SnowparkClientExceptionMessages.DF_JOIN_INVALID_USING_JOIN_TYPE(
                tpe.__class__.__name__
            )
        self.sql = "USING " + tpe.sql
        self.tpe = tpe
        self.using_columns = using_columns


class Join(BinaryNode):
    def __init__(
        self,
        left: LogicalPlan,
        right: LogicalPlan,
        join_type: JoinType,
        join_condition: Optional["Expression"],
        match_condition: Optional["Expression"],
    ) -> None:
        super().__init__(left, right)
        self.join_type = join_type
        self.join_condition = join_condition
        self.match_condition = match_condition

    @property
    def sql(self) -> str:
        return self.join_type.sql

    @property
    def plan_node_category(self) -> PlanNodeCategory:
        return PlanNodeCategory.JOIN

    @property
    def individual_node_complexity(self) -> Dict[PlanNodeCategory, int]:
        # SELECT * FROM (left) AS left_alias join_type_sql JOIN (right) AS right_alias match_cond, using_cond, join_cond
        complexity = {self.plan_node_category: 1}
        if isinstance(self.join_type, UsingJoin) and self.join_type.using_columns:
            complexity = sum_node_complexities(
                complexity,
                {PlanNodeCategory.COLUMN: len(self.join_type.using_columns)},
            )
        complexity = (
            sum_node_complexities(
                complexity, self.join_condition.cumulative_node_complexity
            )
            if self.join_condition
            else complexity
        )

        complexity = (
            sum_node_complexities(
                complexity, self.match_condition.cumulative_node_complexity
            )
            if self.match_condition
            else complexity
        )
        return complexity
