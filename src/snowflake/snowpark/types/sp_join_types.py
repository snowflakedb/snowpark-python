#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

# JoinType
# See https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/joinTypes.scala
from typing import List


class JoinType:
    sql = None

    @classmethod
    def from_string(cls, join_type: str) -> "JoinType":
        jt = join_type.strip().lower().replace("_", "")

        if jt == "inner":
            return Inner()

        if jt in ["outer", "full", "fullouter"]:
            return FullOuter()

        if jt in ["leftouter", "left"]:
            return LefOuter()

        if jt in ["rightouter", "right"]:
            return RightOuter()

        if jt in ["leftsemi", "semi"]:
            return LeftSemi()

        if jt in ["leftanti", "anti"]:
            return LeftAnti()

        if jt == "cross":
            return Cross()

        supported = [
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
        ]

        raise Exception(
            f"Unsupported join type {join_type}. Supported join types include"
            f" {'.'.join(supported)}."
        )


class InnerLike(JoinType):
    pass


class Inner(InnerLike):
    sql = "INNER"


class Cross(InnerLike):
    sql = "CROSS"


class LefOuter(JoinType):
    sql = "LEFT OUTER"


class RightOuter(JoinType):
    sql = "RIGHT OUTER"


class FullOuter(JoinType):
    sql = "FULL OUTER"


class LeftSemi(JoinType):
    sql = "LEFT SEMI"


class LeftAnti(JoinType):
    sql = "LEFT ANTI"


class NaturalJoin(JoinType):
    def __init__(self, tpe: JoinType):
        assert type(tpe) in [
            Inner,
            LefOuter,
            RightOuter,
            FullOuter,
        ], f"Unsupported natural join type {tpe}"
        self.tpe = tpe
        self.sql = "NATURAL " + tpe.sql


class UsingJoin(JoinType):
    def __init__(self, tpe: JoinType, using_columns: List[str]):
        assert type(tpe) in [
            Inner,
            LefOuter,
            LeftSemi,
            RightOuter,
            FullOuter,
            LeftAnti,
            Cross,
        ], f"Unsupported using join type {tpe}"
        self.sql = "USING " + tpe.sql
        self.tpe = tpe
        self.using_columns = using_columns
