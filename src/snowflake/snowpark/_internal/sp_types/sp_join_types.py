#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Apache Software Foundation as part of the
# Apache Spark project, under the Apache License, Version 2.0.

# JoinType
# See https://github.com/apache/spark/blob/master/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/plans/joinTypes.scala
from typing import List

from snowflake.snowpark._internal.error_message import SnowparkClientExceptionMessages


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

        raise SnowparkClientExceptionMessages.DF_JOIN_INVALID_JOIN_TYPE(
            join_type, ", ".join(supported)
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
        if type(tpe) not in [
            Inner,
            LefOuter,
            RightOuter,
            FullOuter,
        ]:
            raise SnowparkClientExceptionMessages.DF_JOIN_INVALID_NATURAL_JOIN_TYPE(
                tpe.__class__.__name__
            )
        self.tpe = tpe
        self.sql = "NATURAL " + tpe.sql


class UsingJoin(JoinType):
    def __init__(self, tpe: JoinType, using_columns: List[str]):
        if type(tpe) not in [
            Inner,
            LefOuter,
            LeftSemi,
            RightOuter,
            FullOuter,
            LeftAnti,
        ]:
            raise SnowparkClientExceptionMessages.DF_JOIN_INVALID_USING_JOIN_TYPE(
                tpe.__class__.__name__
            )
        self.sql = "USING " + tpe.sql
        self.tpe = tpe
        self.using_columns = using_columns
