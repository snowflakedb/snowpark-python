#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Apache Software Foundation as part of the
# Apache Spark project, under the Apache License, Version 2.0.

# See https://github.com/apache/spark/blob/1dd0ca23f64acfc7a3dc697e19627a1b74012a2d/sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/util/package.scala#L128

from snowflake.snowpark.internal.sp_expressions import (
    Attribute as SPAttribute,
    Expression as SPExpression,
    Literal as SPLiteral,
    PrettyAttribute as SPPrettyAttribute,
)
from snowflake.snowpark.types.sp_data_types import (
    NumericType as SPNumericType,
    StringType as SPStringType,
)


def use_pretty_expression(e: SPExpression) -> SPExpression:
    if isinstance(e, SPAttribute):
        return SPPrettyAttribute.this(e)
    if isinstance(e, SPLiteral):
        if type(e.datatype) == SPStringType:
            return SPPrettyAttribute(str(e.value), SPStringType())
        if isinstance(e.datatype, SPNumericType):
            if e.value:
                return SPPrettyAttribute(str(e.value), SPStringType())
        if not e.value:
            return SPPrettyAttribute("NULL", e.datatype)

    # TODO
    # GetStructField
    # GetArrayStructFields
    # RuntimeReplaceable
    # CastBase


def to_pretty_sql(e: SPExpression) -> str:
    try:
        return use_pretty_expression(e).sql()
    except:
        try:
            return e.to_string()
        except:
            return e.name
