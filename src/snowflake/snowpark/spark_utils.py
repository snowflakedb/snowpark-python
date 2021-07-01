#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from snowflake.snowpark.internal.analyzer.sp_utils import to_pretty_sql
from snowflake.snowpark.internal.sp_expressions import AggregateExpression, Expression
from snowflake.snowpark.types.sp_data_types import DataType, IntegralType


# utils function to allow package properties accessible from outside
class SparkUtils:
    @staticmethod
    def column_generate_alias(expr: Expression) -> str:
        # https://github.com/apache/spark/blob/0494dc90af48ce7da0625485a4dc6917a244d580/sql/core/src/main/scala/org/apache/spark/sql/Column.scala#L43
        if isinstance(expr, AggregateExpression):
            if isinstance(expr.aggregate_function, TypedAggregateExpression):
                return expr.aggregate_function.to_string()
        else:
            return to_pretty_sql(expr)

    # TODO
    def struct_type_to_attributes(self, struct_type):
        pass

    # TODO
    def window_spec_with_aggregate(self, window, expr):
        pass

    # TODO
    def default_window_spec(self):
        pass

    # TODO
    def create_window_spec(self):
        pass

    @staticmethod
    def is_numeric_type(datatype: DataType) -> bool:
        return isinstance(datatype, IntegralType)
