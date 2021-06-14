#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from src.snowflake.snowpark.column import Column
from src.snowflake.snowpark.internal.analyzer.sp_utils import to_pretty_sql
from src.snowflake.snowpark.internal.sp_expressions import Expression as SPExpression, NamedExpression as SPNamedExpression, UnresolvedAttribute as SPUnresolvedAttribute, UnresolvedAlias as SPUnresolvedAlias, Alias as SPAlias, Cube as SPCube, Rollup as SPRollup, AggregateExpression as SPAggregateExpression, TypedAggregateExpression as SPTypedAggregateExpression, Literal as SPLiteral, Count as SPCount, UnresolvedFunction as SPUnresolvedFunction, Star as SPStar
from src.snowflake.snowpark.plans.logical.basic_logical_operators import Aggregate as SPAggregate, Pivot as SPPivot
from src.snowflake.snowpark.internal.analyzer.expression import GroupingSets
from src.snowflake.snowpark.dataframe import DataFrame
from src.snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from src.snowflake.snowpark.spark_utils import SparkUtils
from src.snowflake.snowpark.types.sp_data_types import IntegerType as SPInteger
import src.snowflake.snowpark.functions as functions

import re
from typing import List, Tuple, Union


class GroupType:
    def to_string(self):
        # TODO revisit
        return self.__class__.__name__.strip('$').strip('Type')


class GroupByType(GroupType):
    pass


class CubeType(GroupType):
    pass


class RollupType(GroupType):
    pass


class PivotType(GroupType):
    def __init__(self, pivot_col: SPExpression, values:List[SPExpression]):
        self.pivot_col = pivot_col
        self.values = values


class RelationalGroupedDataFrame:
    """ Represents an underlying DataFrame with rows that are grouped by ccommon values. Can be used
     to define aggregations on these groupedbDataFrames.


     Example:
        val groupedDf: RelationalGroupedDataFrame = df.groupBy("dept")
   valaggDf: DataFrame = groupedDf.agg(groupedDf("salary") -> "mean")
 }}}

  The methods [[DataFrame.groupBy(cols* DataFrame.groupBy]],
  [[DataFrame.cube(cols* DataFrame.cube]] and
  [[DataFrame.rollup(cols* DataFrame.rollup]]
  return an instance of type [[RelationalGroupedDataFrame]]"""

    def __init__(self, df, grouping_exprs : List[SPExpression], group_type: GroupType):
        self.df = df
        self.grouping_exprs = grouping_exprs
        self.group_type = group_type

    # subscriptable returns new object

    def toDF(self, agg_exprs: List[SPExpression]):
        aliased_agg = []
        for grouping_expr in self.grouping_exprs:
            if isinstance(grouping_expr, GroupingSets):
                aliased_agg.extend(list(set(grouping_expr.args)))
            else:
                aliased_agg.append(grouping_expr)

        aliased_agg.extend(agg_exprs)
        aliases_agg = [self.alias(a) for a in list(set(aliased_agg))]

        if type(self.group_type) == GroupByType:
            return DataFrame(self.df.session,
                             SPAggregate(self.grouping_exprs, aliases_agg, self.df._DataFrame__plan))
        if type(self.group_type) == RollupType:
            return DataFrame(self.df.session,
                             SPAggregate([SPRollup(self.grouping_exprs)], aliases_agg, self.df.__plan))
        if type(self.group_type) == CubeType:
            return DataFrame(self.df.session,
                             SPAggregate([SPCube(self.grouping_exprs)], aliases_agg, self.df.__plan))
        if type(self.group_type) == PivotType:
            if len(agg_exprs) != 1:
                raise SnowparkClientException("Only one aggregate is supported with pivot")
            return DataFrame(self.df.session,
                             SPPivot([self.alias(e) for e in grouping_expr],
                                     self.group_type.pivot_col,
                                     self.group_type.values,
                                     agg_exprs,
                                     self.df.__plan))

    def as_(self, expr: SPExpression) -> SPNamedExpression:
        return self.alias(expr)

    def alias(self, expr: SPExpression) -> SPNamedExpression:
        if isinstance(expr, SPUnresolvedAttribute):
            return SPUnresolvedAlias(expr, None)
        elif isinstance(expr, SPNamedExpression):
            return expr
        elif isinstance(expr, SPAggregateExpression) and isinstance(expr.aggregate_function, SPTypedAggregateExpression):
            return SPUnresolvedAlias(expr,
                                     lambda s: self.__strip_invalid_sf_identifier_chars(SparkUtils.column_generate_alias(s)))
        else:
            return SPAlias(expr, self.__strip_invalid_sf_identifier_chars(to_pretty_sql(expr).upper()))

    @staticmethod
    def __strip_invalid_sf_identifier_chars(identifier: str):
        p = re.compile("[^\\x20-\\x7E]")
        return p.sub("", identifier.replace('\"', ''))

    def __str_to_expr(self, expr: str):
        return lambda input_expr: self.__expr_to_func(expr, input_expr)

    @staticmethod
    def __expr_to_func(expr: str, input_expr: SPExpression):
        lowered = expr.lower()
        if lowered in ['avg', 'average', 'mean']:
            return SPUnresolvedFunction('avg', None, is_distinct=False)
        elif lowered in ['stddev', 'std']:
            return SPUnresolvedFunction('stddev', None, is_distinct=False)
        elif lowered in ['count', 'size']:
            if isinstance(input_expr, SPStar):
                return SPCount(SPLiteral(1, SPInteger())).to_aggregate_expression()
            else:
                return SPCount(input_expr).to_aggregate_expression()
        else:
            return SPUnresolvedFunction(expr, None, is_distinct=False)

    def agg(self, exprs: List[Union[Column, Tuple[Column, str]]]):
        if all(type(e) == Column for e in exprs):
            return self.toDF([e.expression for e in exprs])
        elif all(type(e) == tuple and type(e[0]) == Column and type(e[1]) == str for e in exprs):
            return self.toDF([self.__str_to_expr(expr)(col.expr) for col, expr in exprs])
        else:
            raise SnowparkClientException("Invalid input types for agg()")

    def avg(self, *cols: Column):
        """Return the average for the specified numeric columns. """
        return self.__non_empty_argument_function("avg", list(cols))

    def mean(self, *cols: Column):
        """Return the average for the specified numeric columns. Alias of avg."""
        return self.avg(*cols)

    def sum(self, *cols: Column):
        """Return the sum for the specified numeric columns."""
        return self.__non_empty_argument_function("sum", list(cols))

    def median(self, *cols: Column):
        """Return the median for the specified numeric columns."""
        return self.__non_empty_argument_function("median", list(cols))

    def min(self, *cols: Column):
        return self.__non_empty_argument_function("min", list(cols))

    def max(self, *cols: Column):
        return self.__non_empty_argument_function("max", list(cols))

    def count(self):
        return self.toDF(
            [SPAlias(SPCount(SPLiteral(1, SPInteger())).to_aggregate_expression(), "count")])

    def builtin(self, agg_name: str, *cols: Column):
        agg_exprs = []
        for c in cols:
            expr = functions.builtin(agg_name)(c.expression).expression
            agg_exprs.append(expr)
        return self.toDF(agg_exprs)

    def __non_empty_argument_function(self, func_name: str, cols: List[Column]):
        if not cols:
            raise SnowparkClientException(f"the argument of {func_name} function can't be empty")
        else:
            return self.builtin(func_name, *cols)
