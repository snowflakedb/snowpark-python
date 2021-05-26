#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from src.snowflake.snowpark.internal.analyzer.snowflake_plan import SnowflakePlan
from src.snowflake.snowpark.internal.analyzer.sf_attribute import Attribute
from .dataframe import DataFrame
from .row import Row
from .types.sf_types import DataType, ArrayType, StringType, VariantType, MapType, GeographyType, BooleanType,\
    BinaryType, TimeType, TimestampType, DateType, DecimalType, DoubleType, LongType

from snowflake.connector import SnowflakeConnection
from snowflake.connector.constants import FIELD_ID_TO_NAME
from typing import (
    Any,
    List,
    Tuple,
)


def _get_data_type(column_type_name: str, precision: int, scale: int) -> DataType:
    if column_type_name == 'ARRAY':
        return ArrayType(StringType)
    if column_type_name == 'VARIANT':
        return VariantType
    if column_type_name == 'OBJECT':
        return MapType(StringType, StringType)
    if column_type_name == 'GEOGRAPHY':  # not supported by python connector
        return GeographyType
    if column_type_name == 'BOOLEAN':
        return BooleanType
    if column_type_name == 'BINARY':
        return BinaryType
    if column_type_name == 'TEXT':
        return StringType
    if column_type_name == 'TIME':
        return TimeType
    if column_type_name == 'TIMESTAMP' or column_type_name == 'TIMESTAMP_LTZ' or column_type_name == 'TIMESTAMP_TZ' or \
            column_type_name == 'TIMESTAMP_NTZ':
        return TimestampType
    if column_type_name == 'DATE':
        return DateType
    if column_type_name == 'DECIMAL' or (column_type_name == 'FIXED' and scale != 0):
        if precision != 0 or scale != 0:
            if precision > DecimalType.MAX_PRECISION:
                return DecimalType(DecimalType.MAX_PRECISION, scale + precision - DecimalType.MAX_SCALE)
            else:
                return DecimalType(precision, scale)
        else:
            return DecimalType(38, 15)  # Spark 1.5.0 default
    if column_type_name == 'REAL':
        return DoubleType
    if column_type_name == 'FIXED' and scale == 0:
        return LongType
    raise NotImplementedError("Unsupported type: {}, precision: {}, scale: {}".format(column_type_name, precision,
                                                                                      scale))


def convert_result_meta_to_attribute(meta: List[Tuple[Any, ...]]) -> List['Attribute']:
    attributes = []
    for column_name, type_value, _, _, precision, scale, nullable in meta:
        attributes.append(Attribute(column_name, _get_data_type(FIELD_ID_TO_NAME[type_value], precision, scale),
                                    nullable))
    return attributes


class ServerConnection:

    def __init__(self, conn: SnowflakeConnection):
        self.__conn = conn
        self._cursor = self.__conn.cursor()

    def close(self):
        self.__conn.close()

    def get_session_id(self):
        return self.__conn.session_id

    def get_result_attributes(self, query: str) -> List['Attribute']:
        lowercase = query.strip().lower()
        if lowercase.startswith("put") or lowercase.startswith("get"):
            return []
        else:
            # TODO: SNOW-361263: remove this after python connector has `describe` function
            if hasattr(self._cursor, 'describe'):
                self._cursor.describe(query)
            else:
                self._cursor.execute(query)
            return convert_result_meta_to_attribute(self._cursor.description)

    def run_query(self, query):
        results_cursor = self._cursor.execute(query)
        data = results_cursor.fetchall()
        return data

    # TODO revisit
    def result_set_to_rows(self, result_set):
        rows = [Row(row) for row in result_set]
        return rows

    def execute(self, input):
        # Handle scenarios for whether we pass queries or a DF
        if type(input) == DataFrame:
            return self.result_set_to_rows(self.get_result_set_from_DF(input))
        if type(input) == SnowflakePlan:
            return self.result_set_to_rows(self.get_result_set_from_plan(input))
        # TODO cleanup
        raise Exception("Should not have reached here. Serverconnection.execute")

    def get_result_set_from_DF(self, df):
        queries = df._get_sql_queries_for_df()
        action_id = df.session._generate_new_action_id()

        result = None
        try:
            placeholders = {}
            for query in queries:
                final_query = query
                for holder, Id in placeholders.items():
                    # TODO revisit
                    final_query = final_query.replace(holder, Id)
                if action_id < df.session.get_last_canceled_id():
                    raise Exception("Query was canceled by user")
                result = self.run_query(final_query)
                # TODO revisit
                # last_id = result.get_query_id()
                # placeholders[query.query_id_placeholder] = last_id
        finally:
            # delete create tmp object
            # TODO get plan.postActions
            pass

        return result

    def get_result_set_from_plan(self, plan):
        action_id = plan.session._generate_new_action_id()

        result = None
        try:
            placeholders = {}
            for query in plan.queries:
                final_query = query.sql
                for holder, Id in placeholders.items():
                    # TODO revisit
                    final_query = final_query.replace(holder, Id)
                if action_id < plan.session.get_last_canceled_id():
                    raise Exception("Query was canceled by user")
                result = self.run_query(final_query)
                # TODO revisit
                # last_id = result.get_query_id()
                # placeholders[query.query_id_placeholder] = last_id
        finally:
            # delete create tmp object
            # TODO get plan.postActions
            pass

        return result

    def get_result_and_metadata(self, plan: SnowflakePlan) -> (List['Row'], List['Attribute']):
        result_set = self.get_result_set_from_plan(plan)
        result = self.result_set_to_rows(result_set)
        meta = convert_result_meta_to_attribute(self._cursor.description)
        return result, meta

    # TODO
    def get_parameter_value(self, parameter_name):
        pass

    # TODO
    def wrap_exception(self):
        pass
