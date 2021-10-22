#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark import Column
from snowflake.snowpark.functions import sql_expr


class Imputer:
    def __init__(self, session=None):
        self.session = session

    __TEMP_TABLE = "table_temp"

    def fit(self) -> None:
        pass

    def transform(self, c: Column) -> Column:
        mean = sql_expr(f"select mean from {self.__TEMP_TABLE}")
        mode = sql_expr(f"select mode from {self.__TEMP_TABLE}")
