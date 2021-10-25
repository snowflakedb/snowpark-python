#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark import Column
from snowflake.snowpark.functions import sql_expr


class StandardScaler:
    def __init__(self, session=None):
        self.session = session

    __TEMP_TABLE = "table_temp"

    def fit(self) -> None:
        pass

    def transform(self, c: Column) -> Column:
        name = c.getName()
        df = self.session.sql(
            f"select ((select {name} from table) - mean) / stddev from {self.__TEMP_TABLE}"
        )
        return df.col(name)
