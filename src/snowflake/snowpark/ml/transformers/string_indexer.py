#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark import Column


class StringIndexer:
    def __init__(self, session=None):
        self.session = session

    __TEMP_TABLE = "table_temp"

    def fit(self) -> None:
        pass

    def transform(self, c: Column) -> Column:
        # input = self.session.createDataFrame([c])
        # temp = self.session.table(self.__TEMP_TABLE)
        name = c.getName()
        df = self.session.sql(
            f"select {name}, index from table left join {self.__TEMP_TABLE} on {name}={self.__TEMP_TABLE}.distinct_values"
        )
        return df.col(name)
