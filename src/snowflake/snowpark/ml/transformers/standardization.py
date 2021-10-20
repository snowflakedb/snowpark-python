#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#


class Standardization:
    def __init__(self, session=None):
        self.session = session

    __TEMP_TABLE = "table_temp"

    def fit(self) -> None:
        pass

    def transform(self, value: float) -> float:
        mean = self.session.table(self.__TEMP_TABLE).select("mean")
        stddev = self.session.table(self.__TEMP_TABLE).select("stddev")
        return (value - mean) / stddev
