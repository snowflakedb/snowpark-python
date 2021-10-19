#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#


class StringIndexer:
    def __init__(self, session=None):
        self.session = session

    __TEMP_TABLE = "table_temp"

    def fit(self) -> None:
        pass

    def transform(self, value) -> int:
        return (
            self.session.table(self.__TEMP_TABLE)
            .select("id")
            .where(col("distinct_values") == value)
        )
