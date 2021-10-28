#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark import Column, DataFrame
from snowflake.snowpark.functions import builtin


class StringIndexer:
    def __init__(self, session=None):
        self.session = session

    def fit(self, df: DataFrame) -> str:
        pass

    def transform(self, c: Column) -> Column:
        return builtin("hayu.stringindexer.transform")(c)
