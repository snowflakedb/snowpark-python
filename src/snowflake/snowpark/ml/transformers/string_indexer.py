#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark import Column, DataFrame
from snowflake.snowpark.functions import builtin


class StringIndexer:
    __DATABASE = "hayu"
    __SCHEMA = "stringindexer"
    __BUNDLE = f"{__DATABASE}.{__SCHEMA}"
    __SESSION_SCHEMA = "stringindexer_clone"

    def __init__(self, session, input_col):
        self.session = session
        self.input_col = input_col

    def fit(self, input_df: DataFrame) -> str:
        if not isinstance(input_df, DataFrame):
            raise TypeError(
                f"StringIndexer.fit() input type must be DataFrame. Got: {input_df.__class__}"
            )

        # clone a stringindexer schema
        # self.session.sql(
        #     f"create or replace schema {self.__SESSION_SCHEMA} clone {self.__BUNDLE}"
        # ).collect()

        query = input_df.select(self.input_col)._DataFrame__plan.queries[-1].sql
        res = self.session.sql(
            f"call {self.__SESSION_SCHEMA}.fit($${query}$$)"
        ).collect()
        return res[0][0]

    def transform(self, col: Column) -> Column:
        if not isinstance(col, Column):
            raise TypeError(
                f"StringIndexer.transform() input type must be Column. Got: {col.__class__}"
            )

        return builtin(f"{self.__SESSION_SCHEMA}.transform")(col)
