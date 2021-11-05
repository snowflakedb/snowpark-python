#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark import Column, DataFrame
from snowflake.snowpark.functions import builtin


class Imputer:
    __DATABASE = "hayu"
    __SCHEMA = "imputer"
    __BUNDLE = f"{__DATABASE}.{__SCHEMA}"

    def __init__(self, session=None, input_col=None):
        self.session = session
        self.input_col = input_col

    def fit(self, input_df: DataFrame, is_numerical: bool) -> str:
        if type(input_df) is not DataFrame:
            raise TypeError(
                f"Imputer.fit() input input_df type must be DataFrame. Got: {type(input_df)}"
            )

        if type(is_numerical) is not bool:
            raise TypeError(
                f"Imputer.fit() input is_numerical type must be bool. Got: {type(is_numerical)}"
            )

        query = input_df.select(self.input_col)._DataFrame__plan.queries[-1].sql
        res = self.session.sql(
            f"call {self.__BUNDLE}.fit($${query}$$, {is_numerical})"
        ).collect()
        return res[0][0]

    def transform(self, col: Column) -> Column:
        if type(col) is not Column:
            raise TypeError(
                f"Imputer.transform() input type must be Column. Got: {type(col)}"
            )

        return builtin(f"{self.__BUNDLE}.transform")(col)
