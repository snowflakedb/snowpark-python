#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import pytest

from snowflake.connector.errors import ProgrammingError
from src.snowflake.snowpark.functions import count_distinct
from src.snowflake.snowpark.row import Row


def test_count_distinct(session_cnx, db_parameters):
    with session_cnx(db_parameters) as session:
        df = session.createDataFrame(
            [["a", 1, 1], ["b", 2, 2], ["c", 1, None], ["d", 5, None]]
        ).toDF(["id", "value", "other"])

        res = df.select(
            count_distinct(df["id"]),
            count_distinct(df["value"]),
            count_distinct(df["other"]),
        ).collect()
        assert res == [Row([4, 3, 2])]

        res = df.select(count_distinct([df["id"], df["value"]])).collect()
        assert res == [Row([4])]

        # Pass invalid type - str
        with pytest.raises(TypeError) as ex_info:
            df.select(count_distinct("abc"))
        assert "Invalid input to count_distinct()." in str(ex_info)

        # Pass invalid type - list of str
        with pytest.raises(TypeError) as ex_info:
            df.select(count_distinct(["abc", "abc"]))
        assert "Invalid input to count_distinct()." in str(ex_info)

        with pytest.raises(ProgrammingError) as ex_info:
            df.select(count_distinct([df["*"]])).collect()
        assert "Unsupported feature 'TOK_STAR'" in str(ex_info)
