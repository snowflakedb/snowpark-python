#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import List

from snowflake.connector import SnowflakeConnection
from snowflake.connector.cursor import ResultMetadata
from snowflake.snowpark._internal.utils import result_set_to_iter, result_set_to_rows


class AsyncJob:
    def __init__(
        self,
        query_id: str,
        query: str,
        conn: SnowflakeConnection,
        result_meta: List[ResultMetadata],
        data_type: str = "row",
    ) -> None:
        self.query_id = query_id
        self.query = query
        self._conn = conn
        self._cursor = self._conn.cursor()
        self._data_type = data_type
        self._result_meta = result_meta
        if not conn:
            raise ValueError("connection cannot be empty")
        return

    def is_done(self) -> bool:
        # return a bool value to indicate whether the query is finished
        status = self._conn.get_query_status(self.query_id)
        is_running = self._conn.is_still_running(status)

        return not is_running

    def cancel(self) -> None:
        # stop and cancel current query id
        self._conn._cancel_query(self.query, self.query_id)

    def result(self):
        # return result of the query, in the form of a list of Row object
        self._cursor.get_results_from_sfqid(self.query_id)
        result_data = self._cursor.fetchall()
        if self._data_type == "row":
            return result_set_to_rows(result_data, self._result_meta)
        elif self._data_type == "iterator":
            return result_set_to_iter(result_data, self._result_meta)
        elif self._data_type == "pandas":
            return result_data
        else:
            raise ValueError(f"{self._data_type} is not a supported data type")
