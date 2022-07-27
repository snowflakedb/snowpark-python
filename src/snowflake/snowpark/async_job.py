#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from snowflake.snowpark._internal.server_connection import SnowflakeConnection


class AsyncJob:
    def __init__(
        self,
        query_id: str = None,
        query: str = None,
        conn: SnowflakeConnection = None,
    ) -> None:
        self.query_id = query_id
        self.query = query
        self._conn = conn
        self._cursor = self._conn.cursor()

        if not conn:
            raise ValueError("connection cannot be empty")
        return

    @property
    def query_id(self) -> str:
        return self.query_id

    @query_id.setter
    def query_id(self, value: str) -> None:
        self.query_id = value

    @property
    def query(self) -> str:
        return self.query

    @query.setter
    def query(self, value: str) -> None:
        self.query = value

    # iterator
    def __iter__(self):
        pass

    def __next__(self):
        pass

    def is_done(self) -> bool:
        # return a bool value to indicate whether the query is finished
        status = self._conn.get_query_status(self.query_id)
        is_running = self._conn.is_still_running(status)

        return not is_running

    def cancel(self) -> None:
        # stop and cancel current query id
        self._conn._cancel_query(self.query, self.query_id)

    def collect(self):
        # return result of the query, in the form of a list of Row object
        self._cursor.get_results_from_sfqid(self.query_id)
        return self._cursor.fetchall()
