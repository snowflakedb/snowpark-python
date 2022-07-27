#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#


class AsyncJob:
    def __init__(self, query_id: str) -> None:
        self.query_id = query_id
        return

    @property
    def query_id(self) -> str:
        return self.query_id

    @query_id.setter
    def query_id(self, value: str) -> None:
        self.query_id = value

    def is_done(self) -> bool:
        # return a bool value to indicate whether the query is finished
        pass

    def cancel(self) -> None:
        # stop and cancel current query id
        pass

    def collect(self):
        # return result of the query, in the form of a list of Row object
        pass

    def iterator(self):
        # return result of the query, in the form of a iterator of Row object
        pass
