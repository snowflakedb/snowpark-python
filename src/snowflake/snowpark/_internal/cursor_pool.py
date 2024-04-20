#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from threading import Condition, RLock
from typing import List

from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor


class _CursorWrapper:
    def __init__(
        self,
        cursor: SnowflakeCursor,
        free_cursors: List[SnowflakeCursor],
        lock: RLock,
        condition: Condition,
    ) -> None:
        self.cursor = cursor
        self.free_cursors = free_cursors
        self.lock = lock
        self.condition = condition

    def __enter__(self) -> SnowflakeCursor:
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        # Release the cursor back to the pool
        with self.lock:
            self.cursor.reset()
            self.free_cursors.append(self.cursor)
            self.condition.notify()


class SnowflakeCursorPool:
    def __init__(self, conn: SnowflakeConnection, capacity: int) -> None:
        self._free_cursors: List[SnowflakeCursor] = []
        self.conn = conn
        self._lock = RLock()
        self._condition = Condition(self._lock)
        self._capacity = capacity
        self._created_cursors = 0

    def cursor(self) -> _CursorWrapper:
        with self._lock:
            # Wait until a cursor is available if the total number of cursors created reaches capacity
            while self._created_cursors >= self._capacity and not self._free_cursors:
                self._condition.wait()

            if self._free_cursors:
                cursor = self._free_cursors.pop()
            else:
                cursor = self.conn.cursor()
                self._created_cursors += 1

        return _CursorWrapper(cursor, self._free_cursors, self._lock, self._condition)
