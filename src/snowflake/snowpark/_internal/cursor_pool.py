#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from threading import Condition, RLock
from typing import List

from snowflake.connector.connection import SnowflakeConnection
from snowflake.connector.cursor import SnowflakeCursor


class CursorWrapper:
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
        # Return the cursor for use
        return self.cursor

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        # Release the cursor back to the pool
        with self.lock:
            self.cursor.reset()
            self.free_cursors.append(self.cursor)
            self.condition.notify()


class CursorPool:
    def __init__(self, conn: SnowflakeConnection, capacity: int) -> None:
        self.free_cursors: List[SnowflakeCursor] = []  # Pool of free cursor objects
        self.conn = conn
        self.lock = RLock()
        self.condition = Condition(self.lock)
        self.capacity = capacity
        self.created_cursors = 0

    def cursor(self) -> CursorWrapper:
        with self.lock:
            # Wait until a cursor is available if the total number of cursors created reaches capacity
            while self.created_cursors >= self.capacity and not self.free_cursors:
                self.condition.wait()

            if self.free_cursors:
                # Get a cursor from the pool
                cursor = self.free_cursors.pop()
            elif self.created_cursors < self.capacity:
                # Create a new cursor
                cursor = self.conn.cursor()
                self.created_cursors += 1
        return CursorWrapper(
            cursor, self.free_cursors, self.lock, self.condition
        )  # Wrap the cursor in CursorWrapper and return
