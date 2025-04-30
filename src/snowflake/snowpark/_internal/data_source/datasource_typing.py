#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from typing import Protocol, List, Tuple, Any


class Connection(Protocol):
    """External datasource connection created from user-input create_connection function."""

    def cursor(self) -> "Cursor":
        pass

    def close(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass


class Cursor(Protocol):
    """Cursor created from external datasource connection"""

    def execute(self, sql: str, *params: Any) -> "Cursor":
        pass

    def fetchall(self) -> List[Tuple]:
        pass

    def fetchone(self):
        pass

    def fetchmany(self, size: int):
        pass

    def close(self):
        pass
