from typing import Any, Optional

class IdentifierWithDatabase:
    database: Optional[str]

class TableIdentifier(IdentifierWithDatabase):
    table: Any
    database: Any
    def __init__(self, table: str, database_name: Optional[str] = ...) -> None: ...
