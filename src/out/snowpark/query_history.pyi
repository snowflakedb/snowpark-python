from typing import Any, List, NamedTuple

import snowflake.snowpark

class QueryRecord(NamedTuple):
    query_id: str
    sql_text: str

class QueryHistory:
    session: Any
    def __init__(self, session: snowflake.snowpark.session.Session) -> None: ...
    def __enter__(self): ...
    def __exit__(self, exc_type, exc_val, exc_tb) -> None: ...
    @property
    def queries(self) -> List[QueryRecord]: ...
