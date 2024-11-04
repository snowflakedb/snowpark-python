#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import List, Optional, Union

from snowflake.core import Root
from snowflake.core.database import Database
from snowflake.core.schema import Schema


class Catalog:
    def __init__(self, session: "snowflake.snowpark.Session") -> None:
        self._session = session
        self._root = Root(session)

    def list_databases(
        self,
        *,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: Optional[int] = None,
        from_name: Optional[str] = None
    ) -> List[Database]:
        db_collection = self._root.databases
        return list(
            db_collection.iter(
                like=like, starts_with=starts_with, limit=limit, from_name=from_name
            )
        )

    def list_schemas(
        self,
        database: Union[str, Database],
        *,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: Optional[int] = None,
        from_name: Optional[str] = None
    ) -> List[Schema]:
        db_name = database.name if isinstance(database, Database) else database
        schema = self._root.databases[db_name].schemas
        return list(
            schema.iter(
                like=like, starts_with=starts_with, limit=limit, from_name=from_name
            )
        )

    def list_tables():
        pass
