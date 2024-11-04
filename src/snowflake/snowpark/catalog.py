#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import List, Optional, Union

from snowflake.core import Root
from snowflake.core.database import Database
from snowflake.core.function import Function
from snowflake.core.procedure import Procedure
from snowflake.core.schema import Schema
from snowflake.core.table import Table
from snowflake.core.user_defined_function import UserDefinedFunction

import snowflake.snowpark


class Catalog:
    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
        self._session = session
        self._root = Root(session)

    def _parse_database(self, database: Optional[Union[str, Database]]) -> str:
        if isinstance(database, str):
            return database
        if isinstance(database, Database):
            return database.name
        if database is None:
            return self._session.get_current_database()
        raise ValueError("")

    def _parse_schema(self, schema: Optional[Union[str, Schema]]) -> str:
        if isinstance(schema, str):
            return schema
        if isinstance(schema, Schema):
            return schema.name
        if schema is None:
            return self._session.get_current_schema()
        raise ValueError("")

    # List methods
    def list_databases(
        self,
        *,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: Optional[int] = None,
        from_name: Optional[str] = None,
    ) -> List[Database]:
        db_collection = self._root.databases
        return list(
            db_collection.iter(
                like=like, starts_with=starts_with, limit=limit, from_name=from_name
            )
        )

    def list_schemas(
        self,
        *,
        database: Optional[Union[str, Database]] = None,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: Optional[int] = None,
        from_name: Optional[str] = None,
    ) -> List[Schema]:
        db_name = self._parse_database(database)
        schema_collection = self._root.databases[db_name].schemas
        return list(
            schema_collection.iter(
                like=like, starts_with=starts_with, limit=limit, from_name=from_name
            )
        )

    def list_tables(
        self,
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: Optional[int] = None,
        from_name: Optional[str] = None,
        history: bool = False,
        deep: bool = False,
    ) -> List[Table]:
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)

        table_collection = self._root.databases[db_name].schemas[schema_name].tables
        return list(
            table_collection.iter(
                like=like,
                starts_with=starts_with,
                limit=limit,
                from_name=from_name,
                history=history,
                deep=deep,
            )
        )

    def list_functions(
        self,
        *,
        database: Optional[Union[str, Database]],
        schema: Optional[Union[str, Schema]] = None,
        like: Optional[str] = None,
    ) -> List[Function]:
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)

        function_collection = (
            self._root.databases[db_name].schemas[schema_name].functions
        )
        return list(function_collection.iter(like=like))

    def list_procedures(
        self,
        *,
        database: Optional[Union[str, Database]],
        schema: Optional[Union[str, Schema]] = None,
        like: Optional[str] = None,
    ) -> List[Procedure]:
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)

        procedure_collection = (
            self._root.databases[db_name].schemas[schema_name].procedures
        )
        return list(procedure_collection.iter(like=like))

    def list_user_defined_functions(
        self,
        *,
        database: Optional[Union[str, Database]],
        schema: Optional[Union[str, Schema]] = None,
        like: Optional[str] = None,
    ) -> List[UserDefinedFunction]:
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)

        udf_collection = (
            self._root.databases[db_name].schemas[schema_name].user_defined_functions
        )
        return list(udf_collection.iter(like=like))

    # get methods
    def get_current_database(self) -> Database:
        current_db_name = self._session.get_current_database()
        return self._root.databases[current_db_name]

    def get_current_schema(self) -> Schema:
        current_db = self.get_current_database()
        current_schema_name = self._session.get_current_schema()
        return current_db.schemas[current_schema_name]

    def get_table(self, table_name: str) -> Table:
        if not self.table_exists(table_name):
            raise ValueError(f"Table {table_name} does not exist.")
        pass

    def get_function() -> Function:
        pass

    def get_procedure() -> Procedure:
        pass

    def get_user_defined_function() -> UserDefinedFunction:
        pass

    # set methods
    def set_current_database():
        pass

    def set_current_schema():
        pass

    # exists methods
    def database_exists() -> bool:
        pass

    def schema_exists() -> bool:
        pass

    def table_exists() -> bool:
        pass

    def function_exists() -> bool:
        pass

    def procedure_exists() -> bool:
        pass

    def user_defined_function_exists() -> bool:
        pass

    # TODO: consider if we should be added drop methods
