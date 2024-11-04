#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from typing import List, Optional, Tuple, Union

from snowflake.core import Root
from snowflake.core.database import Database
from snowflake.core.exceptions import NotFoundError
from snowflake.core.function import Function
from snowflake.core.procedure import Procedure
from snowflake.core.schema import Schema
from snowflake.core.table import Table
from snowflake.core.user_defined_function import UserDefinedFunction
from snowflake.core.view import View

import snowflake.snowpark

DatabaseOrStr = Union[str, Database]
SchemaOrStr = Union[str, Schema]


class Catalog:
    def __init__(self, session: "snowflake.snowpark.session.Session") -> None:
        self._session = session
        self._root = Root(session)

    def _parse_database(self, database: Optional[DatabaseOrStr]) -> str:
        if isinstance(database, str):
            return database
        if isinstance(database, Database):
            return database.name
        if database is None:
            return self._session.get_current_database()
        raise ValueError("")

    def _parse_schema(self, schema: Optional[SchemaOrStr]) -> str:
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
        database: Optional[DatabaseOrStr] = None,
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
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr] = None,
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

    def list_views(
        self,
        *,
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr] = None,
        like: Optional[str] = None,
        starts_with: Optional[str] = None,
        limit: Optional[int] = None,
        from_name: Optional[str] = None,
        deep: bool = False,
    ) -> List[View]:
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)

        view_collection = self._root.databases[db_name].schemas[schema_name].views
        return list(
            view_collection.iter(
                like=like,
                starts_with=starts_with,
                show_limit=limit,
                from_name=from_name,
                deep=deep,
            )
        )

    def list_functions(
        self,
        *,
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr] = None,
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
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr] = None,
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
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr] = None,
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

    def get_table(
        self,
        table_name: str,
        *,
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr] = None,
    ) -> Table:
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)
        return self._root.databases[db_name].schemas[schema_name].tables[table_name]

    def get_view(
        self,
        view_name: str,
        *,
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr] = None,
    ) -> View:
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)
        return self._root.databases[db_name].schemas[schema_name].views[view_name]

    def get_function(
        self, function_name: str, arg_types: Union[List, Tuple]
    ) -> Function:
        pass

    def get_procedure(
        self, procedure_name: str, arg_types: Union[List, Tuple]
    ) -> Procedure:
        pass

    def get_user_defined_function(
        self, udf_name: str, arg_types: Union[List, Tuple]
    ) -> UserDefinedFunction:
        pass

    # set methods
    def set_current_database(self, database: DatabaseOrStr) -> None:
        db_name = self._parse_database(database)
        self._session.sql(f"USE DATABASE {db_name}")._internal_collect_with_tag()

    def set_current_schema(self, schema: SchemaOrStr) -> None:
        schema_name = self._parse_schema(schema)
        self._session.sql(f"USE SCHEMA {schema_name}")._internal_collect_with_tag()

    # exists methods
    def database_exists(self, database: DatabaseOrStr) -> bool:
        db_name = self._parse_database(database)
        try:
            self._root.databases[db_name].fetch()
            return True
        except NotFoundError:
            return False

    def schema_exists(
        self, schema: SchemaOrStr, *, database: Optional[DatabaseOrStr] = None
    ) -> bool:
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)
        try:
            self._root.databases[db_name].schemas[schema_name].fetch()
            return True
        except NotFoundError:
            return False

    def table_exists(
        self,
        table: Union[str, Table],
        *,
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr] = None,
    ) -> bool:
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)
        table_name = table if isinstance(table, str) else table.name
        try:
            self._root.databases[db_name].schemas[schema_name].tables[
                table_name
            ].fetch()
            return True
        except NotFoundError:
            return False

    def view_exists(
        self,
        view: Union[str, View],
        *,
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr] = None,
    ) -> bool:
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)
        view_name = view if isinstance(view, str) else view.name
        try:
            self._root.databases[db_name].schemas[schema_name].views[view_name].fetch()
            return True
        except NotFoundError:
            return False

    def function_exists(
        self,
        function: Union[str, Function],
        arg_types: Union[List, Tuple],
        *,
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr],
    ) -> bool:
        pass

    def procedure_exists(
        self,
        procedure: Union[str, Procedure],
        arg_types: Union[List, Tuple],
        *,
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr] = None,
    ) -> bool:
        pass

    def user_defined_function_exists(
        self,
        udf: Union[str, UserDefinedFunction],
        arg_types: Union[List, Tuple],
        *,
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr],
    ) -> bool:
        pass

    # drop methods
    def drop_database(self, database: DatabaseOrStr) -> None:
        db_name = self._parse_database(database)
        self._root.databases[db_name].drop()

    def drop_schema(
        self, schema: SchemaOrStr, *, database: Optional[DatabaseOrStr] = None
    ) -> None:
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)
        self._root.databases[db_name].schemas[schema_name].drop()

    def drop_table(
        self,
        table: Union[str, Table],
        *,
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr] = None,
    ) -> None:
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)
        table_name = table if isinstance(table, str) else table.name

        self._root.databases[db_name].schemas[schema_name].tables[table_name].drop()

    def drop_view(
        self,
        view: Union[str, View],
        *,
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr] = None,
    ) -> None:
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)
        view_name = view if isinstance(view, str) else view.name

        self._root.databases[db_name].schemas[schema_name].views[view_name].drop()

    def drop_function(
        self,
        function: Union[str, Function],
        arg_types: Union[List, Tuple],
        *,
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr] = None,
    ) -> None:
        pass

    def drop_procedure(
        self,
        procedure: Union[str, Procedure],
        arg_types: Union[List, Tuple],
        *,
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr] = None,
    ) -> None:
        pass

    def drop_user_defined_function(
        self,
        udf: Union[str, UserDefinedFunction],
        arg_types: Union[List, Tuple],
        *,
        database: Optional[DatabaseOrStr] = None,
        schema: Optional[SchemaOrStr] = None,
    ) -> None:
        pass
