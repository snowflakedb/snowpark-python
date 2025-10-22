#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from ctypes import ArgumentError
import re
from typing import List, Optional, Union, TYPE_CHECKING

from snowflake.snowpark._internal.analyzer.analyzer_utils import unquote_if_quoted
from snowflake.snowpark.exceptions import SnowparkSQLException, NotFoundError

try:
    from snowflake.core.database import Database  # type: ignore
    from snowflake.core.database._generated.models import Database as ModelDatabase  # type: ignore
    from snowflake.core.procedure import Procedure
    from snowflake.core.schema import Schema  # type: ignore
    from snowflake.core.schema._generated.models import ModelSchema  # type: ignore
    from snowflake.core.table import Table, TableColumn
    from snowflake.core.user_defined_function import UserDefinedFunction
    from snowflake.core.view import View
except ImportError as e:
    raise ImportError(
        "Missing optional dependency: 'snowflake.core'."
    ) from e  # pragma: no cover

from snowflake.snowpark._internal.type_utils import (
    convert_sp_to_sf_type,
    type_string_to_type_object,
)
from snowflake.snowpark.functions import lit, parse_json
from snowflake.snowpark.types import DataType

if TYPE_CHECKING:
    from snowflake.snowpark.session import Session


class Catalog:
    """The Catalog class provides methods to interact with and manage the Snowflake objects.
    It allows users to list, get, and drop various database objects such as databases, schemas, tables,
    views, functions, etc.
    """

    def __init__(self, session: "Session") -> None:  # type: ignore
        self._session = session
        self._python_regex_udf = None

    def _parse_database(
        self,
        database: object,
        model_obj: Optional[
            Union[str, Schema, Table, View, Procedure, UserDefinedFunction]
        ] = None,
    ) -> str:
        if isinstance(model_obj, (Schema, Table, View, Procedure, UserDefinedFunction)):
            db_name = model_obj.database_name
            assert db_name is not None  # pyright
            return db_name

        if isinstance(database, str) and database:
            return database
        if isinstance(database, Database):
            return database.name
        if not database:
            current_database = self._session.get_current_database()
            if current_database is None:
                raise ValueError(
                    "No database detected. Please provide database to proceed."
                )
            return current_database
        raise ValueError(
            f"Unexpected type. Expected str or Database, got '{type(database)}'"
        )

    def _parse_schema(
        self,
        schema: object,
        model_obj: Optional[
            Union[str, Table, View, Procedure, UserDefinedFunction]
        ] = None,
    ) -> str:
        if isinstance(model_obj, (Table, View, Procedure, UserDefinedFunction)):
            schema_name = model_obj.schema_name
            assert schema_name is not None  # pyright
            return schema_name

        if isinstance(schema, str) and schema:
            return schema
        if isinstance(schema, Schema):
            return schema.name
        if not schema:
            current_schema = self._session.get_current_schema()
            if current_schema is None:
                raise ValueError(
                    "No schema detected. Please provide schema to proceed."
                )
            return current_schema
        raise ValueError(
            f"Unexpected type. Expected str or Schema, got '{type(schema)}'"
        )

    def _parse_function_or_procedure(
        self,
        fn: Union[str, Procedure, UserDefinedFunction],
        arg_types: Optional[List[DataType]],
    ) -> str:
        if isinstance(fn, str):
            if arg_types is None:
                raise ValueError(
                    "arg_types must be provided when function/procedure is a string"
                )
            arg_types_str = ", ".join(
                [convert_sp_to_sf_type(arg_type) for arg_type in arg_types]
            )
            return f"{fn}({arg_types_str})"

        arg_types_str = ", ".join(arg.datatype for arg in fn.arguments)
        return f"{fn.name}({arg_types_str})"

    def _initialize_regex_udf(self) -> None:
        with self._session._lock:
            if self._python_regex_udf is not None:
                return

            def python_regex_filter(pattern: str, input: str) -> bool:
                return bool(re.match(pattern, input))

            self._python_regex_udf = self._session.udf.register(python_regex_filter)

    def _list_objects(
        self,
        *,
        object_name: str,
        object_class,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
        pattern: Optional[str] = None,
        like: Optional[str] = None,
    ):
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)

        like_str = f"LIKE '{like}'" if like else ""

        df = self._session.sql(
            f"SHOW AS RESOURCE {object_name} {like_str} IN {db_name}.{schema_name} -- catalog api"
        )
        if pattern:
            # initialize udf
            self._initialize_regex_udf()
            assert self._python_regex_udf is not None  # pyright

            # The result of SHOW AS RESOURCE query is a json string which contains
            # key 'name' to store the name of the object. We parse json for the returned
            # result and apply the filter on name.
            df = df.filter(
                self._python_regex_udf(
                    lit(pattern), parse_json('"As Resource"')["name"]
                )
            )

        return list(map(lambda row: object_class.from_json(row[0]), df.collect()))

    # List methods
    def list_databases(
        self,
        *,
        pattern: Optional[str] = None,
        like: Optional[str] = None,
    ) -> List[Database]:
        """List databases in the current session.

        Args:
            pattern: the python regex pattern of name to match. Defaults to None.
            like: the sql style pattern for name to match. Default to None.
        """
        like_str = f"LIKE '{like}'" if like else ""
        df = self._session.sql(f"SHOW AS RESOURCE DATABASES {like_str}")
        if pattern:
            # initialize udf
            self._initialize_regex_udf()
            assert self._python_regex_udf is not None  # pyright

            # The result of SHOW AS RESOURCE query is a json string which contains
            # key 'name' to store the name of the object. We parse json for the returned
            # result and apply the filter on name.
            df = df.filter(
                self._python_regex_udf(
                    lit(pattern), parse_json('"As Resource"')["name"]
                )
            )

        return list(
            map(
                lambda row: Database._from_model(ModelDatabase.from_json(str(row[0]))),
                df.collect(),
            )
        )

    def list_schemas(
        self,
        *,
        database: Optional[Union[str, Database]] = None,
        pattern: Optional[str] = None,
        like: Optional[str] = None,
    ) -> List[Schema]:
        """List schemas in the current session. If database is provided, list schemas in the
        database, otherwise list schemas in the current database.

        Args:
            database: database name or ``Database`` object. Defaults to None.
            pattern: the python regex pattern of name to match. Defaults to None.
            like: the sql style pattern for name to match. Default to None.
        """
        db_name = self._parse_database(database)
        like_str = f"LIKE '{like}'" if like else ""
        df = self._session.sql(f"SHOW AS RESOURCE SCHEMAS {like_str} IN {db_name}")
        if pattern:
            # initialize udf
            self._initialize_regex_udf()
            assert self._python_regex_udf is not None  # pyright

            # The result of SHOW AS RESOURCE query is a json string which contains
            # key 'name' to store the name of the object. We parse json for the returned
            # result and apply the filter on name.
            df = df.filter(
                self._python_regex_udf(
                    lit(pattern), parse_json('"As Resource"')["name"]
                )
            )

        return list(
            map(
                lambda row: Schema._from_model(ModelSchema.from_json(str(row[0]))),
                df.collect(),
            )
        )

    def list_tables(
        self,
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
        pattern: Optional[str] = None,
        like: Optional[str] = None,
    ) -> List[Table]:
        """List tables in the current session. If database or schema are provided, list tables
        in the given database or schema, otherwise list tables in the current database/schema.

        Args:
            database: database name or ``Database`` object. Defaults to None.
            schema: schema name or ``Schema`` object. Defaults to None.
            pattern: the python regex pattern of name to match. Defaults to None.
            like: the sql style pattern for name to match. Default to None.
        """
        return self._list_objects(
            object_name="TABLES",
            object_class=Table,
            database=database,
            schema=schema,
            pattern=pattern,
            like=like,
        )

    def list_views(
        self,
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
        pattern: Optional[str] = None,
        like: Optional[str] = None,
    ) -> List[View]:
        """List views in the current session. If database or schema are provided, list views
        in the given database or schema, otherwise list views in the current database/schema.

        Args:
            database: database name or ``Database`` object. Defaults to None.
            schema: schema name or ``Schema`` object. Defaults to None.
            pattern: the python regex pattern of name to match. Defaults to None.
            like: the sql style pattern for name to match. Default to None.
        """
        return self._list_objects(
            object_name="VIEWS",
            object_class=View,
            database=database,
            schema=schema,
            pattern=pattern,
            like=like,
        )

    def list_columns(
        self,
        table_name: Union[str, Table],
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
    ) -> List[TableColumn]:
        """List columns in the given table.

        Args:
            table_name: table name.
            database: database name or ``Database`` object. Defaults to None.
            schema: schema name or ``Schema`` object. Defaults to None.
        """
        if isinstance(table_name, str):
            table = self.get_table(table_name, database=database, schema=schema)
        else:
            table = table_name
        cols = table.columns
        assert cols is not None
        return cols

    def list_procedures(
        self,
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
        pattern: Optional[str] = None,
        like: Optional[str] = None,
    ) -> List[Procedure]:
        """List of procedures in the given database and schema. If database or schema are not
        provided, list procedures in the current database and schema.

        Args:
            database: database name or ``Database`` object. Defaults to None.
            schema: schema name or ``Schema`` object. Defaults to None.
            pattern: the python regex pattern of name to match. Defaults to None.
            like: the sql style pattern for name to match. Default to None.
        """
        return self._list_objects(
            object_name="PROCEDURES",
            object_class=Procedure,
            database=database,
            schema=schema,
            pattern=pattern,
            like=like,
        )

    def list_user_defined_functions(
        self,
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
        pattern: Optional[str] = None,
        like: Optional[str] = None,
    ) -> List[UserDefinedFunction]:
        """List of user defined functions in the given database and schema. If database or schema
        are not provided, list user defined functions in the current database and schema.
        Args:
            database: database name or ``Database`` object. Defaults to None.
            schema: schema name or ``Schema`` object. Defaults to None.
            pattern: the python regex pattern of name to match. Defaults to None.
            like: the sql style pattern for name to match. Default to None.
        """
        return self._list_objects(
            object_name="USER FUNCTIONS",
            object_class=UserDefinedFunction,
            database=database,
            schema=schema,
            pattern=pattern,
            like=like,
        )

    # get methods
    def get_current_database(self) -> Optional[str]:
        """Get the current database."""
        return self._session.get_current_database()

    def get_current_schema(self) -> Optional[str]:
        """Get the current schema."""
        return self._session.get_current_schema()

    def get_database(self, database: str) -> Database:
        """Name of the database to get"""
        try:
            return self.list_databases(like=unquote_if_quoted(database))[0]
        except IndexError:
            raise NotFoundError(f"Database with name {database} could not be found")

    def get_schema(
        self, schema: str, *, database: Optional[Union[str, Database]] = None
    ) -> Schema:
        """Name of the schema to get."""
        db_name = self._parse_database(database)
        try:
            return self.list_schemas(database=db_name, like=unquote_if_quoted(schema))[
                0
            ]
        except (
            IndexError,  # schema with this name doesn't exist
            SnowparkSQLException,  # database in which we are looking doesn't exist
        ):
            raise NotFoundError(
                f"Schema with name {schema} could not be found in database '{db_name}'"
            )

    def get_table(
        self,
        table_name: str,
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
    ) -> Table:
        """Get the table by name in given database and schema. If database or schema are not
        provided, get the table in the current database and schema.

        Args:
            table_name: name of the table.
            database: database name or ``Database`` object. Defaults to None.
            schema: schema name or ``Schema`` object. Defaults to None.
        """
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)
        try:
            return self.listTables(
                database=db_name,
                schema=schema_name,
                like=unquote_if_quoted(table_name),
            )[0]
        except IndexError:
            raise NotFoundError(
                f"Table with name {table_name} could not be found in schema '{db_name}.{schema_name}'"
            )

    def get_view(
        self,
        view_name: str,
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
    ) -> View:
        """Get the view by name in given database and schema. If database or schema are not
        provided, get the view in the current database and schema.

        Args:
            view_name: name of the view.
            database: database name or ``Database`` object. Defaults to None.
            schema: schema name or ``Schema`` object. Defaults to None.
        """
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)
        try:
            return self.list_views(
                database=db_name,
                schema=schema_name,
                like=unquote_if_quoted(view_name),
            )[0]
        except IndexError:
            raise NotFoundError(
                f"View with name {view_name} could not be found in schema '{db_name}.{schema_name}'"
            )

    def get_procedure(
        self,
        procedure_name: str,
        arg_types: List[DataType],
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
    ) -> Procedure:
        """Get the procedure by name and argument types in given database and schema. If database or
        schema are not provided, get the procedure in the current database and schema.

        Args:
            procedure_name: name of the procedure.
            arg_types: list of argument types to uniquely identify the procedure.
            database: database name or ``Database`` object. Defaults to None.
            schema: schema name or ``Schema`` object. Defaults to None.
        """
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)
        procedure_id = self._parse_function_or_procedure(procedure_name, arg_types)

        try:
            procedures = self._session.sql(
                f"DESCRIBE AS RESOURCE PROCEDURE {db_name}.{schema_name}.{procedure_id}"
            ).collect()
            return Procedure.from_json(str(procedures[0][0]))
        except (
            IndexError,  # when sql returned no results
            SnowparkSQLException,  # when database, or schema doesn't exist
        ):
            raise NotFoundError(
                f"Procedure with name {procedure_name} and arguments {arg_types} could not be found in schema '{db_name}.{schema_name}'"
            )

    def get_user_defined_function(
        self,
        udf_name: str,
        arg_types: List[DataType],
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
    ) -> UserDefinedFunction:
        """Get the user defined function by name and argument types in given database and schema.
        If database or schema are not provided, get the user defined function in the current
        database and schema.

        Args:
            udf_name: name of the user defined function.
            arg_types: list of argument types to uniquely identify the user defined function.
            database: database name or ``Database`` object. Defaults to None.
            schema: schema name or ``Schema`` object. Defaults to None.
        """
        db_name = self._parse_database(database)
        schema_name = self._parse_schema(schema)
        function_id = self._parse_function_or_procedure(udf_name, arg_types)

        try:
            procedures = self._session.sql(
                f"DESCRIBE AS RESOURCE FUNCTION {db_name}.{schema_name}.{function_id}"
            ).collect()
            return UserDefinedFunction.from_json(str(procedures[0][0]))
        except (
            IndexError,  # when sql returned no results
            SnowparkSQLException,  # when database, or schema doesn't exist
        ):
            raise NotFoundError(
                f"Function with name {udf_name} and arguments {arg_types} could not be found in schema '{db_name}.{schema_name}'"
            )

    # set methods
    def set_current_database(self, database: Union[str, Database]) -> None:
        """Set the current default database for the session.

        Args:
            database: database name or ``Database`` object.
        """
        db_name = self._parse_database(database)
        self._session.use_database(db_name)

    def set_current_schema(self, schema: Union[str, Schema]) -> None:
        """Set the current default schema for the session.

        Args:
            schema: schema name or ``Schema`` object.
        """
        schema_name = self._parse_schema(schema)
        self._session.use_schema(schema_name)

    # exists methods
    def database_exists(self, database: Union[str, Database]) -> bool:
        """Check if the given database exists.

        Args:
            database: database name or ``Database`` object.
        """
        db_name = self._parse_database(database)
        try:
            self.get_database(db_name)
            return True
        except NotFoundError:
            return False

    def schema_exists(
        self,
        schema: Union[str, Schema],
        *,
        database: Optional[Union[str, Database]] = None,
    ) -> bool:
        """Check if the given schema exists in the given database. If database is not provided,
        check if the schema exists in the current database.

        Args:
            schema: schema name or ``Schema`` object.
            database: database name or ``Database`` object. Defaults to None.
        """
        db_name = self._parse_database(database, schema)
        schema_name = self._parse_schema(schema)
        try:
            self.get_schema(schema=schema_name, database=db_name)
            return True
        except NotFoundError:
            return False

    def table_exists(
        self,
        table: Union[str, Table],
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
    ) -> bool:
        """Check if the given table exists in the given database and schema. If database or schema
        are not provided, check if the table exists in the current database and schema.

        Args:
            table: table name or ``Table`` object.
            database: database name or ``Database`` object. Defaults to None.
            schema: schema name or ``Schema`` object. Defaults to None.
        """
        db_name = self._parse_database(database, table)
        schema_name = self._parse_schema(schema, table)
        table_name = table if isinstance(table, str) else table.name
        try:
            self.get_table(table_name=table_name, database=db_name, schema=schema_name)
            return True
        except NotFoundError:
            return False

    def view_exists(
        self,
        view: Union[str, View],
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
    ) -> bool:
        """Check if the given view exists in the given database and schema. If database or schema
        are not provided, check if the view exists in the current database and schema.

        Args:
            view: view name or ``View`` object.
            database: database name or ``Database`` object. Defaults to None.
            schema: schema name or ``Schema`` object. Defaults to None.
        """
        db_name = self._parse_database(database, view)
        schema_name = self._parse_schema(schema, view)
        view_name = view if isinstance(view, str) else view.name
        try:
            self.get_view(view_name=view_name, database=db_name, schema=schema_name)
            return True
        except NotFoundError:
            return False

    def procedure_exists(
        self,
        procedure: Union[str, Procedure],
        arg_types: Optional[List[DataType]] = None,
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
    ) -> bool:
        """Check if the given procedure exists in the given database and schema. If database or
        schema are not provided, check if the procedure exists in the current database and schema.

        Args:
            procedure: procedure name or ``Procedure`` object.
            arg_types: list of argument types to uniquely identify the procedure. Defaults to None.
            database: database name or ``Database`` object. Defaults to None.
            schema: schema name or ``Schema`` object. Defaults to None.
        """
        try:
            if isinstance(procedure, Procedure):
                if arg_types is not None or database is not None or schema is not None:
                    raise ArgumentError(
                        "When provided procedure is a Procedure class no other arguments can be provided"
                    )
                database = procedure.database_name
                schema = procedure.schema_name
                arg_types = [
                    type_string_to_type_object(a.datatype) for a in procedure.arguments
                ]
                procedure = procedure.name
            self.get_procedure(
                procedure_name=procedure,
                arg_types=arg_types,
                database=database,
                schema=schema,
            )
            return True
        except NotFoundError:
            return False

    def user_defined_function_exists(
        self,
        udf: Union[str, UserDefinedFunction],
        arg_types: Optional[List[DataType]] = None,
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
    ) -> bool:
        """Check if the given user defined function exists in the given database and schema. If
        database or schema are not provided, check if the user defined function exists in the
        current database and schema.

        Args:
            udf: user defined function name or ``UserDefinedFunction`` object.
            arg_types: list of argument types to uniquely identify the user defined function.
                Defaults to None.
            database: database name or ``Database`` object. Defaults to None.
            schema: schema name or ``Schema`` object. Defaults to None.
        """
        try:
            if isinstance(udf, UserDefinedFunction):
                if arg_types is not None or database is not None or schema is not None:
                    raise ArgumentError(
                        "When provided udf is a UserDefinedFunction class no other arguments can be provided"
                    )
                database = udf.database_name
                schema = udf.schema_name
                arg_types = [
                    type_string_to_type_object(a.datatype) for a in udf.arguments
                ]
                udf = udf.name
            self.get_user_defined_function(
                udf_name=udf,
                arg_types=arg_types,
                database=database,
                schema=schema,
            )
            return True
        except NotFoundError:
            return False

    # drop methods
    def drop_database(self, database: Union[str, Database]) -> None:
        """Drop the given database.

        Args:
            database: database name or ``Database`` object.
        """
        db_name = self._parse_database(database)
        self._session.sql(f"DROP DATABASE {db_name}").collect()

    def drop_schema(
        self,
        schema: Union[str, Schema],
        *,
        database: Optional[Union[str, Database]] = None,
    ) -> None:
        """Drop the given schema in the given database. If database is not provided, drop the
        schema in the current database.

        Args:
            schema: schema name or ``Schema`` object.
            database: database name or ``Database`` object. Defaults to None.
        """
        db_name = self._parse_database(database, schema)
        schema_name = self._parse_schema(schema)
        self._session.sql(f"DROP SCHEMA {db_name}.{schema_name}").collect()

    def drop_table(
        self,
        table: Union[str, Table],
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
    ) -> None:
        """Drop the given table in the given database and schema. If database or schema are not
        provided, drop the table in the current database and schema.

        Args:
            table: table name or ``Table`` object.
            database: database name or ``Database`` object. Defaults to None.
            schema: schema name or ``Schema`` object. Defaults to None.
        """
        db_name = self._parse_database(database, table)
        schema_name = self._parse_schema(schema, table)
        table_name = table if isinstance(table, str) else table.name

        self._session.sql(f"DROP TABLE {db_name}.{schema_name}.{table_name}").collect()

    def drop_view(
        self,
        view: Union[str, View],
        *,
        database: Optional[Union[str, Database]] = None,
        schema: Optional[Union[str, Schema]] = None,
    ) -> None:
        """Drop the given view in the given database and schema. If database or schema are not
        provided, drop the view in the current database and schema.

        Args:
            view: view name or ``View`` object.
            database: database name or ``Database`` object. Defaults to None.
            schema: schema name or ``Schema`` object. Defaults to None.
        """
        db_name = self._parse_database(database, view)
        schema_name = self._parse_schema(schema, view)
        view_name = view if isinstance(view, str) else view.name

        self._session.sql(f"DROP VIEW {db_name}.{schema_name}.{view_name}").collect()

    # aliases
    listDatabases = list_databases
    listSchemas = list_schemas
    listTables = list_tables
    listViews = list_views
    listColumns = list_columns
    listProcedures = list_procedures
    listUserDefinedFunctions = list_user_defined_functions

    getCurrentDatabase = get_current_database
    getCurrentSchema = get_current_schema
    getDatabase = get_database
    getSchema = get_schema
    getTable = get_table
    getView = get_view
    getProcedure = get_procedure
    getUserDefinedFunction = get_user_defined_function

    setCurrentDatabase = set_current_database
    setCurrentSchema = set_current_schema

    databaseExists = database_exists
    schemaExists = schema_exists
    tableExists = table_exists
    viewExists = view_exists
    procedureExists = procedure_exists
    userDefinedFunctionExists = user_defined_function_exists

    dropDatabase = drop_database
    dropSchema = drop_schema
    dropTable = drop_table
    dropView = drop_view
