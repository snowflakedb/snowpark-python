#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Connection factory functions for different DBMS types.

Each function returns a callable that creates a DBAPI 2.0 connection.
This callable is passed to session.read.dbapi().
"""


def create_mysql_connection(host, port, user, password, database, **kwargs):
    """Create MySQL connection factory."""
    import pymysql

    def _connect():
        return pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )

    return _connect


def create_postgres_connection(host, port, user, password, database, **kwargs):
    """Create PostgreSQL connection factory."""
    import psycopg2

    def _connect():
        return psycopg2.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database,
        )

    return _connect


def create_mssql_connection(
    host, port, user, password, database, driver=None, **kwargs
):
    """Create MS SQL Server connection factory."""
    import pyodbc

    driver = driver or "{ODBC Driver 18 for SQL Server}"

    def _connect():
        connection_string = (
            f"DRIVER={driver};"
            f"SERVER={host},{port};"
            f"DATABASE={database};"
            f"UID={user};"
            f"PWD={password};"
            f"TrustServerCertificate=yes;"
        )
        return pyodbc.connect(connection_string)

    return _connect


def create_oracle_connection(host, port, user, password, service_name, **kwargs):
    """Create Oracle connection factory."""
    import oracledb

    def _connect():
        dsn = f"{host}:{port}/{service_name}"
        return oracledb.connect(
            user=user,
            password=password,
            dsn=dsn,
        )

    return _connect


def create_databricks_connection(server_hostname, http_path, access_token, **kwargs):
    """Create Databricks connection factory."""
    from databricks import sql

    def _connect():
        return sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token,
        )

    return _connect


# Registry mapping DBMS names to connection factory functions
CONNECTION_FACTORIES = {
    "mysql": create_mysql_connection,
    "postgres": create_postgres_connection,
    "postgresql": create_postgres_connection,
    "mssql": create_mssql_connection,
    "sqlserver": create_mssql_connection,
    "oracle": create_oracle_connection,
    "databricks": create_databricks_connection,
    "dbx": create_databricks_connection,
}


def get_connection_factory(dbms_type, params):
    """
    Get connection factory for a given DBMS type.

    Args:
        dbms_type: Type of DBMS (mysql, postgres, mssql, oracle, databricks)
        params: Dict of connection parameters

    Returns:
        Callable that creates a DBAPI connection
    """
    dbms_type = dbms_type.lower()

    if dbms_type not in CONNECTION_FACTORIES:
        raise ValueError(
            f"Unknown DBMS type: {dbms_type}. "
            f"Supported: {', '.join(CONNECTION_FACTORIES.keys())}"
        )

    factory_func = CONNECTION_FACTORIES[dbms_type]
    return factory_func(**params)


def get_jdbc_url(dbms_type, params):
    """
    Generate JDBC connection URL from database parameters.

    Args:
        dbms_type: Type of DBMS (mysql, postgres, mssql, oracle, databricks)
        params: Dict of connection parameters

    Returns:
        JDBC connection URL string
    """
    dbms_type = dbms_type.lower()

    if dbms_type == "mysql":
        return f"jdbc:mysql://{params['host']}:{params['port']}/{params['database']}"

    elif dbms_type in ("postgres", "postgresql"):
        return (
            f"jdbc:postgresql://{params['host']}:{params['port']}/{params['database']}"
        )

    elif dbms_type in ("mssql", "sqlserver"):
        return f"jdbc:sqlserver://{params['host']}:{params['port']};databaseName={params['database']}"

    elif dbms_type == "oracle":
        return f"jdbc:oracle:thin:@//{params['host']}:{params['port']}/{params['service_name']}"

    elif dbms_type in ("databricks", "dbx"):
        return f"jdbc:databricks://{params['server_hostname']};httpPath={params['http_path']}"

    else:
        raise ValueError(f"Unknown DBMS type for JDBC: {dbms_type}")


def get_jdbc_driver_class(dbms_type):
    """
    Get JDBC driver class name for a given DBMS type.

    Args:
        dbms_type: Type of DBMS (mysql, postgres, mssql, oracle, databricks)

    Returns:
        JDBC driver class name string
    """
    # Support both direct execution and module import
    try:
        from . import config
    except ImportError:
        import config

    dbms_type = dbms_type.lower()

    if dbms_type not in config.JDBC_DRIVER_CLASSES:
        raise ValueError(
            f"Unknown DBMS type for JDBC driver class: {dbms_type}. "
            f"Supported: {', '.join(config.JDBC_DRIVER_CLASSES.keys())}"
        )

    return config.JDBC_DRIVER_CLASSES[dbms_type]
