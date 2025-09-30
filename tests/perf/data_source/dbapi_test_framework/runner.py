#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Test runner with 4 ingestion methods.

Each method measures timing and prints results.
"""

import time
from snowflake.snowpark import Session
from .connections import get_connection_factory
from . import config


def run_local_ingestion(session, create_connection, table, target_table, dbapi_params):
    """
    Method 1: Local ingestion using session.read.dbapi()

    Data is fetched locally and uploaded to Snowflake.
    """
    print(f"\n{'='*60}")
    print("Running: LOCAL INGESTION")
    print(f"{'='*60}")

    start_time = time.time()

    # Read from source database
    df = session.read.dbapi(
        create_connection=create_connection, table=table, **dbapi_params
    )

    # Write to Snowflake
    df.write.save_as_table(target_table, mode="overwrite")

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"✓ Completed in {elapsed:.2f} seconds")
    return elapsed


def run_udtf_ingestion(
    session, create_connection, table, target_table, dbapi_params, udtf_configs
):
    """
    Method 2: UDTF ingestion using session.read.dbapi() with udtf_configs

    Data is fetched via UDTF running on Snowflake.
    """
    print(f"\n{'='*60}")
    print("Running: UDTF INGESTION")
    print(f"{'='*60}")

    start_time = time.time()

    # Read using UDTF
    df = session.read.dbapi(
        create_connection=create_connection,
        table=table,
        udtf_configs=udtf_configs,
        **dbapi_params,
    )

    # Write to Snowflake
    df.write.save_as_table(target_table, mode="overwrite")

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"✓ Completed in {elapsed:.2f} seconds")
    return elapsed


def run_local_ingestion_in_sproc(
    session, create_connection, table, target_table, dbapi_params
):
    """
    Method 3: Local ingestion inside a stored procedure

    The local ingestion logic runs inside a Snowflake stored procedure.
    """
    print(f"\n{'='*60}")
    print("Running: LOCAL INGESTION IN STORED PROCEDURE")
    print(f"{'='*60}")

    start_time = time.time()

    # Define the ingestion function
    def ingestion_sproc(
        _session: Session,
        create_connection_func,
        table_name: str,
        target: str,
        params: dict,
    ):
        df = _session.read.dbapi(
            create_connection=create_connection_func, table=table_name, **params
        )
        df.write.save_as_table(target, mode="overwrite")
        return "Success"

    # Register as stored procedure
    sproc = session.sproc.register(
        func=ingestion_sproc,
        name="temp_local_ingestion_sproc",
        replace=True,
        is_permanent=False,
    )

    # Call the stored procedure
    result = sproc(create_connection, table, target_table, dbapi_params)

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"✓ Completed in {elapsed:.2f} seconds")
    print(f"  Result: {result}")
    return elapsed


def run_udtf_ingestion_in_sproc(
    session, create_connection, table, target_table, dbapi_params, udtf_configs
):
    """
    Method 4: UDTF ingestion inside a stored procedure

    The UDTF ingestion logic runs inside a Snowflake stored procedure.
    """
    print(f"\n{'='*60}")
    print("Running: UDTF INGESTION IN STORED PROCEDURE")
    print(f"{'='*60}")

    start_time = time.time()

    # Define the ingestion function
    def ingestion_sproc(
        _session: Session,
        create_connection_func,
        table_name: str,
        target: str,
        params: dict,
        udtf_cfg: dict,
    ):
        df = _session.read.dbapi(
            create_connection=create_connection_func,
            table=table_name,
            udtf_configs=udtf_cfg,
            **params,
        )
        df.write.save_as_table(target, mode="overwrite")
        return "Success"

    # Register as stored procedure
    sproc = session.sproc.register(
        func=ingestion_sproc,
        name="temp_udtf_ingestion_sproc",
        replace=True,
        is_permanent=False,
    )

    # Call the stored procedure
    result = sproc(create_connection, table, target_table, dbapi_params, udtf_configs)

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"✓ Completed in {elapsed:.2f} seconds")
    print(f"  Result: {result}")
    return elapsed


def run_test(test_config):
    """
    Run a single test based on configuration.

    Args:
        test_config: Dict with keys:
            - dbms: database type
            - table: source table name
            - ingestion_method: 'local', 'udtf', 'local_sproc', 'udtf_sproc'
            - dbapi_params: optional DBAPI parameters override
            - udtf_configs: optional UDTF configs override

    Returns:
        Elapsed time in seconds
    """
    dbms = test_config["dbms"]
    table = test_config["table"]
    method = test_config["ingestion_method"]

    print(f"\n{'#'*60}")
    print(f"TEST: {dbms.upper()} - {method.upper()}")
    print(f"Source Table: {table}")
    print(f"{'#'*60}")

    # Get connection parameters based on DBMS type
    dbms_params_map = {
        "mysql": config.MYSQL_PARAMS,
        "postgres": config.POSTGRES_PARAMS,
        "postgresql": config.POSTGRES_PARAMS,
        "mssql": config.MSSQL_PARAMS,
        "sqlserver": config.MSSQL_PARAMS,
        "oracle": config.ORACLE_PARAMS,
        "databricks": config.DATABRICKS_PARAMS,
        "dbx": config.DATABRICKS_PARAMS,
    }

    dbms_params = dbms_params_map.get(dbms.lower())
    if not dbms_params:
        raise ValueError(f"Unknown DBMS: {dbms}")

    # Create connection factory
    create_connection = get_connection_factory(dbms, dbms_params)

    # Get DBAPI and UDTF parameters
    dbapi_params = test_config.get("dbapi_params", config.DBAPI_PARAMS.copy())

    # Get UDTF configs for the specific DBMS
    dbms_key = dbms.lower()
    if "udtf_configs" in test_config:
        udtf_configs = test_config["udtf_configs"]
    elif dbms_key in config.UDTF_CONFIGS:
        udtf_configs = config.UDTF_CONFIGS[dbms_key].copy()
    else:
        # Fallback to generic if DBMS not found
        udtf_configs = {
            "external_access_integration": "YOUR_EXTERNAL_ACCESS_INTEGRATION"
        }

    # Create Snowflake session
    session = Session.builder.configs(config.SNOWFLAKE_PARAMS).create()

    try:
        # Generate target table name
        target_table = f"TEST_{dbms.upper()}_{method.upper()}_{int(time.time())}"

        # Run appropriate ingestion method
        if method == "local":
            elapsed = run_local_ingestion(
                session, create_connection, table, target_table, dbapi_params
            )

        elif method == "udtf":
            elapsed = run_udtf_ingestion(
                session,
                create_connection,
                table,
                target_table,
                dbapi_params,
                udtf_configs,
            )

        elif method == "local_sproc":
            elapsed = run_local_ingestion_in_sproc(
                session, create_connection, table, target_table, dbapi_params
            )

        elif method == "udtf_sproc":
            elapsed = run_udtf_ingestion_in_sproc(
                session,
                create_connection,
                table,
                target_table,
                dbapi_params,
                udtf_configs,
            )

        else:
            raise ValueError(f"Unknown ingestion method: {method}")

        return {
            "dbms": dbms,
            "method": method,
            "table": table,
            "target_table": target_table,
            "elapsed_time": elapsed,
            "status": "success",
        }

    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        return {
            "dbms": dbms,
            "method": method,
            "table": table,
            "elapsed_time": None,
            "status": "failed",
            "error": str(e),
        }

    finally:
        session.close()


def run_test_matrix(test_matrix):
    """
    Run multiple tests from a test matrix.

    Args:
        test_matrix: List of test configurations

    Returns:
        List of test results
    """
    results = []

    print(f"\n{'='*60}")
    print(f"RUNNING TEST MATRIX: {len(test_matrix)} tests")
    print(f"{'='*60}")

    for i, test_config in enumerate(test_matrix, 1):
        print(f"\n\nTest {i}/{len(test_matrix)}")
        result = run_test(test_config)
        results.append(result)

    # Print summary
    print(f"\n\n{'='*60}")
    print("TEST SUMMARY")
    print(f"{'='*60}")

    for result in results:
        status_symbol = "✓" if result["status"] == "success" else "✗"
        time_str = f"{result['elapsed_time']:.2f}s" if result["elapsed_time"] else "N/A"
        print(
            f"{status_symbol} {result['dbms']:12} {result['method']:15} {time_str:>10}"
        )

    successful = sum(1 for r in results if r["status"] == "success")
    print(
        f"\nTotal: {len(results)} | Success: {successful} | Failed: {len(results) - successful}"
    )

    return results
