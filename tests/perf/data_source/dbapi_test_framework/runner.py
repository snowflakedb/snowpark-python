#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Test runner with 4 ingestion methods.

Each method measures timing and prints results.
"""

import time
from snowflake.snowpark import Session

# Support both direct execution and module import
try:
    from .connections import get_connection_factory
    from . import config
except ImportError:
    from connections import get_connection_factory
    import config


def run_local_ingestion(
    session, create_connection, source_type, source_value, target_table, dbapi_params
):
    """
    Method 1: Local ingestion using session.read.dbapi()

    Data is fetched locally and uploaded to Snowflake.
    Args:
        source_type: "table" or "query"
        source_value: Table name or SQL query string
    """
    print(f"\n{'='*60}")
    print("Running: LOCAL INGESTION")
    print(f"{'='*60}")

    start_time = time.time()

    # Build source kwargs: {source_type: source_value}
    # e.g., {"table": "DBAPI_TEST_TABLE"} or {"query": "SELECT * FROM ..."}
    source_kwargs = {source_type: source_value}

    # Read from source database
    df = session.read.dbapi(
        create_connection=create_connection, **source_kwargs, **dbapi_params
    )

    # Write to Snowflake
    df.write.save_as_table(target_table, mode="overwrite")

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"✓ Completed in {elapsed:.2f} seconds")
    return elapsed


def run_udtf_ingestion(
    session,
    create_connection,
    source_type,
    source_value,
    target_table,
    dbapi_params,
    udtf_configs,
):
    """
    Method 2: UDTF ingestion using session.read.dbapi() with udtf_configs

    Data is fetched via UDTF running on Snowflake.
    Args:
        source_type: "table" or "query"
        source_value: Table name or SQL query string
    """
    print(f"\n{'='*60}")
    print("Running: UDTF INGESTION")
    print(f"{'='*60}")

    start_time = time.time()

    # Build source kwargs
    source_kwargs = {source_type: source_value}

    # Read using UDTF
    df = session.read.dbapi(
        create_connection=create_connection,
        udtf_configs=udtf_configs,
        **source_kwargs,
        **dbapi_params,
    )

    # Write to Snowflake
    df.write.save_as_table(target_table, mode="overwrite")

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"✓ Completed in {elapsed:.2f} seconds")
    return elapsed


def run_local_ingestion_in_sproc(
    session,
    create_connection,
    source_type,
    source_value,
    target_table,
    dbapi_params,
    udtf_configs,
    dbms,
):
    """
    Method 3: Local ingestion inside a stored procedure

    The local ingestion logic runs inside a Snowflake stored procedure.
    Args:
        source_type: "table" or "query"
        source_value: Table name or SQL query string
        udtf_configs: External access integration configs (needed for sproc to access external DB)
        dbms: DBMS type (for getting correct packages)
    """
    print(f"\n{'='*60}")
    print("Running: LOCAL INGESTION IN STORED PROCEDURE")
    print(f"{'='*60}")

    source_dict = {source_type: source_value}
    params = dbapi_params
    target = target_table
    external_access_integrations = [udtf_configs.get("external_access_integration")]
    packages = config.SPROC_PACKAGES.get(dbms.lower(), [])

    # Define the ingestion function
    def ingestion_sproc(
        _session: Session,
    ):
        df = _session.read.dbapi(
            create_connection=create_connection, **source_dict, **params
        )
        df.write.save_as_table(target, mode="overwrite")
        return "Success"

    # Register as stored procedure with external access integration
    from snowflake.snowpark.types import StringType

    sproc = session.sproc.register(
        func=ingestion_sproc,
        name="temp_local_ingestion_sproc",
        return_type=StringType(),
        input_types=None,
        replace=True,
        is_permanent=False,
        packages=packages,
        external_access_integrations=external_access_integrations,
    )

    start_time = time.time()

    # Call the stored procedure
    result = sproc()

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"✓ Completed in {elapsed:.2f} seconds")
    print(f"  Result: {result}")
    return elapsed


def run_udtf_ingestion_in_sproc(
    session,
    create_connection,
    source_type,
    source_value,
    target_table,
    dbapi_params,
    udtf_configs,
    dbms,
):
    """
    Method 4: UDTF ingestion inside a stored procedure

    The UDTF ingestion logic runs inside a Snowflake stored procedure.
    Args:
        source_type: "table" or "query"
        source_value: Table name or SQL query string
        udtf_configs: External access integration configs
        dbms: DBMS type (for getting correct packages)
    """
    print(f"\n{'='*60}")
    print("Running: UDTF INGESTION IN STORED PROCEDURE")
    print(f"{'='*60}")

    source_dict = {source_type: source_value}
    external_access_integrations = [udtf_configs.get("external_access_integration")]
    packages = config.SPROC_PACKAGES.get(dbms.lower(), [])

    # Define the ingestion function
    def ingestion_sproc(
        _session: Session,
    ):
        df = _session.read.dbapi(
            create_connection=create_connection,
            udtf_configs=udtf_configs,
            **source_dict,
            **dbapi_params,
        )
        df.write.save_as_table(target_table, mode="overwrite")
        return "Success"

    # Register as stored procedure with external access integration
    from snowflake.snowpark.types import StringType

    sproc = session.sproc.register(
        func=ingestion_sproc,
        name="temp_udtf_ingestion_sproc",
        return_type=StringType(),
        input_types=None,
        replace=True,
        is_permanent=False,
        packages=packages,
        external_access_integrations=external_access_integrations,
    )

    start_time = time.time()

    # Call the stored procedure
    result = sproc()

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
    method = test_config["ingestion_method"]

    # Get source configuration - supports both old and new format
    if "source" in test_config:
        # New format: {"source": {"type": "table|query", "value": "..."}}
        source_config = test_config["source"]
        source_type = source_config["type"]
        source_value = source_config["value"]
    else:
        # Legacy format: {"table": "..."} or {"query": "..."}
        if "table" in test_config:
            source_type = "table"
            source_value = test_config["table"]
        elif "query" in test_config:
            source_type = "query"
            source_value = test_config["query"]
        else:
            raise ValueError(
                "Test config must specify 'source' or legacy 'table'/'query'"
            )

    if source_type not in ("table", "query"):
        raise ValueError(f"source.type must be 'table' or 'query', got: {source_type}")

    print(f"\n{'#'*60}")
    print(f"TEST: {dbms.upper()} - {method.upper()}")
    print(f"Source Type: {source_type.upper()}")
    print(f"Source Value: {source_value}")
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

    if not udtf_configs:
        raise ValueError(f"UDTF configs not found for {dbms}")

    # Create Snowflake session
    session = Session.builder.configs(config.SNOWFLAKE_PARAMS).create()

    try:
        # Generate target table name
        target_table = f"TEST_{dbms.upper()}_{method.upper()}_{int(time.time())}"

        # Run appropriate ingestion method
        # Pass source_type and source_value separately
        if method == "local":
            elapsed = run_local_ingestion(
                session,
                create_connection,
                source_type,
                source_value,
                target_table,
                dbapi_params,
            )

        elif method == "udtf":
            elapsed = run_udtf_ingestion(
                session,
                create_connection,
                source_type,
                source_value,
                target_table,
                dbapi_params,
                udtf_configs,
            )

        elif method == "local_sproc":
            elapsed = run_local_ingestion_in_sproc(
                session,
                create_connection,
                source_type,
                source_value,
                target_table,
                dbapi_params,
                udtf_configs,
                dbms,
            )

        elif method == "udtf_sproc":
            elapsed = run_udtf_ingestion_in_sproc(
                session,
                create_connection,
                source_type,
                source_value,
                target_table,
                dbapi_params,
                udtf_configs,
                dbms,
            )

        else:
            raise ValueError(f"Unknown ingestion method: {method}")

        # Show target table info if configured
        if config.SHOW_TARGET_TABLE_INFO:
            try:
                print(f"\n{'='*60}")
                print("TARGET TABLE INFO")
                print(f"{'='*60}")

                # Get row count
                row_count = session.table(target_table).count()
                print(f"Row count: {row_count}")

                # Show first row
                print("\nFirst row:")
                session.table(target_table).show(n=1)

            except Exception as info_error:
                print(f"\n⚠ Warning: Could not retrieve table info: {info_error}")

        # Cleanup target table if configured
        if config.CLEANUP_TARGET_TABLES:
            try:
                session.sql(f"DROP TABLE IF EXISTS {target_table}").collect()
                print(f"\n✓ Cleaned up target table: {target_table}")
            except Exception as cleanup_error:
                print(
                    f"\n⚠ Warning: Could not clean up table {target_table}: {cleanup_error}"
                )

        return {
            "dbms": dbms,
            "method": method,
            "source_type": source_type,
            "source_value": source_value,
            "target_table": target_table,
            "elapsed_time": elapsed,
            "status": "success",
        }

    except Exception as e:
        print(f"\n✗ ERROR: {str(e)}")
        # Try cleanup even on failure if configured
        if config.CLEANUP_TARGET_TABLES and "target_table" in locals():
            try:
                session.sql(f"DROP TABLE IF EXISTS {target_table}").collect()
                print(f"\n✓ Cleaned up target table: {target_table}")
            except Exception:
                pass  # Silently ignore cleanup errors on failure

        return {
            "dbms": dbms,
            "method": method,
            "source_type": source_type if "source_type" in locals() else None,
            "source_value": source_value if "source_value" in locals() else None,
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
    print(f"\n\n{'='*80}")
    print("TEST SUMMARY")
    print(f"{'='*80}")
    print(
        f"{'Status':^8} {'DBMS':^12} {'Method':^15} {'Source':^8} {'Value':^25} {'Time':^10}"
    )
    print("-" * 80)

    for result in results:
        status_symbol = "✓" if result["status"] == "success" else "✗"
        time_str = f"{result['elapsed_time']:.2f}s" if result["elapsed_time"] else "N/A"
        source_type = result.get("source_type", "N/A")
        source_value = result.get("source_value", "N/A")

        # Clean up multi-line queries and truncate
        source_value = " ".join(
            source_value.split()
        )  # Collapse whitespace to single spaces
        if len(source_value) > 25:
            source_value = source_value[:22] + "..."

        print(
            f"{status_symbol:^8} {result['dbms']:^12} {result['method']:^15} {source_type:^8} {source_value:^25} {time_str:^10}"
        )

    successful = sum(1 for r in results if r["status"] == "success")
    print("-" * 80)
    print(
        f"Total: {len(results)} | Success: {successful} | Failed: {len(results) - successful}"
    )

    return results
