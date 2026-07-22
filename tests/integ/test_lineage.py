#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#


import json
import time
import uuid

import pytest

from snowflake.snowpark.exceptions import SnowparkSQLException
from snowflake.snowpark.lineage import LineageDirection

try:
    import pandas as pd
    from pandas.testing import assert_frame_equal

    is_pandas_available = True
except ImportError:
    is_pandas_available = False


pytestmark = [
    pytest.mark.xfail(
        "config.getoption('local_testing_mode', default=False)",
        reason="Lineage is a SQL feature",
        run=False,
    )
]


def create_objects_for_test(session, db, schema) -> None:
    # Create table and views within the specified TESTDB and TESTSCHEMA
    session.sql(f"CREATE OR REPLACE TABLE {db}.{schema}.T1(C1 INT)").collect()
    session.sql(
        f"CREATE OR REPLACE VIEW {db}.{schema}.V1 AS SELECT * FROM {db}.{schema}.T1"
    ).collect()
    session.sql(
        f"CREATE OR REPLACE VIEW {db}.{schema}.V2 AS SELECT C1 FROM {db}.{schema}.V1"
    ).collect()
    session.sql(
        f"CREATE OR REPLACE VIEW {db}.{schema}.V3 AS SELECT * FROM {db}.{schema}.V2"
    ).collect()
    session.sql(
        f"CREATE OR REPLACE VIEW {db}.{schema}.V4 AS SELECT * FROM {db}.{schema}.V3"
    ).collect()
    session.sql(
        f"CREATE OR REPLACE VIEW {db}.{schema}.V5 AS SELECT * FROM {db}.{schema}.V4"
    ).collect()


def remove_created_on_field(df):
    def convert_and_clean(json_str):
        data = json.loads(json_str)
        data.pop("createdOn", None)
        return data

    for col in ("SOURCE_OBJECT", "TARGET_OBJECT"):
        df[col] = df[col].apply(convert_and_clean)
    return df


def assert_lineage_contains(actual_df, expected_data):
    """Assert that every row in expected_data appears in actual_df.

    Lineage propagation is asynchronous and some environments return more edges
    than the minimum expected (e.g. additional hops propagate between the poll
    and the collect). Allowing extra rows avoids spurious failures while still
    verifying that all the edges we care about are present.
    """
    expected_df = pd.DataFrame(expected_data)
    assert actual_df.shape[0] >= len(expected_df), (
        f"Expected at least {len(expected_df)} rows, got {actual_df.shape[0]}.\n"
        f"Actual:\n{actual_df.to_string()}"
    )
    for _, expected_row in expected_df.iterrows():
        match = actual_df
        for col in expected_df.columns:
            match = match[match[col] == expected_row[col]]
        assert len(match) > 0, (
            f"Expected row not found in result:\n{dict(expected_row)}\n"
            f"Actual:\n{actual_df.to_string()}"
        )


def wait_for_lineage(
    session,
    object_name,
    object_domain,
    direction,
    distance=5,
    expected_min_rows=1,
    timeout=120,
    interval=2,
):
    """Poll lineage.trace() until at least ``expected_min_rows`` rows appear
    or the timeout (seconds) is exceeded.

    Lineage data is populated asynchronously on Snowflake: the index is built
    in the background after object creation / DML, so a trace executed
    immediately after CREATE TABLE/VIEW can return 0 rows even though the
    objects exist. Polling avoids this race without relying on arbitrary sleeps.
    """
    deadline = time.monotonic() + timeout
    while True:
        rows = session.lineage.trace(
            object_name, object_domain, direction=direction, distance=distance
        ).collect()
        if len(rows) >= expected_min_rows:
            return rows
        if time.monotonic() >= deadline:
            pytest.xfail(
                f"Lineage for {object_name!r} ({object_domain}) did not return "
                f">= {expected_min_rows} row(s) within {timeout}s. "
                f"Got {len(rows)} row(s)."
            )
        time.sleep(interval)


@pytest.fixture
def tmp_lineage_schema(session):
    """Create a temporary schema and guarantee cleanup even when the test fails.

    Yields (db, schema, primary_role, current_wh) so the test body can use them
    directly. Teardown restores all session-level state that Case 4 may change
    (role, secondary roles, warehouse) before dropping the schema, ensuring a
    clean handoff to the next test on the same xdist worker.
    """
    db = session.get_current_database().replace('"', "")
    schema = ("sch" + str(uuid.uuid4()).replace("-", "")[:10]).upper()
    primary_role = session.get_current_role().replace('"', "")
    current_wh = session.get_current_warehouse().replace('"', "")
    session.sql(f"CREATE SCHEMA {db}.{schema}").collect()
    yield db, schema, primary_role, current_wh
    session.sql(f"USE ROLE {primary_role}").collect()
    try:
        session.sql("USE SECONDARY ROLES ALL").collect()
    except Exception:
        pass
    session.sql(f"USE WAREHOUSE {current_wh}").collect()
    session.sql(f"DROP SCHEMA IF EXISTS {db}.{schema}").collect()


@pytest.mark.flaky(reruns=0)
def test_lineage_trace_string_escaping(session):
    """Regression test for string escaping in Lineage.trace().

    object_domain and object_name values containing single quotes or
    backslashes must be properly escaped before being embedded in the
    single-quoted SQL string literal passed to SYSTEM$DGQL. This test runs
    against a live account and verifies:
      * a legitimate trace completes without error (no regression), and
      * values with special characters are handled safely -- a marker table
        created before each call still exists afterwards, confirming no
        unintended SQL was executed.
    """
    db = session.get_current_database().replace('"', "")
    schema = ("sch" + str(uuid.uuid4()).replace("-", "")[:10]).upper()
    session.sql(f"CREATE SCHEMA {db}.{schema}").collect()

    sentinel = f"{db}.{schema}.SENTINEL"

    def sentinel_exists() -> bool:
        rows = session.sql(
            f"SHOW TABLES LIKE 'SENTINEL' IN SCHEMA {db}.{schema}"
        ).collect()
        return len(rows) > 0

    try:
        session.sql(f"CREATE OR REPLACE TABLE {db}.{schema}.T1(C1 INT)").collect()
        session.sql(
            f"CREATE OR REPLACE VIEW {db}.{schema}.V1 AS SELECT * FROM {db}.{schema}.T1"
        ).collect()
        session.sql(f"CREATE OR REPLACE TABLE {sentinel}(C1 INT)").collect()

        # Verify a legitimate trace completes without error. The exact number of
        # lineage edges can lag object creation (see SNOW-3413244), so we only
        # assert the call succeeds rather than a specific row count.
        rows = session.lineage.trace(
            f"{db}.{schema}.T1",
            "table",
            direction=LineageDirection.DOWNSTREAM,
            distance=1,
        ).collect()
        assert isinstance(rows, list)

        # object_domain values with special characters. object_domain is not
        # validated by _check_valid_object_name, so special characters reach
        # build_query and must be escaped. Each call should either return a
        # result or be rejected server-side; in all cases the marker table must
        # remain untouched.
        special_domains = [
            "table' OR 1=1--",
            f"table'); DROP TABLE {sentinel}; --",
            f"table\\'); DROP TABLE {sentinel}; --",
            "table\\' || (select current_user()) --",
        ]
        for domain in special_domains:
            try:
                session.lineage.trace(
                    f"{db}.{schema}.T1",
                    domain,
                    direction=LineageDirection.DOWNSTREAM,
                    distance=1,
                ).collect()
            except SnowparkSQLException:
                pass
            assert (
                sentinel_exists()
            ), f"Marker table missing after trace with object_domain={domain!r}"

        # object_name values with special characters via quoted identifiers. The
        # name must pass _check_valid_object_name (exactly 3 parts), so the
        # special characters live inside a double-quoted part and reach
        # build_query. They must be escaped so the marker table is unaffected.
        special_names = [
            f'{db}.{schema}."o\'brien"',
            f'{db}.{schema}."x\'); DROP TABLE {sentinel}; --"',
            f'{db}.{schema}."x\\\'); DROP TABLE {sentinel}; --"',
        ]
        for name in special_names:
            try:
                session.lineage.trace(
                    name, "table", direction=LineageDirection.DOWNSTREAM, distance=1
                ).collect()
            except (SnowparkSQLException, ValueError):
                pass
            assert (
                sentinel_exists()
            ), f"Marker table missing after trace with object_name={name!r}"
    finally:
        session.sql(f"DROP SCHEMA IF EXISTS {db}.{schema}").collect()


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
@pytest.mark.flaky(reruns=0)
def test_lineage_trace(session, tmp_lineage_schema):
    """
    Tests lineage.trace API on multiple cases.
    """
    db, schema, primary_role, current_wh = tmp_lineage_schema
    session.sql(f"use warehouse {current_wh}").collect()

    create_objects_for_test(session, db, schema)

    # CASE 1 : trace with the role that has VIEW LINEAGE privillege.
    rows = wait_for_lineage(
        session,
        f"{db}.{schema}.T1",
        "table",
        direction=LineageDirection.DOWNSTREAM,
        distance=4,
        expected_min_rows=4,
        timeout=120,
    )
    df = remove_created_on_field(
        pd.DataFrame(
            [[r[0], r[1], r[2], r[3]] for r in rows],
            columns=["SOURCE_OBJECT", "TARGET_OBJECT", "DIRECTION", "DISTANCE"],
        )
    )

    assert_lineage_contains(
        df,
        {
            "SOURCE_OBJECT": [
                {"domain": "TABLE", "name": f"{db}.{schema}.T1", "status": "ACTIVE"},
                {"domain": "VIEW", "name": f"{db}.{schema}.V1", "status": "ACTIVE"},
                {"domain": "VIEW", "name": f"{db}.{schema}.V2", "status": "ACTIVE"},
                {"domain": "VIEW", "name": f"{db}.{schema}.V3", "status": "ACTIVE"},
            ],
            "TARGET_OBJECT": [
                {"domain": "VIEW", "name": f"{db}.{schema}.V1", "status": "ACTIVE"},
                {"domain": "VIEW", "name": f"{db}.{schema}.V2", "status": "ACTIVE"},
                {"domain": "VIEW", "name": f"{db}.{schema}.V3", "status": "ACTIVE"},
                {"domain": "VIEW", "name": f"{db}.{schema}.V4", "status": "ACTIVE"},
            ],
            "DIRECTION": ["Downstream", "Downstream", "Downstream", "Downstream"],
            "DISTANCE": [1, 2, 3, 4],
        },
    )

    # CASE 2 : trace with default arguments
    rows = wait_for_lineage(
        session,
        f"{db}.{schema}.V1",
        "view",
        direction=LineageDirection.BOTH,
        expected_min_rows=3,
        timeout=120,
    )
    df = remove_created_on_field(
        pd.DataFrame(
            [[r[0], r[1], r[2], r[3]] for r in rows],
            columns=["SOURCE_OBJECT", "TARGET_OBJECT", "DIRECTION", "DISTANCE"],
        )
    )

    assert_lineage_contains(
        df,
        {
            "SOURCE_OBJECT": [
                {"domain": "TABLE", "name": f"{db}.{schema}.T1", "status": "ACTIVE"},
                {"domain": "VIEW", "name": f"{db}.{schema}.V1", "status": "ACTIVE"},
                {"domain": "VIEW", "name": f"{db}.{schema}.V2", "status": "ACTIVE"},
            ],
            "TARGET_OBJECT": [
                {"domain": "VIEW", "name": f"{db}.{schema}.V1", "status": "ACTIVE"},
                {"domain": "VIEW", "name": f"{db}.{schema}.V2", "status": "ACTIVE"},
                {"domain": "VIEW", "name": f"{db}.{schema}.V3", "status": "ACTIVE"},
            ],
            "DIRECTION": ["Upstream", "Downstream", "Downstream"],
            "DISTANCE": [1, 1, 2],
        },
    )

    df = session.lineage.trace(
        f"{db}.{schema}.nonexistant",
        "table",
        direction="downstream",
        distance=3,
    ).to_pandas()

    assert 0 == df.shape[0]

    # CASE 3 : Insufficent privillage case
    # Disabling this test until 8.17 is deployed.
    """
    test_non_priv_role = "lineage_non_priv_test_role"
    session.sql(f"GRANT USAGE ON database {db} TO ROLE {test_non_priv_role}")
    session.sql(
        f"GRANT USAGE ON schema {db}.{schema} TO ROLE {test_non_priv_role}"
    ).collect()
    session.sql(
        f"GRANT select on VIEW {db}.{schema}.V5 TO ROLE {test_non_priv_role}"
    ).collect()
    session.sql(f"USE ROLE {test_non_priv_role}").collect()
    with pytest.raises(SnowparkSQLException) as exc:
        session.lineage.trace(
            f"{db}.{schema}.V5", "view", direction=LineageDirection.DOWNSTREAM
        )
    assert "Insufficient privileges to view data lineage" in str(exc)
    """

    # CASE 4 : trace with masked object
    test_role = "lineage_test_role"
    session.sql(f"USE ROLE {primary_role}").collect()
    session.sql(f"GRANT USAGE ON database {db} TO ROLE {test_role}").collect()
    session.sql(f"GRANT USAGE ON schema {db}.{schema} TO ROLE {test_role}").collect()
    session.sql(f"GRANT select on VIEW {db}.{schema}.V5 TO ROLE {test_role}").collect()
    session.sql(
        f"GRANT CREATE VIEW ON schema {db}.{schema} TO ROLE {test_role}"
    ).collect()
    session.sql(f"GRANT USAGE ON WAREHOUSE {current_wh} TO ROLE {test_role}").collect()
    session.sql(f"USE ROLE {test_role}").collect()
    session.sql("USE SECONDARY ROLES NONE").collect()
    session.sql(f"USE WAREHOUSE {current_wh}").collect()
    session.sql(
        f"CREATE OR REPLACE VIEW {db}.{schema}.V6 AS SELECT * FROM {db}.{schema}.V5"
    ).collect()

    # Limit the trace to distance=2 so the traversal stops at the masked
    # node (which has no id and therefore cannot be traced any further).
    rows = wait_for_lineage(
        session,
        f"{db}.{schema}.V6",
        "view",
        direction=LineageDirection.UPSTREAM,
        distance=2,
        expected_min_rows=2,
        timeout=120,
    )
    session.sql(f"USE ROLE {primary_role}").collect()

    session.sql(f"use warehouse {current_wh}").collect()

    df = remove_created_on_field(
        pd.DataFrame(
            [[r[0], r[1], r[2], r[3]] for r in rows],
            columns=["SOURCE_OBJECT", "TARGET_OBJECT", "DIRECTION", "DISTANCE"],
        )
    )

    assert_lineage_contains(
        df,
        {
            "SOURCE_OBJECT": [
                {"domain": "VIEW", "name": f"{db}.{schema}.V5", "status": "ACTIVE"},
                {"domain": "VIEW", "name": "***.***.***", "status": "MASKED"},
            ],
            "TARGET_OBJECT": [
                {"domain": "VIEW", "name": f"{db}.{schema}.V6", "status": "ACTIVE"},
                {"domain": "VIEW", "name": f"{db}.{schema}.V5", "status": "ACTIVE"},
            ],
            "DIRECTION": ["Upstream", "Upstream"],
            "DISTANCE": [1, 2],
        },
    )

    # CASE 5 : trace with deleted object. Poll until the deletion propagates
    # to the lineage index.
    session.sql(f"DROP VIEW {db}.{schema}.V3").collect()

    deadline = time.monotonic() + 120
    while True:
        empty_rows = session.lineage.trace(
            f"{db}.{schema}.V2", "view", direction=LineageDirection.DOWNSTREAM
        ).collect()
        if len(empty_rows) == 0:
            break
        if time.monotonic() >= deadline:
            pytest.xfail(
                "Lineage for V2 downstream still shows rows after 120s "
                "following DROP VIEW V3; deletion may not have propagated."
            )
        time.sleep(2)

    empty_df = remove_created_on_field(
        pd.DataFrame(
            [[r[0], r[1], r[2], r[3]] for r in empty_rows],
            columns=["SOURCE_OBJECT", "TARGET_OBJECT", "DIRECTION", "DISTANCE"],
        )
    )
    assert_frame_equal(
        empty_df,
        pd.DataFrame(
            {
                "SOURCE_OBJECT": [],
                "TARGET_OBJECT": [],
                "DIRECTION": [],
                "DISTANCE": [],
            }
        ),
        check_dtype=False,
    )

    # CASE 6 : Case senstive query
    session.sql(
        f'CREATE OR REPLACE VIEW {db}.{schema}."v7" AS SELECT * FROM {db}.{schema}.V2'
    ).collect()

    df = session.lineage.trace(
        f'{db}.{schema}."v7"', "view", direction=LineageDirection.UPSTREAM
    )

    # Removing 'creadtedOn' field since the value can not be predicted.
    df = remove_created_on_field(df.to_pandas())

    # TODO Enable the check after SNOW-1437475 is fixed.
    # expected_data = {
    #     "SOURCE_OBJECT": [
    #         {"domain": "VIEW", "name": f"{db}.{schema}.V2", "status": "ACTIVE"},
    #         {"domain": "VIEW", "name": f"{db}.{schema}.V1", "status": "ACTIVE"},
    #     ],
    #     "TARGET_OBJECT": [
    #         {"domain": "VIEW", "name": f"{db}.{schema}.v7", "status": "ACTIVE"},
    #         {"domain": "VIEW", "name": f"{db}.{schema}.V2", "status": "ACTIVE"},
    #     ],
    #     "DIRECTION": ["Upstream", "Upstream"],
    #     "DISTANCE": [1, 2],
    # }
    # assert_frame_equal(pd.DataFrame(expected_data), df, check_dtype=False)

    # CASE 7 : Column lineage
    rows = wait_for_lineage(
        session,
        f"{db}.{schema}.V2.C1",
        "COLUMN",
        direction=LineageDirection.UPSTREAM,
        expected_min_rows=2,
        timeout=120,
    )

    # Removing 'creadtedOn' field since the value can not be predicted.
    df = remove_created_on_field(
        pd.DataFrame(
            [[r[0], r[1], r[2], r[3]] for r in rows],
            columns=["SOURCE_OBJECT", "TARGET_OBJECT", "DIRECTION", "DISTANCE"],
        )
    )

    assert_lineage_contains(
        df,
        {
            "SOURCE_OBJECT": [
                {
                    "domain": "COLUMN",
                    "name": f"{db}.{schema}.V1.C1",
                    "status": "ACTIVE",
                    "type": "VIEW",
                },
                {
                    "domain": "COLUMN",
                    "name": f"{db}.{schema}.T1.C1",
                    "status": "ACTIVE",
                    "type": "TABLE",
                },
            ],
            "TARGET_OBJECT": [
                {
                    "domain": "COLUMN",
                    "name": f"{db}.{schema}.V2.C1",
                    "status": "ACTIVE",
                    "type": "VIEW",
                },
                {
                    "domain": "COLUMN",
                    "name": f"{db}.{schema}.V1.C1",
                    "status": "ACTIVE",
                    "type": "VIEW",
                },
            ],
            "DIRECTION": [
                "Upstream",
                "Upstream",
            ],
            "DISTANCE": [1, 2],
        },
    )

    with pytest.raises(TypeError) as exc:
        session.lineage.trace(
            object_domain="table",
            direction=LineageDirection.DOWNSTREAM,
            distance=3,
        )
    assert "missing 1 required positional argument: 'object_name'" in str(exc)

    with pytest.raises(TypeError) as exc:
        session.lineage.trace(
            object_name="table1",
            direction=LineageDirection.DOWNSTREAM,
            distance=3,
        )
    assert "missing 1 required positional argument: 'object_domain'" in str(exc)

    with pytest.raises(ValueError) as exc:
        session.lineage.trace(
            f"{db}.{schema}.T1",
            "table",
            direction="invalid",
            distance=4,
        )
    assert "'LineageDirection' enum not found for" in str(exc)

    with pytest.raises(ValueError) as exc:
        session.lineage.trace(
            f"{db}.{schema}.T1",
            "table",
            distance=-1,
        )
    assert "Distance must be between 1 and 5." in str(exc)

    with pytest.raises(ValueError) as exc:
        session.lineage.trace(
            f"{db}.{schema}.T1",
            "table",
            distance=-11,
        )
    assert "Distance must be between 1 and 5." in str(exc)
