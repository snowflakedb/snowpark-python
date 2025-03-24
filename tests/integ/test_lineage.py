#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#


import json
import uuid

import pytest

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


@pytest.mark.skipif(not is_pandas_available, reason="pandas is required")
def test_lineage_trace(session):
    """
    Tests lineage.trace API on multiple cases.
    """
    db = session.get_current_database().replace('"', "")
    schema = ("sch" + str(uuid.uuid4()).replace("-", "")[:10]).upper()
    primary_role = session.get_current_role().replace('"', "")
    current_wh = session.get_current_warehouse().replace('"', "")
    session.sql(f"CREATE SCHEMA {db}.{schema}").collect()
    session.sql(f"use warehouse {current_wh}").collect()

    create_objects_for_test(session, db, schema)

    # CASE 1 : trace with the role that has VIEW LINEAGE privillege.
    df = session.lineage.trace(
        f"{db}.{schema}.T1",
        "table",
        direction=LineageDirection.DOWNSTREAM,
        distance=4,
    )
    df = remove_created_on_field(df.to_pandas())

    expected_data = {
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
    }

    expected_df = pd.DataFrame(expected_data)
    assert_frame_equal(df, expected_df, check_dtype=False)

    # CASE 2 : trace with default arguments
    df = session.lineage.trace(f"{db}.{schema}.V1", "view")
    df = remove_created_on_field(df.to_pandas())

    expected_data = {
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
    }
    expected_df = pd.DataFrame(expected_data)
    assert_frame_equal(df, expected_df, check_dtype=False)

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
    session.sql(f"GRANT CREATE VIEW ON schema {schema} TO ROLE {test_role}").collect()
    session.sql(f"USE ROLE {test_role}").collect()
    session.sql("USE SECONDARY ROLES NONE").collect()
    session.sql(
        f"CREATE OR REPLACE VIEW {db}.{schema}.V6 AS SELECT * FROM {db}.{schema}.V5"
    ).collect()

    df = session.lineage.trace(
        f"{db}.{schema}.V6", "view", direction=LineageDirection.UPSTREAM
    )
    session.sql(f"USE ROLE {primary_role}").collect()

    session.sql(f"use warehouse {current_wh}").collect()

    df = remove_created_on_field(df.to_pandas())

    expected_data = {
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
    }

    expected_df = pd.DataFrame(expected_data)
    assert_frame_equal(df, expected_df, check_dtype=False)

    # CASE 5 : trace with deleted object
    session.sql(f"DROP VIEW {db}.{schema}.V3").collect()

    df = session.lineage.trace(
        f"{db}.{schema}.V2", "view", direction=LineageDirection.DOWNSTREAM
    )

    # Removing 'creadtedOn' field since the value can not be predicted.
    df = remove_created_on_field(df.to_pandas())

    expected_data = {
        "SOURCE_OBJECT": [],
        "TARGET_OBJECT": [],
        "DIRECTION": [],
        "DISTANCE": [],
    }

    expected_df = pd.DataFrame(expected_data)
    assert_frame_equal(df, expected_df, check_dtype=False)

    # CASE 6 : Case senstive query
    session.sql(
        f'CREATE OR REPLACE VIEW {db}.{schema}."v7" AS SELECT * FROM {db}.{schema}.V2'
    ).collect()

    df = session.lineage.trace(
        f'{db}.{schema}."v7"', "view", direction=LineageDirection.UPSTREAM
    )

    # Removing 'creadtedOn' field since the value can not be predicted.
    df = remove_created_on_field(df.to_pandas())

    expected_data = {
        "SOURCE_OBJECT": [
            {"domain": "VIEW", "name": f"{db}.{schema}.V2", "status": "ACTIVE"},
            {"domain": "VIEW", "name": f"{db}.{schema}.V1", "status": "ACTIVE"},
        ],
        "TARGET_OBJECT": [
            {"domain": "VIEW", "name": f"{db}.{schema}.v7", "status": "ACTIVE"},
            {"domain": "VIEW", "name": f"{db}.{schema}.V2", "status": "ACTIVE"},
        ],
        "DIRECTION": [
            "Upstream",
            "Upstream",
        ],
        "DISTANCE": [1, 2],
    }

    expected_df = pd.DataFrame(expected_data)
    # TODO Enable the check after SNOW-1437475 is fixed.
    # assert_frame_equal(df, expected_df, check_dtype=False)

    # CASE 7 : Column lineage
    df = session.lineage.trace(
        f"{db}.{schema}.V2.C1", "COLUMN", direction=LineageDirection.UPSTREAM
    )

    # Removing 'creadtedOn' field since the value can not be predicted.
    df = remove_created_on_field(df.to_pandas())

    expected_data = {
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
    }
    expected_df = pd.DataFrame(expected_data)
    assert_frame_equal(df, expected_df, check_dtype=False)

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

    session.sql(f"drop schema {db}.{schema}").collect()

    df = session.lineage.trace(
        f'{db}.{schema}."v7"', "view", direction=LineageDirection.UPSTREAM
    )
