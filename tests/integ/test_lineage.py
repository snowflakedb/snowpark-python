#!/usr/bin/env python3
#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import json
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


def setup_test(session, db, schema, warehouse, test_role) -> None:
    session.sql("SELECT CURRENT_ROLE()").collect()
    session.sql("USE ROLE ACCOUNTADMIN").collect()
    session.sql(f"CREATE OR REPLACE ROLE {test_role}").collect()
    session.sql(f"GRANT ROLE {test_role} TO ROLE ACCOUNTADMIN").collect()
    session.sql(f"GRANT USAGE ON database {db} TO ROLE {test_role}").collect()
    session.sql(f"GRANT CREATE SCHEMA ON database {db} TO ROLE {test_role}").collect()
    session.sql(f"GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE {test_role}").collect()
    session.sql(f"USE ROLE {test_role}").collect()
    session.sql(f"CREATE SCHEMA {db}.{schema}").collect()


def create_objects_for_test(session, db, schema) -> None:
    # Create table and views within the specified TESTDB and TESTSCHEMA
    session.sql(f"CREATE OR REPLACE TABLE {db}.{schema}.T1(C1 INT)").collect()
    session.sql(
        f"CREATE OR REPLACE VIEW {db}.{schema}.V1 AS SELECT * FROM {db}.{schema}.T1"
    ).collect()
    session.sql(
        f"CREATE OR REPLACE VIEW {db}.{schema}.V2 AS SELECT * FROM {db}.{schema}.V1"
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
    warehouse = session.get_current_warehouse().replace('"', "")
    test_role = "test_role"

    setup_test(session, db, schema, warehouse, test_role)
    create_objects_for_test(session, db, schema)

    # CASE 1 : trace with the role that does not have VIEW LINEAGE privillege.
    with pytest.raises(SnowparkSQLException) as exc:
        session.lineage.trace(f"{db}.{schema}.V1", "view").collect()
    assert "Insufficient privileges to view data lineage" in str(exc)

    # CASE 2 : trace with the role that has VIEW LINEAGE privillege.
    session.sql("USE ROLE ACCOUNTADMIN").collect()
    session.sql(f"GRANT VIEW LINEAGE ON ACCOUNT TO ROLE {test_role};").collect()
    session.sql(f"USE ROLE {test_role}").collect()

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

    # CASE 3 : trace with default arguments
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

    # CASE 4 : trace with masked object
    session.sql("USE ROLE ACCOUNTADMIN").collect()
    session.sql(
        f"CREATE OR REPLACE VIEW {db}.{schema}.V6 AS SELECT * FROM {db}.{schema}.V5"
    ).collect()
    session.sql(f"USE ROLE {test_role}").collect()

    df = session.lineage.trace(
        f"{db}.{schema}.V4", "view", direction=LineageDirection.DOWNSTREAM
    )
    df = remove_created_on_field(df.to_pandas())

    expected_data = {
        "SOURCE_OBJECT": [
            {"domain": "VIEW", "name": f"{db}.{schema}.V4", "status": "ACTIVE"},
            {"domain": "VIEW", "name": f"{db}.{schema}.V5", "status": "ACTIVE"},
        ],
        "TARGET_OBJECT": [
            {"domain": "VIEW", "name": f"{db}.{schema}.V5", "status": "ACTIVE"},
            {"domain": "VIEW", "name": "***.***.***", "status": "MASKED"},
        ],
        "DIRECTION": ["Downstream", "Downstream"],
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
        "SOURCE_OBJECT": [
            {"domain": "VIEW", "name": f"{db}.{schema}.V2", "status": "ACTIVE"}
        ],
        "TARGET_OBJECT": [
            {"domain": "VIEW", "name": f"{db}.{schema}.V3", "status": "DELETED"}
        ],
        "DIRECTION": [
            "Downstream",
        ],
        "DISTANCE": [1],
    }

    expected_df = pd.DataFrame(expected_data)
    assert_frame_equal(df, expected_df, check_dtype=False)

    session.sql(f"drop schema {db}.{schema}").collect()

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
