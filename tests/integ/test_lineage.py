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


def create_objects_for_test(session, db, schema):
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
    db = session.get_current_database().replace('"', "")
    schema = ("sch" + str(uuid.uuid4()).replace("-", "")[:10]).upper()
    warehouse = session.get_current_warehouse().replace('"', "")

    new_role = "test_role"

    session.sql("USE ROLE ACCOUNTADMIN").collect()
    session.sql(f"CREATE OR REPLACE ROLE {new_role}").collect()
    session.sql(f"GRANT ROLE {new_role} TO ROLE ACCOUNTADMIN").collect()
    session.sql(f"GRANT USAGE ON database {db} TO ROLE {new_role}").collect()
    session.sql(f"GRANT CREATE SCHEMA ON database {db} TO ROLE {new_role}").collect()
    session.sql(f"GRANT USAGE ON WAREHOUSE {warehouse} TO ROLE {new_role}").collect()
    session.sql(f"USE ROLE {new_role}").collect()
    session.sql(f"CREATE SCHEMA {db}.{schema}").collect()

    create_objects_for_test(session, db, schema)

    with pytest.raises(SnowparkSQLException) as exc:
        session.lineage.trace(f"{db}.{schema}.V1", "view").collect()
    assert "Insufficient privileges to view data lineage" in str(exc)

    session.sql("USE ROLE ACCOUNTADMIN").collect()
    session.sql(f"GRANT VIEW LINEAGE ON ACCOUNT TO ROLE {new_role};").collect()
    session.sql(f"USE ROLE {new_role}").collect()

    df = session.lineage.trace(
        f"{db}.{schema}.T1",
        "table",
        direction=LineageDirection.DOWNSTREAM,
        depth=4,
    )
    df.show()
    # Removing 'creadtedOn' field since the value can not be predicted.
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
        "LINEAGE": ["Downstream", "Downstream", "Downstream", "Downstream"],
        "DEPTH": [1, 2, 3, 4],
    }

    expected_df = pd.DataFrame(expected_data)
    assert_frame_equal(df, expected_df, check_dtype=False)

    df = session.lineage.trace(f"{db}.{schema}.V1", "view")
    # Removing 'creadtedOn' field since the value can not be predicted.
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
        "LINEAGE": ["Upstream", "Downstream", "Downstream"],
        "DEPTH": [1, 1, 2],
    }
    expected_df = pd.DataFrame(expected_data)
    assert_frame_equal(df, expected_df, check_dtype=False)

    df = session.lineage.trace(
        f"{db}.{schema}.nonexistant",
        "table",
        direction=LineageDirection.DOWNSTREAM,
        depth=3,
    ).to_pandas()

    assert 0 == df.shape[0]

    session.sql("USE ROLE ACCOUNTADMIN").collect()
    session.sql(
        f"CREATE OR REPLACE VIEW {db}.{schema}.V6 AS SELECT * FROM {db}.{schema}.V5"
    ).collect()
    session.sql(f"USE ROLE {new_role}").collect()

    df = session.lineage.trace(
        f"{db}.{schema}.V4", "view", direction=LineageDirection.DOWNSTREAM
    )
    # Removing 'creadtedOn' field since the value can not be predicted.
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
        "LINEAGE": ["Downstream", "Downstream"],
        "DEPTH": [1, 2],
    }

    expected_df = pd.DataFrame(expected_data)
    assert_frame_equal(df, expected_df, check_dtype=False)

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
        "LINEAGE": [
            "Downstream",
        ],
        "DEPTH": [1],
    }

    expected_df = pd.DataFrame(expected_data)
    assert_frame_equal(df, expected_df, check_dtype=False)

    session.sql(f"drop schema {db}.{schema}").collect()
