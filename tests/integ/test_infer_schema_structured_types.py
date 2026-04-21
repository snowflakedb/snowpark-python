#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""End-to-end integration tests for the structured-type INFER_SCHEMA path.

These tests cover the new ``_use_structured_type_infer_schema`` session flag
introduced for SNOW-3237416. They build a small Parquet file *locally* with
``pyarrow`` (so it carries native ``struct``/``list``/``map`` logical types),
``PUT`` it to a temp stage, then read it back with the flag enabled and
assert that the inferred schema is correctly materialized as
``StructType``/``ArrayType``/``MapType`` (rather than collapsing to
``VariantType``).

.. note::

    The whole module is currently disabled. The new client-side parser path
    only fires when ``INFER_SCHEMA`` returns *detailed* structured type
    strings (e.g. ``OBJECT(city VARCHAR, zip NUMBER(38,0))``). That behavior
    is gated on a server-side parameter,
    ``ENABLE_STRUCTURED_TYPE_INFER_SCHEMA_FOR_PARQUET``, which has not yet
    been released. With the parameter off, ``INFER_SCHEMA`` collapses every
    nested column to bare ``VARIANT`` (empirically verified against
    ``sfctest0`` on AWS_US_WEST_2 with ``pyarrow`` 18.x) and the new code
    path is never exercised — making any assertion below either trivial or
    false.

    Once the server parameter ships, this test can be enabled by:

    1. Removing the ``pytest.mark.skip`` below (and reinstating the
       local-testing-mode ``skipif`` if desired).
    2. Adding ``alter session set
       ENABLE_STRUCTURED_TYPE_INFER_SCHEMA_FOR_PARQUET=true`` to the
       ``structured_infer_session`` fixture (with the matching unset on
       teardown).

    Note: this test deliberately does *not* use ``structured_types_supported``
    or ``structured_types_enabled_session`` from ``tests/utils``. Those
    helpers gate on a different feature (structured types in FDN tables /
    native-Arrow client response) whose session parameters
    (``ENABLE_STRUCTURED_TYPES_IN_FDN_TABLES`` etc.) do not affect what
    ``INFER_SCHEMA`` returns for a parquet file.

    Until then, parser correctness is fully covered by the unit tests in
    ``tests/unit/test_dataframe_reader_type_parsing.py`` (in particular
    ``TestInferSchemaStructuredTypePath``), which mock the exact
    ``INFER_SCHEMA`` rows the server is expected to emit for structured
    columns.

    The ``df.write.parquet`` (``COPY INTO ... PARQUET``) approach is *not*
    used here because the server cannot currently unload ``MAP`` columns
    (``Cannot determine equivalent parquet data type for snowflake data.
    SFLogicalType: MAP, SFPhysicalType: LOB``). Building the fixture locally
    with ``pyarrow`` and uploading via ``PUT`` sidesteps that limitation.
"""

import os
import tempfile

import pytest

from snowflake.snowpark._internal.utils import TempObjectType
from snowflake.snowpark.types import (
    ArrayType,
    DoubleType,
    LongType,
    MapType,
    StringType,
    StructType,
)
from tests.utils import Utils


pytestmark = pytest.mark.skip(
    reason=(
        "Disabled until the server parameter "
        "ENABLE_STRUCTURED_TYPE_INFER_SCHEMA_FOR_PARQUET is released. "
        "Without it INFER_SCHEMA returns bare VARIANT for nested columns, "
        "so the new structured-type parser path cannot be exercised "
        "end-to-end. See module docstring for details."
    ),
)


@pytest.fixture
def structured_infer_session(session):
    """Yield a session with the client-side structured-type INFER_SCHEMA flag on.

    Restores the original flag value on teardown.

    When ``ENABLE_STRUCTURED_TYPE_INFER_SCHEMA_FOR_PARQUET`` is released,
    add ``alter session set ENABLE_STRUCTURED_TYPE_INFER_SCHEMA_FOR_PARQUET=true``
    here (with a matching unset in the ``finally`` block).
    """
    original = session._use_structured_type_infer_schema
    session._use_structured_type_infer_schema = True
    try:
        yield session
    finally:
        session._use_structured_type_infer_schema = original


def _build_structured_parquet_local(local_path: str) -> None:
    """Build a single-row Parquet file with struct/list/map columns.

    The file uses pyarrow's native nested types so that, once the server
    parameter ``ENABLE_STRUCTURED_TYPE_INFER_SCHEMA_FOR_PARQUET`` is on,
    ``INFER_SCHEMA`` will surface them as detailed ``OBJECT(...)`` /
    ``ARRAY(...)`` / ``MAP(...)`` strings.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    struct_type = pa.struct([("city", pa.string()), ("zip", pa.int64())])
    map_type = pa.map_(pa.string(), pa.int64())

    table = pa.table(
        {
            "obj_col": pa.array([{"city": "SF", "zip": 94016}], type=struct_type),
            "arr_col": pa.array([[1.0, 2.5, 3.25]], type=pa.list_(pa.float64())),
            "map_col": pa.array([[("k1", 1), ("k2", 2)]], type=map_type),
        }
    )
    pq.write_table(table, local_path)


def test_structured_infer_schema_object_map_array(structured_infer_session):
    """End-to-end: PUT a pyarrow-built parquet, read it back, verify schema.

    With ``_use_structured_type_infer_schema = True`` (and the server-side
    parameter on, see module docstring), ``read.parquet`` should materialize
    the nested columns as ``MapType`` / ``StructType`` / ``ArrayType`` with
    ``structured=True`` rather than falling back to ``VariantType``.
    """
    session = structured_infer_session
    temp_stage = Utils.random_name_for_temp_object(TempObjectType.STAGE)
    Utils.create_stage(session, temp_stage, is_temporary=True)

    try:
        with tempfile.TemporaryDirectory() as d:
            local_path = os.path.join(d, "structured.parquet")
            _build_structured_parquet_local(local_path)
            Utils.upload_to_stage(session, temp_stage, local_path, compress=False)

        parquet_path = f"@{temp_stage}/structured.parquet"

        df = session.read.parquet(parquet_path)
        schema_by_name = {f.name.strip('"').upper(): f for f in df.schema.fields}

        assert "MAP_COL" in schema_by_name
        assert "OBJ_COL" in schema_by_name
        assert "ARR_COL" in schema_by_name

        map_dt = schema_by_name["MAP_COL"].datatype
        assert isinstance(
            map_dt, MapType
        ), f"expected MapType for MAP_COL, got {type(map_dt).__name__}"
        assert map_dt.structured is True
        assert isinstance(map_dt.key_type, StringType)
        # NUMBER(38,0) -> LongType via convert_sf_to_sp_type when scale=0.
        assert isinstance(map_dt.value_type, LongType)

        obj_dt = schema_by_name["OBJ_COL"].datatype
        assert isinstance(
            obj_dt, StructType
        ), f"expected StructType for OBJ_COL, got {type(obj_dt).__name__}"
        assert obj_dt.structured is True
        nested_names = {f.name.strip('"').upper() for f in obj_dt.fields}
        assert nested_names == {"CITY", "ZIP"}

        arr_dt = schema_by_name["ARR_COL"].datatype
        assert isinstance(
            arr_dt, ArrayType
        ), f"expected ArrayType for ARR_COL, got {type(arr_dt).__name__}"
        assert arr_dt.structured is True
        assert isinstance(arr_dt.element_type, DoubleType)

        rows = df.collect()
        assert len(rows) == 1
    finally:
        Utils.drop_stage(session, temp_stage)
