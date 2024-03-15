#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import io
import os
import tempfile

import pytest

from snowflake.snowpark._internal.utils import normalize_local_file
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.mock._stage_registry import (
    StageEntity,
    StageEntityRegistry,
    extract_stage_name_and_prefix,
)


@pytest.mark.localtest
def test_util():
    assert extract_stage_name_and_prefix("@stage") == ("stage", "")
    assert extract_stage_name_and_prefix("@stage/dir") == ("stage", "dir")
    assert extract_stage_name_and_prefix('@"stage.abc"/dir') == ("stage.abc", "dir")
    assert extract_stage_name_and_prefix('@"st123a/ge.abc"/dir/subdir') == (
        "st123a/ge.abc",
        "dir/subdir",
    )


@pytest.mark.localtest
def test_stage_put_file():
    stage_registry = StageEntityRegistry(MockServerConnection())
    stage_registry.create_or_replace_stage("test_stage")

    stage = stage_registry["test_stage"]

    result_df = stage_registry.put(
        normalize_local_file("files/test_file_1"),
        "@test_stage/test_parent_dir/test_child_dir",
    )
    assert len(result_df) == 1
    result = result_df.iloc[0]
    assert result.source == result.target == "test_file_1"
    assert result.source_size is not None
    assert result.target_size is not None
    assert result.source_compression == "NONE"
    assert result.target_compression == "NONE"
    assert result.status == "UPLOADED"
    assert result.message == ""

    assert os.path.isfile(
        os.path.join(
            stage._working_directory,
            "test_parent_dir",
            "test_child_dir",
            f"test_file_1{StageEntity.FILE_SUFFIX}",
        )
    )

    result_df = stage_registry.put(
        normalize_local_file("files/*"), "@test_stage/test_parent_dir"
    )
    assert len(result_df) == 2

    result_1 = result_df.iloc[0]
    result_2 = result_df.iloc[1]
    assert result_1.source == result_1.target == "test_file_1"
    assert result_2.source == result_2.target == "test_file_2"
    assert result_1.source_size is not None and result_2.source_size is not None
    assert result_1.target_size is not None and result_1.target_size is not None
    assert result_1.source_compression == result_1.target_compression == "NONE"
    assert result_1.target_compression == result_1.target_compression == "NONE"
    assert result_1.status == result_2.status == "UPLOADED"
    assert result_1.message == result_2.message == ""

    assert os.path.isfile(
        os.path.join(
            stage._working_directory,
            "test_parent_dir",
            f"test_file_1{StageEntity.FILE_SUFFIX}",
        )
    )
    assert os.path.isfile(
        os.path.join(
            stage._working_directory,
            "test_parent_dir",
            f"test_file_2{StageEntity.FILE_SUFFIX}",
        )
    )

    # skip uploading if existing
    result_df = stage_registry.put(
        normalize_local_file("files/*"), "@test_stage/test_parent_dir"
    )
    assert len(result_df) == 2

    result_1 = result_df.iloc[0]
    result_2 = result_df.iloc[1]
    assert result_1.status == result_2.status == "SKIPPED"

    # test file name is the same as the directory
    result_df = stage_registry.put(
        normalize_local_file("files/test_file_1"),
        "@test_stage/test_parent_dir/test_file_1",
    )
    assert result_df.iloc[0].status == "UPLOADED"
    assert os.path.isfile(
        os.path.join(
            stage._working_directory,
            "test_parent_dir",
            "test_file_1",
            f"test_file_1{StageEntity.FILE_SUFFIX}",
        )
    )


@pytest.mark.localtest
def test_stage_put_stream():
    stage_registry = StageEntityRegistry(MockServerConnection())
    stage_registry.create_or_replace_stage("test_stage")

    stage = stage_registry["test_stage"]

    bytes_data = b"sample data"
    bytes_io = io.BytesIO(bytes_data)

    result1 = stage_registry.upload_stream(
        input_stream=bytes_io,
        stage_location="@test_stage/test_parent_dir",
        file_name="test_file_1",
    )
    assert os.path.isfile(
        os.path.join(
            stage._working_directory,
            "test_parent_dir",
            f"test_file_1{StageEntity.FILE_SUFFIX}",
        )
    )

    bytes_io.seek(0)
    result2 = stage_registry.upload_stream(
        input_stream=bytes_io,
        stage_location="@test_stage/test_parent_dir",
        file_name="test_file_2",
    )
    assert os.path.isfile(
        os.path.join(
            stage._working_directory,
            "test_parent_dir",
            f"test_file_2{StageEntity.FILE_SUFFIX}",
        )
    )

    bytes_io.seek(0)
    result3 = stage_registry.upload_stream(
        input_stream=bytes_io,
        stage_location="@test_stage/test_parent_dir",
        file_name="test_file_2",
    )
    assert os.path.isfile(
        os.path.join(
            stage._working_directory,
            "test_parent_dir",
            f"test_file_2{StageEntity.FILE_SUFFIX}",
        )
    )

    bytes_io.close()
    assert result1 == {
        "data": [
            ("test_file_1", "test_file_1", 11, 11, "NONE", "NONE", "UPLOADED", "")
        ],
        "sfqid": None,
    }
    assert result2 == {
        "data": [
            ("test_file_2", "test_file_2", 11, 11, "NONE", "NONE", "UPLOADED", "")
        ],
        "sfqid": None,
    }
    assert result3 == {
        "data": [("test_file_2", "test_file_2", 11, 11, "NONE", "NONE", "SKIPPED", "")],
        "sfqid": None,
    }


@pytest.mark.locatest
def test_stage_get_file():
    stage_registry = StageEntityRegistry(MockServerConnection())
    stage_registry.put(
        normalize_local_file("files/*"),
        "@test_stage/test_parent_dir/test_child_dir",
    )

    with tempfile.TemporaryDirectory() as temp_dir:
        # test normalized file path
        stage_registry.get(
            "@test_stage/test_parent_dir/test_child_dir/", f"'file://{temp_dir}'"
        )
        assert os.path.isfile(os.path.join(temp_dir, "test_file_1")) and os.path.isfile(
            os.path.join(temp_dir, "test_file_2")
        )

    with tempfile.TemporaryDirectory() as temp_dir:
        # test unnormalized file path
        stage_registry.get("@test_stage/test_parent_dir/test_child_dir/", temp_dir)
        assert os.path.isfile(os.path.join(temp_dir, "test_file_1")) and os.path.isfile(
            os.path.join(temp_dir, "test_file_2")
        )
