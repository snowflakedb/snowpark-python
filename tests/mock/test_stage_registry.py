#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import io
import os
import tempfile
import re
import pytest

from snowflake.snowpark._internal.utils import normalize_local_file
from snowflake.snowpark.mock._connection import MockServerConnection
from snowflake.snowpark.mock._stage_registry import (
    StageEntityRegistry,
    extract_stage_name_and_prefix,
    _INVALID_STAGE_LOCATION_ERR_MSG,
)
from snowflake.snowpark.functions import sproc
from snowflake.snowpark.session import Session
from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException


def test_util():
    assert extract_stage_name_and_prefix("@stage") == ("stage", "")
    assert extract_stage_name_and_prefix("@stage/dir") == ("stage", "dir")
    assert extract_stage_name_and_prefix('@"stage.abc"/dir') == ("stage.abc", "dir")
    assert extract_stage_name_and_prefix('@"st123a/ge.abc"/dir/subdir') == (
        "st123a/ge.abc",
        f"dir{os.sep}subdir",
    )


def test_stage_put_file():
    stage_registry = StageEntityRegistry(MockServerConnection())
    stage_registry.create_or_replace_stage("test_stage")

    stage = stage_registry["test_stage"]

    result_df = stage_registry.put(
        normalize_local_file(
            f"{os.path.dirname(os.path.abspath(__file__))}/files/test_file_1"
        ),
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
            "test_file_1",
        )
    )

    result_df = stage_registry.put(
        normalize_local_file(
            f"{os.path.dirname(os.path.abspath(__file__))}/files/test_file*"
        ),
        "@test_stage/test_parent_dir",
    )
    assert len(result_df) == 2

    result_1 = result_df.iloc[0]
    result_2 = result_df.iloc[1]
    assert result_1.source == result_1.target in ("test_file_1", "test_file_2")
    assert result_2.source == result_2.target in ("test_file_1", "test_file_2")
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
            "test_file_1",
        )
    )
    assert os.path.isfile(
        os.path.join(
            stage._working_directory,
            "test_parent_dir",
            "test_file_2",
        )
    )

    # skip uploading if existing
    result_df = stage_registry.put(
        normalize_local_file(
            f"{os.path.dirname(os.path.abspath(__file__))}/files/test_file*"
        ),
        "@test_stage/test_parent_dir",
    )
    assert len(result_df) == 2

    result_1 = result_df.iloc[0]
    result_2 = result_df.iloc[1]
    assert result_1.status == result_2.status == "SKIPPED"

    # test file sharing the same name as the directory is not supported in local testing
    # check https://snowflakecomputing.atlassian.net/browse/SNOW-1254908 for more context
    with pytest.raises(NotImplementedError):
        result_df = stage_registry.put(
            normalize_local_file(
                f"{os.path.dirname(os.path.abspath(__file__))}/files/test_file_1"
            ),
            "@test_stage/test_parent_dir/test_file_1",
        )
        assert result_df.iloc[0].status == "UPLOADED"
        assert os.path.isfile(
            os.path.join(
                stage._working_directory,
                "test_parent_dir",
                "test_file_1",
                "test_file_1",
            )
        )


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
            "test_file_1",
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
            "test_file_2",
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
            "test_file_2",
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


def test_stage_get_file():
    stage_registry = StageEntityRegistry(MockServerConnection())
    stage_registry.put(
        normalize_local_file(f"{os.path.dirname(os.path.abspath(__file__))}/files/*"),
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


def test_stage_get_and_put_sproc(session):
    test_file = f"{os.path.dirname(os.path.abspath(__file__))}/files/test_file_1"
    with open(test_file, "rb") as f:
        test_content = f.read()

    @sproc
    def read_and_write_file(session_: Session) -> str:
        put_result = session_.file.put(
            normalize_local_file(test_file),
            "@test_output_stage/test_parent_dir/test_child_dir",
            auto_compress=False,
        )
        assert len(put_result) == 1
        put_result = put_result[0]
        assert put_result.source == put_result.target == "test_file_1"
        assert put_result.source_size is not None
        assert put_result.target_size is not None
        assert put_result.source_compression == "NONE"
        assert put_result.target_compression == "NONE"
        assert put_result.status == "UPLOADED"
        assert put_result.message == ""

        with tempfile.TemporaryDirectory() as temp_dir:
            get_results = session_.file.get(
                "@test_output_stage/test_parent_dir/test_child_dir/test_file_1",
                f"'file://{temp_dir}'",
            )
            assert len(get_results) == 1
            get_result = get_results[0]
            assert get_result.file == "test_file_1"
            assert get_result.size is not None
            assert get_result.status == "DOWNLOADED"
            assert get_result.message == ""
            with open(os.path.join(temp_dir, "test_file_1"), "rb") as f:
                content = f.read()

        return content

    content = read_and_write_file()
    assert content == test_content


def test_stage_invalid_stage_location(session):
    stage_registry = StageEntityRegistry(MockServerConnection())
    test_file = f"{os.path.dirname(os.path.abspath(__file__))}/files/test_file_1"
    invalid_snowurl = f"sNoW://test{test_file}"

    with pytest.raises(
        SnowparkLocalTestingException,
        match=re.escape(_INVALID_STAGE_LOCATION_ERR_MSG(invalid_snowurl)),
    ):
        stage_registry.read_file(invalid_snowurl, "", [], "", {})

    with pytest.raises(
        SnowparkLocalTestingException,
        match=re.escape(_INVALID_STAGE_LOCATION_ERR_MSG(invalid_snowurl)),
    ):
        with tempfile.TemporaryDirectory() as temp_dir:
            stage_registry.get(invalid_snowurl, temp_dir)
