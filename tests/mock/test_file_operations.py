#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import os
import tempfile
import pytest
from snowflake.snowpark._internal.utils import normalize_local_file
from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException


def test_get_and_put_snowurl(session):
    test_file = f"{os.path.dirname(os.path.abspath(__file__))}/files/test_file_1"
    with open(test_file, "rb") as f:
        test_content = f.read()

    with tempfile.TemporaryDirectory() as temp_dir:
        snowurl = f"snow://{temp_dir}"
        put_results = session.file.put(
            normalize_local_file(test_file),
            snowurl,
            auto_compress=False,
        )

        assert len(put_results) == 1
        put_result = put_results[0]
        assert put_result.source == put_result.target == "test_file_1"
        assert put_result.source_size is not None
        assert put_result.target_size is not None
        assert put_result.source_compression == "NONE"
        assert put_result.target_compression == "NONE"
        assert put_result.status == "UPLOADED"
        assert put_result.message == ""

        with tempfile.TemporaryDirectory() as temp_dir:
            # Test that the file can be retrieved with a trailing slash
            get_result = session.file.get(
                f"{snowurl}/",
                temp_dir,
            )
            assert len(get_result) == 1
            assert os.path.isfile(os.path.join(temp_dir, "test_file_1"))
            with open(os.path.join(temp_dir, "test_file_1"), "rb") as f:
                content = f.read()
                assert content == test_content

        with tempfile.TemporaryDirectory() as temp_dir:
            # Test that the file can be retrieved without a trailing slash
            get_result = session.file.get(
                snowurl,
                temp_dir,
            )
            assert len(get_result) == 1
            assert os.path.isfile(os.path.join(temp_dir, "test_file_1"))
            with open(os.path.join(temp_dir, "test_file_1"), "rb") as f:
                content = f.read()
                assert content == test_content


def test_get_and_read_invalid_snowurl(session):
    test_file = f"{os.path.dirname(os.path.abspath(__file__))}/files/test_file_1"

    with tempfile.TemporaryDirectory() as temp_dir:
        invalid_snowurl = f"sNoW://test{test_file}"
        with pytest.raises(SnowparkLocalTestingException):
            session.file.get(
                invalid_snowurl,
                temp_dir,
            )

        invalid_snowurl = f"snow:{test_file}"
        with pytest.raises(SnowparkLocalTestingException):
            session.file.get(
                invalid_snowurl,
                temp_dir,
            )
