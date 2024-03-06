#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.snowpark import GetResult, PutResult


@pytest.mark.localtest
def test_put_result():
    test_dict = {
        "source": "test_source",
        "target": "test_target",
        "source_size": 10,
        "target_size": 12,
        "source_compression": "NONE",
        "target_compression": "GZIP",
        "status": "UPLOADED",
        "message": "Test Message",
    }
    put_result = PutResult(**test_dict)

    assert put_result.source == test_dict["source"]
    assert put_result.target == test_dict["target"]
    assert put_result.source_size == test_dict["source_size"]
    assert put_result.target_size == test_dict["target_size"]
    assert put_result.source_compression == test_dict["source_compression"]
    assert put_result.target_compression == test_dict["target_compression"]
    assert put_result.status == test_dict["status"]
    assert put_result.message == test_dict["message"]


@pytest.mark.localtest
def test_get_result():
    test_dict = {
        "file": "test_file",
        "size": 100,
        "status": "DOWNLOADED",
        "message": "Test Message",
    }
    get_result = GetResult(**test_dict)

    assert get_result.file == test_dict["file"]
    assert get_result.size == test_dict["size"]
    assert get_result.status == test_dict["status"]
    assert get_result.message == test_dict["message"]
