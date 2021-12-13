#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark import GetResult, PutResult


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
    put_result = PutResult(test_dict)

    assert put_result.source == put_result["source"] == test_dict["source"]
    assert put_result.target == put_result["target"] == test_dict["target"]
    assert (
        put_result.source_size == put_result["source_size"] == test_dict["source_size"]
    )
    assert (
        put_result.target_size == put_result["target_size"] == test_dict["target_size"]
    )
    assert (
        put_result.source_compression
        == put_result["source_compression"]
        == test_dict["source_compression"]
    )
    assert (
        put_result.target_compression
        == put_result["target_compression"]
        == test_dict["target_compression"]
    )
    assert put_result.status == put_result["status"] == test_dict["status"]
    assert put_result.message == put_result["message"] == test_dict["message"]


def test_put_no_data():
    put_result = PutResult({})
    assert put_result.source is None
    assert put_result.target is None
    assert put_result.source_size is None
    assert put_result.target_size is None
    assert put_result.source_compression is None
    assert put_result.target_compression is None
    assert put_result.status is None
    assert put_result.message is None


def test_get_result():
    test_dict = {
        "file": "test_file",
        "size": 100,
        "status": "DOWNLOADED",
        "message": "Test Message",
    }
    get_result = GetResult(test_dict)

    assert get_result.file == get_result["file"] == test_dict["file"]
    assert get_result.size == get_result["size"] == test_dict["size"]
    assert get_result.status == get_result["status"] == test_dict["status"]
    assert get_result.message == get_result["message"] == test_dict["message"]


def test_get_no_data():
    get_result = GetResult({})
    assert get_result.file is None
    assert get_result.size is None
    assert get_result.status is None
    assert get_result.message is None
