#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import gzip

from snowflake.snowpark import GetResult, PutResult
from snowflake.snowpark.file_operation import _decompress_gzip_payload_in_place


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


def test_decompress_gzip_payload_in_place(tmp_path, monkeypatch):
    monkeypatch.setattr("snowflake.snowpark.file_operation.IS_V5_DRIVER", True)
    payload = b"PAR1-payload"
    gzip_parquet = tmp_path / "names.parquet"
    with gzip.open(gzip_parquet, "wb") as handle:
        handle.write(payload)
    _decompress_gzip_payload_in_place(str(gzip_parquet))
    assert gzip_parquet.read_bytes() == payload

    plain = tmp_path / "already.parquet"
    plain.write_bytes(payload)
    _decompress_gzip_payload_in_place(str(plain))
    assert plain.read_bytes() == payload

    named_gz = tmp_path / "data.csv.gz"
    with gzip.open(named_gz, "wb") as handle:
        handle.write(b"1,one")
    original = named_gz.read_bytes()
    _decompress_gzip_payload_in_place(str(named_gz))
    assert named_gz.read_bytes() == original

    corrupt = tmp_path / "corrupt.parquet"
    corrupt.write_bytes(b"\x1f\x8bnot-gzip")
    _decompress_gzip_payload_in_place(str(corrupt))
    assert corrupt.read_bytes() == b"\x1f\x8bnot-gzip"

    monkeypatch.setattr("snowflake.snowpark.file_operation.IS_V5_DRIVER", False)
    skipped = tmp_path / "skipped.parquet"
    with gzip.open(skipped, "wb") as handle:
        handle.write(payload)
    compressed = skipped.read_bytes()
    _decompress_gzip_payload_in_place(str(skipped))
    assert skipped.read_bytes() == compressed
