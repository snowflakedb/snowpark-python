#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import os
import tempfile

from snowflake.snowpark._internal.utils import normalize_local_file


def test_get_and_put_snowurl(session):
    with tempfile.TemporaryDirectory() as temp_dir:
        snowurl = f"snow://{temp_dir}"
        result = session.file.put(
            normalize_local_file(
                f"{os.path.dirname(os.path.abspath(__file__))}/files/test_file_1"
            ),
            snowurl,
            auto_compress=False,
        )

        assert len(result) == 1
        result = result[0]
        assert result.source == result.target == "test_file_1"
        assert result.source_size is not None
        assert result.target_size is not None
        assert result.source_compression == "NONE"
        assert result.target_compression == "NONE"
        assert result.status == "UPLOADED"
        assert result.message == ""

        with tempfile.TemporaryDirectory() as temp_dir:
            # Test that the file can be retrieved with a trailing slash
            session.file.get(
                f"{snowurl}/",
                temp_dir,
            )
            assert os.path.isfile(os.path.join(temp_dir, "test_file_1"))

    # Test put without tempdir
    snowurl = "snow://test_file_1"
    result = session.file.put(
        normalize_local_file(
            f"{os.path.dirname(os.path.abspath(__file__))}/files/test_file_1"
        ),
        snowurl,
        auto_compress=False,
    )
    assert len(result) == 1
    result = result[0]
    assert result.source == result.target == "test_file_1"
    assert result.source_size is not None
    assert result.target_size is not None
    assert result.source_compression == "NONE"
    assert result.target_compression == "NONE"
    assert result.status == "UPLOADED"
    assert result.message == ""

    with tempfile.TemporaryDirectory() as temp_dir:
        # Test that the non-tempdir file can be retrieved without a trailing slash
        session.file.get(
            snowurl,
            temp_dir,
        )
        assert os.path.isfile(os.path.join(temp_dir, "test_file_1"))
