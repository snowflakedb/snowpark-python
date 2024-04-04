#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark._internal import utils


@pytest.mark.parametrize(
    "path, expected_dir, expected_file",
    [
        ("stage/", "stage", ""),
        ("stage/file.txt", "stage", "file.txt"),
        ("dir/subdir/file.txt", "dir/subdir", "file.txt"),
        ("@stage/dir/subdir/file.txt", "@stage/dir/subdir", "file.txt"),
        ("'@stage/dir/subdir/file.txt'", "@stage/dir/subdir", "file.txt"),
        (
            "snow://domain/test_entity/versions/test_version/file.txt",
            "snow://domain/test_entity/versions/test_version",
            "file.txt",
        ),
        (
            "'snow://domain/test_entity/versions/test_version/file.txt'",
            "snow://domain/test_entity/versions/test_version",
            "file.txt",
        ),
    ],
)
def test_split_path(path: str, expected_dir: str, expected_file: str) -> None:
    dir, file = utils.split_path(path)
    assert expected_dir == dir
    assert expected_file == file


@pytest.mark.parametrize(
    "path, is_local, expected",
    [
        ("dir/file.txt", True, "'file://dir/file.txt'"),
        ("dir/subdir/file.txt", True, "'file://dir/subdir/file.txt'"),
        ("'dir/subdir/file.txt'", True, "'dir/subdir/file.txt'"),
        ("file://dir/subdir/file.txt", True, "'file://dir/subdir/file.txt'"),
        ("stage/", False, "'@stage/'"),
        ("stage/file.txt", False, "'@stage/file.txt'"),
        ("'stage/file.txt'", False, "'stage/file.txt'"),
        (
            "stage/'embedded_quote'/file.txt",
            False,
            "'@stage/\\'embedded_quote\\'/file.txt'",
        ),
        ("@stage/dir/subdir/file.txt", False, "'@stage/dir/subdir/file.txt'"),
        ("'@stage/dir/subdir/file.txt'", False, "'@stage/dir/subdir/file.txt'"),
        (
            "snow://domain/test_entity/versions/test_version/file.txt",
            False,
            "'snow://domain/test_entity/versions/test_version/file.txt'",
        ),
        (
            "'snow://domain/test_entity/versions/test_version/file.txt'",
            False,
            "'snow://domain/test_entity/versions/test_version/file.txt'",
        ),
    ],
)
def test_normalize_path(path: str, is_local: bool, expected: str) -> None:
    actual = utils.normalize_path(path, is_local)
    assert expected == actual
