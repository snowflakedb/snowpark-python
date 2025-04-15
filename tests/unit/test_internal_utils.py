#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
import concurrent.futures
import random
import pytest
from snowflake.connector.options import MissingPandas

from snowflake.snowpark._internal import utils
from snowflake.snowpark._internal.utils import (
    _pandas_importer,
    generate_random_alphanumeric,
    split_dot_string,
)


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
        ("/some/file.yml", False, "'/some/file.yml'"),
        ("'/some/file.yml'", False, "'/some/file.yml'"),
    ],
)
def test_normalize_path(path: str, is_local: bool, expected: str) -> None:
    actual = utils.normalize_path(path, is_local)
    assert expected == actual


def test__pandas_importer():
    imported_pandas = _pandas_importer()
    try:
        import pandas

        assert imported_pandas == pandas
    except ImportError:
        assert isinstance(imported_pandas, MissingPandas)


def test_generate_random_alphanumeric():
    random.seed(42)
    random_string1 = generate_random_alphanumeric()
    random.seed(42)
    random_string2 = generate_random_alphanumeric()
    assert (
        isinstance(random_string1, str)
        and isinstance(random_string2, str)
        and random_string1 != random_string2
    )

    with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
        # Submit tasks to the pool and get future objects
        futures = [executor.submit(generate_random_alphanumeric) for _ in range(5)]
        res = [f.result() for f in futures]
        assert len(set(res)) == 5  # no duplicate string


@pytest.mark.parametrize(
    "string, expected_result",
    [
        ('foo.bar."hello.world".baz', ["foo", "bar", '"hello.world"', "baz"]),
        ('"a.b".c."d.e.f".g', ['"a.b"', "c", '"d.e.f"', "g"]),
        ('x."y.z".w', ["x", '"y.z"', "w"]),
        ("noquotes.here", ["noquotes", "here"]),
        ('"only.one.token"', ['"only.one.token"']),
        ('start."middle.with.dots".end', ["start", '"middle.with.dots"', "end"]),
        ('part.with"quote".inside', ["part", 'with"quote"', "inside"]),
        (
            '"quoted with ""embedded"" quotes".end',
            ['"quoted with ""embedded"" quotes"', "end"],
        ),
        (
            '"quoted with ""embedded.and.some.dots"" quotes".end',
            ['"quoted with ""embedded.and.some.dots"" quotes"', "end"],
        ),
        (
            '"quoted with ""embedded.and.some.dots"" quotes escaped".end',
            ['"quoted with ""embedded.and.some.dots"" quotes escaped"', "end"],
        ),
        (
            '"open.quotes.end',
            ['"open', "quotes", "end"],
        ),
    ],
)
def test_split_dot_string(string, expected_result):
    assert split_dot_string(string) == expected_result
