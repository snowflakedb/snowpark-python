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
    split_snowflake_identifier_with_dot,
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


def _decode_snowflake_literal(literal: str) -> str:
    """Simulate Snowflake's decoding of a single-quoted string literal.

    Snowflake treats ``\\`` as an escape character inside a single-quoted literal,
    so ``\\\\`` decodes to one backslash and ``\\'`` decodes to one single quote.
    An unescaped single quote closes the literal. This helper returns the decoded
    literal value and raises if the literal is closed early -- which would mean the
    path was not escaped correctly and the generated SQL is invalid.
    """
    assert literal.startswith("'") and literal.endswith(
        "'"
    ), f"not a quoted literal: {literal!r}"
    body = literal[1:-1]
    out = []
    i = 0
    while i < len(body):
        ch = body[i]
        if ch == "\\" and i + 1 < len(body):
            out.append(body[i + 1])
            i += 2
        elif ch == "'":
            raise AssertionError(
                f"unescaped quote closes literal early at index {i}: {literal!r}"
            )
        else:
            out.append(ch)
            i += 1
    return "".join(out)


@pytest.mark.parametrize("is_local", [True, False])
@pytest.mark.parametrize(
    "raw_path",
    [
        # Paths containing a backslash immediately followed by a single quote,
        # plus parentheses, commas and a trailing ``--``. Before the fix the
        # backslash was not escaped, so ``\'`` was written as ``\\'`` and closed
        # the literal early, producing invalid SQL.
        "@~/out\\' , (note) FILE_FORMAT=(TYPE=CSV) -- draft",
        "report\\' , (v2) draft --",
        # Plain special characters that must round-trip as literal data.
        "@stage/o'clock/file.csv",
        "@stage/back\\slash/file.csv",
        "@stage/double\\\\back/file.csv",
        '@stage/dquote"/file.csv',
        "@stage/uniécode/file.csv",
        "@stage/all\\'\"mix/file.csv",
    ],
)
def test_normalize_path_escapes_backslash_and_quote(raw_path, is_local):
    """``normalize_path`` must produce a Snowflake string literal that decodes back
    to the original path. A backslash followed by a single quote must stay inside
    the literal and not close it early, so the generated SQL is always valid and
    the path is treated as literal data."""
    literal = utils.normalize_path(raw_path, is_local)
    # The output must be a well-formed single-quoted literal: decoding it must not
    # raise (i.e. the literal is not closed early).
    decoded = _decode_snowflake_literal(literal)
    # The decoded literal must end with the (stripped) raw path -- the prefix may
    # differ only by an added ``@`` / ``file://`` scheme prefix.
    expected_tail = raw_path.strip()
    # Local paths on Windows are normalized (backslashes -> forward slashes)
    # before escaping, so mirror that transform here. This only affects the
    # round-trip comparison; the escaping guarantee checked above (the literal
    # never closes early) still holds for every input on every platform.
    if is_local and utils.OPERATING_SYSTEM == "Windows":
        expected_tail = expected_tail.replace("\\", "/")
    assert decoded.endswith(
        expected_tail
    ), f"decoded={decoded!r} does not end with {expected_tail!r}"


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
        (
            'a."b.""c".d."e.f.g".h.i.j."k"".""l."',
            ["a", '"b.""c"', "d", '"e.f.g"', "h", "i", "j", '"k"".""l."'],
        ),
    ],
)
def test_split_snowflake_identifier_with_dot(string, expected_result):
    assert split_snowflake_identifier_with_dot(string) == expected_result
