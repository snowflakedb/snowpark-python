#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from functools import partial
import pytest
import re
import os
import io
from io import UnsupportedOperation

try:
    import pandas  # noqa: F401

    is_pandas_available = True
except ImportError:
    is_pandas_available = False

from snowflake.snowpark.files import SnowflakeFile, _DEFER_IMPLEMENTATION_ERR_MSG
from typing import Union

from snowflake.snowpark._internal.utils import generate_random_alphanumeric
from snowflake.snowpark.mock.exceptions import SnowparkLocalTestingException
from snowflake.snowpark.exceptions import SnowparkSessionException
from snowflake.snowpark import Session
from tests.utils import IS_WINDOWS, Utils
import logging

_logger = logging.getLogger(__name__)

_STANDARD_ARGS = ["read_mode", "write_mode", "use_stage"]
_STANDARD_ARGVALUES = [
    ("r", "w", True),
    ("r", "w", False),
    ("rb", "wb", True),
    ("rb", "wb", False),
]

pytestmark = [
    pytest.mark.skipif(
        not is_pandas_available,
        reason="pandas is not available",
    ),
]


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_create_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        _, temp_file = Utils.write_test_msg(write_mode, tmp_path)
    else:
        _, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

    def sf_open_new_result(mode: str) -> None:
        with SnowflakeFile.open_new_result(mode) as f:
            assert f._file_location is not None
            assert f._mode == write_mode

    def sf_open(file_location: str, mode: str) -> None:
        with SnowflakeFile.open(file_location, mode) as f:
            assert f._file_location == file_location
            assert f._mode == read_mode

    sf_open_new_result(write_mode)
    sf_open(temp_file, read_mode)


@pytest.mark.parametrize("use_stage", [True, False])
def test_invalid_mode_snowflakefile(use_stage, tmp_path, tmp_stage):
    def sf_open_new_result(mode: str) -> None:
        SnowflakeFile.open_new_result(mode)

    def sf_open(file_location: str, mode: str) -> None:
        SnowflakeFile.open(file_location, mode)

    with pytest.raises(
        ValueError,
        match=re.escape(
            "Invalid mode 'w+' for SnowflakeFile.open_new_result. Supported modes are 'w' and 'wb'."
        ),
    ):
        sf_open_new_result("w+")

    with pytest.raises(
        ValueError,
        match="Invalid mode 'rw' for SnowflakeFile.open. Supported modes are 'r' and 'rb'.",
    ):
        if not use_stage:
            sf_open(tmp_path, "rw")
        else:
            sf_open(f"@{tmp_stage}/test", "rw")


@pytest.mark.parametrize("use_stage", [True, False])
def test_default_mode_snowflakefile(use_stage, tmp_path, tmp_stage, session):
    if not use_stage:
        _, temp_file = Utils.write_test_msg("w", tmp_path)
    else:
        _, temp_file = Utils.write_test_msg_to_stage("w", tmp_path, tmp_stage, session)

    def sf_open_new_result() -> None:
        with SnowflakeFile.open_new_result() as f:
            assert f._file_location is not None
            assert f._mode == "w"

    def sf_open(file_location: str) -> None:
        with SnowflakeFile.open(file_location) as f:
            assert f._file_location is not None
            assert f._mode == "r"

    sf_open_new_result()
    sf_open(temp_file)


@pytest.mark.parametrize("use_stage", [True, False])
def test_snowflake_file_attribute(use_stage, tmp_path, tmp_stage, session):
    if not use_stage:
        _, temp_file = Utils.write_test_msg("w", tmp_path)
    else:
        _, temp_file = Utils.write_test_msg_to_stage("w", tmp_path, tmp_stage, session)

    def sf_open_new_result() -> None:
        with SnowflakeFile.open_new_result() as f:
            assert f.buffer is None
            assert f.encoding is None
            assert f.errors is None

    def sf_open(file_location: str) -> None:
        with SnowflakeFile.open(file_location) as f:
            assert f.buffer is None
            assert f.encoding is None
            assert f.errors is None

    sf_open_new_result()
    sf_open(temp_file)


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_read_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        test_msg, temp_file = Utils.write_test_msg(write_mode, tmp_path)
    else:
        test_msg, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

    def read_file(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    assert read_file(temp_file, read_mode) == test_msg


def test_read_no_stage_snowflakefile(tmp_path, tmp_stage, session):
    test_msg, temp_file = Utils.write_test_msg_to_stage(
        "w", tmp_path, tmp_stage, session
    )

    def read_file(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    with pytest.raises(SnowparkLocalTestingException, match="the file does not exist:"):
        assert read_file(f"@nostage/{temp_file}", "r") == test_msg


def test_read_multiple_sessions_snowflakefile(tmp_path, tmp_stage, session):
    new_session = Session.builder.config("local_testing", True).create()
    test_msg, temp_file = Utils.write_test_msg_to_stage(
        "w", tmp_path, tmp_stage, session
    )

    def read_file(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    with pytest.raises(
        SnowparkSessionException, match="More than one active session is detected."
    ):
        assert read_file(f"@nostage/{temp_file}", "r") == test_msg
    new_session.close()


def test_read_snowurl_snowflakefile(tmp_path, session):
    test_msg, temp_file = Utils.write_test_msg("w", tmp_path)
    snowurl = "snow://"
    session.file.put(temp_file, snowurl, auto_compress=False)

    def read_file(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    assert read_file(f"{snowurl}{temp_file}", "r") == test_msg


@pytest.mark.parametrize(
    ["mode", "use_stage"], [("r", True), ("r", False), ("rb", True), ("rb", False)]
)
def test_read_empty_file_snowflakefile(mode, use_stage, tmp_path, tmp_stage, session):
    if not use_stage:
        _, temp_file = Utils.write_test_msg("w", tmp_path, "")
    else:
        _, temp_file = Utils.write_test_msg_to_stage(
            "w", tmp_path, tmp_stage, session, ""
        )

    def read_file(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    content = read_file(temp_file, mode)
    if mode == "rb":
        assert content == b""
    else:
        assert content == ""


@pytest.mark.parametrize(["read_mode", "write_mode"], [("r", "w"), ("rb", "wb")])
def test_read_large_file_snowflakefile_local(read_mode, write_mode, tmp_path):
    test_msg, temp_file = Utils.write_test_msg(
        write_mode, tmp_path, generate_random_alphanumeric(5000)
    )

    def read_file(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readall()

    assert read_file(temp_file, read_mode) == test_msg


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "size", "use_stage"],
    [
        ("r", "w", 1, True),
        ("r", "w", 1, False),
        ("r", "w", 0, True),
        ("r", "w", 0, False),
        ("rb", "wb", 1, True),
        ("rb", "wb", 1, False),
        ("rb", "wb", 0, True),
        ("rb", "wb", 0, False),
    ],
)
def test_read_with_size_snowflakefile(
    read_mode, write_mode, size, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        test_msg, temp_file = Utils.write_test_msg(write_mode, tmp_path)
    else:
        test_msg, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

    def read_file(file_location: str, mode: str, size: int) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read(size)

    content = read_file(temp_file, read_mode, size)
    if size == -1:
        assert content == test_msg
    else:
        assert content == test_msg[:size]


@pytest.mark.parametrize(
    ["mode", "use_stage"], [("r", True), ("r", False), ("rb", True), ("rb", False)]
)
def test_read_non_existent_snowflakefile(mode, use_stage, tmp_path, tmp_stage, session):
    def read_file(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    if not use_stage:
        not_found_file_path = os.path.join(tmp_path, "non_existent_file.txt")
        with pytest.raises(FileNotFoundError):
            assert read_file(not_found_file_path, mode)
    else:
        # For stage, simulate a missing file by using a non-existent stage location
        not_found_file_path = f"@{tmp_stage}/notfound"
        with pytest.raises(SnowparkLocalTestingException):
            assert read_file(not_found_file_path, mode)


@pytest.mark.parametrize("mode", ["w", "wb"])
def test_read_api_in_write_mode_snowflakefile(mode):
    def sf_throw_errors(mode: str) -> None:
        with SnowflakeFile.open_new_result(mode=mode) as f:
            methods = [
                f.read,
                f.read1,
                f.readall,
                partial(f.readinto, bytearray(1)),
                partial(f.readinto1, bytearray(1)),
                f.readline,
                f.readlines,
                partial(f.seek, 1),
                f.tell,
            ]
            for method in methods:
                with pytest.raises(
                    UnsupportedOperation, match=f"Not readable mode={mode}"
                ):
                    method()

    sf_throw_errors(mode)


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "test_msg", "use_stage"],
    [
        ("r", "w", "a", True),
        ("r", "w", "a", False),
        ("r", "w", "This is a test message with escape characters: \n\t", True),
        ("r", "w", "This is a test message with escape characters: \n\t", False),
        ("rb", "wb", "a", True),
        ("rb", "wb", "a", False),
        ("rb", "wb", "This is a test message with escape characters: \n\t", True),
        ("rb", "wb", "This is a test message with escape characters: \n\t", False),
    ],
)
def test_read_special_msg_snowflakefile(
    read_mode, write_mode, test_msg, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        test_msg, temp_file = Utils.write_test_msg(write_mode, tmp_path, test_msg)
    else:
        test_msg, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session, test_msg
        )

    def read_file(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    assert (
        read_file(temp_file, read_mode) == test_msg
        or read_file(temp_file, read_mode) == test_msg[:-3] + " \r\n\t"
    )  # Windows adds a \r before the \n when we read a file


@pytest.mark.parametrize("mode", ["w", "wb"])
def test_read_deleted_snowflakefile(mode, tmp_path):
    _, temp_file = Utils.write_test_msg(mode, tmp_path)
    os.remove(temp_file)

    with pytest.raises(
        FileNotFoundError,
        match=re.escape("No such file or directory"),
    ):

        def sf_open(file_location: str) -> None:
            SnowflakeFile.open(file_location)

        sf_open(temp_file)


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "use_tmp_path"],
    [("r", "w", True), ("r", "w", False), ("rb", "wb", True), ("rb", "wb", False)],
)
def test_read_relative_path_snowflakefile(
    read_mode, write_mode, use_tmp_path, tmp_path
):
    if use_tmp_path:
        if IS_WINDOWS:
            pytest.skip(
                "Windows does not support relpath between drives. tmp_dir is on C: while the working dir on a mounted drive."
            )
        test_msg, temp_file = Utils.write_test_msg(write_mode, tmp_path)
    else:
        # Write to a temp file in the current directory
        test_msg, temp_file = Utils.write_test_msg(write_mode, "")
    temp_file = os.path.relpath(temp_file)

    def read_file(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    assert read_file(temp_file, read_mode) == test_msg
    if not use_tmp_path:
        os.remove(temp_file)


def test_read_scoped_url_snowflakefile():
    scoped_url = "https://example.com/path/to/file.txt"

    def read_file(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read()

    with pytest.raises(
        ValueError, match="Scoped and Stage URLs are not yet supported."
    ):
        read_file(scoped_url, "r")


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_detach_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        _, temp_file = Utils.write_test_msg(write_mode, tmp_path)
    else:
        _, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )
    detach_error = "Detaching stream from file is unsupported"

    def sf_detach_write(mode: str) -> None:
        with SnowflakeFile.open_new_result(mode) as f:
            f.detach()

    def sf_detach_read(file_location: str, mode: str) -> None:
        with SnowflakeFile.open(file_location, mode) as f:
            f.detach()

    with pytest.raises(UnsupportedOperation, match=detach_error):
        sf_detach_write(write_mode)

    with pytest.raises(UnsupportedOperation, match=detach_error):
        sf_detach_read(temp_file, read_mode)


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_fileno_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        _, temp_file = Utils.write_test_msg(write_mode, tmp_path)
    else:
        _, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )
    fileno_error = "This object does not use a file descriptor"

    def sf_fileno_write(mode: str) -> int:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.fileno()

    def sf_fileno_read(file_location: str, mode: str) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.fileno()

    with pytest.raises(OSError, match=fileno_error):
        fileno = sf_fileno_write(write_mode)
        assert fileno

    with pytest.raises(OSError, match=fileno_error):
        fileno = sf_fileno_read(temp_file, read_mode)
        assert fileno


def test_flush_snowflakefile():
    # Flush currently has no implementation
    pass


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_isatty_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        _, temp_file = Utils.write_test_msg(write_mode, tmp_path)
    else:
        _, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

    def get_atty_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.isatty()

    def get_atty_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.isatty()

    assert not get_atty_write(write_mode)
    assert not get_atty_read(temp_file, read_mode)


@pytest.mark.parametrize(
    ["mode", "use_stage"], [("r", True), ("r", False), ("rb", True), ("rb", False)]
)
def test_truncate_read_mode_snowflakefile(
    mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        _, temp_file = Utils.write_test_msg("w", tmp_path)
    else:
        _, temp_file = Utils.write_test_msg_to_stage("w", tmp_path, tmp_stage, session)

    def sf_truncate(file_location: str, mode: str) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.truncate(1)

    with pytest.raises(UnsupportedOperation, match="Not writable mode"):
        num_bytes = sf_truncate(temp_file, mode)
        assert num_bytes


@pytest.mark.parametrize("mode", ["w", "wb"])
def test_truncate_write_mode_snowflakefile(mode):
    def sf_truncate(mode: str) -> int:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.truncate(1)

    with pytest.raises(NotImplementedError, match=_DEFER_IMPLEMENTATION_ERR_MSG):
        num_bytes = sf_truncate(mode)
        assert num_bytes


@pytest.mark.parametrize(
    ["mode", "use_stage"], [("r", True), ("r", False), ("rb", True), ("rb", False)]
)
def test_methods_closed_snowflakefile(tmp_path, mode, use_stage, tmp_stage, session):
    if not use_stage:
        _, temp_file = Utils.write_test_msg("w", tmp_path)
    else:
        _, temp_file = Utils.write_test_msg_to_stage("w", tmp_path, tmp_stage, session)

    def sf_closed(file_location: str, mode: str) -> None:
        with SnowflakeFile.open(file_location, mode) as f:
            f.close()

            methods = [
                f.detach,
                f.fileno,
                f.flush,
                f.isatty,
                f.read,
                f.read1,
                f.readable,
                f.readall,
                partial(f.readinto, bytearray(1)),
                partial(f.readinto1, bytearray(1)),
                f.readline,
                f.readlines,
                partial(f.seek, 1),
                f.seekable,
                f.tell,
                f.truncate,
                f.writable,
            ]
            for method in methods:
                with pytest.raises(ValueError, match="I/O operation on closed file."):
                    method()

    sf_closed(temp_file, mode)


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_readable_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        _, temp_file = Utils.write_test_msg("w", tmp_path)
    else:
        _, temp_file = Utils.write_test_msg_to_stage("w", tmp_path, tmp_stage, session)

    def is_readable_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.readable()

    def is_readable_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readable()

    assert not is_readable_write(write_mode)
    assert is_readable_read(temp_file, read_mode)


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_readall_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    num_lines = 5
    if not use_stage:
        lines, temp_file = Utils.generate_and_write_lines(
            num_lines, write_mode, tmp_path
        )
    else:
        lines, temp_file = Utils.generate_and_write_lines_to_stage(
            num_lines, write_mode, tmp_path, tmp_stage, session
        )

    def sf_readall(file_location: str, mode: str) -> list:
        with SnowflakeFile.open(file_location, mode) as snowflake_file:
            return snowflake_file.readall()

    content = sf_readall(temp_file, read_mode)
    windows_lines = [
        line[:-1] + b"\r\n" if read_mode == "rb" else line[:-1] + "\r\n"
        for line in lines
    ]  # need for windows testing as \r is added
    if write_mode == "wb":
        assert content == b"".join(lines) or content == b"".join(windows_lines)
    else:
        assert content == "".join(lines) or content == "".join(windows_lines)


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_readline_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    num_lines = 5
    if not use_stage:
        lines, temp_file = Utils.generate_and_write_lines(
            num_lines, write_mode, tmp_path
        )
    else:
        lines, temp_file = Utils.generate_and_write_lines_to_stage(
            num_lines, write_mode, tmp_path, tmp_stage, session
        )

    def sf_readline(file_location: str, mode: str) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readline()

    def sf_read_num_lines(file_location: str, mode: str, num_lines: int) -> list:
        with SnowflakeFile.open(file_location, mode) as f:
            return [f.readline() for _ in range(num_lines + 1)]

    windows_lines = [
        line[:-1] + b"\r\n" if read_mode == "rb" else line[:-1] + "\r\n"
        for line in lines
    ]  # need for windows testing as \r is added
    first_line = sf_readline(temp_file, read_mode)
    assert first_line == lines[0] or first_line == windows_lines[0]
    content = sf_read_num_lines(temp_file, read_mode, num_lines)

    for i in range(num_lines):
        assert content[i] == lines[i] or content[i] == windows_lines[i]

    if write_mode == "wb":
        assert content[-1] == b""
    else:
        assert content[-1] == ""


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "size", "use_stage"],
    [
        ("r", "w", 2, True),
        ("r", "w", 2, False),
        ("r", "w", 0, True),
        ("r", "w", 0, False),
        ("rb", "wb", 2, True),
        ("rb", "wb", 2, False),
        ("rb", "wb", 0, True),
        ("rb", "wb", 0, False),
    ],
)
def test_readline_with_size_snowflakefile(
    read_mode, write_mode, size, use_stage, tmp_path, tmp_stage, session
):
    num_lines = 5
    if not use_stage:
        lines, temp_file = Utils.generate_and_write_lines(
            num_lines, write_mode, tmp_path
        )
    else:
        lines, temp_file = Utils.generate_and_write_lines_to_stage(
            num_lines, write_mode, tmp_path, tmp_stage, session
        )

    def sf_readline_with_size(
        file_location: str, mode: str, size: int
    ) -> Union[str, bytes]:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readline(size)

    content = sf_readline_with_size(temp_file, read_mode, size)
    windows_lines = [
        line[:-1] + b"\r\n" if read_mode == "rb" else line[:-1] + "\r\n"
        for line in lines
    ]  # need for windows testing as \r is added
    if size == -1:
        assert content == lines[0] or content == windows_lines[0]
    else:
        assert content == lines[0][:size] or content == windows_lines[0][:size]


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_readlines_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    num_lines = 5
    if not use_stage:
        lines, temp_file = Utils.generate_and_write_lines(
            num_lines, write_mode, tmp_path
        )
    else:
        lines, temp_file = Utils.generate_and_write_lines_to_stage(
            num_lines, write_mode, tmp_path, tmp_stage, session
        )

    def sf_readlines(file_location: str, mode: str) -> list:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readlines()

    content = sf_readlines(temp_file, read_mode)
    windows_lines = [
        line[:-1] + b"\r\n" if read_mode == "rb" else line[:-1] + "\r\n"
        for line in lines
    ]  # need for windows testing as \r is added
    for i in range(num_lines):
        assert content[i] == lines[i] or content[i] == windows_lines[i]


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_readlines_with_unicode_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    num_lines = 5
    if not use_stage:
        lines, temp_file = Utils.generate_and_write_lines(
            num_lines, write_mode, tmp_path, "これはUnicodeテストです"
        )
    else:
        lines, temp_file = Utils.generate_and_write_lines_to_stage(
            num_lines, write_mode, tmp_path, tmp_stage, session, "これはUnicodeテストです"
        )

    def sf_readlines(file_location: str, mode: str) -> list:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readlines()

    content = sf_readlines(temp_file, read_mode)
    windows_lines = [
        line[:-1] + b"\r\n" if read_mode == "rb" else line[:-1] + "\r\n"
        for line in lines
    ]  # need for windows testing as \r is added
    for i in range(num_lines):
        assert content[i] == lines[i] or content[i] == windows_lines[i]


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "hint", "use_stage"],
    [
        ("r", "w", 2, True),
        ("r", "w", 2, False),
        ("r", "w", 0, True),
        ("r", "w", 0, False),
        ("rb", "wb", 2, True),
        ("rb", "wb", 2, False),
        ("rb", "wb", 0, True),
        ("rb", "wb", 0, False),
    ],
)
def test_readlines_with_hint_snowflakefile(
    read_mode, write_mode, hint, use_stage, tmp_path, tmp_stage, session
):
    num_lines = 5
    if not use_stage:
        lines, temp_file = Utils.generate_and_write_lines(
            num_lines, write_mode, tmp_path
        )
    else:
        lines, temp_file = Utils.generate_and_write_lines_to_stage(
            num_lines, write_mode, tmp_path, tmp_stage, session
        )

    def sf_readlines_with_hint(file_location: str, mode: str, hint: int) -> list:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readlines(hint=hint)

    content = sf_readlines_with_hint(temp_file, read_mode, hint)
    windows_lines = [
        line[:-1] + b"\r\n" if read_mode == "rb" else line[:-1] + "\r\n"
        for line in lines
    ]  # need for windows testing as \r is added
    for i in range(min(num_lines, len(content))):
        assert content[i] == lines[i] or content[i] == windows_lines[i]


# Tests seek relative to the starting pos, seeking forward and backwards
# relative to the current stream pos after seeking forward by 5, and
# seeking backwards from the end of the stream.
@pytest.mark.parametrize(
    ["read_mode", "write_mode", "offset", "whence", "use_stage"],
    [
        ("r", "w", 0, io.SEEK_SET, True),
        ("r", "w", 0, io.SEEK_SET, False),
        ("r", "w", 3, io.SEEK_SET, True),
        ("r", "w", 3, io.SEEK_SET, False),
        ("rb", "wb", 0, io.SEEK_SET, True),
        ("rb", "wb", 0, io.SEEK_SET, False),
        ("rb", "wb", 3, io.SEEK_SET, True),
        ("rb", "wb", 3, io.SEEK_SET, False),
        ("r", "w", 3, io.SEEK_CUR, True),
        ("r", "w", 3, io.SEEK_CUR, False),
        ("r", "w", -3, io.SEEK_CUR, True),
        ("r", "w", -3, io.SEEK_CUR, False),
        ("rb", "wb", 3, io.SEEK_CUR, True),
        ("rb", "wb", 3, io.SEEK_CUR, False),
        ("rb", "wb", -3, io.SEEK_CUR, True),
        ("rb", "wb", -3, io.SEEK_CUR, False),
        ("r", "w", -3, io.SEEK_END, True),
        ("r", "w", -3, io.SEEK_END, False),
        ("rb", "wb", -3, io.SEEK_END, True),
        ("rb", "wb", -3, io.SEEK_END, False),
    ],
)
def test_seek_snowflakefile(
    read_mode, write_mode, offset, whence, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        test_msg, temp_file = Utils.write_test_msg(write_mode, tmp_path)
    else:
        test_msg, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

    def sf_seek(
        file_location: str, mode: str, initial_offset: int, offset: int, whence: int
    ) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            f.seek(initial_offset)
            return f.seek(offset, whence)

    # Used to support the negative seeks for seek_cur
    initial_offset = 5
    pos = sf_seek(temp_file, read_mode, initial_offset, offset, whence)
    if whence == io.SEEK_SET:
        assert pos == offset
    elif whence == io.SEEK_CUR:
        assert pos == offset + initial_offset
    else:
        assert pos == (len(test_msg) + offset)


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "offset", "whence", "use_stage"],
    [
        ("r", "w", -1, io.SEEK_SET, True),
        ("r", "w", -1, io.SEEK_SET, False),
        ("rb", "wb", -1, io.SEEK_SET, True),
        ("rb", "wb", -1, io.SEEK_SET, False),
        ("r", "w", 3, 4, True),
        ("r", "w", 3, 4, False),
        ("rb", "wb", 3, 4, True),
        ("rb", "wb", 3, 4, False),
    ],
)
def test_seek_error_snowflakefile(
    read_mode, write_mode, offset, whence, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        _, temp_file = Utils.write_test_msg(write_mode, tmp_path)
    else:
        _, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

    def sf_seek(file_location: str, mode: str, offset: int, whence: int) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.seek(offset, whence)

    if whence not in range(3):
        with pytest.raises(
            NotImplementedError, match=f"Unsupported whence value {whence}"
        ):
            assert sf_seek(temp_file, read_mode, offset, whence)
    else:
        with pytest.raises(ValueError, match="Negative seek position"):
            assert sf_seek(temp_file, read_mode, offset, whence)


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_seekable_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        _, temp_file = Utils.write_test_msg(write_mode, tmp_path)
    else:
        _, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

    def is_seekable_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.seekable()

    def is_seekable_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.seekable()

    assert not is_seekable_write(write_mode)
    assert is_seekable_read(temp_file, read_mode)


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_tell_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    temp_file = os.path.join(tmp_path, "test.txt")
    test_msg = generate_random_alphanumeric(9) + "\n"
    length = len(test_msg)
    if write_mode == "wb":
        test_msg = test_msg.encode()

    with open(temp_file, mode=write_mode) as f:
        f.writelines(
            [test_msg, test_msg]
        )  # used to test reads that don't read an entire file
        counter = 2

    if use_stage:
        Utils.upload_to_stage(session, tmp_stage, temp_file, compress=False)

    def sf_tell(file_location: str, mode: str, counter: int, length: int) -> None:
        with SnowflakeFile.open(file_location, mode) as f:
            # Methods that read the entire file
            methods = [
                f.read,
                f.readall,
                f.readlines,
            ]

            if read_mode == "rb":
                methods.append(f.read1)

            for method in methods:
                method()
                assert f.tell() == counter * length
                f.seek(0)

            # Methods that read a portion of the file
            methods = [
                f.readline,
                partial(f.read, length),
                partial(f.seek, length),
            ]

            for method in methods:
                method()
                assert f.tell() == length
                f.seek(0)

            if read_mode == "rb":
                size = 5
                methods = [
                    partial(f.readinto, bytearray(size)),
                    partial(f.readinto1, bytearray(size)),
                ]

                for method in methods:
                    method()
                    assert f.tell() == size
                    f.seek(0)

    sf_tell(temp_file, read_mode, counter, length)


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_writable_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        _, temp_file = Utils.write_test_msg(write_mode, tmp_path)
    else:
        _, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )

    def is_writable_write(mode: str) -> bool:
        with SnowflakeFile.open_new_result(mode) as f:
            return f.writable()

    def is_writable_read(file_location: str, mode: str) -> bool:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.writable()

    assert is_writable_write(write_mode)
    assert not is_writable_read(temp_file, read_mode)


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_read1_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        test_msg, temp_file = Utils.write_test_msg(write_mode, tmp_path)
    else:
        test_msg, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )
    if type(test_msg) is str:
        test_msg = test_msg.encode()

    def sf_read1(file_location: str, mode: str) -> bytes:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read1()

    assert sf_read1(temp_file, read_mode) == test_msg


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "size", "use_stage"],
    [
        ("r", "w", 1, True),
        ("r", "w", 1, False),
        ("r", "w", 0, True),
        ("r", "w", 0, False),
        ("rb", "wb", 1, True),
        ("rb", "wb", 1, False),
        ("rb", "wb", 0, True),
        ("rb", "wb", 0, False),
    ],
)
def test_read1_with_size_snowflakefile(
    read_mode, write_mode, size, use_stage, tmp_path, tmp_stage, session
):
    if not use_stage:
        test_msg, temp_file = Utils.write_test_msg(write_mode, tmp_path)
    else:
        test_msg, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session
        )
    if type(test_msg) is str:
        test_msg = test_msg.encode()

    def sf_read1(file_location: str, mode: str, size: int) -> bytes:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.read1(size=size)

    content = sf_read1(temp_file, read_mode, size)
    if size == -1:
        assert content == test_msg
    else:
        assert content == test_msg[:size]


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "size", "use_stage"],
    [
        ("r", "w", 0, True),
        ("r", "w", 0, False),
        ("r", "w", 1, True),
        ("r", "w", 1, False),
        ("rb", "wb", 0, True),
        ("rb", "wb", 0, False),
        ("rb", "wb", 1, True),
        ("rb", "wb", 1, False),
    ],
)
def test_readinto_snowflakefile(
    read_mode, write_mode, size, use_stage, tmp_path, tmp_stage, session
):
    test_msg = generate_random_alphanumeric(size)
    if not use_stage:
        _, temp_file = Utils.write_test_msg(write_mode, tmp_path, test_msg)
    else:
        _, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session, test_msg
        )
    encoded_test_msg = test_msg.encode()

    def sf_readinto(file_location: str, mode: str, buffer: bytearray) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readinto(buffer)

    buffer_size = 5
    buffer = bytearray(buffer_size)
    length = sf_readinto(temp_file, read_mode, buffer)
    num_read = min(size, buffer_size)
    buffer = bytes(buffer)

    assert length == num_read
    assert buffer[:num_read] == encoded_test_msg[:num_read]
    for byte in buffer[num_read:]:
        assert byte == 0


@pytest.mark.parametrize(
    _STANDARD_ARGS,
    _STANDARD_ARGVALUES,
)
def test_readinto_escape_chars_snowflakefile(
    read_mode, write_mode, use_stage, tmp_path, tmp_stage, session
):
    test_msg = "This is a test message with escape characters: \n\t"
    if not use_stage:
        _, temp_file = Utils.write_test_msg(write_mode, tmp_path, test_msg)
    else:
        _, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session, test_msg
        )
    encoded_test_msg = test_msg.encode()

    def sf_readinto(file_location: str, mode: str, buffer: bytearray) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readinto(buffer)

    buffer_size = 50
    buffer = bytearray(buffer_size)
    length = sf_readinto(temp_file, read_mode, buffer)
    buffer = bytes(buffer)

    assert (
        length == len(encoded_test_msg) or length == len(encoded_test_msg) + 1
    )  # Windows adds a \r before the \n when we read a file
    assert (
        buffer[:length] == encoded_test_msg[:length]
        or buffer[:length] == encoded_test_msg[: length - 3] + b"\r\n\t"
    )
    for byte in buffer[length:]:
        assert byte == 0


@pytest.mark.parametrize(
    ["read_mode", "write_mode", "size", "use_stage"],
    [
        ("r", "w", 0, True),
        ("r", "w", 0, False),
        ("r", "w", 1, True),
        ("r", "w", 1, False),
        ("rb", "wb", 0, True),
        ("rb", "wb", 0, False),
        ("rb", "wb", 1, True),
        ("rb", "wb", 1, False),
    ],
)
def test_readinto1_snowflakefile(
    read_mode, write_mode, size, use_stage, tmp_path, tmp_stage, session
):
    test_msg = generate_random_alphanumeric(size)
    if not use_stage:
        _, temp_file = Utils.write_test_msg(write_mode, tmp_path, test_msg)
    else:
        _, temp_file = Utils.write_test_msg_to_stage(
            write_mode, tmp_path, tmp_stage, session, test_msg
        )

    encoded_test_msg = test_msg.encode()

    def sf_readinto1(file_location: str, mode: str, buffer: bytearray) -> int:
        with SnowflakeFile.open(file_location, mode) as f:
            return f.readinto1(buffer)

    buffer_size = 5
    buffer = bytearray(buffer_size)
    length = sf_readinto1(temp_file, read_mode, buffer)
    num_read = min(size, buffer_size)
    buffer = bytes(buffer)

    assert length == num_read
    assert buffer[:num_read] == encoded_test_msg[:num_read]
    for byte in buffer[num_read:]:
        assert byte == 0
