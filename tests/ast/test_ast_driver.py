#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import datetime
import importlib.util
import logging
import os
import pathlib
import platform
import re
import sys
import tempfile
import time
from dataclasses import dataclass

import dateutil
import google.protobuf
import pytest
from dateutil.tz import tzlocal

from snowflake.snowpark._internal.ast.utils import (
    ClearTempTables,
    base64_lines_to_textproto,
    clear_line_no_in_request,
    textproto_to_request,
)
from snowflake.snowpark._internal.utils import global_counter
from tests.ast.ast_test_utils import render
from tests.utils import TestFiles

_logger = logging.getLogger(__name__)

TEST_DIR = pathlib.Path(__file__).parent

DATA_DIR = TEST_DIR / "data"


@dataclass
class TestCase:
    filename: str
    source: str
    expected_ast_encoded: str
    expected_ast_unparsed: str
    __test__: bool = False  # Add this to suppress pytest collection warning.


def parse_file(file):
    """Parses a test case file."""
    # TODO(oplaton): Auto-formatters keep simplifying open(file, "r", ...) to drop the mode.
    # Find a way to keep it as is.
    with open(file, encoding="utf-8") as f:
        src = f.readlines()

    try:
        test_case_start = src.index("## TEST CASE\n")
    except ValueError:
        raise ValueError(
            "Required header ## TEST CASE missing in the file: " + file.name
        )

    try:
        expected_ast_unparsed_start = src.index("## EXPECTED UNPARSER OUTPUT\n")
    except ValueError:
        raise ValueError(
            "Required header ## EXPECTED UNPARSER OUTPUT missing in the file: "
            + file.name
        )

    try:
        expected_ast_encoded_start = src.index("## EXPECTED ENCODED AST\n")
    except ValueError:
        raise ValueError(
            "Required header ## EXPECTED ENCODED AST missing in the file: " + file.name
        )

    test_case = "".join(src[test_case_start + 1 : expected_ast_unparsed_start])
    expected_ast_unparsed = "".join(
        src[expected_ast_unparsed_start + 1 : expected_ast_encoded_start]
    )
    expected_ast_encoded = "".join(src[expected_ast_encoded_start + 1 :])

    return TestCase(
        os.path.basename(file.name),
        test_case,
        expected_ast_encoded,
        expected_ast_unparsed,
    )


def load_test_cases():
    """
    Loads and parses test files from the data/ subdirectory. The files must be named '*.test'.

    Returns: a list of test cases.
    """
    test_files = DATA_DIR.glob("*.test")
    if sys.version_info[0] == 3 and sys.version_info[1] < 9:
        # Remove the `to_snowpark_pandas` test since Snowpark pandas is only supported in Python 3.9+.
        test_files = filter(
            lambda file: "to_snowpark_pandas" not in file.name, test_files
        )
    return [parse_file(file) for file in test_files]


def idfn(val):
    return val.filename


def indent_lines(source: str, n_indents: int = 0):
    indent = "    "
    source = source.replace("\t", indent)  # convert tabs to spaces.

    return "\n".join(map(lambda line: indent * n_indents + line, source.split("\n")))


def normalize_temp_names(s: str):
    """Replace random part of db object names with constant so value is deterministic for comparisons"""
    return re.sub(
        r"SNOWPARK_TEMP_([a-zA-Z]*(_FUNCTION)?)_(\w+)", r"SNOWPARK_TEMP_\1_xxx", s
    )


def run_test(session, tables, test_name, test_source, resources_path):
    override_time_zone()
    os.chdir(DATA_DIR)

    test_files = TestFiles(resources_path)
    source = f"""
import snowflake.snowpark.functions as functions
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
from snowflake.snowpark import Table
from snowflake.snowpark._internal.ast.batch import AstBatch
import snowflake.snowpark._internal.ast.utils as ast_utils

import uuid

# Set up the request ID generator.
AstBatch.generate_request_id = lambda: uuid.uuid5(uuid.NAMESPACE_DNS, "id-gen")

ast_utils.SRC_POSITION_TEST_MODE = True

def run_test(session, tables, test_files):
    # Reset the entity ID generator.
    session._ast_batch._reset_id_gen()

    session._ast_batch.flush()  # Clear the AST.

    # Run the test.
    with session.ast_listener() as al:
        {indent_lines(test_source, 2)}
        # Perform extra-flush for any pending statements.
        _, last_batch = session._ast_batch.flush()

    # Retrieve the ASTs corresponding to the test.
    result = al.base64_batches
    if last_batch:
        result.append(last_batch)
    return result
"""
    # We don't care about the results, and also want to test some APIs that can't be mocked. This suppresses an error
    # that would otherwise be thrown.
    session._conn._suppress_not_implemented_error = True

    global_counter.reset()
    # We use temp files to enable symbol capture in the AST. Having an underlying file instead of simply calling
    # `exec(source, globals)` allows the Python interpreter to correctly capture symbols, which we use in the AST.
    test_file = tempfile.NamedTemporaryFile(
        mode="w", prefix="test_ast", suffix=".py", delete=False
    )
    try:
        test_file.write(source)
        test_file.close()

        spec = importlib.util.spec_from_file_location(test_name, test_file.name)
        test_module = importlib.util.module_from_spec(spec)
        sys.modules[test_name] = test_module
        spec.loader.exec_module(test_module)
        base64_batches = test_module.run_test(session, tables, test_files)
        raw_unparser_output = (
            render(base64_batches, pytest.unparser_jar) if pytest.unparser_jar else ""
        )
        unparser_output = normalize_temp_names(raw_unparser_output)
        return unparser_output, "\n".join(base64_batches)
    except Exception as e:
        raise Exception("Generated AST test failed") from e
    finally:
        os.unlink(test_file.name)


@pytest.mark.parametrize("test_case", load_test_cases(), ids=idfn)
def test_ast(session, tables, test_case, resources_path):
    _logger.info(f"Testing AST encoding with protobuf {google.protobuf.__version__}.")

    # Reset string interning (avoids issues for testing).
    from snowflake.snowpark._internal.ast.utils import (
        __intern_string,
        __reset_interning_map,
    )

    __reset_interning_map()
    assert __intern_string("SRC_POSITION_TEST_MODE") == 2

    actual, base64_str = run_test(
        session,
        tables,
        test_case.filename.replace(".", "_"),
        test_case.source,
        resources_path,
    )

    if pytest.update_expectations:
        assert pytest.unparser_jar, (
            "Can only update expectations with unparser jar set. Either run the test with --unparser-jar=<path> or"
            " update the environment variable MONOREPO_DIR."
        )
        with open(DATA_DIR / test_case.filename, "w", encoding="utf-8") as f:
            f.writelines(
                [
                    "## TEST CASE\n",
                    test_case.source,
                    "## EXPECTED UNPARSER OUTPUT\n\n",
                    actual.strip(),
                    "\n\n## EXPECTED ENCODED AST\n\n",
                    normalize_temp_names(base64_lines_to_textproto(base64_str.strip())),
                ]
            )
    else:
        try:
            # Protobuf serialization is non-deterministic (cf. https://gist.github.com/kchristidis/39c8b310fd9da43d515c4394c3cd9510)
            # Therefore unparse from base64, and then check equality using deterministic (python) protobuf serialization.
            actual_message = textproto_to_request(
                normalize_temp_names(base64_lines_to_textproto(base64_str.strip()))
            )
            expected_message = textproto_to_request(
                test_case.expected_ast_encoded.strip()
            )

            # Actual and expected may have been encoded by different client language versions, e.g. Python 3.8.10 and
            # Python 3.9.3. Make comparison here client-language agnostic by removing the data from the message.
            actual_message.ClearField("client_language")
            expected_message.ClearField("client_language")

            # Similarly, for create_dataframe with temporary tables clear them as they may differ from session to session.
            ClearTempTables(actual_message)
            ClearTempTables(expected_message)

            # If this is not python 3.11+, then for the purposes of the expectation tests we will ignore the line_no
            # information since it can be different based on various python bug fixes.
            if sys.version_info.minor <= 10:
                clear_line_no_in_request(actual_message)
                clear_line_no_in_request(expected_message)

            det_actual_message = actual_message.SerializeToString(deterministic=True)
            det_expected_message = expected_message.SerializeToString(
                deterministic=True
            )

            assert det_actual_message == det_expected_message

            if pytest.unparser_jar:
                assert actual.strip() == test_case.expected_ast_unparsed.strip()

        except AssertionError as e:

            actual_lines = str(actual_message).splitlines()
            expected_lines = str(expected_message).splitlines()

            from difflib import Differ

            differ = Differ()
            diffed_lines = [
                line for line in differ.compare(actual_lines, expected_lines)
            ]

            _logger.error(
                "expected vs. actual encoded protobuf:\n" + "\n".join(diffed_lines)
            )

            raise AssertionError(
                f"If the expectation is incorrect, run pytest --update-expectations:\n\n{base64_str}\n{e}"
            ) from e


def override_time_zone(tz_name: str = "EST") -> None:
    # Use any time zone other than America/Los_Angeles and UTC, to minimize the
    # odds of tests passing by luck.

    tz = dateutil.tz.gettz(tz_name)
    tz_code = tz.tzname(datetime.datetime.now())
    _logger.debug(f"Overriding time zone to {tz_name} ({tz_code}).")

    if platform.system() != "Windows":
        # This works only under Unix systems.
        os.environ["TZ"] = tz_name
        time.tzset()
    else:
        # Under windows the CPython implementation uses localtime_s (https://learn.microsoft.com/en-us/cpp/c-runtime-library/reference/localtime-s-localtime32-s-localtime64-s?view=msvc-170)
        # cf. https://github.com/python/cpython/blob/3.8/Python/pytime.c#L1052. However, there's a well-known bug that
        # the timezone settings get only initialized at process startup, so they can not be modified from within
        # the process. Even calling the underlying msvcrt runtime dll will not propagate the changes to localtime_s.
        # As a fix to run testing, the process needs to be started up with the correct environment variable.
        # Hence, check here that the environment variable TZ is present.
        env_tz = os.environ.get("TZ") or ""
        assert env_tz == tz_code, (
            f"Testing with timezone={tz_name} ({tz_code}) but"
            f" not set as environment variable(is: '{env_tz}'). Add TZ=\"{tz_name}\" to environment"
            f" and restart process."
        )

        # Possible modification code (not working):
        # Use direct msvcrt.dll override (only for this process, does not work for child processes).
        # cf. https://learn.microsoft.com/en-us/cpp/c-runtime-library/reference/tzset?view=msvc-170
        _logger.debug(
            f"Windows current time (before msvcrt set): {datetime.datetime.now()}"
        )

        from ctypes import cdll

        cdll.msvcrt._putenv(f"TZ={tz_code}")
        cdll.msvcrt._tzset()
        # If working, we would expect this to show output adjusted to the timezone referred to by tz_code.
        _logger.debug(
            f"Windows current time (after msvcrt set): {datetime.datetime.now()}"
        )
        # Other python libraries would have been updated then as well.
        from tzlocal import get_localzone

        _logger.debug(
            f"Windows: tzlocal={get_localzone()} TZ={env_tz}, will be using TZ when encoding for AST."
        )

    tz_name = datetime.datetime.now(tzlocal()).tzname()
    _logger.debug(f"Local time zone is now: {tz_name}.")


if __name__ == "__main__":
    pytest.main()
