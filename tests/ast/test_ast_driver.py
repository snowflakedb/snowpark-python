#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import importlib.util
import os
import pathlib
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from typing import List, Union

import pytest

TEST_DIR = pathlib.Path(__file__).parent

DATA_DIR = TEST_DIR / "data"


@dataclass
class TestCase:
    filename: str
    source: str
    expected_ast_base64: str
    expected_ast_unparsed: str


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
        expected_ast_base64_start = src.index("## EXPECTED ENCODED AST\n")
    except ValueError:
        raise ValueError(
            "Required header ## EXPECTED ENCODED AST missing in the file: " + file.name
        )

    try:
        expected_ast_unparsed_start = src.index("## EXPECTED UNPARSER OUTPUT\n")
    except ValueError:
        raise ValueError(
            "Required header ## EXPECTED UNPARSER OUTPUT missing in the file: " + file.name
        )

    test_case = "".join(src[test_case_start + 1 : expected_ast_base64_start])
    expected_ast_base64 = "".join(src[expected_ast_base64_start + 1 : expected_ast_unparsed_start])
    expected_ast_unparsed = "".join(src[expected_ast_unparsed_start + 1 :])

    return TestCase(os.path.basename(file.name), test_case, expected_ast_base64, expected_ast_unparsed)


def load_test_cases():
    """
    Loads and parses test files from the data/ subdirectory. The files must be named '*.test'.

    Returns: a list of test cases.
    """
    test_files = DATA_DIR.glob("*.test")
    return [parse_file(file) for file in test_files]


def idfn(val):
    return val.filename


def render(ast_base64: Union[str, List[str]]) -> str:
    """Uses the unparser to render the AST."""
    assert (
        pytest.unparser_jar
    ), "A valid Unparser JAR path must be supplied either via --unparser-jar=<path> or the environment variable SNOWPARK_UNPARSER_JAR"

    if isinstance(ast_base64, str):
        ast_base64 = [ast_base64]

    res = subprocess.run(
        [
            "java",
            "-cp",
            pytest.unparser_jar,
            "com.snowflake.snowpark.experimental.unparser.UnparserCli",
            ",".join(
                ast_base64
            ),  # base64 strings will not contain , so pass multiple batches comma-separated.
        ],
        capture_output=True,
        text=True,
        check=True,
    )

    return res.stdout


def indent_lines(source: str, n_indents: int = 0):
    indent = "    "
    source = source.replace("\t", indent)  # convert tabs to spaces.

    return "\n".join(map(lambda line: indent * n_indents + line, source.split("\n")))


def run_test(session, test_name, test_source):
    override_time_zone()
    os.chdir(DATA_DIR)

    source = f"""
import snowflake.snowpark.functions as functions
from snowflake.snowpark.functions import *
from snowflake.snowpark.types import *
from snowflake.snowpark import Table
from snowflake.snowpark._internal.ast import AstBatch
import snowflake.snowpark._internal.ast_utils as ast_utils

import uuid

# Set up the request ID generator.
AstBatch.request_id_gen_fn = lambda: uuid.uuid5(uuid.NAMESPACE_DNS, "id-gen")

ast_utils.SRC_POSITION_TEST_MODE = True

def run_test(session):
    # Reset the entity ID generator.
    session._ast_batch.__init__(session)

    # Set up mock data.
    mock = session.create_dataframe(
        [
            [1, "one"],
            [2, "two"],
            [3, "three"],
        ],
        schema=['num', 'str']
    )
    mock.write.save_as_table("test_table")
    mock = session.create_dataframe(
        [
            [1, "one"],
            [2, "two"],
            [3, "three"],
        ],
        schema=['num', 'Owner\\'s""opinion.s']
    )
    mock.write.save_as_table("\\"the#qui.ck#bro.wn#\\"\\"Fox\\"\\"won\\'t#jump!\\"")

    # Set up data used for set operation tests.
    mock = session.create_dataframe(
        [
            [1, 2],
            [3, 4],
        ],
        schema=["a", "b"]
    )
    mock.write.save_as_table("test_df1")
    mock = session.create_dataframe(
        [
            [0, 1],
            [3, 4],
        ],
        schema=["c", "d"]
    )
    mock.write.save_as_table("test_df2")
    mock = session.create_dataframe(
        [
            [1, 2],
        ],
        schema=["a", "b"]
    )
    mock.write.save_as_table("test_df3")
    mock = session.create_dataframe(
        [
            [2, 1],
        ],
        schema=["b", "a"]
    )
    mock.write.save_as_table("test_df4")

    mock = session.create_dataframe(
        [[1, [1, 2, 3], {{"Ashi Garami": "Single Leg X"}}, "Kimura"],
        [2, [11, 22], {{"Sankaku": "Triangle"}}, "Coffee"],
        [3, [], {{}}, "Tea"]],
        schema=["idx", "lists", "maps", "strs"])
    mock.write.save_as_table("test_table2")

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
        base64_batches = test_module.run_test(session)
        unparser_output = render(base64_batches) if pytest.unparser_jar else ""
        return unparser_output, "\n".join(base64_batches)
    finally:
        os.unlink(test_file.name)


@pytest.mark.parametrize("test_case", load_test_cases(), ids=idfn)
def test_ast(session, test_case):
    actual, base64 = run_test(
        session, test_case.filename.replace(".", "_"), test_case.source
    )
    if pytest.update_expectations:
        with open(DATA_DIR / test_case.filename, "w", encoding="utf-8") as f:
            f.writelines(
                [
                    "## TEST CASE\n",
                    test_case.source,
                    "## EXPECTED ENCODED AST\n\n",
                    base64.strip(),
                    "\n\n## EXPECTED UNPARSER OUTPUT\n\n",
                    actual.strip(),
                    "\n",
                ]
            )
    else:
        try:
            assert base64.strip() == test_case.expected_ast_base64.strip()
            if pytest.unparser_jar:
                assert actual.strip() == test_case.expected_ast_unparsed.strip()
        except AssertionError as e:
            raise AssertionError(
                f"If the expectation is incorrect, run pytest --update-expectations:\n{e}\nFUFU\n\n\n{base64} FUFU \n\n\n{test_case.expected_ast_base64}"
            ) from e


def override_time_zone() -> None:
    # Use any time zone other than America/Los_Angeles and UTC, to minimize the
    # odds of tests passing by luck.
    os.environ["TZ"] = "America/New_York"
    time.tzset()


if __name__ == "__main__":
    pytest.main()
