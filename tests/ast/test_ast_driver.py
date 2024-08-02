#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import os
import pathlib
import subprocess
from dataclasses import dataclass
from typing import List, Union

import pytest

TEST_DIR = pathlib.Path(__file__).parent

DATA_DIR = TEST_DIR / "data"


@dataclass
class TestCase:
    filename: str
    source: str
    expected_output: str


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
        expected_output_start = src.index("## EXPECTED OUTPUT\n")
    except ValueError:
        raise ValueError(
            "Required header ## EXPECTED OUTPUT missing in the file: " + file.name
        )

    test_case = "".join(src[test_case_start + 1 : expected_output_start])
    expected_output = "".join(src[expected_output_start + 1 :])

    return TestCase(os.path.basename(file.name), test_case, expected_output)


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


def run_test(session, test_source):
    os.chdir(DATA_DIR)

    source = f"""
import snowflake.snowpark.functions as functions
from snowflake.snowpark.functions import *
from snowflake.snowpark import Table

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

session._ast_batch.flush()  # Clear the AST.

# Run the test.
with session.ast_listener() as al:
    {indent_lines(test_source, 1)}
    # Perform extra-flush for any pending statements.
    _, last_batch = session._ast_batch.flush()

# Retrieve the ASTs corresponding to the test.
result = al.base64_batches
if last_batch:
    result.append(last_batch)
"""
    # We don't care about the results, and also want to test some APIs that can't be mocked. This suppresses an error
    # that would otherwise be thrown.
    session._conn._suppress_not_implemented_error = True

    locals = {"session": session}
    exec(source, locals)
    base64_batches = locals["result"]
    return render(base64_batches), "\n".join(base64_batches)


@pytest.mark.parametrize("test_case", load_test_cases(), ids=idfn)
def test_ast(session, test_case):
    actual, base64 = run_test(session, test_case.source)
    if pytest.update_expectations:
        with open(DATA_DIR / test_case.filename, "w", encoding="utf-8") as f:
            f.writelines(
                [
                    "## TEST CASE\n",
                    test_case.source,
                    "## EXPECTED OUTPUT\n\n",
                    actual.strip(),
                    "\n",
                ]
            )
    else:
        try:
            assert actual.strip() == test_case.expected_output.strip()
        except AssertionError as e:
            raise AssertionError(
                f"If the expectation is incorrect, run pytest --update-expectations:\n\n{base64}\n{e}"
            ) from e


if __name__ == "__main__":
    # Use any time zone other than America/Los_Angeles and UTC, to minimize the
    # odds of tests passing by luck.
    os.environ["TZ"] = "America/New_York"
    pytest.main()
