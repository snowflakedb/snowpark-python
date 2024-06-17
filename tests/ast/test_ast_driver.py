#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import os
import pathlib
import subprocess
from dataclasses import dataclass

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
    with open(file, "r", encoding="utf-8") as f:
        src = f.readlines()

    try:
        test_case_start = src.index("## TEST CASE\n")
    except ValueError as e:
        raise ValueError(
            "Required header ## TEST CASE missing in the file: " + file.name
        )

    try:
        expected_output_start = src.index("## EXPECTED OUTPUT\n")
    except ValueError as e:
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


def render(ast_base64: str) -> str:
    """Uses the unparser to render the AST."""
    assert (
        pytest.unparser_jar
    ), "A valid Unparser JAR path must be supplied either via --unparser-jar=<path> or the environment variable SNOWPARK_UNPARSER_JAR"
    res = subprocess.run(
        [
            "java",
            "-cp",
            pytest.unparser_jar,
            "com.snowflake.snowpark.experimental.unparser.UnparserCli",
            ast_base64,
        ],
        capture_output=True,
        text=True,
    )
    return res.stdout


def run_test(session, test_source):
    source = f"""
import snowflake.snowpark.functions as functions
from snowflake.snowpark.functions import col

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
session._ast_batch.flush()  # Clear the AST.

# Run the test.
{test_source}

# Retrieve the AST corresponding to the test.
(_, result) = session._ast_batch.flush()
"""
    locals = {"session": session}
    exec(source, locals)
    base64 = locals["result"]
    return render(base64), base64


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
            )


if __name__ == "__main__":
    pytest.main()
