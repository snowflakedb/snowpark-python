#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from dataclasses import dataclass
import os
import pathlib
import pytest
import subprocess

TEST_DIR = pathlib.Path(__file__).parent / "data"

UNPARSER_PATH="/Users/azwiegincew/src/snowflake/Snowpark/unparser/run.sh"

@dataclass
class TestCase:
    filename: str
    source: str
    expected_output: str


def parse_file(file):
    """Parses a test case file. N.B. Pytest requires a tuple wrapper, so we turn it into a 1-tuple."""
    with open(file, "r", encoding="utf-8") as f:
        src = f.readlines()

    try:
        test_case_start = src.index("## TEST CASE\n")
        expected_output_start = src.index("## EXPECTED OUTPUT\n")
    except ValueError as e:
        raise ValueError("Required header missing in the file: " + file)
    
    test_case = "".join(src[test_case_start + 1:expected_output_start])
    expected_output = "".join(src[expected_output_start + 1:])

    return TestCase(os.path.basename(file.name), test_case, expected_output)
   

def load_test_cases():
    """
    Loads and parses test files from the data/ subdirectory. The files must be named '*.test'.
    
    Returns: a list of test cases.
    """
    test_files = TEST_DIR.glob("*.test")
    test_cases = []
    for file in test_files:
        test_cases.append(parse_file(file))

    return test_cases


def idfn(val):
    return val.filename


def render(ast_base64: str) -> str:
    """Uses the unparser to render the AST."""
    res = subprocess.run([UNPARSER_PATH, ast_base64, "--lang", "python"], capture_output=True, text=True)
    return res.stdout


def run_test(session, test_source):
    source = f"""
{test_source}

(_, result) = session._ast_batch.flush()
"""
    locals = {"session": session}
    exec(source, locals)
    return render(locals["result"])


@pytest.mark.parametrize("test_case", load_test_cases(), ids=idfn)
def test_ast(session, test_case):
    actual = run_test(session, test_case.source)
    if pytest.update_expectations:
        with open(TEST_DIR / test_case.filename, "w", encoding="utf-8") as f:
            f.writelines([
                "## TEST CASE\n",
                test_case.source,
                "## EXPECTED OUTPUT\n\n",
                actual.strip(),
                "\n",
            ])
    else:
        try:
            assert actual.strip() == test_case.expected_output.strip()
        except AssertionError as e:
            raise AssertionError("If the expectation is incorrect, run pytest --update-expectations:\n\n" + str(e))


if __name__ == "__main__":
    pytest.main()