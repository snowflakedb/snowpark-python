import modin.pandas as pd
import pytest

from tests.integ.utils.sql_counter import sql_count_checker

@sql_count_checker(query_count=0)
def test_complete(capsys):
    """
    Tests the monkey-patched complete() method on a Snowpark pandas DataFrame.
    """
    # Create a simple Snowpark pandas DataFrame.
    df = pd.DataFrame([1, 2, 3])
    # Call the complete() method, which is expected to print "Hello Complete".
    df.ai.complete()
    # Capture the output.
    captured = capsys.readouterr()
    # Verify that the output is correct.
    assert captured.out == "Hello Complete\n"
