import modin.pandas as pd
import pytest

from tests.integ.utils.sql_counter import sql_count_checker

@sql_count_checker(query_count=0)
def test_complete():
    df = pd.DataFrame([1, 2, 3])
    assert df.ai.complete() == "Hello Complete\n"
