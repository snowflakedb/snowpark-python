#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_snowpark_pandas_equal_to_pandas
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=1)
def test_title():
    s = pd.Series(["FOO", "BAR", "Blah", "blurg"], dtype=object)
    result = s.str.title()
    expected = native_pd.Series(["Foo", "Bar", "Blah", "Blurg"], dtype=object)
    assert_snowpark_pandas_equal_to_pandas(result, expected)


@sql_count_checker(query_count=0)
def test_casefold_not_implemented():
    s = pd.Series(["ß", "case", "ßd"])
    msg = "Snowpark pandas does not yet support the method Series.str.casefold"
    with pytest.raises(NotImplementedError, match=msg):
        s.str.casefold()
