#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import pytest
from copy import copy
from unittest.mock import patch, Mock


from snowflake.snowpark.functions import col, lit, max


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="debug_mode not used in local testing mode",
)
@pytest.mark.parametrize("debug_mode", [True, False])
@pytest.mark.parametrize(
    "transform",
    [
        pytest.param(lambda x: copy(x), id="copy"),
        pytest.param(lambda x: x.to_df(["C", "D"]), id="to_df"),
        pytest.param(lambda x: x.distinct(), id="distinct"),
        pytest.param(lambda x: x.drop_duplicates(), id="drop_duplicates"),
        pytest.param(lambda x: x.limit(1), id="limit"),
        pytest.param(lambda x: x.union(x), id="union"),
        pytest.param(lambda x: x.union_all(x), id="union_all"),
        pytest.param(lambda x: x.union_by_name(x), id="union_by_name"),
        pytest.param(lambda x: x.union_all_by_name(x), id="union_all_by_name"),
        pytest.param(lambda x: x.intersect(x), id="intersect"),
        pytest.param(lambda x: x.natural_join(x), id="natural_join"),
        pytest.param(lambda x: x.cross_join(x), id="cross_join"),
        pytest.param(lambda x: x.sample(n=1), id="sample"),
        pytest.param(
            lambda x: x.with_column_renamed(col("A"), "B"), id="with_column_renamed"
        ),
        # Unpivot already validates names
        pytest.param(lambda x: x.unpivot("x", "y", ["A"]), id="unpivot"),
        # The following functions do not error early because their schema_query do not contain
        # information about the transformation being called.
        pytest.param(lambda x: x.drop(col("A")), id="drop"),
        pytest.param(lambda x: x.filter(col("A") == lit(1)), id="filter"),
        pytest.param(lambda x: x.sort(col("A").desc()), id="sort"),
    ],
)
def test_early_attributes(session, transform, debug_mode):
    with patch.object(session, "_debug_mode", debug_mode):
        df = session.create_dataframe([(1, "A"), (2, "B"), (3, "C")], ["A", "B"])

        transformed = transform(df)

        # When debug mode is enabled the dataframe plan attributes are populated early
        if debug_mode:
            assert transformed._plan._metadata.attributes is not None
        else:
            assert transformed._plan._metadata.attributes is None


@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="debug_mode not used in local testing mode",
)
@pytest.mark.parametrize("debug_mode", [True, False])
@pytest.mark.parametrize(
    "transform",
    [
        pytest.param(lambda x: x.select("B"), id="select"),
        pytest.param(lambda x: x.select_expr("cast(b as str)"), id="select_expr"),
        pytest.param(lambda x: x.agg(max("B")), id="agg"),
        pytest.param(lambda x: x.join(copy(x), on=(col("A") == col("B"))), id="join"),
        pytest.param(
            lambda x: x.join_table_function("flatten", col("B")),
            id="join_table_function",
        ),
        pytest.param(lambda x: x.with_column("C", col("B")), id="with_column"),
        pytest.param(lambda x: x.with_columns(["C"], [col("B")]), id="with_columns"),
    ],
)
def test_early_error(session, transform, debug_mode):
    with patch.object(session, "_debug_mode", debug_mode):
        df = session.create_dataframe([1, 2, 3], ["A"])

        show_mock = Mock()
        show_mock.__qualname__ = "show"
        show_mock.__name__ = "show"

        with patch("snowflake.snowpark.dataframe.DataFrame.show", show_mock):
            try:
                transformed = transform(df)
                transformed.show()
            except Exception:
                pass
            # When debug mode is enabled the error is thrown before reaching show.
            # Without debug mode the error only shows up once show is called.
            if debug_mode:
                show_mock.assert_not_called()
                assert df._plan._metadata.attributes is not None
            else:
                show_mock.assert_called()
                assert df._plan._metadata.attributes is None
