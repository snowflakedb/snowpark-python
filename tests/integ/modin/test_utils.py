#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
import pytest

from snowflake.snowpark._internal.analyzer.analyzer_utils import quote_name
from snowflake.snowpark._internal.utils import (
    TempObjectType,
    random_name_for_temp_object,
)
from snowflake.snowpark.modin.plugin._internal.utils import (
    create_ordered_dataframe_with_readonly_temp_table,
)
from tests.integ.modin.sql_counter import sql_count_checker


@pytest.mark.parametrize("columns", [["A", "b", "C"], ['"a"', '"B"', '"c"']])
@sql_count_checker(query_count=3)
def test_create_snowpark_dataframe_with_readonly_temp_table(session, columns):
    num_rows = 10
    data = [[0] * len(columns) for _ in range(num_rows)]
    test_table_name = random_name_for_temp_object(TempObjectType.TABLE)
    snowpark_df = session.create_dataframe(data, schema=columns)
    snowpark_df.write.save_as_table(test_table_name, mode="overwrite")

    (
        ordered_df,
        row_position_quoted_identifier,
    ) = create_ordered_dataframe_with_readonly_temp_table(test_table_name)

    # verify the ordered df columns are row_position_quoted_identifier + quoted_identifiers
    assert ordered_df.projected_column_snowflake_quoted_identifiers == [
        row_position_quoted_identifier
    ] + [quote_name(c) for c in columns]
    assert [
        row[0] for row in ordered_df.select(row_position_quoted_identifier).collect()
    ] == list(range(num_rows))
