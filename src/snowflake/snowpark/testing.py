#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#
#
# from snowflake.snowpark.dataframe import DataFrame
#
#
# def assert_dataframe_equal(
#     actual: DataFrame,
#     expected: DataFrame,
#     rtol: float = 1e-5
#     atol: float = 1e-8,
# ) -> None:
#     actual_schema = actual.schema
#     expected_schema = expected.schema
#     for actual_field, expected_field in zip(
#         actual_schema.fields, expected_schema.fields
#     ):
#         assert actual_field == expected_field
#
#     actual_rows = actual.collect()
#     expected_rows = expected.collect()
