#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""SNOW-4139794 Azure GHA UDF-worker flake skip list. No Snowpark import."""

import os

AZURE_UDF_WORKER_FLAKE_SKIP_REASON = (
    "SNOW-4139794: Azure Python UDF worker XP_WORKER_FAILURE (000603)"
)
AZURE_UDF_WORKER_FLAKE_TEST_NAMES = frozenset(
    {
        "snowpark.functions.xpath",
        "snowpark.functions.xpath_boolean",
        "snowpark.functions.xpath_int",
        "snowpark.functions.xpath_number",
        "snowpark.functions.xpath_string",
        "snowpark.stored_procedure.StoredProcedureRegistration",
        "snowpark.udf.UDFRegistration",
        "snowpark.udtf.UDTFRegistration",
        "test_select_table_function",
        "test_infer_date",
        "test_sampling_ratio_schema_books_flat",
        "test_user_schema_takes_precedence_over_infer_schema",
        "test_session_register_udf",
        "test_value_tag_custom_schema",
        "test_anonymous_udf",
        "test_infer_root_value_attrs_only",
        "test_pandas_udf_return_types",
        "test_register_udf_with_optional_args",
        "test_basic_udtf_word_count_without_end_partition",
        "test_sampling_ratio_hetero_may_narrow_schema",
        "test_read_xml_row_tag",
        "test_read_xml_namespace_user_schema",
        "test_pandas_udf_return_variant",
        "test_infer_root_value_with_child_elements",
        "test_basic_udtf_word_count_with_end_partition",
        "test_sampling_ratio_nested_schema_preserved",
        "test_dynamic_table_join_table_function_with_more_layers",
        "test_infer_schema_false_resource_files",
        "test_udf_replace",
        "test_read_malformed_xml",
        "test_xpath",
        "test_infer_complicated_nested",
        "test_udf_if_not_exists",
        "test_permissive_captures_a_parse_error_without_aborting",
        "test_xpath_string",
        "test_infer_attribute_on_leaf",
        "test_basic_pandas_udf",
        "test_xpath_boolean",
        "test_type_hints",
        "test_infer_missing_nested_struct",
        "test_xpath_float_and_int",
        "test_read_xml_null_value",
        "test_read_xml_infer_schema_books_flat",
        "test_read_xml_ignore_surrounding_whitespace",
        "test_read_xml_infer_schema_books2_nested",
        "test_read_xml_namespace_infer_schema",
        "test_basic_jdbc",
    }
)


def is_azure_ci() -> bool:
    return os.getenv("cloud_provider") == "azure"


def matches_azure_udf_worker_flake(item_name: str) -> bool:
    return item_name.split("[", 1)[0] in AZURE_UDF_WORKER_FLAKE_TEST_NAMES
