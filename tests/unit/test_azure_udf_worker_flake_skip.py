#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

from tests.azure_udf_worker_flake import (
    is_azure_ci,
    matches_azure_udf_worker_flake,
)


def test_matches_required_job_failures_and_ignores_params():
    assert matches_azure_udf_worker_flake("snowpark.functions.xpath")
    assert matches_azure_udf_worker_flake("test_xpath_boolean")
    assert matches_azure_udf_worker_flake("test_register_udf_with_optional_args[True]")
    assert matches_azure_udf_worker_flake("test_basic_pandas_udf")
    assert not matches_azure_udf_worker_flake("test_session")
    assert not matches_azure_udf_worker_flake("test_ai_redact_detect_mode")


def test_is_azure_ci_reads_cloud_provider(monkeypatch):
    monkeypatch.delenv("cloud_provider", raising=False)
    assert is_azure_ci() is False
    monkeypatch.setenv("cloud_provider", "aws")
    assert is_azure_ci() is False
    monkeypatch.setenv("cloud_provider", "azure")
    assert is_azure_ci() is True
