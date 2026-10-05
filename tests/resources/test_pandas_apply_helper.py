#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
# Helper module for SNOW-4174500 integration tests.
# Must be a staged file whose module-level functions cloudpickle serializes by
# reference, triggering the nested-UDxF import-inheritance bug.


def double_col(pdf):
    result = pdf.copy()
    result["V"] = result["V"] * 2
    return result


def double_scalar(x):
    return x * 2
