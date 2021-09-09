#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from snowflake.snowpark.snowpark_client_exception import SnowparkClientException


class SnowparkClientExceptionMessages:
    """Holds all of the error messages that could be used in the SnowparkClientException Class.

    IMPORTANT: keep this file in numerical order of the error-code."""

    # TODO Add the rest of the exception messages
    @staticmethod
    def INTERNAL_TEST_MESSAGE(message: str) -> SnowparkClientException:
        return SnowparkClientException(f"internal test message: {message}.", "0010")

    @staticmethod
    def DF_JOIN_INVALID_JOIN_TYPE(type1: str, types: str) -> SnowparkClientException:
        return SnowparkClientException(
            f"Unsupported join type '{type1}'. Supported join types include: {types}.",
            "0116",
        )

    @staticmethod
    def DF_JOIN_INVALID_NATURAL_JOIN_TYPE(tpe: str) -> SnowparkClientException:
        return SnowparkClientException(
            f"Unsupported natural join type '{tpe}'.", "0117"
        )

    @staticmethod
    def DF_JOIN_INVALID_USING_JOIN_TYPE(tpe: str) -> SnowparkClientException:
        return SnowparkClientException(f"Unsupported using join type '{tpe}'.", "0118")

    @staticmethod
    def PLAN_SAMPLING_NEED_ONE_PARAMETER() -> SnowparkClientException:
        return SnowparkClientException(
            "You must specify either the fraction of rows or the number of rows to sample.",
            "0303",
        )

    @staticmethod
    def PLAN_PYTHON_REPORT_UNEXPECTED_ALIAS() -> SnowparkClientException:
        return SnowparkClientException(
            "You can only define aliases for the root Columns in a DataFrame returned by "
            "select() and agg(). You cannot use aliases for Columns in expressions.",
            "0308",
        )

    @staticmethod
    def PLAN_PYTHON_REPORT_INVALID_ID(name: str) -> SnowparkClientException:
        return SnowparkClientException(
            f'The column specified in df("{name}") '
            f"is not present in the output of the DataFrame.",
            "0309",
        )

    @staticmethod
    def PLAN_PYTHON_REPORT_JOIN_AMBIGUOUS(c1: str, c2: str) -> SnowparkClientException:
        return SnowparkClientException(
            f"The reference to the column '{c1}' is ambiguous. The column is "
            f"present in both DataFrames used in the join. To identify the "
            f"DataFrame that you want to use in the reference, use the syntax "
            f'<df>("{c2}") in join conditions and in select() calls on the '
            f"result of the join.",
            "0310",
        )
