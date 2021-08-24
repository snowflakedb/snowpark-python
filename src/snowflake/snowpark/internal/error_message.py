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
    def DF_JOIN_INVALID_JOIN_TYPE(type1: str, types: str):
        return SnowparkClientException(
            f"Unsupported join type '{type1}'. Supported join types include: {types}.",
            "0116",
        )

    @staticmethod
    def DF_JOIN_INVALID_NATURAL_JOIN_TYPE(tpe: str):
        return SnowparkClientException(
            f"Unsupported natural join type '{tpe}'.", "0117"
        )

    @staticmethod
    def DF_JOIN_INVALID_USING_JOIN_TYPE(tpe: str):
        return SnowparkClientException(f"Unsupported using join type '{tpe}'.", "0118")

    @staticmethod
    def PLAN_SAMPLING_NEED_ONE_PARAMETER():
        return SnowparkClientException(
            "You must specify either the fraction of rows or the number of rows to sample.",
            "0303",
        )

    @staticmethod
    def PLAN_UNSUPPORTED_FILE_FORMAT_TYPE(format: str):
        return SnowparkClientException(
            f"Internal error: unsupported file format type: '{format}'.", "0313"
        )
