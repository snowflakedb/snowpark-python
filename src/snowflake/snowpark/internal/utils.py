#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import platform
import re

from snowflake.connector.version import VERSION as connector_version

from snowflake.snowpark.snowpark_client_exception import SnowparkClientException
from snowflake.snowpark.version import VERSION as snowpark_version


class Utils:
    @staticmethod
    def validate_object_name(name: str):
        # Valid name can be:
        #   identifier,
        #   identifier.identifier,
        #   identifier.identifier.identifier
        #   identifier..identifier
        unquoted_id_pattern = r"([a-zA-Z_][\w$]*)"
        quoted_id_pattern = '("([^"]|"")+")'
        id_pattern = f"({unquoted_id_pattern}|{quoted_id_pattern})"
        pattern = re.compile(
            f"^(({id_pattern}\\.){{0,2}}|({id_pattern}\\.\\.)){id_pattern}$$"
        )
        if not pattern.match(name):
            raise SnowparkClientException(f"The object name {name} is invalid.")

    @staticmethod
    def get_version() -> str:
        return ".".join([str(d) for d in snowpark_version if d is not None])

    @staticmethod
    def get_python_version() -> str:
        return platform.python_version()

    @staticmethod
    def get_connector_version() -> str:
        return ".".join([str(d) for d in connector_version if d is not None])

    @staticmethod
    def get_os_name() -> str:
        return platform.system()
