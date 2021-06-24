#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
import re

from src.snowflake.snowpark.snowpark_client_exception import SnowparkClientException


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
