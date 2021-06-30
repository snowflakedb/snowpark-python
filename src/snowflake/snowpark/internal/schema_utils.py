#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

import random
import re
import string

from src.snowflake.snowpark.internal.analyzer.sf_attribute import Attribute

from ..types.sf_types import DecimalType, LongType, StringType


class SchemaUtils:
    """Original comment: All functions in this object are temporary solutions."""

    @staticmethod
    def command_attributes():
        return [Attribute('"status"', StringType())]

    @staticmethod
    def list_stage_attributes():
        return [
            Attribute('"name"', StringType()),
            Attribute('"size"', LongType()),
            Attribute('"md5"', StringType()),
            Attribute('"last_modified"', StringType()),
        ]

    @staticmethod
    def remove_state_file_attributes():
        return [Attribute('"name"', StringType()), Attribute('"result"', StringType())]

    @staticmethod
    def put_attributes():
        return [
            Attribute('"source"', StringType(), nullable=False),
            Attribute('"target"', StringType(), nullable=False),
            Attribute('"source_size"', DecimalType(10, 0), nullable=False),
            Attribute('"target_size"', DecimalType(10, 0), nullable=False),
            Attribute('"source_compression"', StringType(), nullable=False),
            Attribute('"target_compression"', StringType(), nullable=False),
            Attribute('"status"', StringType(), nullable=False),
            Attribute('"encryption"', StringType(), nullable=False),
            Attribute('"message"', StringType(), nullable=False),
        ]

    @staticmethod
    def get_attributes():
        return [
            Attribute('"file"', StringType(), nullable=False),
            Attribute('"size"', DecimalType(10, 0), nullable=False),
            Attribute('"status"', StringType(), nullable=False),
            Attribute('"encryption"', StringType(), nullable=False),
            Attribute('"message"', StringType(), nullable=False),
        ]

    @staticmethod
    def analyze_attributes(sql: str, session):
        attributes = session.get_result_attributes(sql)
        if attributes:
            return attributes
        else:
            tokens = [
                s.lower()
                for s in filter(lambda s: len(s) > 0, re.split("\\s", sql.strip()))
            ]
            if not tokens:
                return []
            head = tokens[0]
            if head in ["alter", "drop", "use", "create", "grant", "revoke"]:
                return SchemaUtils.command_attributes()
            if head in ["ls", "list"]:
                return SchemaUtils.list_stage_attributes()
            if head in ["rm", "remove"]:
                return SchemaUtils.remove_state_file_attributes()
            if head in ["put"]:
                return SchemaUtils.put_attributes()
            if head in ["get"]:
                return SchemaUtils.get_attributes()
            if head in ["describe"]:
                return session.conn.convert_result_metadata_to_attribute(
                    session.runQuery(sql).get_metadta
                )
            return []

    @staticmethod
    def random_string():
        alphanumeric = string.ascii_lowercase + string.digits
        return "".join(random.choice(alphanumeric) for _ in range(10))
