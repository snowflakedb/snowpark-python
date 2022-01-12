#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

from snowflake.snowpark.types import DataType


class Attribute:
    """Snowflake version of Attribute."""

    def __init__(self, name: str, datatype: DataType, nullable=True):
        self.name = name
        self.datatype = datatype
        self.nullable = nullable
