#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from snowflake.snowpark._internal.sp_types import DataType


class Attribute:
    """Snowflake version of Attribute."""

    def __init__(self, name: str, datatype: DataType, nullable=True):
        self.name = name
        self.datatype = datatype
        self.nullable = nullable
