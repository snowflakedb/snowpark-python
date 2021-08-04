#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#
from typing import Optional


class SnowparkClientException(Exception):
    def __init__(self, message: str, error_code: Optional[str] = None):
        self.message = message
        self.error_code = error_code
        self.telemetry_message = message

    # TODO: SNOW-363951 handle telemetry
