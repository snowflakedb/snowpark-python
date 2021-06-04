#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

class SnowparkClientException(Exception):

    def __init__(self, message):
        self.message = message
        self.telemetry_message = message

    # TODO: SNOW-363951 handle telemetry
