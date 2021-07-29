#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from enum import Enum


class SnowparkClientExceptionMessages(Enum):
    """Holds all of the error messages that could be used in the SnowparkClientException Class"""

    PLAN_SAMPLING_NEED_ONE_PARAMETER = (
        "You must specify either the fraction of rows or the number of rows to sample."
    )


class SnowparkClientException(Exception):
    def __init__(self, message):
        self.message = message
        self.telemetry_message = message

    # TODO: SNOW-363951 handle telemetry
