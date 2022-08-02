#!/usr/bin/env python3
#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from enum import Enum


class StateTable(str, Enum):
    TRANSFORMER_CONTEXT = "transformer_context"
    COLUMNS_METADATA = "columns_metadata"
    TRANSFORMER_STATE = "transformer_state"
    TRANSFORMER_DEFINITION = "transformer_definition"
    PIPELINE = "pipeline"
    DICTIONARY_STATE = "dictionary_state"


class ColumnsMetadataColumn(str, Enum):
    VERSION = "VERSION"
    COLUMN_NAME = "COLUMN_NAME"
    BASIC_STATISTICS = "BASIC_STATISTICS"
    NUMERIC_STATISTICS = "NUMERIC_STATISTICS"


class NumericStatistics(str, Enum):
    MEAN = "mean"
    STDDEV = "stddev"


class BasicStatistics(str, Enum):
    MODE = "mode"
