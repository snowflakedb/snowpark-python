#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All right reserved.
#

from src.snowflake.snowpark.types.sf_types import StructField, StructType, DataType
from src.snowflake.snowpark.types.types_package import _infer_type, sp_type_to_snow_type
from typing import List, Union


class TypeToSchemaConverter:

    # only support a list of types now
    # TODO: align with scala snowpark, which has a finer inference method
    @staticmethod
    def infer_schema(data) -> StructType:
        if type(data) == list:
            return StructType([StructField("_{}".format(idx+1), *TypeToSchemaConverter.analyze_type(obj))
                               for idx, obj in enumerate(data)])
        else:
            return StructType([StructField("VALUE", *TypeToSchemaConverter.analyze_type(data))])

    # TODO: different nullable for different type
    @staticmethod
    def analyze_type(obj) -> (DataType, bool):
        return sp_type_to_snow_type(_infer_type(obj)), True
