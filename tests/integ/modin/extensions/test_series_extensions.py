#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

# Licensed to Modin Development Team under one or more contributor license agreements.
# See the NOTICE file distributed with this work for additional information regarding
# copyright ownership.  The Modin Development Team licenses this file to you under the
# Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under
# the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific language
# governing permissions and limitations under the License.

import functools
import modin.pandas as pd
import pandas as native_pd
from modin.pandas.api.extensions import register_series_accessor

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.utils.sql_counter import sql_count_checker

EXTENSIONS_DICT = pd.dataframe._SERIES_EXTENSIONS_["Snowflake"]

register_series_accessor = functools.partial(
    register_series_accessor, backend="Snowflake"
)


@sql_count_checker(query_count=0)
def test_series_extension_simple_method():
    expected_string_val = "Some string value"
    method_name = "new_method"
    ser = pd.Series([1, 2, 3])

    @register_series_accessor(method_name)
    def my_method_implementation(self):
        return expected_string_val

    assert method_name in EXTENSIONS_DICT.keys()
    assert EXTENSIONS_DICT[method_name] is my_method_implementation
    assert ser.new_method() == expected_string_val


@sql_count_checker(query_count=0)
def test_series_extension_non_method():
    expected_val = 4
    attribute_name = "four"
    register_series_accessor(attribute_name)(expected_val)
    ser = pd.Series([1, 2, 3])

    assert attribute_name in EXTENSIONS_DICT.keys()
    assert EXTENSIONS_DICT[attribute_name] == 4
    assert ser.four == expected_val


@sql_count_checker(query_count=2)
def test_series_extension_access_existing_methods():
    ser = pd.Series([1, 2, 3])
    native_ser = native_pd.Series([1, 2, 3])
    method_name = "self_accessor"
    expected_result = native_ser.sum() / native_ser.count()

    @register_series_accessor(method_name)
    def ext_new_method(self):
        return self.sum() / self.count()

    assert method_name in EXTENSIONS_DICT.keys()
    assert EXTENSIONS_DICT[method_name] is ext_new_method
    assert ser.self_accessor() == expected_result


@sql_count_checker(query_count=0)
def test_series_extension_override_method():
    # Override the existing `sum` method on the Series class
    ser = pd.Series([1, 2, 3])
    method_name = "sum"
    expected_result = 100

    original_method = pd.Series.sum

    try:

        @register_series_accessor(method_name)
        def my_method(self):
            return expected_result

        assert method_name in EXTENSIONS_DICT.keys()
        assert EXTENSIONS_DICT[method_name] is my_method
        assert ser.sum() == expected_result
    finally:
        # Because we're overriding a method on the Series class, we need to restore the original method
        # after we're done, or else other tests that use Series.sum will fail
        register_series_accessor(method_name)(original_method)
        del EXTENSIONS_DICT[method_name]
