#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
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

import modin.pandas as pd
import pandas as native_pd
from modin.pandas.api.extensions import register_dataframe_accessor

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import assert_series_equal
from tests.integ.utils.sql_counter import sql_count_checker


@sql_count_checker(query_count=0)
def test_dataframe_extension_simple_method():
    expected_string_val = "Some string value"
    method_name = "new_method"
    df = pd.DataFrame([1, 2, 3])

    @register_dataframe_accessor(method_name)
    def my_method_implementation(self):
        return expected_string_val

    assert method_name in pd.dataframe._DATAFRAME_EXTENSIONS_.keys()
    assert pd.dataframe._DATAFRAME_EXTENSIONS_[method_name] is my_method_implementation
    assert df.new_method() == expected_string_val


@sql_count_checker(query_count=0)
def test_dataframe_extension_non_method():
    expected_val = 4
    attribute_name = "four"
    register_dataframe_accessor(attribute_name)(expected_val)
    df = pd.DataFrame([1, 2, 3])

    assert attribute_name in pd.dataframe._DATAFRAME_EXTENSIONS_.keys()
    assert pd.dataframe._DATAFRAME_EXTENSIONS_[attribute_name] == 4
    assert df.four == expected_val


@sql_count_checker(query_count=1, join_count=1)
def test_dataframe_extension_access_existing_methods():
    df = pd.DataFrame([1, 2, 3])
    native_df = native_pd.DataFrame([1, 2, 3])
    method_name = "self_accessor"
    expected_result = native_df.sum() * native_df.count()

    @register_dataframe_accessor(method_name)
    def ext_new_method(self):
        return self.sum() * self.count()

    assert method_name in pd.dataframe._DATAFRAME_EXTENSIONS_.keys()
    assert pd.dataframe._DATAFRAME_EXTENSIONS_[method_name] is ext_new_method
    assert_series_equal(
        df.self_accessor(), expected_result, check_index_type=False, check_dtype=False
    )


@sql_count_checker(query_count=0)
def test_dataframe_extension_override_method():
    # Override the existing `sum` method on the DataFrame class
    df = pd.DataFrame([1, 2, 3])
    method_name = "sum"
    expected_result = 100

    original_method = pd.DataFrame.sum

    try:

        @register_dataframe_accessor(method_name)
        def ext_override_sum(self):
            return expected_result

        assert method_name in pd.dataframe._DATAFRAME_EXTENSIONS_.keys()
        assert pd.dataframe._DATAFRAME_EXTENSIONS_[method_name] is ext_override_sum
        assert df.sum() == expected_result
    finally:
        # Because we're overriding a method on the DataFrame class, we need to restore the original method
        # after we're done, or else other tests that use DataFrame.sum will fail
        register_dataframe_accessor(method_name)(original_method)
        del pd.dataframe._DATAFRAME_EXTENSIONS_[method_name]
