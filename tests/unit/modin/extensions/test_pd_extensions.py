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
from modin.pandas.api.extensions import register_pd_accessor as _register_pd_accessor

import snowflake.snowpark.modin.plugin  # noqa: F401
from snowflake.snowpark.modin.plugin._internal.utils import MODIN_IS_AT_LEAST_0_33_0


if MODIN_IS_AT_LEAST_0_33_0:
    from modin.core.storage_formats.pandas.query_compiler_caster import (
        _GENERAL_EXTENSIONS,
    )

    register_pd_accessor = functools.partial(_register_pd_accessor, backend="Snowflake")
    PD_EXTENSIONS = _GENERAL_EXTENSIONS["Snowflake"]
else:  # pragma: no branch
    PD_EXTENSIONS = pd._PD_EXTENSIONS_
    register_pd_accessor = _register_pd_accessor


def test_pd_extension_simple_method():
    expected_string_val = "Some string value"
    method_name = "new_method"

    @register_pd_accessor(method_name)
    def my_method_implementation():
        return expected_string_val

    assert method_name in PD_EXTENSIONS.keys()
    assert PD_EXTENSIONS[method_name] is my_method_implementation
    assert pd.new_method() == expected_string_val


def test_pd_extension_non_method():
    expected_val = 4
    attribute_name = "four"
    register_pd_accessor(attribute_name)(expected_val)
    assert attribute_name in PD_EXTENSIONS.keys()
    assert PD_EXTENSIONS[attribute_name] == 4
    assert pd.four == expected_val
