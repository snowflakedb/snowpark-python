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

# Code in this file may constitute partial or total reimplementation, or modification of
# existing code originally distributed by the Modin project, under the Apache License,
# Version 2.0.

"""
Implement Series's accessors public API as pandas does.

Accessors: `Series.cat`, `Series.str`, `Series.dt`
"""
from typing import TYPE_CHECKING

import numpy as np
import pandas as native_pd

from snowflake.snowpark.modin.pandas import DataFrame, Series

if TYPE_CHECKING:
    from datetime import tzinfo

    from pandas._typing import npt

# add this line to enable doc tests to run
from snowflake.snowpark.modin import pandas as pd  # noqa: F401
from snowflake.snowpark.modin.plugin.utils.error_message import ErrorMessage
from snowflake.snowpark.modin.utils import _inherit_docstrings


class CategoryMethods:
    # CategoricalDType is not supported with Snowpark pandas API. Mark all methods
    # to be unsupported.
    category_not_supported_message = "CategoricalDType and corresponding methods is not available in Snowpark pandas API yet!"

    def __init__(self, series) -> None:
        self._series = series
        self._query_compiler = series._query_compiler

    @property
    def categories(self):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    @categories.setter
    def categories(self, categories):
        ErrorMessage.not_implemented(
            self.category_not_supported_message
        )  # pragma: no cover

    @property
    def ordered(self):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    @property
    def codes(self):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def rename_categories(self, new_categories, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def reorder_categories(self, new_categories, ordered=None, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def add_categories(self, new_categories, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def remove_categories(self, removals, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def remove_unused_categories(self, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def set_categories(self, new_categories, ordered=None, rename=False, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def as_ordered(self, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)

    def as_unordered(self, inplace=False):
        ErrorMessage.not_implemented(self.category_not_supported_message)


class DatetimeProperties:
    def __init__(self, series) -> None:
        self._series = series
        self._query_compiler = series._query_compiler

    @property
    def date(self):
        return Series(query_compiler=self._query_compiler.dt_property("date"))

    @property
    def time(self):
        return Series(query_compiler=self._query_compiler.dt_property("time"))

    @property
    def timetz(self):
        return Series(query_compiler=self._query_compiler.dt_property("timetz"))

    @property
    def year(self):
        return Series(query_compiler=self._query_compiler.dt_property("year"))

    @property
    def month(self):
        return Series(query_compiler=self._query_compiler.dt_property("month"))

    @property
    def day(self):
        return Series(query_compiler=self._query_compiler.dt_property("day"))

    @property
    def hour(self):
        return Series(query_compiler=self._query_compiler.dt_property("hour"))

    @property
    def minute(self):
        return Series(query_compiler=self._query_compiler.dt_property("minute"))

    @property
    def second(self):
        return Series(query_compiler=self._query_compiler.dt_property("second"))

    @property
    def microsecond(self):
        return Series(query_compiler=self._query_compiler.dt_property("microsecond"))

    @property
    def nanosecond(self):
        return Series(query_compiler=self._query_compiler.dt_property("nanosecond"))

    @property
    def dayofweek(self):
        return Series(query_compiler=self._query_compiler.dt_property("dayofweek"))

    @property
    def weekday(self):
        return Series(query_compiler=self._query_compiler.dt_property("weekday"))

    @property
    def dayofyear(self):
        return Series(query_compiler=self._query_compiler.dt_property("dayofyear"))

    @property
    def quarter(self):
        return Series(query_compiler=self._query_compiler.dt_property("quarter"))

    @property
    def is_month_start(self):
        return Series(query_compiler=self._query_compiler.dt_property("is_month_start"))

    @property
    def is_month_end(self):
        return Series(query_compiler=self._query_compiler.dt_property("is_month_end"))

    @property
    def is_quarter_start(self):
        return Series(
            query_compiler=self._query_compiler.dt_property("is_quarter_start")
        )

    @property
    def is_quarter_end(self):
        return Series(query_compiler=self._query_compiler.dt_property("is_quarter_end"))

    @property
    def is_year_start(self):
        return Series(query_compiler=self._query_compiler.dt_property("is_year_start"))

    @property
    def is_year_end(self):
        return Series(query_compiler=self._query_compiler.dt_property("is_year_end"))

    @property
    def is_leap_year(self):
        return Series(query_compiler=self._query_compiler.dt_property("is_leap_year"))

    @property
    def daysinmonth(self):
        return Series(query_compiler=self._query_compiler.dt_property("daysinmonth"))

    @property
    def days_in_month(self):
        return Series(query_compiler=self._query_compiler.dt_property("days_in_month"))

    @property
    def tz(self) -> "tzinfo | None":
        dtype = self._series.dtype
        if isinstance(dtype, np.dtype):
            return None
        return dtype.tz

    @property
    def freq(self):
        return self._query_compiler.dt_property("freq").to_pandas().squeeze()

    def to_period(self, *args, **kwargs):
        return Series(query_compiler=self._query_compiler.dt_to_period(*args, **kwargs))

    def to_pydatetime(self):
        return Series(query_compiler=self._query_compiler.dt_to_pydatetime()).to_numpy()

    def tz_localize(self, *args, **kwargs):
        return Series(
            query_compiler=self._query_compiler.dt_tz_localize(*args, **kwargs)
        )

    def tz_convert(self, *args, **kwargs):
        return Series(
            query_compiler=self._query_compiler.dt_tz_convert(*args, **kwargs)
        )

    def normalize(self, *args, **kwargs):
        return Series(query_compiler=self._query_compiler.dt_normalize(*args, **kwargs))

    def strftime(self, *args, **kwargs):
        return Series(query_compiler=self._query_compiler.dt_strftime(*args, **kwargs))

    def round(self, *args, **kwargs):
        return Series(query_compiler=self._query_compiler.dt_round(*args, **kwargs))

    def floor(self, *args, **kwargs):
        return Series(query_compiler=self._query_compiler.dt_floor(*args, **kwargs))

    def ceil(self, *args, **kwargs):
        return Series(query_compiler=self._query_compiler.dt_ceil(*args, **kwargs))

    def month_name(self, *args, **kwargs):
        return Series(
            query_compiler=self._query_compiler.dt_month_name(*args, **kwargs)
        )

    def day_name(self, *args, **kwargs):
        return Series(query_compiler=self._query_compiler.dt_day_name(*args, **kwargs))

    def total_seconds(self, *args, **kwargs):
        return Series(
            query_compiler=self._query_compiler.dt_total_seconds(*args, **kwargs)
        )

    def to_pytimedelta(self) -> "npt.NDArray[np.object_]":
        res = self._query_compiler.dt_to_pytimedelta()
        return res.to_numpy()[:, 0]

    @property
    def seconds(self):
        return Series(query_compiler=self._query_compiler.dt_property("seconds"))

    @property
    def days(self):
        return Series(query_compiler=self._query_compiler.dt_property("days"))

    @property
    def microseconds(self):
        return Series(query_compiler=self._query_compiler.dt_property("microseconds"))

    @property
    def nanoseconds(self):
        return Series(query_compiler=self._query_compiler.dt_property("nanoseconds"))

    @property
    def components(self):

        return DataFrame(query_compiler=self._query_compiler.dt_property("components"))

    @property
    def qyear(self):
        return Series(query_compiler=self._query_compiler.dt_property("qyear"))

    @property
    def start_time(self):
        return Series(query_compiler=self._query_compiler.dt_property("start_time"))

    @property
    def end_time(self):
        return Series(query_compiler=self._query_compiler.dt_property("end_time"))

    def to_timestamp(self, *args, **kwargs):
        return Series(
            query_compiler=self._query_compiler.dt_to_timestamp(*args, **kwargs)
        )
