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
import re
import sys
from typing import TYPE_CHECKING, Callable, Optional, Union

import numpy as np
import pandas as native_pd

from snowflake.snowpark.modin.pandas import DataFrame, Series

if sys.version_info[0] == 3 and sys.version_info[1] >= 7:
    # Python >= 3.7
    from re import Pattern as _pattern_type
else:
    # Python <= 3.6
    from re import _pattern_type

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


@_inherit_docstrings(native_pd.core.strings.accessor.StringMethods)
class StringMethods:
    def __init__(self, series) -> None:
        # Check if dtypes is objects

        self._series = series
        self._query_compiler = series._query_compiler

    def casefold(self):
        ErrorMessage.method_not_implemented_error("casefold", "Series.str")
        return Series(query_compiler=self._query_compiler.str_casefold())

    def cat(self, others=None, sep=None, na_rep=None, join=None):
        ErrorMessage.method_not_implemented_error("cat", "Series.str")
        compiler_result = self._query_compiler.str_cat(
            others=others, sep=sep, na_rep=na_rep, join=join
        )
        # if others is None, result is a string. otherwise, it's a series.
        return (
            compiler_result.to_pandas().squeeze()
            if others is None
            else Series(query_compiler=compiler_result)
        )

    def decode(self, encoding, errors="strict"):
        ErrorMessage.method_not_implemented_error("decode", "Series.str")
        return Series(
            query_compiler=self._query_compiler.str_decode(encoding, errors=errors)
        )

    def split(
        self,
        pat: Optional[str] = None,
        n: int = -1,
        expand: bool = False,
        regex: Optional[bool] = None,
    ) -> Series:
        if not pat and pat is not None:
            raise ValueError("split() requires a non-empty pattern match.")

        else:
            return Series(
                query_compiler=self._query_compiler.str_split(
                    pat=pat, n=n, expand=expand, regex=regex
                )
            )

    def rsplit(self, pat=None, n=-1, expand=False):
        ErrorMessage.method_not_implemented_error("rsplit", "Series.str")

        if not pat and pat is not None:
            raise ValueError("rsplit() requires a non-empty pattern match.")

        else:
            return Series(
                query_compiler=self._query_compiler.str_rsplit(
                    pat=pat, n=n, expand=expand
                )
            )

    def get(self, i):
        ErrorMessage.method_not_implemented_error("get", "Series.str")
        return Series(query_compiler=self._query_compiler.str_get(i))

    def join(self, sep):
        ErrorMessage.method_not_implemented_error("join", "Series.str")
        if sep is None:
            raise AttributeError("'NoneType' object has no attribute 'join'")
        return Series(query_compiler=self._query_compiler.str_join(sep))

    def get_dummies(self, sep="|"):
        ErrorMessage.method_not_implemented_error("get_dummies", "Series.str")
        return DataFrame(query_compiler=self._query_compiler.str_get_dummies(sep))

    def contains(
        self,
        pat: str,
        case: bool = True,
        flags: int = 0,
        na: object = None,
        regex: bool = True,
    ):
        return Series(
            query_compiler=self._query_compiler.str_contains(
                pat, case=case, flags=flags, na=na, regex=regex
            )
        )

    def replace(
        self,
        pat: str,
        repl: Union[str, Callable],
        n: int = -1,
        case: Optional[bool] = None,
        flags: int = 0,
        regex: bool = True,
    ) -> Series:
        if not (isinstance(repl, str) or callable(repl)):
            raise TypeError("repl must be a string or callable")
        return Series(
            query_compiler=self._query_compiler.str_replace(
                pat, repl, n=n, case=case, flags=flags, regex=regex
            )
        )

    def pad(self, width, side="left", fillchar=" "):
        ErrorMessage.method_not_implemented_error("pad", "Series.str")
        if len(fillchar) != 1:
            raise TypeError("fillchar must be a character, not str")
        return Series(
            query_compiler=self._query_compiler.str_pad(
                width, side=side, fillchar=fillchar
            )
        )

    def center(self, width, fillchar=" "):
        ErrorMessage.method_not_implemented_error("center", "Series.str")
        if len(fillchar) != 1:
            raise TypeError("fillchar must be a character, not str")
        return Series(
            query_compiler=self._query_compiler.str_center(width, fillchar=fillchar)
        )

    def ljust(self, width, fillchar=" "):
        ErrorMessage.method_not_implemented_error("ljust", "Series.str")
        if len(fillchar) != 1:
            raise TypeError("fillchar must be a character, not str")
        return Series(
            query_compiler=self._query_compiler.str_ljust(width, fillchar=fillchar)
        )

    def rjust(self, width, fillchar=" "):
        ErrorMessage.method_not_implemented_error("rjust", "Series.str")
        if len(fillchar) != 1:
            raise TypeError("fillchar must be a character, not str")
        return Series(
            query_compiler=self._query_compiler.str_rjust(width, fillchar=fillchar)
        )

    def zfill(self, width):
        ErrorMessage.method_not_implemented_error("zfill", "Series.str")
        return Series(query_compiler=self._query_compiler.str_zfill(width))

    def wrap(self, width, **kwargs):
        ErrorMessage.method_not_implemented_error("wrap", "Series.str")
        if width <= 0:
            raise ValueError(f"invalid width {width} (must be > 0)")
        return Series(query_compiler=self._query_compiler.str_wrap(width, **kwargs))

    def slice(
        self,
        start: Optional[int] = None,
        stop: Optional[int] = None,
        step: Optional[int] = None,
    ):
        if step == 0:
            raise ValueError("slice step cannot be zero")
        return Series(
            query_compiler=self._query_compiler.str_slice(
                start=start, stop=stop, step=step
            )
        )

    def slice_replace(self, start=None, stop=None, repl=None):
        ErrorMessage.method_not_implemented_error("slice_replace", "Series.str")
        return Series(
            query_compiler=self._query_compiler.str_slice_replace(
                start=start, stop=stop, repl=repl
            )
        )

    def count(self, pat: str, flags: int = 0, **kwargs):
        if not isinstance(pat, (str, _pattern_type)):
            raise TypeError("first argument must be string or compiled pattern")
        return Series(
            query_compiler=self._query_compiler.str_count(pat, flags=flags, **kwargs)
        )

    def startswith(self, pat, na=np.NaN):
        return Series(query_compiler=self._query_compiler.str_startswith(pat, na=na))

    def encode(self, encoding, errors="strict"):
        ErrorMessage.method_not_implemented_error("encode", "Series.str")
        return Series(
            query_compiler=self._query_compiler.str_encode(encoding, errors=errors)
        )

    def endswith(self, pat, na=np.NaN):
        return Series(query_compiler=self._query_compiler.str_endswith(pat, na=na))

    def findall(self, pat, flags=0, **kwargs):
        ErrorMessage.method_not_implemented_error("findall", "Series.str")
        if not isinstance(pat, (str, _pattern_type)):
            raise TypeError("first argument must be string or compiled pattern")
        return Series(
            query_compiler=self._query_compiler.str_findall(pat, flags=flags, **kwargs)
        )

    def fullmatch(self, pat, case=True, flags=0, na=None):
        ErrorMessage.method_not_implemented_error("fullmatch", "Series.str")
        if not isinstance(pat, (str, re.Pattern)):
            raise TypeError("first argument must be string or compiled pattern")
        return self._Series(
            query_compiler=self._query_compiler.str_fullmatch(
                pat, case=case, flags=flags, na=na
            )
        )

    def match(self, pat, case=True, flags=0, na=np.NaN):
        ErrorMessage.method_not_implemented_error("match", "Series.str")
        if not isinstance(pat, (str, _pattern_type)):
            raise TypeError("first argument must be string or compiled pattern")
        return Series(
            query_compiler=self._query_compiler.str_match(pat, flags=flags, na=na)
        )

    def extract(self, pat, flags=0, expand=True):
        ErrorMessage.method_not_implemented_error("extract", "Series.str")
        query_compiler = self._query_compiler.str_extract(
            pat, flags=flags, expand=expand
        )
        return (
            DataFrame(query_compiler=query_compiler)
            if expand or re.compile(pat).groups > 1
            else Series(query_compiler=query_compiler)
        )

    def extractall(self, pat, flags=0):
        ErrorMessage.method_not_implemented_error("extractall", "Series.str")
        return Series(query_compiler=self._query_compiler.str_extractall(pat, flags))

    def len(self):
        return Series(query_compiler=self._query_compiler.str_len())

    def strip(self, to_strip: str = None) -> Series:
        return Series(query_compiler=self._query_compiler.str_strip(to_strip=to_strip))

    def rstrip(self, to_strip=None):
        ErrorMessage.method_not_implemented_error("rstrip", "Series.str")
        return Series(query_compiler=self._query_compiler.str_rstrip(to_strip=to_strip))

    def lstrip(self, to_strip=None):
        ErrorMessage.method_not_implemented_error("lstrip", "Series.str")
        return Series(query_compiler=self._query_compiler.str_lstrip(to_strip=to_strip))

    def partition(self, sep=" ", expand=True):
        ErrorMessage.method_not_implemented_error("partition", "Series.str")
        if sep is not None and len(sep) == 0:
            raise ValueError("empty separator")

        return (DataFrame if expand else Series)(
            query_compiler=self._query_compiler.str_partition(sep=sep, expand=expand)
        )

    def removeprefix(self, prefix):
        ErrorMessage.method_not_implemented_error("removeprefix", "Series.str")
        return Series(query_compiler=self._query_compiler.str_removeprefix(prefix))

    def removesuffix(self, suffix):
        ErrorMessage.method_not_implemented_error("removesuffix", "Series.str")
        return Series(query_compiler=self._query_compiler.str_removesuffix(suffix))

    def repeat(self, repeats):
        ErrorMessage.method_not_implemented_error("repeat", "Series.str")
        return Series(query_compiler=self._query_compiler.str_repeat(repeats))

    def rpartition(self, sep=" ", expand=True):
        ErrorMessage.method_not_implemented_error("rpartition", "Series.str")
        if sep is not None and len(sep) == 0:
            raise ValueError("empty separator")

        else:
            return Series(
                query_compiler=self._query_compiler.str_rpartition(
                    sep=sep, expand=expand
                )
            )

    def lower(self):
        return Series(query_compiler=self._query_compiler.str_lower())

    def upper(self):
        return Series(query_compiler=self._query_compiler.str_upper())

    def title(self):
        return Series(query_compiler=self._query_compiler.str_title())

    def find(self, sub, start=0, end=None):
        ErrorMessage.method_not_implemented_error("find", "Series.str")
        if not isinstance(sub, str):
            raise TypeError(f"expected a string object, not {type(sub).__name__}")
        return Series(
            query_compiler=self._query_compiler.str_find(sub, start=start, end=end)
        )

    def rfind(self, sub, start=0, end=None):
        ErrorMessage.method_not_implemented_error("rfind", "Series.str")
        if not isinstance(sub, str):
            raise TypeError(f"expected a string object, not {type(sub).__name__}")
        return Series(
            query_compiler=self._query_compiler.str_rfind(sub, start=start, end=end)
        )

    def index(self, sub, start=0, end=None):
        ErrorMessage.method_not_implemented_error("index", "Series.str")
        if not isinstance(sub, str):
            raise TypeError(f"expected a string object, not {type(sub).__name__}")
        return Series(
            query_compiler=self._query_compiler.str_index(sub, start=start, end=end)
        )

    def rindex(self, sub, start=0, end=None):
        ErrorMessage.method_not_implemented_error("rindex", "Series.str")
        if not isinstance(sub, str):
            raise TypeError(f"expected a string object, not {type(sub).__name__}")
        return Series(
            query_compiler=self._query_compiler.str_rindex(sub, start=start, end=end)
        )

    def capitalize(self):
        return Series(query_compiler=self._query_compiler.str_capitalize())

    def swapcase(self):
        ErrorMessage.method_not_implemented_error("swapcase", "Series.str")
        return Series(query_compiler=self._query_compiler.str_swapcase())

    def normalize(self, form):
        ErrorMessage.method_not_implemented_error("normalize", "Series.str")
        return Series(query_compiler=self._query_compiler.str_normalize(form))

    def translate(self, table):
        ErrorMessage.method_not_implemented_error("translate", "Series.str")
        return Series(query_compiler=self._query_compiler.str_translate(table))

    def isalnum(self):
        ErrorMessage.method_not_implemented_error("isalnum", "Series.str")
        return Series(query_compiler=self._query_compiler.str_isalnum())

    def isalpha(self):
        ErrorMessage.method_not_implemented_error("isalpha", "Series.str")
        return Series(query_compiler=self._query_compiler.str_isalpha())

    def isdigit(self):
        return Series(query_compiler=self._query_compiler.str_isdigit())

    def isspace(self):
        ErrorMessage.method_not_implemented_error("isspace", "Series.str")
        return Series(query_compiler=self._query_compiler.str_isspace())

    def islower(self):
        return Series(query_compiler=self._query_compiler.str_islower())

    def isupper(self):
        return Series(query_compiler=self._query_compiler.str_isupper())

    def istitle(self):
        return Series(query_compiler=self._query_compiler.str_istitle())

    def isnumeric(self):
        ErrorMessage.method_not_implemented_error("isnumeric", "Series.str")
        return Series(query_compiler=self._query_compiler.str_isnumeric())

    def isdecimal(self):
        ErrorMessage.method_not_implemented_error("isdecimal", "Series.str")
        return Series(query_compiler=self._query_compiler.str_isdecimal())


@_inherit_docstrings(native_pd.core.indexes.accessors.CombinedDatetimelikeProperties)
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
