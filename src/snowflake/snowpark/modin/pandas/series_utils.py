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
        """
        Split strings around given separator/delimiter.

        Splits the string in the Series/Index from the beginning, at the specified delimiter string.

        Parameters
        ----------
        pat : str, optional
            String to split on. If not specified, split on whitespace.
        n : int, default -1 (all)
            Limit number of splits in output. None, 0 and -1 will be interpreted as return all splits.
        expand : bool, default False (Not implemented yet, should be set to False)
            Expand the split strings into separate columns.
            - If True, return DataFrame/MultiIndex expanding dimensionality.
            - If False, return Series/Index, containing lists of strings.
        regex : bool, default None (Not implemented yet, should be set to False or None)
            Determines if the passed-in pattern is a regular expression:
            - If True, assumes the passed-in pattern is a regular expression
            - If False or None, treats the pattern as a literal string.

        Returns
        -------
        Series, Index, DataFrame or MultiIndex
            Type matches caller unless expand=True (see Notes).

        See also
        --------
        Series.str.split
            Split strings around given separator/delimiter.
        Series.str.rsplit
            Splits string around given separator/delimiter, starting from the right.
        Series.str.join
            Join lists contained as elements in the Series/Index with passed delimiter.
        str.split
            Standard library version for split.
        str.rsplit
            Standard library version for rsplit.

        Notes
        -----
        The handling of the n keyword depends on the number of found splits:

            - If found splits > n, make first n splits only
            - If found splits <= n, make all splits
            - If for a certain row the number of found splits < n, append None for padding up to n if expand=True
            - If using expand=True, Series and Index callers return DataFrame and MultiIndex objects, respectively.

        Examples
        --------
        >>> s = pd.Series(
        ...     [
        ...         "this is a regular sentence",
        ...         "https://docs.python.org/3/tutorial/index.html",
        ...         np.nan
        ...     ]
        ... )
        >>> s
        0                       this is a regular sentence
        1    https://docs.python.org/3/tutorial/index.html
        2                                             None
        dtype: object

        In the default setting, the string is split by whitespace.

        >>> s.str.split()
        0                   [this, is, a, regular, sentence]
        1    [https://docs.python.org/3/tutorial/index.html]
        2                                               None
        dtype: object

        The n parameter can be used to limit the number of splits on the delimiter.

        >>> s.str.split(n=2)
        0                     [this, is, a regular sentence]
        1    [https://docs.python.org/3/tutorial/index.html]
        2                                               None
        dtype: object

        The pat parameter can be used to split by other characters.

        >>> s.str.split(pat="/")
        0                         [this is a regular sentence]
        1    [https:, , docs.python.org, 3, tutorial, index...
        2                                                 None
        dtype: object
        """
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
        """
        Test if pattern or regex is contained within a string of a Series or Index.

        Return boolean Series or Index based on whether a given pattern or regex is contained within a string of a Series or Index.

        Parameters
        ----------
        pat : str
            Character sequence or regular expression.
        case : bool, default True
            If True, case sensitive.
        flags : int, default 0 (no flags)
            Flags to pass through to the re module, e.g. re.IGNORECASE.
        na : scalar, optional
            Fill value for missing values. The default depends on dtype of the array. For object-dtype, numpy.nan is used. For StringDtype, pandas.NA is used.
        regex : bool, default True
            If True, assumes the pat is a regular expression.
            If False, treats the pat as a literal string.

        Returns
        -------
        Series or Index of boolean values
            A Series or Index of boolean values indicating whether the given pattern is contained within the string of each element of the Series or Index.

        See also
        --------
        match
            Analogous, but stricter, relying on re.match instead of re.search.
        Series.str.startswith
            Test if the start of each string element matches a pattern.
        Series.str.endswith
            Same as startswith, but tests the end of string.

        Examples
        --------
        Returning a Series of booleans using only a literal pattern.

        >>> s1 = pd.Series(['Mouse', 'dog', 'house and parrot', '23', np.NaN])
        >>> s1.str.contains('og', regex=False)
        0    False
        1     True
        2    False
        3    False
        4     None
        dtype: object

        Returning an Index of booleans using only a literal pattern.

        >>> ind = pd.Index(['Mouse', 'dog', 'house and parrot', '23.0', np.NaN])
        >>> ind.str.contains('23', regex=False)
        Index([False, False, False, True, nan], dtype='object')

        Specifying case sensitivity using case.

        >>> s1.str.contains('oG', case=True, regex=True)
        0    False
        1    False
        2    False
        3    False
        4     None
        dtype: object

        Specifying na to be False instead of NaN replaces NaN values with False. If Series or Index does not contain NaN values the resultant dtype will be bool, otherwise, an object dtype.

        >>> s1.str.contains('og', na=False, regex=True)
        0    False
        1     True
        2    False
        3    False
        4    False
        dtype: bool

        Returning ‘house’ or ‘dog’ when either expression occurs in a string.

        >>> s1.str.contains('house|dog', regex=True)
        0    False
        1     True
        2     True
        3    False
        4     None
        dtype: object

        Ignoring case sensitivity using flags with regex.

        >>> import re
        >>> s1.str.contains('PARROT', flags=re.IGNORECASE, regex=True)
        0    False
        1    False
        2     True
        3    False
        4     None
        dtype: object

        Returning any digit using regular expression.

        >>> s1.str.contains('\\d', regex=True)
        0    False
        1    False
        2    False
        3     True
        4     None
        dtype: object

        Ensure that `pat` is not a literal pattern when `regex` is set to True. Note in the following example, one might expect only s2[1] and s2[3] to return True. However, ‘.0’ as a regex matches any character followed by a 0.

        >>> s2 = pd.Series(['40', '40.0', '41', '41.0', '35'])
        >>> s2.str.contains('.0', regex=True)
        0     True
        1     True
        2    False
        3     True
        4    False
        dtype: bool
        """
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
        r"""
        Replace each occurrence of pattern/regex in the Series/Index.

        Equivalent to str.replace() or re.sub(), depending on the regex value.

        Parameters
        ----------
        pat : str
            String can be a character sequence or regular expression.
        repl : str or callable
            Replacement string or a callable. The callable is passed the regex match object and must return a replacement string to be used. See re.sub().
        n : int, default -1 (all)
            Number of replacements to make from start.
        case : bool, default None
            Determines if replace is case sensitive:
            - If True, case sensitive (the default if pat is a string)
            - Set to False for case insensitive
            - Cannot be set if pat is a compiled regex.
        flags : int, default 0 (no flags)
            Regex module flags, e.g. re.IGNORECASE. Cannot be set if pat is a compiled regex.
        regex : bool, default False
            Determines if the passed-in pattern is a regular expression:
            - If True, assumes the passed-in pattern is a regular expression.
            - If False, treats the pattern as a literal string
            - Cannot be set to False if pat is a compiled regex or repl is a callable.

        Returns
        -------
        Series or Index of object
            A copy of the object with all matching occurrences of pat replaced by repl.

        Raises
        ------
        ValueError
            - if regex is False and repl is a callable or pat is a compiled regex
            - if pat is a compiled regex and case or flags is set

        Notes
        -----
        When pat is a compiled regex, all flags should be included in the compiled regex. Use of case, flags, or regex=False with a compiled regex will raise an error.

        Examples
        --------
        When pat is a string and regex is True, the given pat is compiled as a regex. When repl is a string, it replaces matching regex patterns as with re.sub(). NaN value(s) in the Series are left as is:

        >>> pd.Series(['foo', 'fuz', np.nan]).str.replace('f.', 'ba', regex=True)
        0     bao
        1     baz
        2    None
        dtype: object

        When pat is a string and regex is False, every pat is replaced with repl as with str.replace():

        >>> pd.Series(['f.o', 'fuz', np.nan]).str.replace('f.', 'ba', regex=False)
        0     bao
        1     fuz
        2    None
        dtype: object

        Using a compiled regex with flags
        """
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
        """
        Slice substrings from each element in the Series or Index.

        Parameters
        ----------
        start : int, optional
            Start position for slice operation.
        stop : int, optional
            Stop position for slice operation.
        step : int, optional
            Step size for slice operation.

        Returns
        -------
        Series or Index of object
            Series or Index from sliced substring from original string object.

        See also
        --------
        Series.str.slice_replace
            Replace a slice with a string.
        Series.str.get
            Return element at position. Equivalent to Series.str.slice(start=i, stop=i+1) with i being the position.

        Examples
        --------
        >>> s = pd.Series(["koala", "dog", "chameleon"])
        >>> s
        0        koala
        1          dog
        2    chameleon
        dtype: object

        >>> s.str.slice(start=1)
        0        oala
        1          og
        2    hameleon
        dtype: object

        >>> s.str.slice(start=-1)
        0    a
        1    g
        2    n
        dtype: object

        >>> s.str.slice(stop=2)
        0    ko
        1    do
        2    ch
        dtype: object

        >>> s.str.slice(step=2)
        0      kaa
        1       dg
        2    caeen
        dtype: object

        >>> s.str.slice(start=0, stop=5, step=3)
        0    kl
        1     d
        2    cm
        dtype: object
        """
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
        """
        Count occurrences of pattern in each string of the Series/Index.

        This function is used to count the number of times a particular regex pattern is repeated in each of the string elements of the Series.

        Parameters
        ----------
        pat : str
            Valid regular expression.
        flags : int, default 0, meaning no flags
            Flags for the re module.
        **kwargs
            For compatibility with other string methods. Not used.

        Returns
        -------
        Series or Index
            Same type as the calling object containing the integer counts.

        See also
        --------
        re
            Standard library module for regular expressions.
        str.count
            Standard library version, without regular expression support.

        Notes
        -----
        Some characters need to be escaped when passing in pat. eg. '$' has a special meaning in regex and must be escaped when finding this literal character.

        Examples
        --------
        >>> s = pd.Series(['A', 'B', 'Aaba', 'Baca', np.nan, 'CABA', 'cat'])
        >>> s.str.count('a')
        0    0.0
        1    0.0
        2    2.0
        3    2.0
        4    NaN
        5    0.0
        6    1.0
        dtype: float64

        Escape '$' to find the literal dollar sign.

        >>> s = pd.Series(['$', 'B', 'Aab$', '$$ca', 'C$B$', 'cat'])
        >>> s.str.count('\\$')
        0    1
        1    0
        2    1
        3    2
        4    2
        5    0
        dtype: int64

        This is also available on Index

        >>> pd.Index(['A', 'A', 'Aaba', 'cat']).str.count('a')
        Index([0, 0, 2, 1], dtype='int64')
        """
        if not isinstance(pat, (str, _pattern_type)):
            raise TypeError("first argument must be string or compiled pattern")
        return Series(
            query_compiler=self._query_compiler.str_count(pat, flags=flags, **kwargs)
        )

    def startswith(self, pat, na=np.NaN):
        """
        Test if the start of each string element matches a pattern.

        Parameters
        ----------
        pat : str or tuple[str, ...]
            Character sequence or tuple of strings. Regular expressions are not accepted.
        na : object, default NaN
            Object shown if element tested is not a string. The default depends on dtype of the array. For object-dtype, numpy.nan is used. For StringDtype, pandas.NA is used.

        Returns
        -------
        Series or Index of bool
            A Series of booleans indicating whether the given pattern matches the start of each string element.

        See also
        --------
        str.startswith
            Python standard library string method.
        Series.str.endswith
            Same as startswith, but tests the end of string.
        Series.str.contains
            Tests if string element contains a pattern.

        Examples
        --------
        >>> s = pd.Series(['bat', 'Bear', 'cat', np.nan])
        >>> s
        0     bat
        1    Bear
        2     cat
        3    None
        dtype: object

        >>> s.str.startswith('b')
        0     True
        1    False
        2    False
        3     None
        dtype: object

        >>> s.str.startswith(('b', 'B'))
        0     True
        1     True
        2    False
        3     None
        dtype: object

        Specifying na to be False instead of NaN.

        >>> s.str.startswith('b', na=False)
        0     True
        1    False
        2    False
        3    False
        dtype: bool
        """
        return Series(query_compiler=self._query_compiler.str_startswith(pat, na=na))

    def encode(self, encoding, errors="strict"):
        ErrorMessage.method_not_implemented_error("encode", "Series.str")
        return Series(
            query_compiler=self._query_compiler.str_encode(encoding, errors=errors)
        )

    def endswith(self, pat, na=np.NaN):
        """
        Test if the end of each string element matches a pattern.

        Parameters
        ----------
        pat : str or tuple[str, …]
            Character sequence or tuple of strings. Regular expressions are not accepted.
        na : object, default NaN
            Object shown if element tested is not a string. The default depends on dtype of the array. For object-dtype, numpy.nan is used. For StringDtype, pandas.NA is used.

        Returns
        -------
        Series or Index of bool
            A Series of booleans indicating whether the given pattern matches the end of each string element.

        See also
        --------
        str.endswith
            Python standard library string method.
        Series.str.startswith
            Same as endswith, but tests the start of string.
        Series.str.contains
            Tests if string element contains a pattern.

        Examples
        --------
        >>> s = pd.Series(['bat', 'bear', 'caT', np.nan])
        >>> s
        0     bat
        1    bear
        2     caT
        3    None
        dtype: object

        >>> s.str.endswith('t')
        0     True
        1    False
        2    False
        3     None
        dtype: object

        >>> s.str.endswith(('t', 'T'))
        0     True
        1    False
        2     True
        3     None
        dtype: object

        Specifying na to be False instead of NaN.

        >>> s.str.endswith('t', na=False)
        0     True
        1    False
        2    False
        3    False
        dtype: bool
        """
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
        """
        Get the length of a string. For non-string values this
        returns the length of the string representation.

        Returns
        -------
        Series
            A Series with the length of each value

        Examples
        --------
        >>> s = pd.Series(['dog',
        ...                 '',
        ...                 5,
        ...                 {'foo' : 'bar'},
        ...                 [2, 3, 5, 7],
        ...                 ('one', 'two', 'three')])
        >>> s.str.len()
        0    3.0
        1    0.0
        2    NaN
        3    NaN
        4    NaN
        5    NaN
        dtype: float64
        """
        return Series(query_compiler=self._query_compiler.str_len())

    def strip(self, to_strip: str = None) -> Series:
        """
        Remove leading and trailing characters.

        Strip whitespaces (including newlines) or a set of specified characters from each string in the Series/Index from left and right sides. Replaces any non-strings in Series with NaNs. Equivalent to str.strip().

        Parameters
        ----------
        to_strip : str or None, default None
            Specifying the set of characters to be removed. All combinations of this set of characters will be stripped. If None then whitespaces are removed.

        Returns
        -------
        Series or Index of object

        See also
        --------
        Series.str.strip
            Remove leading and trailing characters in Series/Index.
        Series.str.lstrip
            Remove leading characters in Series/Index.
        Series.str.rstrip
            Remove trailing characters in Series/Index.

        Examples
        --------
        >>> s = pd.Series(['1. Ant.  ', '2. Bee!\\n', '3. Cat?\\t', np.nan, 10, True])
        >>> s  # doctest: +NORMALIZE_WHITESPACE
        0    1. Ant.
        1    2. Bee!\\n
        2    3. Cat?\\t
        3         None
        4           10
        5         True
        dtype: object

        >>> s.str.strip()
        0    1. Ant.
        1    2. Bee!
        2    3. Cat?
        3       None
        4       None
        5       None
        dtype: object

        >>> s.str.strip('123.!? \\n\\t')
        0     Ant
        1     Bee
        2     Cat
        3    None
        4    None
        5    None
        dtype: object
        """
        # TODO: SNOW-1432420 fix bug in docstring.
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
        """
        Convert strings in the Series/Index to be titlecased .

        Returns
        -------
        Series or Index of object

        See also
        --------
        Series.str.lower
            Converts all characters to lowercase.

        Series.str.upper
            Converts all characters to uppercase.

        Series.str.title
            Converts first character of each word to uppercase and remaining to lowercase.

        Series.str.capitalize
            Converts first character to uppercase and remaining to lowercase.

        Series.str.swapcase
            Converts uppercase to lowercase and lowercase to uppercase.

        Series.str.casefold
            Removes all case distinctions in the string.

        Examples
        --------
        >>> s = pd.Series(['lower', 'CAPITALS', 'this is a sentence', 'SwApCaSe'])
        >>> s
        0                 lower
        1              CAPITALS
        2    this is a sentence
        3              SwApCaSe
        dtype: object

        >>> s.str.title()
        0                 Lower
        1              Capitals
        2    This Is A Sentence
        3              Swapcase
        dtype: object
        """
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
        """
        Convert strings in the Series/Index to be capitalized.

        Returns
        -------
        Series or Index of object

        See also
        --------
        Series.str.lower
            Converts all characters to lowercase.

        Series.str.upper
            Converts all characters to uppercase.

        Series.str.title
            Converts first character of each word to uppercase and remaining to lowercase.

        Series.str.capitalize
            Converts first character to uppercase and remaining to lowercase.

        Series.str.swapcase
            Converts uppercase to lowercase and lowercase to uppercase.

        Series.str.casefold
            Removes all case distinctions in the string.

        Examples
        --------
        >>> s = pd.Series(['lower', 'CAPITALS', 'this is a sentence', 'SwApCaSe'])
        >>> s
        0                 lower
        1              CAPITALS
        2    this is a sentence
        3              SwApCaSe
        dtype: object

        >>> s.str.capitalize()
        0                 Lower
        1              Capitals
        2    This is a sentence
        3              Swapcase
        dtype: object
        """
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
        """
        Check whether all characters in each string are digits.

        This is equivalent to running the Python string method str.isdigit() for each element of the Series. If a string has zero characters, False is returned for that check.

        Returns
        -------
        Series of boolean values with the same length as the original Series.

        Examples
        --------
        >>> s = pd.Series(['23', '³', '⅕', ''])

        The `s.str.isdigit` method checks for characters used to form numbers in base 10.
        Currently, special digits like superscripted and subscripted digits in unicode are
        not checked for.
        >>> s.str.isdigit()
        0     True
        1    False
        2    False
        3    False
        dtype: bool
        """
        return Series(query_compiler=self._query_compiler.str_isdigit())

    def isspace(self):
        ErrorMessage.method_not_implemented_error("isspace", "Series.str")
        return Series(query_compiler=self._query_compiler.str_isspace())

    def islower(self):
        """
        Check whether all characters in each string are lowercase.

        This is equivalent to running the Python string method str.islower() for each element of the Series. If a string has zero characters, False is returned for that check.

        Returns
        -------
        Series of boolean values with the same length as the original Series.

        Examples
        --------
        >>> s = pd.Series(['leopard', 'Golden Eagle', 'SNAKE', ''])
        >>> s.str.islower()
        0     True
        1    False
        2    False
        3    False
        dtype: bool
        """
        return Series(query_compiler=self._query_compiler.str_islower())

    def isupper(self):
        """
        Check whether all characters in each string are uppercase.

        This is equivalent to running the Python string method str.isupper() for each element of the Series. If a string has zero characters, False is returned for that check.

        Returns
        -------
        Series of boolean values with the same length as the original Series.

        Examples
        --------
        >>> s = pd.Series(['leopard', 'Golden Eagle', 'SNAKE', ''])
        >>> s.str.isupper()
        0    False
        1    False
        2     True
        3    False
        dtype: bool
        """
        return Series(query_compiler=self._query_compiler.str_isupper())

    def istitle(self):
        """
        Check whether all characters in each string are uppercase.

        This is equivalent to running the Python string method str.isupper() for each element of the Series. If a string has zero characters, False is returned for that check.

        Returns
        -------
        Series of boolean values with the same length as the original Series.

        Examples
        --------
        >>> s = pd.Series(['leopard', 'Golden Eagle', 'SNAKE', '', 'Snake'])
        >>> s.str.istitle()
        0    False
        1     True
        2    False
        3    False
        4     True
        dtype: bool
        """
        return Series(query_compiler=self._query_compiler.str_istitle())

    def isnumeric(self):
        ErrorMessage.method_not_implemented_error("isnumeric", "Series.str")
        return Series(query_compiler=self._query_compiler.str_isnumeric())

    def isdecimal(self):
        ErrorMessage.method_not_implemented_error("isdecimal", "Series.str")
        return Series(query_compiler=self._query_compiler.str_isdecimal())


class DatetimeProperties:
    def __init__(self, series) -> None:
        self._series = series
        self._query_compiler = series._query_compiler

    @property
    def date(self):
        """
        Returns a series of python :class:`datetime.date` objects.

        Namely, the date part of Timestamps without time and timezone information.

        Examples
        --------
        For Series:

        >>> s = pd.Series(["2020-01-01 01:23:00", "2020-02-01 12:11:05"])
        >>> s = pd.to_datetime(s)
        >>> s
        0   2020-01-01 01:23:00
        1   2020-02-01 12:11:05
        dtype: datetime64[ns]
        >>> s.dt.date
        0    2020-01-01
        1    2020-02-01
        dtype: object
        """
        return Series(query_compiler=self._query_compiler.dt_property("date"))

    @property
    def time(self):
        return Series(query_compiler=self._query_compiler.dt_property("time"))

    @property
    def timetz(self):
        return Series(query_compiler=self._query_compiler.dt_property("timetz"))

    @property
    def year(self):
        """
        Returns a series of the years of the datetime.

        Examples
        --------
        >>> datetime_series = pd.Series(
        ...     pd.date_range("2000-01-01", periods=3, freq="YE")
        ... )
        >>> datetime_series
        0   2000-12-31
        1   2001-12-31
        2   2002-12-31
        dtype: datetime64[ns]
        >>> datetime_series.dt.year
        0    2000
        1    2001
        2    2002
        dtype: int16
        """
        return Series(query_compiler=self._query_compiler.dt_property("year"))

    @property
    def month(self):
        """
        Returns a series of the months of the datetime.

        Examples
        --------
        >>> datetime_series = pd.Series(
        ...     pd.date_range("2000-01-01", periods=3, freq="ME")
        ... )
        >>> datetime_series
        0   2000-01-31
        1   2000-02-29
        2   2000-03-31
        dtype: datetime64[ns]
        >>> datetime_series.dt.month
        0    1
        1    2
        2    3
        dtype: int8
        """
        return Series(query_compiler=self._query_compiler.dt_property("month"))

    @property
    def day(self):
        """
        Returns a series of the days of the datetime.

        Examples
        --------
        >>> datetime_series = pd.Series(
        ...     pd.date_range("2000-01-01", periods=3, freq="D")
        ... )
        >>> datetime_series
        0   2000-01-01
        1   2000-01-02
        2   2000-01-03
        dtype: datetime64[ns]
        >>> datetime_series.dt.day
        0    1
        1    2
        2    3
        dtype: int8
        """
        return Series(query_compiler=self._query_compiler.dt_property("day"))

    @property
    def hour(self):
        """
        Returns a series of the hours of the datetime.

        Examples
        --------
        >>> datetime_series = pd.Series(
        ...     pd.date_range("2000-01-01", periods=3, freq="h")
        ... )
        >>> datetime_series
        0   2000-01-01 00:00:00
        1   2000-01-01 01:00:00
        2   2000-01-01 02:00:00
        dtype: datetime64[ns]
        >>> datetime_series.dt.hour
        0    0
        1    1
        2    2
        dtype: int8
        """
        return Series(query_compiler=self._query_compiler.dt_property("hour"))

    @property
    def minute(self):
        """
        Returns a series of the minutes of the datetime.

        Examples
        --------
        >>> datetime_series = pd.Series(
        ...     pd.date_range("2000-01-01", periods=3, freq="min")
        ... )
        >>> datetime_series
        0   2000-01-01 00:00:00
        1   2000-01-01 00:01:00
        2   2000-01-01 00:02:00
        dtype: datetime64[ns]
        >>> datetime_series.dt.minute
        0    0
        1    1
        2    2
        dtype: int8
        """
        return Series(query_compiler=self._query_compiler.dt_property("minute"))

    @property
    def second(self):
        """
        Returns a series of the seconds of the datetime.

        Examples
        --------
        >>> datetime_series = pd.Series(
        ...     pd.date_range("2000-01-01", periods=3, freq="s")
        ... )
        >>> datetime_series
        0   2000-01-01 00:00:00
        1   2000-01-01 00:00:01
        2   2000-01-01 00:00:02
        dtype: datetime64[ns]
        >>> datetime_series.dt.second
        0    0
        1    1
        2    2
        dtype: int8
        """
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
        """
        Returns a series of the quarters of the datetime.

        Examples
        --------
        >>> datetime_series = pd.Series(
        ...     pd.date_range("2000-01-01", periods=3, freq="3ME")
        ... )
        >>> datetime_series
        0   2000-01-31
        1   2000-04-30
        2   2000-07-31
        dtype: datetime64[ns]
        >>> datetime_series.dt.quarter
        0    1
        1    2
        2    3
        dtype: int8
        """
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
