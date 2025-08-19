#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""This module contains StringMethods an DateTimeMethods docstrings that override modin's docstrings."""


class StringMethods:
    def casefold():
        pass

    def cat():
        pass

    def decode():
        pass

    def split():
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
        :class:`~modin.pandas.Series`, Index, :class:`~modin.pandas.DataFrame` or MultiIndex
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

        When using expand=True, the split elements will expand out into separate columns. If NaN is present, it is propagated throughout the columns during the split.

        >>> s.str.split(expand=True)
                                                       0     1     2        3         4
        0                                           this    is     a  regular  sentence
        1  https://docs.python.org/3/tutorial/index.html  None  None     None      None
        2                                           None  None  None     None      None
        """

    def rsplit():
        pass

    def get():
        """
        Extract element from each component at specified position or with specified key.

        Extract element from lists, tuples, dict, or strings in each element in the Series/Index.

        Parameters
        ----------
        i : int
            Position or key of element to extract.

        Returns
        -------
        Series or Index

        Examples
        --------
        >>> s = pd.Series(["String",
        ...            (1, 2, 3),
        ...            ["a", "b", "c"],
        ...            123,
        ...            -456,
        ...            {1: "Hello", "2": "World"}])
        >>> s.str.get(1)
        0       t
        1    None
        2    None
        3    None
        4    None
        5    None
        dtype: object

        >>> s.str.get(-1)
        0       g
        1    None
        2    None
        3    None
        4    None
        5    None
        dtype: object
        """

    def join():
        pass

    def get_dummies():
        pass

    def contains():
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

        >>> s1 = pd.Series(['Mouse', 'dog', 'house and parrot', '23', np.nan])
        >>> s1.str.contains('og', regex=False)
        0    False
        1     True
        2    False
        3    False
        4     None
        dtype: object

        Returning an Index of booleans using only a literal pattern.

        >>> ind = pd.Index(['Mouse', 'dog', 'house and parrot', '23.0', np.nan])
        >>> ind.str.contains('23', regex=False)
        Index([False, False, False, True, None], dtype='object')

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

    def replace():
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

    def pad():
        """
        Pad strings in the Series/Index up to width.

        Parameters
        ----------
        width : int
            Minimum width of resulting string; additional characters will be filled with character defined in fillchar.
        side : {‘left’, ‘right’, ‘both’}, default ‘left’
            Side from which to fill resulting string.
        fillchar : str, default ‘ ‘
            Additional character for filling, default is whitespace.
        Returns
        -------
        Series or Index of object
            Returns Series or Index with minimum number of char in object.

        See also
        --------
        Series.str.rjust
            Fills the left side of strings with an arbitrary character. Equivalent to Series.str.pad(side='left').
        Series.str.ljust
            Fills the right side of strings with an arbitrary character. Equivalent to Series.str.pad(side='right').
        Series.str.center
            Fills both sides of strings with an arbitrary character. Equivalent to Series.str.pad(side='both').
        Series.str.zfill
            Pad strings in the Series/Index by prepending ‘0’ character. Equivalent to Series.str.pad(side='left', fillchar='0').

        Examples
        --------
        >>> s = pd.Series(["caribou", "tiger"])
        >>> s
        0    caribou
        1      tiger
        dtype: object

        >>> s.str.pad(width=10)
        0       caribou
        1         tiger
        dtype: object

        >>> s.str.pad(width=10, side='right', fillchar='-')
        0    caribou---
        1    tiger-----
        dtype: object

        >>> s.str.pad(width=10, side='both', fillchar='-')
        0    -caribou--
        1    --tiger---
        dtype: object
        """

    def center():
        """
        Pad left and right side of strings in the Series/Index.

        Equivalent to str.center().

        Parameters
        ----------
        width : int
            Minimum width of resulting string; additional characters will be filled with fillchar.
        fillchar : str
            Additional character for filling, default is whitespace.

        Returns
        -------
        Series/Index of objects.

        Examples
        --------
        For Series.str.center:

        >>> ser = pd.Series(['dog', 'bird', 'mouse'])
        >>> ser.str.center(8, fillchar='.')
        0    ..dog...
        1    ..bird..
        2    .mouse..
        dtype: object

        For Series.str.ljust:

        >>> ser = pd.Series(['dog', 'bird', 'mouse'])
        >>> ser.str.ljust(8, fillchar='.')
        0    dog.....
        1    bird....
        2    mouse...
        dtype: object

        For Series.str.rjust:

        >>> ser = pd.Series(['dog', 'bird', 'mouse'])
        >>> ser.str.rjust(8, fillchar='.')
        0    .....dog
        1    ....bird
        2    ...mouse
        dtype: object
        """

    def ljust():
        """
        Pad right side of strings in the Series/Index.

        Equivalent to str.ljust().

        Parameters
        ----------
        width : int
            Minimum width of resulting string; additional characters will be filled with fillchar.
        fillchar : str
            Additional character for filling, default is whitespace.

        Returns
        -------
            Series/Index of objects.

        Examples
        --------
        For Series.str.center:

        >>> ser = pd.Series(['dog', 'bird', 'mouse'])
        >>> ser.str.center(8, fillchar='.')
        0    ..dog...
        1    ..bird..
        2    .mouse..
        dtype: object

        For Series.str.ljust:

        >>> ser = pd.Series(['dog', 'bird', 'mouse'])
        >>> ser.str.ljust(8, fillchar='.')
        0    dog.....
        1    bird....
        2    mouse...
        dtype: object

        For Series.str.rjust:

        >>> ser = pd.Series(['dog', 'bird', 'mouse'])
        >>> ser.str.rjust(8, fillchar='.')
        0    .....dog
        1    ....bird
        2    ...mouse
        dtype: object
        """

    def rjust():
        """
        Pad left side of strings in the Series/Index.

        Equivalent to str.rjust().

        Parameters
        ----------
        width : int
            Minimum width of resulting string; additional characters will be filled with fillchar.
        fillchar : str
            Additional character for filling, default is whitespace.
        Returns
        -------
            Series/Index of objects.

        Examples
        --------
        For Series.str.center:

        >>> ser = pd.Series(['dog', 'bird', 'mouse'])
        >>> ser.str.center(8, fillchar='.')
        0    ..dog...
        1    ..bird..
        2    .mouse..
        dtype: object

        For Series.str.ljust:

        >>> ser = pd.Series(['dog', 'bird', 'mouse'])
        >>> ser.str.ljust(8, fillchar='.')
        0    dog.....
        1    bird....
        2    mouse...
        dtype: object

        For Series.str.rjust:

        >>> ser = pd.Series(['dog', 'bird', 'mouse'])
        >>> ser.str.rjust(8, fillchar='.')
        0    .....dog
        1    ....bird
        2    ...mouse
        dtype: object
        """

    def zfill():
        pass

    def wrap():
        pass

    def slice():
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

    def slice_replace():
        pass

    def count():
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

    def startswith():
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

    def encode():
        pass

    def endswith():
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

    def findall():
        pass

    def fullmatch():
        pass

    def match():
        """
        Determine if each string starts with a match of a regular expression.

        Parameters
        ----------
        pat : str
            Character sequence.
        case : bool, default True
            If True, case sensitive.
        flags : int, default 0 (no flags)
            Regex module flags, e.g. re.IGNORECASE.
        na : scalar, optional
            Fill value for missing values. The default depends on dtype of the array. For object-dtype, numpy.nan is used. For StringDtype, pandas.NA is used.

        Returns
        -------
        Series/Index/array of boolean values

        See also
        --------
        fullmatch
            Stricter matching that requires the entire string to match.
        contains
            Analogous, but less strict, relying on re.search instead of re.match.
        extract
            Extract matched groups.

        Examples
        --------
        >>> ser = pd.Series(["horse", "eagle", "donkey"])
        >>> ser.str.match("e")
        0    False
        1     True
        2    False
        dtype: bool
        """

    def extract():
        pass

    def extractall():
        pass

    def len():
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

    def strip():
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

    def rstrip():
        """
        Remove trailing characters.

        Strip whitespaces (including newlines) or a set of specified characters from each string in the Series/Index from right side. Replaces any non-strings in Series with NaNs. Equivalent to str.rstrip().

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

        >>> s.str.rstrip('.!? \\n\\t')
        0    1. Ant
        1    2. Bee
        2    3. Cat
        3      None
        4      None
        5      None
        dtype: object
        """

    def lstrip():
        """
        Remove leading characters.

        Strip whitespaces (including newlines) or a set of specified characters from each string in the Series/Index from left side. Replaces any non-strings in Series with NaNs. Equivalent to str.lstrip().

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

        >>> s.str.lstrip('123.')  # doctest: +NORMALIZE_WHITESPACE
        0    Ant.
        1    Bee!\\n
        2    Cat?\\t
        3      None
        4      None
        5      None
        dtype: object
        """

    def partition():
        pass

    def removeprefix():
        pass

    def removesuffix():
        pass

    def repeat():
        pass

    def rpartition():
        pass

    def lower():
        pass

    def upper():
        pass

    def title():
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

    def find():
        pass

    def rfind():
        pass

    def index():
        pass

    def rindex():
        pass

    def capitalize():
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

    def swapcase():
        pass

    def normalize():
        """
        Convert times to midnight.


        The time component of the date-time is converted to midnight i.e. 00:00:00. This is useful in cases, when the time does not matter. Length is unaltered. The timezones are unaffected.


        This method is available on Series with datetime values under the .dt accessor, and directly on Datetime Array/Index.


        Returns
        -------
        DatetimeArray, DatetimeIndex or Series
            The same type as the original data. Series will have the same name and index. DatetimeIndex will have the same name.

        See also
        --------
        floor
            Floor the datetimes to the specified freq.
        ceil
            Ceil the datetimes to the specified freq.
        round
            Round the datetimes to the specified freq.

        Examples
        --------
        >>> idx = pd.date_range(start='2014-08-01 10:00', freq='h',
        ...                    periods=3, tz='Asia/Calcutta')  # doctest: +SKIP
        >>> idx  # doctest: +SKIP
        DatetimeIndex(['2014-08-01 10:00:00+05:30',
                    '2014-08-01 11:00:00+05:30',
                    '2014-08-01 12:00:00+05:30'],
                        dtype='datetime64[ns, Asia/Calcutta]', freq=None)
        >>> idx.normalize()  # doctest: +SKIP
        DatetimeIndex(['2014-08-01 00:00:00+05:30',
                    '2014-08-01 00:00:00+05:30',
                    '2014-08-01 00:00:00+05:30'],
                    dtype='datetime64[ns, Asia/Calcutta]', freq=None)
        """

    def translate():
        """
        Map all characters in the string through the given mapping table.

        Equivalent to standard :meth:`str.translate`.

        Parameters
        ----------
        table : dict
            Table is a mapping of Unicode ordinals to Unicode ordinals, strings, or
            None. Unmapped characters are left untouched.
            Characters mapped to None are deleted. :meth:`str.maketrans` is a
            helper function for making translation tables.

        Returns
        -------
        Series

        Examples
        --------
        >>> ser = pd.Series(["El niño", "Françoise"])
        >>> mytable = str.maketrans({'ñ': 'n', 'ç': 'c'})
        >>> ser.str.translate(mytable)  # doctest: +NORMALIZE_WHITESPACE
        0   El nino
        1   Francoise
        dtype: object

        Notes
        -----
        Snowpark pandas internally uses the Snowflake SQL `TRANSLATE` function to implement this
        operation. Since this function uses strings instead of unicode codepoints, it will accept
        mappings containing string keys that would be invalid in pandas.

        The following example fails silently in vanilla pandas without `str.maketrans`:

        >>> import pandas
        >>> pandas.Series("aaa").str.translate({"a": "A"})
        0    aaa
        dtype: object
        >>> pandas.Series("aaa").str.translate(str.maketrans({"a": "A"}))
        0    AAA
        dtype: object

        The same code works in Snowpark pandas without `str.maketrans`:

        >>> pd.Series("aaa").str.translate({"a": "A"})
        0    AAA
        dtype: object
        >>> pd.Series("aaa").str.translate(str.maketrans({"a": "A"}))
        0    AAA
        dtype: object

        Furthermore, due to restrictions in the underlying SQL, Snowpark pandas currently requires
        all string values to be one unicode codepoint in length. To create replacements of multiple
        characters, chain calls to `Series.str.replace` as needed.

        Vanilla pandas code:

        >>> import pandas
        >>> pandas.Series("ab").str.translate(str.maketrans({"a": "A", "b": "BBB"}))
        0    ABBB
        dtype: object

        Snowpark pandas equivalent:

        >>> pd.Series("ab").str.translate({"a": "A"}).str.replace("b", "BBB")
        0    ABBB
        dtype: object
        """

    def isalnum():
        pass

    def isalpha():
        pass

    def isdigit():
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

    def isspace():
        pass

    def islower():
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

    def isupper():
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

    def istitle():
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

    def isnumeric():
        pass

    def isdecimal():
        pass


# Docstrings for DatetimeProperties (need to match the name of the pandas class inherited from)
class CombinedDatetimelikeProperties:
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

    @property
    def time():
        """
        Returns numpy array of datetime.time objects.

        The time part of the Timestamps.

        Examples
        --------
        For Series:

        >>> s = pd.Series(["1/1/2020 10:00:00", "2/1/2020 11:00:00"])
        >>> s = pd.to_datetime(s)
        >>> s
        0   2020-01-01 10:00:00
        1   2020-02-01 11:00:00
        dtype: datetime64[ns]
        >>> s.dt.time
        0    10:00:00
        1    11:00:00
        dtype: object

        For DatetimeIndex:

        >>> idx = pd.DatetimeIndex(["1/1/2020 10:00:00+00:00",
        ...                         "2/1/2020 11:00:00+00:00"])
        >>> idx.time
        Index([10:00:00, 11:00:00], dtype='object')
        """

    @property
    def timetz():
        pass

    @property
    def year():
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

    @property
    def month():
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

    @property
    def day():
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

    @property
    def hour():
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

    @property
    def minute():
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

    @property
    def second():
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

    @property
    def microsecond():
        """
        The microseconds of the datetime.

        Examples
        --------
        >>> datetime_series = pd.Series(
        ...     pd.date_range("2000-01-01", periods=3, freq="us")
        ... )
        >>> datetime_series
        0   2000-01-01 00:00:00.000000
        1   2000-01-01 00:00:00.000001
        2   2000-01-01 00:00:00.000002
        dtype: datetime64[ns]
        >>> datetime_series.dt.microsecond
        0    0
        1    1
        2    2
        dtype: int64
        """

    @property
    def nanosecond():
        """
        The nanoseconds of the datetime.

        Examples
        --------
        >>> datetime_series = pd.Series(
        ...     pd.date_range("2000-01-01", periods=3, freq="ns")
        ... )
        >>> datetime_series
        0   2000-01-01 00:00:00.000000000
        1   2000-01-01 00:00:00.000000001
        2   2000-01-01 00:00:00.000000002
        dtype: datetime64[ns]
        >>> datetime_series.dt.nanosecond
        0    0
        1    1
        2    2
        dtype: int32
        """

    @property
    def dayofweek():
        """
        The day of the week with Monday=0, Sunday=6.

        Return the day of the week. It is assumed the week starts on Monday,
        which is denoted by 0, and ends on Sunday, which is denoted by 6.

        Examples
        --------
        >>> s = pd.Series(pd.date_range('2016-12-31', '2017-01-08', freq='D'))
        >>> s
        0   2016-12-31
        1   2017-01-01
        2   2017-01-02
        3   2017-01-03
        4   2017-01-04
        5   2017-01-05
        6   2017-01-06
        7   2017-01-07
        8   2017-01-08
        dtype: datetime64[ns]
        >>> s.dt.dayofweek
        0    5
        1    6
        2    0
        3    1
        4    2
        5    3
        6    4
        7    5
        8    6
        dtype: int16
        """
        pass

    @property
    def weekday():
        """
        The day of the week with Monday=0, Sunday=6.

        Return the day of the week. It is assumed the week starts on Monday, which is denoted by 0 and ends on Sunday which is denoted by 6. This method is available on both Series with datetime values (using the dt accessor) or DatetimeIndex.

        Returns
        -------
        Series or Index
            Containing integers indicating the day number.

        See also
        --------
        Series.dt.dayofweek
            Alias.
        Series.dt.weekday
            Alias.
        Series.dt.day_name
            Returns the name of the day of the week.

        Examples
        --------
        >>> s = pd.date_range('2016-12-31', '2017-01-08', freq='D').to_series()
        >>> s.dt.weekday
        2016-12-31    5
        2017-01-01    6
        2017-01-02    0
        2017-01-03    1
        2017-01-04    2
        2017-01-05    3
        2017-01-06    4
        2017-01-07    5
        2017-01-08    6
        Freq: None, dtype: int16
        """

    @property
    def dayofyear():
        """
        The ordinal day of the year.

        Examples
        --------
        >>> s = pd.Series(pd.to_datetime(["1/1/2020", "2/1/2020"]))
        >>> s
        0   2020-01-01
        1   2020-02-01
        dtype: datetime64[ns]
        >>> s.dt.dayofyear
        0     1
        1    32
        dtype: int16
        """
        pass

    @property
    def quarter():
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

    def isocalendar():
        """
        Calculate year, week, and day according to the ISO 8601 standard.

        Returns
        -------
        :class:`~modin.pandas.DataFrame`
            With columns year, week, and day.

        Examples
        --------
        >>> ser = pd.Series(pd.date_range("2020-05-01", periods=5, freq="4D"))
        >>> ser.dt.isocalendar()
           year  week  day
        0  2020    18    5
        1  2020    19    2
        2  2020    19    6
        3  2020    20    3
        4  2020    20    7
        """

    @property
    def is_month_start():
        """
        Indicates whether the date is the first day of the month.

        Returns
        -------
        Series or array
            For Series, returns a Series with boolean values. For DatetimeIndex, returns a boolean array.

        See also
        --------
        is_month_start
            Return a boolean indicating whether the date is the first day of the month.
        is_month_end
            Return a boolean indicating whether the date is the last day of the month.

        Examples
        --------
        This method is available on Series with datetime values under the .dt accessor, and directly on DatetimeIndex.

        >>> s = pd.Series(pd.date_range("2018-02-27", periods=3))
        >>> s
        0   2018-02-27
        1   2018-02-28
        2   2018-03-01
        dtype: datetime64[ns]
        >>> s.dt.is_month_start
        0    False
        1    False
        2     True
        dtype: bool
        >>> s.dt.is_month_end
        0    False
        1     True
        2    False
        dtype: bool
        """

    @property
    def is_month_end():
        """
        Indicates whether the date is the last day of the month.

        Returns
        -------
        Series or array
            For Series, returns a Series with boolean values. For DatetimeIndex, returns a boolean array.

        See also
        --------
        is_month_start
            Return a boolean indicating whether the date is the first day of the month.
        is_month_end
            Return a boolean indicating whether the date is the last day of the month.

        Examples
        --------
        This method is available on Series with datetime values under the .dt accessor, and directly on DatetimeIndex.

        >>> s = pd.Series(pd.date_range("2018-02-27", periods=3))
        >>> s
        0   2018-02-27
        1   2018-02-28
        2   2018-03-01
        dtype: datetime64[ns]
        >>> s.dt.is_month_start
        0    False
        1    False
        2     True
        dtype: bool
        >>> s.dt.is_month_end
        0    False
        1     True
        2    False
        dtype: bool
        """

    @property
    def is_quarter_start():
        """
        Indicator for whether the date is the first day of a quarter.

        Returns
        -------
        is_quarter_start : Series or DatetimeIndex
            The same type as the original data with boolean values. Series will have the same name and index. DatetimeIndex will have the same name.

        See also
        --------
        quarter
            Return the quarter of the date.
        is_quarter_end
            Similar property for indicating the quarter end.

        Examples
        --------
        This method is available on Series with datetime values under the .dt accessor, and directly on DatetimeIndex.

        >>> df = pd.DataFrame({'dates': pd.date_range("2017-03-30",
        ...                   periods=4)})
        >>> df.assign(quarter=df.dates.dt.quarter,
        ...           is_quarter_start=df.dates.dt.is_quarter_start)
               dates  quarter  is_quarter_start
        0 2017-03-30        1             False
        1 2017-03-31        1             False
        2 2017-04-01        2              True
        3 2017-04-02        2             False
        """

    @property
    def is_quarter_end():
        """
        Indicator for whether the date is the last day of a quarter.

        Returns
        -------
        is_quarter_end : Series or DatetimeIndex
            The same type as the original data with boolean values. Series will have the same name and index. DatetimeIndex will have the same name.

        See also
        --------
        quarter
            Return the quarter of the date.
        is_quarter_start
            Similar property indicating the quarter start.

        Examples
        --------
        This method is available on Series with datetime values under the .dt accessor, and directly on DatetimeIndex.

        >>> df = pd.DataFrame({'dates': pd.date_range("2017-03-30",
        ...                    periods=4)})
        >>> df.assign(quarter=df.dates.dt.quarter,
        ...           is_quarter_end=df.dates.dt.is_quarter_end)
               dates  quarter  is_quarter_end
        0 2017-03-30        1           False
        1 2017-03-31        1            True
        2 2017-04-01        2           False
        3 2017-04-02        2           False
        """

    @property
    def is_year_start():
        """
        Indicate whether the date is the first day of a year.

        Returns
        -------
        Series or DatetimeIndex
            The same type as the original data with boolean values. Series will have the same name and index. DatetimeIndex will have the same name.

        See also
        --------
        is_year_end
            Similar property indicating the last day of the year.

        Examples
        --------
        This method is available on Series with datetime values under the .dt accessor, and directly on DatetimeIndex.

        >>> dates = pd.Series(pd.date_range("2017-12-30", periods=3))
        >>> dates
        0   2017-12-30
        1   2017-12-31
        2   2018-01-01
        dtype: datetime64[ns]

        >>> dates.dt.is_year_start
        0    False
        1    False
        2     True
        dtype: bool
        """

    @property
    def is_year_end():
        """
        Indicate whether the date is the last day of the year.

        Returns
        -------
        Series or DatetimeIndex
            The same type as the original data with boolean values. Series will have the same name and index. DatetimeIndex will have the same name.

        See also
        --------
        is_year_start
            Similar property indicating the start of the year.

        Examples
        --------
        This method is available on Series with datetime values under the .dt accessor, and directly on DatetimeIndex.

        >>> dates = pd.Series(pd.date_range("2017-12-30", periods=3))
        >>> dates
        0   2017-12-30
        1   2017-12-31
        2   2018-01-01
        dtype: datetime64[ns]

        >>> dates.dt.is_year_end
        0    False
        1     True
        2    False
        dtype: bool
        """

    @property
    def is_leap_year():
        """
        Boolean indicator if the date belongs to a leap year.

        A leap year is a year, which has 366 days (instead of 365) including 29th of February as an intercalary day. Leap years are years which are multiples of four with the exception of years divisible by 100 but not by 400.

        Returns
        -------
        Series or ndarray
            Booleans indicating if dates belong to a leap year.

        Examples
        --------
        This method is available on Series with datetime values under the .dt accessor, and directly on DatetimeIndex.

        >>> idx = pd.date_range("2012-01-01", "2015-01-01", freq="YE")
        >>> idx
        DatetimeIndex(['2012-12-31', '2013-12-31', '2014-12-31'], dtype='datetime64[ns]', freq=None)
        >>> idx.is_leap_year  # doctest: +SKIP
        array([ True, False, False])

        >>> dates_series = pd.Series(idx)
        >>> dates_series
        0   2012-12-31
        1   2013-12-31
        2   2014-12-31
        dtype: datetime64[ns]
        >>> dates_series.dt.is_leap_year
        0     True
        1    False
        2    False
        dtype: bool
        """

    @property
    def daysinmonth():
        """
        The number of days in the month.

        Examples
        --------
        For Series:

        period = pd.period_range('2020-1-1 00:00', '2020-3-1 00:00', freq='M')  # doctest: +SKIP
        s = pd.Series(period)  # doctest: +SKIP
        s
        0   2020-01
        1   2020-02
        2   2020-03
        dtype: period[M]
        s.dt.days_in_month  # doctest: +SKIP
        0    31
        1    29
        2    31
        dtype: int64

        For PeriodIndex:

        idx = pd.PeriodIndex(["2023-01", "2023-02", "2023-03"], freq="M")  # doctest: +SKIP
        idx.days_in_month   # It can be also entered as `daysinmonth`  # doctest: +SKIP
        Index([31, 28, 31], dtype='int64')
        """

    @property
    def days_in_month():
        """
        The number of days in the month.

        Examples
        --------
        For Series:

        period = pd.period_range('2020-1-1 00:00', '2020-3-1 00:00', freq='M')  # doctest: +SKIP
        s = pd.Series(period)  # doctest: +SKIP
        s
        0   2020-01
        1   2020-02
        2   2020-03
        dtype: period[M]
        s.dt.days_in_month  # doctest: +SKIP
        0    31
        1    29
        2    31
        dtype: int64

        For PeriodIndex:

        idx = pd.PeriodIndex(["2023-01", "2023-02", "2023-03"], freq="M")  # doctest: +SKIP
        idx.days_in_month   # It can be also entered as `daysinmonth`  # doctest: +SKIP
        Index([31, 28, 31], dtype='int64')
        """

    @property
    def tz():
        pass

    @property
    def freq():
        pass

    def to_period():
        pass

    def to_pydatetime():
        pass

    def tz_localize():
        """
        Localize tz-naive Datetime Array/Index to tz-aware Datetime Array/Index.

        This method takes a time zone (tz) naive Datetime Array/Index object and makes this time zone aware. It does not move the time to another time zone.

        This method can also be used to do the inverse – to create a time zone unaware object from an aware object. To that end, pass tz=None.

        Parameters
        ----------
        tz : str, pytz.timezone, dateutil.tz.tzfile, datetime.tzinfo or None
            Time zone to convert timestamps to. Passing None will remove the time zone information preserving local time.
        ambiguous : ‘infer’, ‘NaT’, bool array, default ‘raise’
            When clocks moved backward due to DST, ambiguous times may arise. For example in Central European Time (UTC+01), when going from 03:00 DST to 02:00 non-DST, 02:30:00 local time occurs both at 00:30:00 UTC and at 01:30:00 UTC. In such a situation, the ambiguous parameter dictates how ambiguous times should be handled.
            - ‘infer’ will attempt to infer fall dst-transition hours based on order
            - bool-ndarray where True signifies a DST time, False signifies a non-DST time (note that this flag is only applicable for ambiguous times)
            - ‘NaT’ will return NaT where there are ambiguous times
            - ‘raise’ will raise an AmbiguousTimeError if there are ambiguous times.
        nonexistent : ‘shift_forward’, ‘shift_backward, ‘NaT’, timedelta, default ‘raise’
            A nonexistent time does not exist in a particular timezone where clocks moved forward due to DST.
            - ‘shift_forward’ will shift the nonexistent time forward to the closest existing time
            - ‘shift_backward’ will shift the nonexistent time backward to the closest existing time
            - ‘NaT’ will return NaT where there are nonexistent times
            - timedelta objects will shift nonexistent times by the timedelta
            - ‘raise’ will raise an NonExistentTimeError if there are nonexistent times.

        Returns
        -------
        Same type as self
            Array/Index converted to the specified time zone.

        Raises
        ------
        TypeError
            If the Datetime Array/Index is tz-aware and tz is not None.

        See also
        --------
        DatetimeIndex.tz_convert
            Convert tz-aware DatetimeIndex from one time zone to another.

        Examples
        --------
        >>> tz_naive = pd.date_range('2018-03-01 09:00', periods=3)
        >>> tz_naive
        DatetimeIndex(['2018-03-01 09:00:00', '2018-03-02 09:00:00',
                       '2018-03-03 09:00:00'],
                      dtype='datetime64[ns]', freq=None)

        Localize DatetimeIndex in US/Eastern time zone:

        >>> tz_aware = tz_naive.tz_localize(tz='US/Eastern')  # doctest: +SKIP
        >>> tz_aware  # doctest: +SKIP
        DatetimeIndex(['2018-03-01 09:00:00-05:00',
                       '2018-03-02 09:00:00-05:00',
                       '2018-03-03 09:00:00-05:00'],
                      dtype='datetime64[ns, US/Eastern]', freq=None)

        With the tz=None, we can remove the time zone information while keeping the local time (not converted to UTC):

        >>> tz_aware.tz_localize(None)  # doctest: +SKIP
        DatetimeIndex(['2018-03-01 09:00:00', '2018-03-02 09:00:00',
                       '2018-03-03 09:00:00'],
                      dtype='datetime64[ns]', freq=None)

        Be careful with DST changes. When there is sequential data, pandas can infer the DST time:

        >>> s = pd.to_datetime(pd.Series(['2018-10-28 01:30:00',
        ...                             '2018-10-28 02:00:00',
        ...                             '2018-10-28 02:30:00',
        ...                             '2018-10-28 02:00:00',
        ...                             '2018-10-28 02:30:00',
        ...                             '2018-10-28 03:00:00',
        ...                             '2018-10-28 03:30:00']))
        >>> s.dt.tz_localize('CET', ambiguous='infer')  # doctest: +SKIP
        0   2018-10-28 01:30:00+02:00
        1   2018-10-28 02:00:00+02:00
        2   2018-10-28 02:30:00+02:00
        3   2018-10-28 02:00:00+01:00
        4   2018-10-28 02:30:00+01:00
        5   2018-10-28 03:00:00+01:00
        6   2018-10-28 03:30:00+01:00
        dtype: datetime64[ns, CET]

        In some cases, inferring the DST is impossible. In such cases, you can pass an ndarray to the ambiguous parameter to set the DST explicitly

        >>> s = pd.to_datetime(pd.Series(['2018-10-28 01:20:00',
        ...                             '2018-10-28 02:36:00',
        ...                             '2018-10-28 03:46:00']))
        >>> s.dt.tz_localize('CET', ambiguous=np.array([True, True, False]))  # doctest: +SKIP
        0   2018-10-28 01:20:00+02:00
        1   2018-10-28 02:36:00+02:00
        2   2018-10-28 03:46:00+01:00
        dtype: datetime64[ns, CET]

        If the DST transition causes nonexistent times, you can shift these dates forward or backwards with a timedelta object or ‘shift_forward’ or ‘shift_backwards’.

        >>> s = pd.to_datetime(pd.Series(['2015-03-29 02:30:00',
        ...                             '2015-03-29 03:30:00']))
        >>> s.dt.tz_localize('Europe/Warsaw', nonexistent='shift_forward')  # doctest: +SKIP
        0   2015-03-29 03:00:00+02:00
        1   2015-03-29 03:30:00+02:00
        dtype: datetime64[ns, Europe/Warsaw]

        >>> s.dt.tz_localize('Europe/Warsaw', nonexistent='shift_backward')  # doctest: +SKIP
        0   2015-03-29 01:59:59.999999999+01:00
        1   2015-03-29 03:30:00+02:00
        dtype: datetime64[ns, Europe/Warsaw]

        >>> s.dt.tz_localize('Europe/Warsaw', nonexistent=pd.Timedelta('1h'))  # doctest: +SKIP
        0   2015-03-29 03:30:00+02:00
        1   2015-03-29 03:30:00+02:00
        dtype: datetime64[ns, Europe/Warsaw]
        """

    def tz_convert():
        """
        Convert tz-aware Datetime Array/Index from one time zone to another.

        Parameters
        ----------
        tz : str, pytz.timezone, dateutil.tz.tzfile, datetime.tzinfo or None
            Time zone for time. Corresponding timestamps would be converted to this time zone of the Datetime Array/Index. A tz of None will convert to UTC and remove the timezone information.

        Returns
        -------
        Array or Index

        Raises
        ------
        TypeError
            If Datetime Array/Index is tz-naive.

        See also
        DatetimeIndex.tz
            A timezone that has a variable offset from UTC.
        DatetimeIndex.tz_localize
            Localize tz-naive DatetimeIndex to a given time zone, or remove timezone from a tz-aware DatetimeIndex.

        Examples
        --------
        With the tz parameter, we can change the DatetimeIndex to other time zones:

        >>> dti = pd.date_range(start='2014-08-01 09:00',
        ...                     freq='h', periods=3, tz='Europe/Berlin')  # doctest: +SKIP

        >>> dti  # doctest: +SKIP
        DatetimeIndex(['2014-08-01 09:00:00+02:00',
                       '2014-08-01 10:00:00+02:00',
                       '2014-08-01 11:00:00+02:00'],
                      dtype='datetime64[ns, Europe/Berlin]', freq='h')

        >>> dti.tz_convert('US/Central')  # doctest: +SKIP
        DatetimeIndex(['2014-08-01 02:00:00-05:00',
                       '2014-08-01 03:00:00-05:00',
                       '2014-08-01 04:00:00-05:00'],
                      dtype='datetime64[ns, US/Central]', freq='h')

        With the tz=None, we can remove the timezone (after converting to UTC if necessary):

        >>> dti = pd.date_range(start='2014-08-01 09:00', freq='h',
        ...                     periods=3, tz='Europe/Berlin')  # doctest: +SKIP

        >>> dti  # doctest: +SKIP
        DatetimeIndex(['2014-08-01 09:00:00+02:00',
                       '2014-08-01 10:00:00+02:00',
                       '2014-08-01 11:00:00+02:00'],
                      dtype='datetime64[ns, Europe/Berlin]', freq='h')

        >>> dti.tz_convert(None)  # doctest: +SKIP
        DatetimeIndex(['2014-08-01 07:00:00',
                       '2014-08-01 08:00:00',
                       '2014-08-01 09:00:00'],
                      dtype='datetime64[ns]', freq='h')
        """
        # TODO (SNOW-1660843): Support tz in pd.date_range and unskip the doctests.

    def normalize():
        pass

    def strftime():
        """
        Convert to Index using specified date_format.

        Return an Index of formatted strings specified by date_format, which supports the same string format as the python standard library. Details of the string format can be found in python string format doc.

        Formats supported by the C strftime API but not by the python string format doc (such as “%R”, “%r”) are not officially supported and should be preferably replaced with their supported equivalents (such as “%H:%M”, “%I:%M:%S %p”).

        Note that PeriodIndex support additional directives, detailed in Period.strftime.

        Parameters
        ----------
        date_format : str
            Date format string (e.g. “%Y-%m-%d”).

        Returns
        -------
        ndarray[object]
            NumPy ndarray of formatted strings.

        See also
        --------
        to_datetime
            Convert the given argument to datetime.
        DatetimeIndex.normalize
            Return DatetimeIndex with times to midnight.
        DatetimeIndex.round
            Round the DatetimeIndex to the specified freq.
        DatetimeIndex.floor
            Floor the DatetimeIndex to the specified freq.
        Timestamp.strftime
            Format a single Timestamp.
        Period.strftime
            Format a single Period.

        Examples
        --------
        >>> rng = pd.date_range(pd.Timestamp("2018-03-10 09:00"),
        ...                     periods=3, freq='s')
        >>> rng.strftime('%B %d, %Y, %r')  # doctest: +SKIP
        Index(['March 10, 2018, 09:00:00 AM', 'March 10, 2018, 09:00:01 AM',
               'March 10, 2018, 09:00:02 AM'],
              dtype='object')
        """

    def round():
        """
        Perform round operation on the data to the specified freq.

        Parameters
        ----------
        freq : str or Offset
            The frequency level to round the index to. Must be a fixed frequency like ‘S’ (second) not ‘ME’ (month end). See frequency aliases for a list of possible freq values.
        ambiguous : ‘infer’, bool-ndarray, ‘NaT’, default ‘raise’
            Only relevant for DatetimeIndex:
            - ‘infer’ will attempt to infer fall dst-transition hours based on order
            - bool-ndarray where True signifies a DST time, False designates a non-DST time (note that this flag is only applicable for ambiguous times)
            - ‘NaT’ will return NaT where there are ambiguous times
            - ‘raise’ will raise an AmbiguousTimeError if there are ambiguous times.
        nonexistent : ‘shift_forward’, ‘shift_backward’, ‘NaT’, timedelta, default ‘raise’
            A nonexistent time does not exist in a particular timezone where clocks moved forward due to DST.
            - ‘shift_forward’ will shift the nonexistent time forward to the closest existing time
            - ‘shift_backward’ will shift the nonexistent time backward to the closest existing time
            - ‘NaT’ will return NaT where there are nonexistent times
            - timedelta objects will shift nonexistent times by the timedelta
            - ‘raise’ will raise an NonExistentTimeError if there are nonexistent times.

        Returns
        -------
        DatetimeIndex, TimedeltaIndex, or Series
            Index of the same type for a DatetimeIndex or TimedeltaIndex, or a Series with the same index for a Series.

        Raises
        ------
        ValueError if the freq cannot be converted.

        Notes
        -----
        If the timestamps have a timezone, rounding will take place relative to the local (“wall”) time and re-localized to the same timezone. When rounding near daylight savings time, use nonexistent and ambiguous to control the re-localization behavior.

        Examples
        ----------
        DatetimeIndex

        >>> rng = pd.date_range('1/1/2018 11:59:00', periods=3, freq='min')
        >>> rng
        DatetimeIndex(['2018-01-01 11:59:00', '2018-01-01 12:00:00',
                       '2018-01-01 12:01:00'],
                      dtype='datetime64[ns]', freq=None)
        >>> rng.round('h')
        DatetimeIndex(['2018-01-01 12:00:00', '2018-01-01 12:00:00',
                       '2018-01-01 12:00:00'],
                      dtype='datetime64[ns]', freq=None)

        Series

        >>> pd.Series(rng).dt.round("h")
        0   2018-01-01 12:00:00
        1   2018-01-01 12:00:00
        2   2018-01-01 12:00:00
        dtype: datetime64[ns]

        When rounding near a daylight savings time transition, use ambiguous or nonexistent to control how the timestamp should be re-localized.

        >>> rng_tz = pd.DatetimeIndex(["2021-10-31 03:30:00"], tz="Europe/Amsterdam")

        >>> rng_tz.floor("2h", ambiguous=False)  # doctest: +SKIP
        DatetimeIndex(['2021-10-31 02:00:00+01:00'],
                      dtype='datetime64[ns, Europe/Amsterdam]', freq=None)

        >>> rng_tz.floor("2h", ambiguous=True)  # doctest: +SKIP
        DatetimeIndex(['2021-10-31 02:00:00+02:00'],
                      dtype='datetime64[ns, Europe/Amsterdam]', freq=None)
        """

    def floor():
        """
        Perform floor operation on the data to the specified freq.

        Parameters
        ----------
        freq : str or Offset
            The frequency level to floor the index to. Must be a fixed frequency like ‘S’ (second) not ‘ME’ (month end). See frequency aliases for a list of possible freq values.
        ambiguous : ‘infer’, bool-ndarray, ‘NaT’, default ‘raise’
            Only relevant for DatetimeIndex:
            - ‘infer’ will attempt to infer fall dst-transition hours based on order
            - bool-ndarray where True signifies a DST time, False designates a non-DST time (note that this flag is only applicable for ambiguous times)
            - ‘NaT’ will return NaT where there are ambiguous times
            - ‘raise’ will raise an AmbiguousTimeError if there are ambiguous times.
        nonexistent : ‘shift_forward’, ‘shift_backward’, ‘NaT’, timedelta, default ‘raise’
            A nonexistent time does not exist in a particular timezone where clocks moved forward due to DST.
            - ‘shift_forward’ will shift the nonexistent time forward to the closest existing time
            - ‘shift_backward’ will shift the nonexistent time backward to the closest existing time
            - ‘NaT’ will return NaT where there are nonexistent times
            - timedelta objects will shift nonexistent times by the timedelta
            - ‘raise’ will raise an NonExistentTimeError if there are nonexistent times.

        Returns
        -------
        DatetimeIndex, TimedeltaIndex, or Series
            Index of the same type for a DatetimeIndex or TimedeltaIndex, or a Series with the same index for a Series.

        Raises
        ------
        ValueError if the freq cannot be converted.

        Notes
        -----
        If the timestamps have a timezone, flooring will take place relative to the local (“wall”) time and re-localized to the same timezone. When flooring near daylight savings time, use nonexistent and ambiguous to control the re-localization behavior.

        Examples
        --------
        DatetimeIndex

        >>> rng = pd.date_range('1/1/2018 11:59:00', periods=3, freq='min')
        >>> rng
        DatetimeIndex(['2018-01-01 11:59:00', '2018-01-01 12:00:00',
                       '2018-01-01 12:01:00'],
                      dtype='datetime64[ns]', freq=None)
        >>> rng.floor('h')
        DatetimeIndex(['2018-01-01 11:00:00', '2018-01-01 12:00:00',
                       '2018-01-01 12:00:00'],
                      dtype='datetime64[ns]', freq=None)

        Series

        >>> pd.Series(rng).dt.floor("h")
        0   2018-01-01 11:00:00
        1   2018-01-01 12:00:00
        2   2018-01-01 12:00:00
        dtype: datetime64[ns]

        When rounding near a daylight savings time transition, use ambiguous or nonexistent to control how the timestamp should be re-localized.

        >>> rng_tz = pd.DatetimeIndex(["2021-10-31 03:30:00"], tz="Europe/Amsterdam")

        >>> rng_tz.floor("2h", ambiguous=False)  # doctest: +SKIP
        DatetimeIndex(['2021-10-31 02:00:00+01:00'],
                    dtype='datetime64[ns, Europe/Amsterdam]', freq=None)

        >>> rng_tz.floor("2h", ambiguous=True)  # doctest: +SKIP
        DatetimeIndex(['2021-10-31 02:00:00+02:00'],
                    dtype='datetime64[ns, Europe/Amsterdam]', freq=None)
        """

    def ceil():
        """
        Perform ceil operation on the data to the specified freq.

        Parameters
        ----------
        freq : str or Offset
            The frequency level to ceil the index to. Must be a fixed frequency like ‘S’ (second) not ‘ME’ (month end). See frequency aliases for a list of possible freq values.
        ambiguous : ‘infer’, bool-ndarray, ‘NaT’, default ‘raise’
            Only relevant for DatetimeIndex:
            - ‘infer’ will attempt to infer fall dst-transition hours based on order
            - bool-ndarray where True signifies a DST time, False designates a non-DST time (note that this flag is only applicable for ambiguous times)
            - ‘NaT’ will return NaT where there are ambiguous times
            - ‘raise’ will raise an AmbiguousTimeError if there are ambiguous times.
        nonexistent : ‘shift_forward’, ‘shift_backward’, ‘NaT’, timedelta, default ‘raise’
            A nonexistent time does not exist in a particular timezone where clocks moved forward due to DST.
            - ‘shift_forward’ will shift the nonexistent time forward to the closest existing time
            - ‘shift_backward’ will shift the nonexistent time backward to the closest existing time
            - ‘NaT’ will return NaT where there are nonexistent times
            - timedelta objects will shift nonexistent times by the timedelta
            - ‘raise’ will raise an NonExistentTimeError if there are nonexistent times.

        Returns
        -------
        DatetimeIndex, TimedeltaIndex, or Series
            Index of the same type for a DatetimeIndex or TimedeltaIndex, or a Series with the same index for a Series.

        Raises
        ------
        ValueError if the freq cannot be converted.

        Notes
        -----
        If the timestamps have a timezone, ceiling will take place relative to the local (“wall”) time and re-localized to the same timezone. When ceiling near daylight savings time, use nonexistent and ambiguous to control the re-localization behavior.

        Examples
        --------
        DatetimeIndex

        >>> rng = pd.date_range('1/1/2018 11:59:00', periods=3, freq='min')
        >>> rng
        DatetimeIndex(['2018-01-01 11:59:00', '2018-01-01 12:00:00',
                       '2018-01-01 12:01:00'],
                      dtype='datetime64[ns]', freq=None)
        >>> rng.ceil('h')
        DatetimeIndex(['2018-01-01 12:00:00', '2018-01-01 12:00:00',
                       '2018-01-01 13:00:00'],
                      dtype='datetime64[ns]', freq=None)

        Series

        >>> pd.Series(rng).dt.ceil("h")
        0   2018-01-01 12:00:00
        1   2018-01-01 12:00:00
        2   2018-01-01 13:00:00
        dtype: datetime64[ns]

        When rounding near a daylight savings time transition, use ambiguous or nonexistent to control how the timestamp should be re-localized.

        >>> rng_tz = pd.DatetimeIndex(["2021-10-31 01:30:00"], tz="Europe/Amsterdam")

        >>> rng_tz.ceil("h", ambiguous=False)  # doctest: +SKIP
        DatetimeIndex(['2021-10-31 02:00:00+01:00'],
                    dtype='datetime64[ns, Europe/Amsterdam]', freq=None)

        >>> rng_tz.ceil("h", ambiguous=True)  # doctest: +SKIP
        DatetimeIndex(['2021-10-31 02:00:00+02:00'],
                    dtype='datetime64[ns, Europe/Amsterdam]', freq=None)
        """

    def month_name():
        """
        Return the month names with specified locale.

        Parameters
        ----------
        locale : str, optional
            Locale determining the language in which to return the month name. Default is English locale ('en_US.utf8'). Use the command locale -a on your terminal on Unix systems to find your locale language code.

        Returns
        -------
        Series or Index
            Series or Index of month names.

        Examples
        --------
        >>> s = pd.Series(pd.date_range(start='2018-01', freq='ME', periods=3))
        >>> s
        0   2018-01-31
        1   2018-02-28
        2   2018-03-31
        dtype: datetime64[ns]
        >>> s.dt.month_name()
        0     January
        1    February
        2       March
        dtype: object

        >>> idx = pd.date_range(start='2018-01', freq='ME', periods=3)
        >>> idx
        DatetimeIndex(['2018-01-31', '2018-02-28', '2018-03-31'], dtype='datetime64[ns]', freq=None)
        >>> idx.month_name()  # doctest: +SKIP
        Index(['January', 'February', 'March'], dtype='object')

        Using the locale parameter you can set a different locale language, for example: idx.month_name(locale='pt_BR.utf8') will return month names in Brazilian Portuguese language.

        >>> idx = pd.date_range(start='2018-01', freq='ME', periods=3)
        >>> idx
        DatetimeIndex(['2018-01-31', '2018-02-28', '2018-03-31'], dtype='datetime64[ns]', freq=None)
        >>> idx.month_name(locale='pt_BR.utf8')  # doctest: +SKIP
        Index(['Janeiro', 'Fevereiro', 'Março'], dtype='object')
        """

    def day_name():
        """
        Return the day names with specified locale.

        Parameters
        ----------
        locale : str, optional
            Locale determining the language in which to return the day name. Default is English locale ('en_US.utf8'). Use the command locale -a on your terminal on Unix systems to find your locale language code.

        Returns
        -------
        Series or Index
            Series or Index of day names.

        Examples
        --------
        >>> s = pd.Series(pd.date_range(start='2018-01-01', freq='D', periods=3))
        >>> s
        0   2018-01-01
        1   2018-01-02
        2   2018-01-03
        dtype: datetime64[ns]
        >>> s.dt.day_name()
        0       Monday
        1      Tuesday
        2    Wednesday
        dtype: object

        >>> idx = pd.date_range(start='2018-01-01', freq='D', periods=3)
        >>> idx
        DatetimeIndex(['2018-01-01', '2018-01-02', '2018-01-03'], dtype='datetime64[ns]', freq=None)
        >>> idx.day_name()  # doctest: +SKIP
        Index(['Monday', 'Tuesday', 'Wednesday'], dtype='object')

        Using the locale parameter you can set a different locale language, for example: idx.day_name(locale='pt_BR.utf8') will return day names in Brazilian Portuguese language.

        >>> idx = pd.date_range(start='2018-01-01', freq='D', periods=3)
        >>> idx
        DatetimeIndex(['2018-01-01', '2018-01-02', '2018-01-03'], dtype='datetime64[ns]', freq=None)
        >>> idx.day_name(locale='pt_BR.utf8')  # doctest: +SKIP
        Index(['Segunda', 'Terça', 'Quarta'], dtype='object')
        """

    def total_seconds():
        """
        Return total duration of each element expressed in seconds.

        This method is available directly on TimedeltaArray, TimedeltaIndex
        and on Series containing timedelta values under the ``.dt`` namespace.

        Returns
        -------
        ndarray, Index or Series
            When the calling object is a TimedeltaArray, the return type
            is ndarray.  When the calling object is a TimedeltaIndex,
            the return type is an Index with a float64 dtype. When the calling object
            is a Series, the return type is Series of type `float64` whose
            index is the same as the original.

        See Also
        --------
        datetime.timedelta.total_seconds : Standard library version
            of this method.
        TimedeltaIndex.components : Return a DataFrame with components of
            each Timedelta.

        Examples
        --------
        **Series**

        >>> s = pd.Series(pd.to_timedelta(np.arange(5), unit='d'))
        >>> s
        0   0 days
        1   1 days
        2   2 days
        3   3 days
        4   4 days
        dtype: timedelta64[ns]

        >>> s.dt.total_seconds()
        0         0.0
        1     86400.0
        2    172800.0
        3    259200.0
        4    345600.0
        dtype: float64

        **TimedeltaIndex**

        >>> idx = pd.to_timedelta(np.arange(5), unit='d')
        >>> idx
        TimedeltaIndex(['0 days', '1 days', '2 days', '3 days', '4 days'], dtype='timedelta64[ns]', freq=None)

        >>> idx.total_seconds()
        Index([0.0, 86400.0, 172800.0, 259200.0, 345600.0], dtype='float64')
        """

    def to_pytimedelta():
        pass

    @property
    def seconds():
        """
        Number of seconds (>= 0 and less than 1 day) for each element.

        Examples
        --------
        For Series:

        >>> ser = pd.Series(pd.to_timedelta([1, 2, 3], unit='s'))
        >>> ser
        0   0 days 00:00:01
        1   0 days 00:00:02
        2   0 days 00:00:03
        dtype: timedelta64[ns]
        >>> ser.dt.seconds
        0    1
        1    2
        2    3
        dtype: int64

        For TimedeltaIndex:

        >>> tdelta_idx = pd.to_timedelta([1, 2, 3], unit='s')
        >>> tdelta_idx
        TimedeltaIndex(['0 days 00:00:01', '0 days 00:00:02', '0 days 00:00:03'], dtype='timedelta64[ns]', freq=None)
        >>> tdelta_idx.seconds
        Index([1, 2, 3], dtype='int64')
        """

    @property
    def days():
        """
        Number of days for each element.

        Examples
        --------
        For Series:

        >>> ser = pd.Series(pd.to_timedelta([1, 2, 3], unit='d'))
        >>> ser
        0   1 days
        1   2 days
        2   3 days
        dtype: timedelta64[ns]
        >>> ser.dt.days
        0    1
        1    2
        2    3
        dtype: int64

        For TimedeltaIndex:

        >>> tdelta_idx = pd.to_timedelta(["0 days", "10 days", "20 days"])
        >>> tdelta_idx
        TimedeltaIndex(['0 days', '10 days', '20 days'], dtype='timedelta64[ns]', freq=None)
        >>> tdelta_idx.days
        Index([0, 10, 20], dtype='int64')
        """

    @property
    def microseconds():
        """
        Number of microseconds (>= 0 and less than 1 second) for each element.

        Examples
        --------
        For Series:

        >>> ser = pd.Series(pd.to_timedelta([1, 2, 3], unit='us'))
        >>> ser
        0   0 days 00:00:00.000001
        1   0 days 00:00:00.000002
        2   0 days 00:00:00.000003
        dtype: timedelta64[ns]
        >>> ser.dt.microseconds
        0    1
        1    2
        2    3
        dtype: int64

        For TimedeltaIndex:

        >>> tdelta_idx = pd.to_timedelta([1, 2, 3], unit='us')
        >>> tdelta_idx
        TimedeltaIndex(['0 days 00:00:00.000001', '0 days 00:00:00.000002',
                        '0 days 00:00:00.000003'],
                       dtype='timedelta64[ns]', freq=None)
        >>> tdelta_idx.microseconds
        Index([1, 2, 3], dtype='int64')
        """

    @property
    def nanoseconds():
        """
        Number of nanoseconds (>= 0 and less than 1 microsecond) for each element.

        Examples
        --------
        For Series:

        >>> ser = pd.Series(pd.to_timedelta([1, 2, 3], unit='ns'))
        >>> ser
        0   0 days 00:00:00.000000001
        1   0 days 00:00:00.000000002
        2   0 days 00:00:00.000000003
        dtype: timedelta64[ns]
        >>> ser.dt.nanoseconds
        0    1
        1    2
        2    3
        dtype: int64

        For TimedeltaIndex:

        >>> tdelta_idx = pd.to_timedelta([1, 2, 3], unit='ns')
        >>> tdelta_idx
        TimedeltaIndex(['0 days 00:00:00.000000001', '0 days 00:00:00.000000002',
                        '0 days 00:00:00.000000003'],
                       dtype='timedelta64[ns]', freq=None)
        >>> tdelta_idx.nanoseconds
        Index([1, 2, 3], dtype='int64')
        """

    @property
    def components():
        pass

    @property
    def qyear():
        pass

    @property
    def start_time():
        pass

    @property
    def end_time():
        pass

    def to_timestamp():
        pass
