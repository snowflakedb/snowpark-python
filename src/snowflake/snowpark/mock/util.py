#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import math
from functools import cmp_to_key, partial
from typing import Any, List

import pandas as pd

from snowflake.snowpark._internal.utils import validate_object_name

# placeholder map helps convert wildcard to reg. In practice, we convert wildcard to a middle string first,
# and then convert middle string to regex. See the following example:
#   wildcard = "_." -> middle: "_<snowflake-regex-placeholder-for-dot>" -> regex = ".\."
# placeholder string should not contain any special characters used in regex or wildcard
regex_special_characters_map = {
    ".": "<snowflake-regex-placeholder-for-dot>",
    "\\": "<snowflake-regex-placeholder-for-backslash>",
    "^": "<snowflake-regex-placeholder-for-caret>",
    "?": "<snowflake-regex-placeholder-for-question>",
    "+": "<snowflake-regex-placeholder-for-add>",
    "|": "<snowflake-regex-placeholder-for-pipe>",
    "$": "<snowflake-regex-placeholder-for-dollar>",
    "*": "<snowflake-regex-placeholder-for-asterisk>",
    "{": "<snowflake-regex-placeholder-for-left-curly-bracket>",
    "}": "<snowflake-regex-placeholder-for-right-curly-bracket>",
    "[": "<snowflake-regex-placeholder-for-left-square-bracket>",
    "]": "<snowflake-regex-placeholder-for-right-square-bracket>",
    "(": "<snowflake-regex-placeholder-for-left-parenthesis>",
    ")": "<snowflake-regex-placeholder-for-right-parenthesis>",
}

escape_regex_special_characters_map = {
    regex_special_characters_map["."]: "\\.",
    regex_special_characters_map["\\"]: "\\\\",
    regex_special_characters_map["^"]: "\\^",
    regex_special_characters_map["?"]: "\\?",
    regex_special_characters_map["+"]: "\\+",
    regex_special_characters_map["|"]: "\\|",
    regex_special_characters_map["$"]: "\\$",
    regex_special_characters_map["*"]: "\\*",
    regex_special_characters_map["{"]: "\\{",
    regex_special_characters_map["}"]: "\\}",
    regex_special_characters_map["["]: "\\[",
    regex_special_characters_map["]"]: "\\]",
    regex_special_characters_map["("]: "\\(",
    regex_special_characters_map[")"]: "\\)",
}


def convert_wildcard_to_regex(wildcard: str):
    # convert regex in wildcard
    for k, v in regex_special_characters_map.items():
        wildcard = wildcard.replace(k, v)

    # replace wildcard special character with regex
    wildcard = wildcard.replace("_", ".")
    wildcard = wildcard.replace("%", ".*")

    # escape regx in wildcard
    for k, v in escape_regex_special_characters_map.items():
        wildcard = wildcard.replace(k, v)

    wildcard = f"^{wildcard}$"
    return wildcard


def custom_comparator(ascend: bool, null_first: bool, pandas_series: pd.Series):
    origin_array = pandas_series.values.tolist()
    array_with_pos = list(zip([i for i in range(len(pandas_series))], origin_array))
    comparator = partial(array_custom_comparator, ascend, null_first)
    array_with_pos.sort(key=cmp_to_key(comparator))
    new_pos = [0] * len(array_with_pos)
    for i in range(len(array_with_pos)):
        new_pos[array_with_pos[i][0]] = i
    return new_pos


def array_custom_comparator(ascend: bool, null_first: bool, a: Any, b: Any):
    value_a, value_b = a[1], b[1]
    if value_a == value_b:
        return 0
    if value_a is None:
        return -1 if null_first else 1
    elif value_b is None:
        return 1 if null_first else -1
    try:
        if math.isnan(value_a) and math.isnan(value_b):
            return 0
        elif math.isnan(value_a):
            ret = 1
        elif math.isnan(value_b):
            ret = -1
        else:
            ret = -1 if value_a < value_b else 1
    except TypeError:
        ret = -1 if value_a < value_b else 1
    return ret if ascend else -1 * ret


# TODO: rebase and get rid of this
def parse_table_name(table_name: str) -> List[str]:
    """
    This function implements the algorithm to parse a table name.

    We parse the table name according to the following rule:
    https://docs.snowflake.com/en/sql-reference/identifiers-syntax

    - Unquoted object identifiers:
        - Start with a letter (A-Z, a-z) or an underscore (“_”).
        - Contain only letters, underscores, decimal digits (0-9), and dollar signs (“$”).
        - Are stored and resolved as uppercase characters (e.g. id is stored and resolved as ID).

    - If you put double quotes around an identifier (e.g. “My identifier with blanks and punctuation.”),
        the following rules apply:
        - The case of the identifier is preserved when storing and resolving the identifier (e.g. "id" is
            stored and resolved as id).
        - The identifier can contain and start with ASCII, extended ASCII, and non-ASCII characters.
    """
    validate_object_name(table_name)
    str_len = len(table_name)
    ret = []

    in_double_quotes = False
    i = 0
    cur_word_start_idx = 0

    while i < str_len:
        cur_char = table_name[i]
        if cur_char == '"':
            if in_double_quotes:
                # we have to check whether this `"` is the ending of a double-quoted identifier
                # or it's an escaping double quote
                # to achieve this, we need to preload one more char
                if i < str_len - 1 and table_name[i + 1] == '"':
                    # two consecutive '"', this is an escaping double quotes
                    # the pointer just keeps moving forward
                    i += 1
                else:
                    # the double quotes indicates the ending of an identifier
                    in_double_quotes = False
                    # it should be followed by a '.' for splitting, or it should reach the end of the str
            else:
                # this is the beginning of another double-quoted identifier
                in_double_quotes = True
        elif cur_char == ".":
            if not in_double_quotes:
                # this dot is to split db.schema.database
                # we concatenate the processed chars into a string
                # and append the string to the return list, and set our cur_word_start_idx to position after the dot
                ret.append(table_name[cur_word_start_idx:i])
                cur_word_start_idx = i + 1
            # else dot is part of the table name
        # else cur_char is part of the name
        i += 1

    ret.append(table_name[cur_word_start_idx:i])
    return ret
