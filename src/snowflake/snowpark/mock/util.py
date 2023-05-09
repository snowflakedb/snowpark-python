#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

import math
from functools import cmp_to_key, partial
from typing import Any

import pandas as pd

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
