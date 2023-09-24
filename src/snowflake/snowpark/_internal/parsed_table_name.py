#!/usr/bin/env python3
#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#

from typing import Iterable, List, Union


def _parse_table_name(table_name: str) -> List[str]:
    """
    This function implements the algorithm to parse a table name.

    We parse the table name according to the following rules:
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
    from snowflake.snowpark._internal.utils import validate_object_name

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


class ParsedTableName:
    def __init__(self, table_name: Union[str, Iterable[str]]) -> None:
        if isinstance(table_name, str):
            self.parts = _parse_table_name(table_name)
            assert str(self) == table_name
        elif isinstance(table_name, Iterable) and all(
            isinstance(x, str) for x in table_name
        ):
            self.parts = list(table_name)
            assert self.parts == ParsedTableName(str(self)).parts
        else:
            raise TypeError(
                f"table_name should be a str or an Iterable[str], got {type(table_name)}"
            )

    def __str__(self):
        return ".".join(self.parts)

    def __eq__(self, other):
        if isinstance(other, ParsedTableName):
            return self.parts == other.parts
        elif isinstance(other, Iterable):
            return self.parts == list(other)
        else:
            return NotImplemented
