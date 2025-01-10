#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import modin.pandas as pd
import pandas as native_pd
import pytest

import snowflake.snowpark.modin.plugin  # noqa: F401
from tests.integ.modin.utils import (
    assert_snowpark_pandas_equal_to_pandas,
    create_test_series,
    eval_snowpark_pandas_result,
)
from tests.integ.utils.sql_counter import sql_count_checker


@pytest.mark.parametrize(
    "data, table",
    [
        (
            # Simple 1-element mapping
            ["aaaaa", "bbbaaa", "cafdsaf;lh"],
            str.maketrans("a", "b"),
        ),
        (
            # Mapping with mixed str, unicode code points, and Nones
            ["aaaaa", "fjkdsajk", "cjghgjqk", "yubikey"],
            str.maketrans(
                {ord("a"): "A", ord("f"): None, "y": "z", "k": None, ord("j"): ""}
            ),
        ),
        (
            # Mapping with special characters
            [
                "Peña",
                "Ordoñez",
                "Raúl",
                "Ibañez",
                "François",
                "øen",
                "2πr = τ",
                "München",
            ],
            str.maketrans(
                {
                    "ñ": "n",
                    "ú": "u",
                    "ç": "c",
                    "ø": "o",
                    "τ": "t",
                    "π": "p",
                    "ü": "u",
                }
            ),
        ),
        (
            # Mapping with compound emojis. Each item in the series renders as a single emoji,
            # but is actually 4 characters. Calling `len` on each element correctly returns 4.
            # https://unicode.org/emoji/charts/emoji-zwj-sequences.html
            # Inputs:
            # - "head shaking horizontally" = 1F642 + 200D + 2194 + FE0F
            # - "heart on fire" = 2764 + FE0F + 200D + 1F525
            # - "judge" = 1F9D1 + 200D + 2696 + FE0F
            # Outputs:
            # - "head shaking vertically" = 1F642 + 200D + 2195 + FE0F
            # - "mending heart" = 2764 + FE0F + 200D + 1FA79
            # - "health worker" = 1F91D1 + 200D + 2695 + FE0F
            ["🙂‍↔️", "❤️‍🔥", "🧑‍⚖️"],
            {
                0x2194: 0x2195,
                0x1F525: 0x1FA79,
                0x2696: 0x2695,
            },
        ),
    ],
)
@sql_count_checker(query_count=1)
def test_translate(data, table):
    eval_snowpark_pandas_result(
        *create_test_series(data), lambda ser: ser.str.translate(table)
    )


@sql_count_checker(query_count=1)
def test_translate_without_maketrans():
    # pandas requires all table keys to be unicode ordinal values, and does not know how to handle
    # string keys that were not converted to ordinals via `ord` or `str.maketrans`. Since Snowflake
    # SQL uses strings in its mappings, we accept string keys as well as ordinals.
    data = ["aaaaa", "fjkdsajk", "cjghgjqk", "yubikey"]
    table = {ord("a"): "A", ord("f"): None, "y": "z", "k": None}
    snow_ser = pd.Series(data)
    assert_snowpark_pandas_equal_to_pandas(
        snow_ser.str.translate(table),
        native_pd.Series(data).str.translate(str.maketrans(table)),
    )
    # Mappings for "y" and "k" are ignored if not passed through str.maketrans because they are
    # not unicode ordinals
    assert (
        not native_pd.Series(data)
        .str.translate(table)
        .equals(native_pd.Series(data).str.translate(str.maketrans(table)))
    )


@pytest.mark.parametrize(
    "table, error",
    [
        ({"😶‍🌫️": "a"}, ValueError),  # This emoji key is secretly 4 code points
        ({"aa": "a"}, ValueError),  # Key is 2 chars
        # Mapping 1 char to multiple is valid in vanilla pandas, but we don't support this
        (
            {ord("a"): "😶‍🌫️"},
            NotImplementedError,
        ),  # This emoji value is secretly 4 code points
        ({ord("a"): "aa"}, NotImplementedError),  # Value is 2 chars
    ],
)
@sql_count_checker(query_count=0)
def test_translate_invalid_mappings(table, error):
    data = ["aaaaa", "fjkdsajk", "cjghgjqk", "yubikey"]
    # native pandas silently treats all of these cases as no-ops. However, since Snowflake SQL uses
    # strings as mappings instead of a dict construct, passing these arguments to the equivalent
    # SQL argument would either cause an inscrutable error or unexpected changes to the output series.
    snow_ser, native_ser = create_test_series(data)
    native_ser.str.translate(table)
    with pytest.raises(error):
        snow_ser.str.translate(table)
