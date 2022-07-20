#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#


import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.ml.transformer import (
    Binarizer,
    KBinsDiscretizer,
    MinMaxScaler,
    Normalizer,
    OneHotEncoder,
    OrdinalEncoder,
    StandardScaler,
)
from tests.utils import Utils


def test_standard_scaler(session):
    df = session.create_dataframe(
        [[1, 2, 3], [2, 3, 4], [4, 5, 6]], schema=["a", "b", "c"]
    )
    standarscaler = StandardScaler()
    standarscaler.input_cols = ["A"]
    standarscaler.output_cols = ["res"]
    model = standarscaler.fit(df)

    # test states table
    df_table = session.table(model._states_table_name)
    Utils.check_answer(
        df_table,
        [
            Row(
                STATES_A='{\n  "mean": 2.333333000000000e+00,\n  "stddev_pop": 1.247219307098796e+00\n}'
            )
        ],
    )
    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(A=1, B=2, C=3, RES=-1.069044547667817),
            Row(A=2, B=3, C=4, RES=-0.26726093647105154),
            Row(A=4, B=5, C=6, RES=1.3363062859224795),
        ],
    )


def test_minmax_scaler(session):
    df = session.create_dataframe(
        [[1, 2, 3], [2, 3, 4], [4, 5, 6]], schema=["a", "b", "c"]
    )
    minmaxscaler = MinMaxScaler()
    minmaxscaler.input_cols = ["A"]
    minmaxscaler.output_cols = ["res"]
    model = minmaxscaler.fit(df)

    # test states table
    df_table = session.table(model._states_table_name)
    Utils.check_answer(df_table, [Row(STATES_A='{\n  "max": 4,\n  "min": 1\n}')])

    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(A=1, B=2, C=3, RES=0.0),
            Row(A=2, B=3, C=4, RES=0.3333333333333333),
            Row(A=4, B=5, C=6, RES=1.0),
        ],
    )


def test_merge_onehot_encoder(session):
    df = session.create_dataframe(
        [[1, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    onehotencoder = OneHotEncoder()
    onehotencoder.input_cols = ["A", "B", "C"]
    onehotencoder.output_cols = ["ONEHOTARRAY"]
    model = onehotencoder.fit(df)

    # test category table
    df_table = session.table(model._category_table_name["A"])
    Utils.check_answer(
        df_table,
        [Row(STATES_A=1, STATES_A_ENCODER=0), Row(STATES_A=2, STATES_A_ENCODER=1)],
    )
    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(
                A=1,
                B=4,
                C=5,
                ONEHOTARRAY="[\n  6,\n  [\n    0,\n    2,\n    4\n  ],\n  1\n]",
            ),
            Row(
                A=2,
                B=4,
                C=5,
                ONEHOTARRAY="[\n  6,\n  [\n    1,\n    2,\n    4\n  ],\n  1\n]",
            ),
            Row(
                A=2,
                B=3,
                C=6,
                ONEHOTARRAY="[\n  6,\n  [\n    1,\n    3,\n    5\n  ],\n  1\n]",
            ),
        ],
    )


def test_no_merge_onehot_encoder(session):
    df = session.create_dataframe(
        [[1, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    onehotencoder = OneHotEncoder()
    onehotencoder.input_cols = ["A", "B", "C"]
    onehotencoder.output_cols = ["A_ONEHOT", "B_ONEHOT", "C_ONEHOT"]
    model = onehotencoder.fit(df)

    # test category table
    df_table = session.table(model._category_table_name["A"])
    Utils.check_answer(
        df_table,
        [Row(STATES_A=1, STATES_A_ENCODER=0), Row(STATES_A=2, STATES_A_ENCODER=1)],
    )

    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(
                A=1,
                B=4,
                C=5,
                A_ONEHOT="[\n  2,\n  0,\n  1\n]",
                B_ONEHOT="[\n  2,\n  0,\n  1\n]",
                C_ONEHOT="[\n  2,\n  0,\n  1\n]",
            ),
            Row(
                A=2,
                B=4,
                C=5,
                A_ONEHOT="[\n  2,\n  1,\n  1\n]",
                B_ONEHOT="[\n  2,\n  0,\n  1\n]",
                C_ONEHOT="[\n  2,\n  0,\n  1\n]",
            ),
            Row(
                A=2,
                B=3,
                C=6,
                A_ONEHOT="[\n  2,\n  1,\n  1\n]",
                B_ONEHOT="[\n  2,\n  1,\n  1\n]",
                C_ONEHOT="[\n  2,\n  1,\n  1\n]",
            ),
        ],
    )


def test_preset_category_onehot_encoder(session):
    df = session.create_dataframe(
        [[1, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    onehotencoder = OneHotEncoder(category=[["1", "2"], ["4", "3"], ["5", "6"]])
    onehotencoder.input_cols = ["A", "B", "C"]
    onehotencoder.output_cols = ["ONEHOTARRAY"]
    model = onehotencoder.fit(df)

    # test category table
    df_table = session.table(model._category_table_name["A"])
    Utils.check_answer(
        df_table,
        [Row(STATES_A="1", STATES_A_ENCODER=0), Row(STATES_A="2", STATES_A_ENCODER=1)],
    )

    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(
                A=1,
                B=4,
                C=5,
                ONEHOTARRAY="[\n  6,\n  [\n    0,\n    2,\n    4\n  ],\n  1\n]",
            ),
            Row(
                A=2,
                B=4,
                C=5,
                ONEHOTARRAY="[\n  6,\n  [\n    1,\n    2,\n    4\n  ],\n  1\n]",
            ),
            Row(
                A=2,
                B=3,
                C=6,
                ONEHOTARRAY="[\n  6,\n  [\n    1,\n    3,\n    5\n  ],\n  1\n]",
            ),
        ],
    )


def test_sparse_onehot_encoder(session):
    df = session.create_dataframe(
        [[1, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    onehotencoder = OneHotEncoder()
    onehotencoder.input_cols = ["A", "B", "C"]
    onehotencoder.output_cols = ["A_ONEHOT", "B_ONEHOT", "C_ONEHOT"]
    model = onehotencoder.fit(df)

    # test category table
    df_table = session.table(model._category_table_name["A"])
    Utils.check_answer(
        df_table,
        [Row(STATES_A=1, STATES_A_ENCODER=0), Row(STATES_A=2, STATES_A_ENCODER=1)],
    )

    res = model.transform(df, sparse=False)
    Utils.check_answer(
        res,
        [
            Row(
                A=1,
                B=4,
                C=5,
                A_ONEHOT="[\n  1,\n  0\n]",
                B_ONEHOT="[\n  1,\n  0\n]",
                C_ONEHOT="[\n  1,\n  0\n]",
            ),
            Row(
                A=2,
                B=4,
                C=5,
                A_ONEHOT="[\n  0,\n  1\n]",
                B_ONEHOT="[\n  1,\n  0\n]",
                C_ONEHOT="[\n  1,\n  0\n]",
            ),
            Row(
                A=2,
                B=3,
                C=6,
                A_ONEHOT="[\n  0,\n  1\n]",
                B_ONEHOT="[\n  0,\n  1\n]",
                C_ONEHOT="[\n  0,\n  1\n]",
            ),
        ],
    )


def test_sparse_merge_onehot_encoder(session):
    df = session.create_dataframe(
        [[1, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    onehotencoder = OneHotEncoder()
    onehotencoder.input_cols = ["A", "B", "C"]
    onehotencoder.output_cols = ["ONEHOTARRAY"]
    model = onehotencoder.fit(df)
    # test category table
    df_table = session.table(model._category_table_name["A"])
    Utils.check_answer(
        df_table,
        [Row(STATES_A=1, STATES_A_ENCODER=0), Row(STATES_A=2, STATES_A_ENCODER=1)],
    )

    res = model.transform(df, sparse=False)
    Utils.check_answer(
        res,
        [
            Row(A=1, B=4, C=5, ONEHOTARRAY="[\n  1,\n  0,\n  1,\n  0,\n  1,\n  0\n]"),
            Row(A=2, B=4, C=5, ONEHOTARRAY="[\n  0,\n  1,\n  1,\n  0,\n  1,\n  0\n]"),
            Row(A=2, B=3, C=6, ONEHOTARRAY="[\n  0,\n  1,\n  0,\n  1,\n  0,\n  1\n]"),
        ],
    )


def test_input_cols_output_cols_negative(session):
    df = session.create_dataframe(
        [[1, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    onehotencoder = OneHotEncoder()
    # test no input column
    with pytest.raises(ValueError) as ex_info:
        onehotencoder.transform(df)
    assert "Input column can not be empty" in str(ex_info.value)

    onehotencoder.input_cols = ["A", "B", "C"]
    # test no output column
    with pytest.raises(ValueError) as ex_info:
        onehotencoder.transform(df)
    assert "Output column can not be empty" in str(ex_info.value)
    onehotencoder.output_cols = ["A", "B", "C"]
    # test call transform before fit
    with pytest.raises(ValueError) as ex_info:
        onehotencoder.transform(df)
    assert (
        "The transformer is not fitted yet, call fit() function before transform"
        in str(ex_info.value)
    )
    onehotencoder.output_cols = ["B", "C"]
    model = onehotencoder.fit(df)
    # test input column and output column dose not match
    with pytest.raises(ValueError) as ex_info:
        model.transform(df)
    assert "The number of output column and input column does not match" in str(
        ex_info.value
    )


def test_preset_category_not_match_real_category(session):
    df = session.create_dataframe(
        [[1, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    onehotencoder = OneHotEncoder(category=[["2"], ["4", "3"], ["5", "6"]])
    onehotencoder.input_cols = ["A", "B", "C"]
    onehotencoder.output_cols = ["ONEHOTARRAY"]
    model = onehotencoder.fit(df)
    # test category table
    df_table = session.table(model._category_table_name["A"])
    Utils.check_answer(
        df_table,
        [Row(STATES_A="2", STATES_A_ENCODER=0)],
    )
    # merge is true and sparse is true
    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(
                A=1,
                B=4,
                C=5,
                ONEHOTARRAY="[\n  5,\n  [\n    1,\n    3\n  ],\n  1\n]",
            ),
            Row(
                A=2,
                B=4,
                C=5,
                ONEHOTARRAY="[\n  5,\n  [\n    0,\n    1,\n    3\n  ],\n  1\n]",
            ),
            Row(
                A=2,
                B=3,
                C=6,
                ONEHOTARRAY="[\n  5,\n  [\n    0,\n    2,\n    4\n  ],\n  1\n]",
            ),
        ],
    )
    # merge is true and sparse is false
    res = model.transform(df, sparse=False)
    Utils.check_answer(
        res,
        [
            Row(A=1, B=4, C=5, ONEHOTARRAY="[\n  0,\n  1,\n  0,\n  1,\n  0\n]"),
            Row(A=2, B=4, C=5, ONEHOTARRAY="[\n  1,\n  1,\n  0,\n  1,\n  0\n]"),
            Row(A=2, B=3, C=6, ONEHOTARRAY="[\n  1,\n  0,\n  1,\n  0,\n  1\n]"),
        ],
    )

    # merge is false and sparse is true
    onehotencoder.output_cols = ["res_a", "res_b", "res_c"]
    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(
                A=1,
                B=4,
                C=5,
                RES_A="[\n  1,\n  1\n]",
                RES_B="[\n  2,\n  0,\n  1\n]",
                RES_C="[\n  2,\n  0,\n  1\n]",
            ),
            Row(
                A=2,
                B=4,
                C=5,
                RES_A="[\n  1,\n  0,\n  1\n]",
                RES_B="[\n  2,\n  0,\n  1\n]",
                RES_C="[\n  2,\n  0,\n  1\n]",
            ),
            Row(
                A=2,
                B=3,
                C=6,
                RES_A="[\n  1,\n  0,\n  1\n]",
                RES_B="[\n  2,\n  1,\n  1\n]",
                RES_C="[\n  2,\n  1,\n  1\n]",
            ),
        ],
    )
    # merge is false and sparse is false
    res = model.transform(df, sparse=False)
    Utils.check_answer(
        res,
        [
            Row(
                A=1,
                B=4,
                C=5,
                RES_A="[\n  0\n]",
                RES_B="[\n  1,\n  0\n]",
                RES_C="[\n  1,\n  0\n]",
            ),
            Row(
                A=2,
                B=4,
                C=5,
                RES_A="[\n  1\n]",
                RES_B="[\n  1,\n  0\n]",
                RES_C="[\n  1,\n  0\n]",
            ),
            Row(
                A=2,
                B=3,
                C=6,
                RES_A="[\n  1\n]",
                RES_B="[\n  0,\n  1\n]",
                RES_C="[\n  0,\n  1\n]",
            ),
        ],
    )


def test_unknown_category_given_by_transformer_onehot_enoder(session):
    df = session.create_dataframe(
        [[1, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    onehotencoder = OneHotEncoder()
    onehotencoder.input_cols = ["A", "B", "C"]
    onehotencoder.output_cols = ["ONEHOTARRAY"]
    model = onehotencoder.fit(df)
    # test category table
    df_table = session.table(model._category_table_name["A"])
    Utils.check_answer(
        df_table,
        [Row(STATES_A=1, STATES_A_ENCODER=0), Row(STATES_A=2, STATES_A_ENCODER=1)],
    )
    df_unknown_value = session.create_dataframe(
        [[6, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    res = model.transform(df_unknown_value)
    Utils.check_answer(
        res,
        [
            Row(
                A=2,
                B=4,
                C=5,
                ONEHOTARRAY="[\n  6,\n  [\n    1,\n    2,\n    4\n  ],\n  1\n]",
            ),
            Row(A=6, B=4, C=5, ONEHOTARRAY="[\n  6,\n  [\n    2,\n    4\n  ],\n  1\n]"),
            Row(
                A=2,
                B=3,
                C=6,
                ONEHOTARRAY="[\n  6,\n  [\n    1,\n    3,\n    5\n  ],\n  1\n]",
            ),
        ],
    )


def test_merge_ordinal_encoder(session):
    df = session.create_dataframe(
        [[1, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    ordinalcoder = OrdinalEncoder()
    ordinalcoder.input_cols = ["A", "B", "C"]
    ordinalcoder.output_cols = ["ORDINALARRAY"]
    model = ordinalcoder.fit(df)

    # test category table
    df_table = session.table(model._category_table_name["A"])
    Utils.check_answer(
        df_table,
        [Row(STATES_A=1, STATES_A_ENCODER=0), Row(STATES_A=2, STATES_A_ENCODER=1)],
    )

    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(A=1, B=4, C=5, ORDINALARRAY="[\n  0,\n  0,\n  0\n]"),
            Row(A=2, B=4, C=5, ORDINALARRAY="[\n  1,\n  0,\n  0\n]"),
            Row(A=2, B=3, C=6, ORDINALARRAY="[\n  1,\n  1,\n  1\n]"),
        ],
    )


def test_preset_category_ordinal_encoder(session):
    df = session.create_dataframe(
        [[1, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    df_unknown = session.create_dataframe(
        [[3, 4, 5], [2, 4, 5], [2, 3, 6]], schema=["a", "b", "c"]
    )
    ordinalencoder = OrdinalEncoder(category=[["1", "2"], ["4", "3"], ["5", "6"]])
    ordinalencoder.input_cols = ["A", "B", "C"]
    ordinalencoder.output_cols = ["ONEHOTARRAY"]
    model = ordinalencoder.fit(df)

    # test category table
    df_table = session.table(model._category_table_name["A"])
    Utils.check_answer(
        df_table,
        [Row(STATES_A="1", STATES_A_ENCODER=0), Row(STATES_A="2", STATES_A_ENCODER=1)],
    )

    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(A=1, B=4, C=5, ORDINALARRAY="[\n  0,\n  0,\n  0\n]"),
            Row(A=2, B=4, C=5, ORDINALARRAY="[\n  1,\n  0,\n  0\n]"),
            Row(A=2, B=3, C=6, ORDINALARRAY="[\n  1,\n  1,\n  1\n]"),
        ],
    )

    # test when there are unknown category in given df
    res = model.transform(df_unknown)
    Utils.check_answer(
        res,
        [
            Row(A=3, B=4, C=5, ONEHOTARRAY="[\n  undefined,\n  0,\n  0\n]"),
            Row(A=2, B=3, C=6, ONEHOTARRAY="[\n  1,\n  1,\n  1\n]"),
            Row(A=2, B=4, C=5, ONEHOTARRAY="[\n  1,\n  0,\n  0\n]"),
        ],
    )


def test_binarizer(session):
    df = session.create_dataframe(
        [[-1, 4, 5], [2, -4, 5], [-10, 3, 6]], schema=["a", "b", "c"]
    )
    df2 = session.create_dataframe(
        [[-1.0, 4.0, 5], [2.0, -4.0, 5], [-10.0, float("nan"), None]],
        schema=["a", "b", "c"],
    )
    binarizer = Binarizer()
    binarizer.input_cols = ["A", "B", "C"]
    binarizer.output_cols = ["A", "B", "C"]
    res = binarizer.transform(df)
    Utils.check_answer(
        res, [Row(A=0, B=1, C=1), Row(A=1, B=0, C=1), Row(A=0, B=1, C=1)]
    )
    res2 = binarizer.transform(df2)
    Utils.check_answer(
        res2, [Row(A=0, B=1, C=1), Row(A=1, B=0, C=1), Row(A=0, B=None, C=None)]
    )


def test_normalizer(session):
    df = session.create_dataframe(
        [[1, 4, 5], [2, 4, 5], [5, 3, 6]], schema=["a", "b", "c"]
    )
    normalizer = Normalizer()
    normalizer.input_cols = ["A", "B"]
    normalizer.output_cols = ["res_A", "res_B"]
    model = normalizer.fit(df)

    # l2 normalization
    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(A=1, B=4, C=5, RES_A=0.18257418583505536, RES_B=0.6246950475544243),
            Row(A=2, B=4, C=5, RES_A=0.3651483716701107, RES_B=0.6246950475544243),
            Row(A=5, B=3, C=6, RES_A=0.9128709291752769, RES_B=0.4685212856658182),
        ],
    )

    # null and nan value in df
    df2 = session.create_dataframe(
        [[float("nan"), None, 5], [2.0, 4, 5], [5.0, 3, 6]], schema=["a", "b", "c"]
    )
    model.fit(df2)
    res = model.transform(df2)
    Utils.check_answer(
        res,
        [
            Row(A=float("nan"), B=None, C=5, RES_A=float("nan"), RES_B=None),
            Row(A=2.0, B=4, C=5, RES_A=float("nan"), RES_B=0.8),
            Row(A=5.0, B=3, C=6, RES_A=float("nan"), RES_B=0.6),
        ],
    )
    # l3 normalization
    normalizer = Normalizer(norm="l3")
    normalizer.input_cols = ["A", "B"]
    normalizer.output_cols = ["res_A", "res_B"]
    model = normalizer.fit(df)
    res = model.transform(df)
    res.show()
    Utils.check_answer(
        res,
        [
            Row(A=1, B=4, C=5, RES_A=0.19541822634010067, RES_B=0.7446452529684849),
            Row(A=2, B=4, C=5, RES_A=0.39083645268020134, RES_B=0.7446452529684849),
            Row(A=5, B=3, C=6, RES_A=0.9770911317005033, RES_B=0.5584839397263637),
        ],
    )
    # l4 normalization
    normalizer = Normalizer(norm="l4")
    normalizer.input_cols = ["A", "B"]
    normalizer.output_cols = ["res_A", "res_B"]
    model = normalizer.fit(df)
    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(A=1, B=4, C=5, RES_A=0.19866265854003093, RES_B=0.8105808102111383),
            Row(A=2, B=4, C=5, RES_A=0.39732531708006186, RES_B=0.8105808102111383),
            Row(A=5, B=3, C=6, RES_A=0.9933132927001546, RES_B=0.6079356076583537),
        ],
    )

    normalizer = Normalizer(norm="max")
    normalizer.input_cols = ["A", "B"]
    normalizer.output_cols = ["res_A", "res_B"]
    model = normalizer.fit(df)
    # max normalization
    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(A=1, B=4, C=5, RES_A=0.200000, RES_B=1.000000),
            Row(A=2, B=4, C=5, RES_A=0.400000, RES_B=1.000000),
            Row(A=5, B=3, C=6, RES_A=1.000000, RES_B=0.750000),
        ],
    )

    # negative test
    with pytest.raises(ValueError) as ex_info:
        Normalizer(norm="l0")
    assert "l0 norm is not supported" in str(ex_info.value)

    with pytest.raises(ValueError) as ex_info:
        Normalizer(norm="xxx")
    assert "Norm must be max norm or l-norm" in str(ex_info.value)


def test_kbins_discretizer(session):
    df = session.create_dataframe(
        [[1.5, 3, 5], [2.0, 4, 7], [3.0, 5, 6], [4.0, 6, 8]], schema=["a", "b", "c"]
    )
    kbins = KBinsDiscretizer(n_bins=4, strategy="quantile")
    kbins.input_cols = ["A", "B"]
    kbins.output_cols = ["res_A", "res_B"]
    model = kbins.fit(df)

    # quantile mode
    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(A=1.5, B=3, C=5, RES_A=1, RES_B=1),
            Row(A=2.0, B=4, C=7, RES_A=2, RES_B=2),
            Row(A=3.0, B=5, C=6, RES_A=3, RES_B=3),
            Row(A=4.0, B=6, C=8, RES_A=4, RES_B=4),
        ],
    )
    kbins = KBinsDiscretizer(n_bins=4, strategy="uniform")
    kbins.input_cols = ["A", "B"]
    kbins.output_cols = ["res_A", "res_B"]
    model = kbins.fit(df)
    # uniform mode
    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(A=1.5, B=3, C=5, RES_A=1, RES_B=1),
            Row(A=2.0, B=4, C=7, RES_A=1, RES_B=2),
            Row(A=3.0, B=5, C=6, RES_A=3, RES_B=3),
            Row(A=4.0, B=6, C=8, RES_A=4, RES_B=4),
        ],
    )

    # negative and nan input
    df = session.createDataFrame(
        [[float("nan"), 3, 5], [2.0, -4, 7], [3.0, 5, 6], [4.0, 6, 8]],
        schema=["a", "b", "c"],
    )
    kbins = KBinsDiscretizer(n_bins=4, strategy="quantile")
    kbins.input_cols = ["A", "B"]
    kbins.output_cols = ["res_A", "res_B"]
    model = kbins.fit(df)
    res = model.transform(df)
    Utils.check_answer(
        res,
        [
            Row(A=float("nan"), B=3, C=5, RES_A=4, RES_B=2),
            Row(A=2.0, B=-4, C=7, RES_A=1, RES_B=1),
            Row(A=3.0, B=5, C=6, RES_A=2, RES_B=3),
            Row(A=4.0, B=6, C=8, RES_A=3, RES_B=4),
        ],
    )

    # negative tests: wrong strategy
    kbins = KBinsDiscretizer(n_bins=4, strategy="xxx")
    kbins.input_cols = ["A", "B"]
    kbins.output_cols = ["res_A", "res_B"]
    with pytest.raises(ValueError) as ex_info:
        kbins.fit(df)
    assert "Wrong strategy, strategy has to be either uniform or quantile" in str(
        ex_info.value
    )

    # negative tests: bad quantile number or dirty data
    df = session.create_dataframe(
        [[1.5, 3, 5], [3.0, 4, 7], [3.0, 5, 6], [4.0, 6, 8]], schema=["a", "b", "c"]
    )
    kbins = KBinsDiscretizer(n_bins=3, strategy="quantile")
    kbins.input_cols = ["A", "B"]
    kbins.output_cols = ["res_A", "res_B"]
    with pytest.raises(ValueError) as ex_info:
        kbins.fit(df)
    assert "Quantile partition can not be the same" in str(ex_info.value)
