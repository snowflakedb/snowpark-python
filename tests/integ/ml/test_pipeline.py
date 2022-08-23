#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#

import pytest

from snowflake.snowpark import Row
from snowflake.snowpark.functions import col
from snowflake.snowpark.ml import MinMaxScaler, OneHotEncoder, Pipeline, StandardScaler
from tests.utils import Utils


def test_pipeline_common(session):
    df = session.create_dataframe(
        [[1, 2, 3], [2, 3, 4], [4, 5, 6]], schema=["a", "b", "c"]
    )
    standarscaler = StandardScaler()
    standarscaler.input_cols = ["A"]
    standarscaler.output_cols = ["A_standard"]

    minmaxscaler = MinMaxScaler()
    minmaxscaler.input_cols = ["A_standard"]
    minmaxscaler.output_cols = ["A_minmax"]

    transformers = [standarscaler, minmaxscaler]
    pipeline = Pipeline(transformers)

    model = pipeline.fit(df)
    df = model.transform(df)
    Utils.check_answer(
        df,
        [
            Row(A=1, B=2, C=3, A_STANDARD=-1.069044547667817, A_MINMAX=0.0),
            Row(
                A=2,
                B=3,
                C=4,
                A_STANDARD=-0.26726093647105154,
                A_MINMAX=0.3333333333333333,
            ),
            Row(A=4, B=5, C=6, A_STANDARD=1.3363062859224795, A_MINMAX=1.0),
        ],
    )


def test_pipeline_negative(session):
    with pytest.raises(ValueError) as ex_info:
        Pipeline([])
    assert "Transformers can not be None" in str(ex_info.value)


def test_pipeline_async(session):
    df = session.create_dataframe(
        [[1, 2, 3], [2, 3, 4], [4, 5, 6]], schema=["a", "b", "c"]
    )
    standarscaler = StandardScaler()
    standarscaler.input_cols = ["A"]
    standarscaler.output_cols = ["A_standard"]

    minmaxscaler = MinMaxScaler()
    minmaxscaler.input_cols = ["A"]
    minmaxscaler.output_cols = ["A_minmax"]

    onehot = OneHotEncoder()
    onehot.input_cols = ["A"]
    onehot.output_cols = ["A_onehot"]

    transformers = [standarscaler, minmaxscaler, onehot]
    pipeline = Pipeline(transformers, block=False)

    model = pipeline.fit(df)
    df = model.transform(df)
    df = df.select(
        [
            col("A"),
            col("B"),
            col("C"),
            col("A_STANDARD"),
            col("A_MINMAX"),
            col("A_ONEHOT"),
        ]
    )
    Utils.check_answer(
        df,
        [
            Row(
                A=1,
                B=2,
                C=3,
                A_STANDARD=-1.069044547667817,
                A_MINMAX=0.0,
                A_ONEHOT="[\n  3,\n  [\n    0\n  ],\n  1\n]",
            ),
            Row(
                A=2,
                B=3,
                C=4,
                A_STANDARD=-0.26726093647105154,
                A_MINMAX=0.3333333333333333,
                A_ONEHOT="[\n  3,\n  [\n    1\n  ],\n  1\n]",
            ),
            Row(
                A=4,
                B=5,
                C=6,
                A_STANDARD=1.3363062859224795,
                A_MINMAX=1.0,
                A_ONEHOT="[\n  3,\n  [\n    2\n  ],\n  1\n]",
            ),
        ],
    )


def test_pipline_recursive(session):
    df = session.create_dataframe(
        [[1, 2, 3], [2, 3, 4], [4, 5, 6]], schema=["a", "b", "c"]
    )
    standardscaler = StandardScaler()
    standardscaler.input_cols = ["a"]
    standardscaler.output_cols = ["a_standard"]

    standardscaler_2 = StandardScaler()
    standardscaler_2.input_cols = ["a_standard"]
    standardscaler_2.output_cols = ["a_stand"]

    minmaxscaler = MinMaxScaler()
    minmaxscaler.input_cols = ["a_standard"]
    minmaxscaler.output_cols = ["a_minmax"]

    onehot = OneHotEncoder()
    onehot.input_cols = ["a_minmax"]
    onehot.output_cols = ["a_onehot"]

    transformer_group_1 = [standardscaler_2, minmaxscaler]
    pipeline = Pipeline(transformer_group_1, block=False)

    transformer_group_2 = [standardscaler, pipeline, onehot]
    pipeline_2 = Pipeline(transformer_group_2)

    model = pipeline_2.fit(df)
    df = model.transform(df)
    Utils.check_answer(
        df,
        [
            Row(
                A=1,
                B=2,
                C=3,
                A_STANDARD=-1.069044547667817,
                A_STAND=-1.0690449676496976,
                A_MINMAX=0.0,
                A_ONEHOT="[\n  3,\n  [\n    0\n  ],\n  1\n]",
            ),
            Row(
                A=2,
                B=3,
                C=4,
                A_STANDARD=-0.26726093647105154,
                A_STAND=-0.26726124191242445,
                A_MINMAX=0.3333333333333333,
                A_ONEHOT="[\n  3,\n  [\n    1\n  ],\n  1\n]",
            ),
            Row(
                A=4,
                B=5,
                C=6,
                A_STANDARD=1.3363062859224795,
                A_STAND=1.336306209562122,
                A_MINMAX=1.0,
                A_ONEHOT="[\n  3,\n  [\n    2\n  ],\n  1\n]",
            ),
        ],
    )
