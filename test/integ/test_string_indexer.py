#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# Copyright (c) 2012-2021 Snowflake Computing Inc. All rights reserved.
#
import logging

from snowflake.snowpark import Row
from snowflake.snowpark.ml.transformers.string_indexer import StringIndexer

logger = logging.getLogger(__name__)


def test_transform(session):
    df = session.createDataFrame([[8815942446845118937], [-3774021408702477068]]).toDF(
        ["value"]
    )
    indexer = StringIndexer()
    output_df = df.select(indexer.transform(df.col("value"))).collect()
    # expected_df = session.createDataFrame([Row(17555), Row(41209)]).toDF("output")
    expected_df = (
        session.createDataFrame([Row(-1), Row(41209)]).toDF(["expected"]).collect()
    )

    logger.info(f"INPUT: {df}")
    logger.info(f"OUTPUT: {output_df}")
    logger.info(f"EXPECTED: {expected_df}")

    assert output_df == expected_df

    # Utils.check_answer(
    #     output_df, [Row(-1), Row(41209)]
    # )
