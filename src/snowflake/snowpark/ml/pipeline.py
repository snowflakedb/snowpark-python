#
# Copyright (c) 2012-2022 Snowflake Computing Inc. All rights reserved.
#
from typing import List

import snowflake
import snowflake.snowpark
from snowflake.snowpark.ml.transformer import Transformer


def _is_read(monitors: List["snowflake.snowpark.AsyncJob"]):
    for monitor in monitors:
        if not monitor.is_done():
            return False
    return True


class Pipeline(Transformer):
    def __init__(
        self, transformers: List[Transformer] = None, block: bool = True
    ) -> None:
        if not transformers:
            raise ValueError("Transformers can not be None")
        super().__init__(None, None)
        self.transformers = transformers
        self.models = []
        self.async_monitors = []
        self.block = block

    def fit(self, df: "snowflake.snowpark.DataFrame", block: bool = True) -> "Pipeline":
        # apply fit() and transform() of each transformer except for the last one
        block = self.block
        if block:
            for transformer in self.transformers[:-1]:
                model = transformer.fit(df)
                self.models.append(model)
                df = model.transform(df)

            # fit the last transformer
            last_transformer = self.transformers[-1]
            last_transformer.fit(df)
            self.models.append(last_transformer)
        else:
            for transformer in self.transformers:
                model, async_monitor = transformer.fit(df, block=block)
                self.models.append(model)
                self.async_monitors.append(async_monitor)

        return self

    def transform(
        self, df: "snowflake.snowpark.DataFrame"
    ) -> "snowflake.snowpark.DataFrame":
        if self.block:
            for model in self.models:
                df = model.transform(df)
        else:
            ready_async_monitors = 0
            is_run = [True for x in self.models]
            while ready_async_monitors < len(self.async_monitors):
                for i, (model, async_monitor, flag) in enumerate(
                    zip(self.models, self.async_monitors, is_run)
                ):
                    if flag and _is_read(async_monitor):
                        df = model.transform(df)
                        ready_async_monitors += 1
                        is_run[i] = False

        return df

    def fit_transform(
        self, df: "snowflake.snowpark.DataFrame"
    ) -> "snowflake.snowpark.DataFrame":
        for transformer in self.transformers:
            transformer.fit(df)
            df = transformer.transform(df)

        return df
