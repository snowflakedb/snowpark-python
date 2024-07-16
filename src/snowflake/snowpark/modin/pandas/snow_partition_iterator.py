#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from collections.abc import Iterator
from typing import Any, Callable

import pandas

import snowflake.snowpark.modin.pandas.dataframe as DataFrame

PARTITION_SIZE = 4096


class SnowparkPandasRowPartitionIterator(Iterator):
    """
    Iterator on partitioned data used by DataFrame.iterrows and DataFrame.itertuples to iterate over axis=0 or rows.

    SnowparkPandasRowPartitionIterator pulls table data in batches (where number of rows = PARTITION_SIZE) to iterate
    over rows. This is to prevent the table from being queried for every single row - the batch of rows pulled in is
    converted to a native pandas DataFrame and completely iterated over before pulling in the next batch. This results
    in to_pandas() query being made per batch; no joins are ever performed in this implementation.

    However, if enable_partition_with_native_pandas is set to False, it behaves just like the PartitionIterator where
    an iloc call is made to the table to pull in every single row. This results in a join query run for every single
    row, which is inefficient because a lot more queries are issued. This option should be used when a Snowpark pandas
    DataFrame or Series is to be returned to avoid downloading and uploading the same data.

    Parameters
    ----------
    df : DataFrame
        The dataframe to iterate over.
    axis : {0, 1}
        Axis to iterate over.
    func : callable
        The function to get inner iterables from each partition.
    enable_partition_with_native_pandas: bool, default False
        When True, retrieve the table as partitions. Each partition is a pandas DataFrame which is iterated over until
        exhausted, and the next partition is pulled in.
        When False, iterate over the Snowpark pandas DataFrame directly row-by-row.
    """

    def __init__(
        self,
        df: DataFrame,
        func: Callable,
        enable_partition_with_native_pandas: bool = False,
    ) -> None:
        self.position = 0  # keep track of position in the iterator
        # To avoid making a query per row to extract row data (like in DataFrame.iterrows and DataFrame.itertuples),
        # a batch of rows of size PARTITION_SIZE is materialized at a time and converted to a pandas DataFrame.
        # This uses fewer queries. Partitions are used instead of materializing the whole table since some tables
        # are too large to be materialized in one go. PARTITION_SIZE is arbitrary and can be tuned for performance.
        self.df = df
        self.func = func
        self.enable_partition_with_native_pandas = enable_partition_with_native_pandas
        # TODO SNOW-1017263: update to_pandas() to return an iterator and use that directly here.
        if self.enable_partition_with_native_pandas:
            self.partition = self.get_next_partition()
            self.num_rows = -1  # unused
        else:
            self.partition = None  # unused
            # The call below triggers eager evaluation for row count - it is used as a stopping condition to raise
            # StopIteration for the iterator.
            self.num_rows = len(self.df)

    def __iter__(self) -> "SnowparkPandasRowPartitionIterator":
        """
        Implement iterator interface.

        Returns
        -------
        SnowparkPandasRowPartitionIterator
            Iterator object.
        """
        return self

    def __next__(self) -> Any:
        """
        Implement iterator interface.

        Returns
        -------
        Any
            Next element in the SnowparkPandasRowPartitionIterator after the callable func is applied.
        """
        # self.position is used to get the integer location of rows.
        if self.enable_partition_with_native_pandas:
            if len(self.partition) <= self.position % PARTITION_SIZE:
                raise StopIteration
            ser = self.partition.iloc[self.position % PARTITION_SIZE]
            self.position += 1
            if self.position and self.position % PARTITION_SIZE == 0:
                # Finished iterating through the current partition, fetch the next partition.
                self.partition = self.get_next_partition()
            return self.func(ser)
        else:
            if self.position < self.num_rows:
                ser = self.df.iloc[self.position]
                self.position += 1
                return self.func(ser)
            else:
                raise StopIteration

    def get_next_partition(self) -> pandas.DataFrame:
        """
        Helper method to retrieve a partition of table data of size PARTITION_SIZE number of rows.
        """
        return self.df.iloc[
            slice(self.position, self.position + PARTITION_SIZE)
        ].to_pandas()
