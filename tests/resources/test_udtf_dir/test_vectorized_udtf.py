import pandas
from _snowflake import vectorized


# End partition UDTFs
class Handler:
    @vectorized(input=pandas.DataFrame)
    def end_partition(self, df):
        result = df.describe().transpose()
        result.insert(loc=0, column="column_name", value=["col1", "col2"])
        return result


class TypeHintedHandler:
    def end_partition(self, df: pandas.DataFrame) -> pandas.DataFrame:
        result = df.describe().transpose()
        result.insert(loc=0, column="column_name", value=["col1", "col2"])
        return result


TypeHintedHandler.end_partition._sf_vectorized_input = pandas.DataFrame


# Process UDTFs
class BasicProcess:
    @vectorized(input=pandas.DataFrame)
    def process(self, df):
        return df


class BasicProcessWithEndPartition:
    def process(self, df):
        return df

    def end_partition(self):
        yield (["a", "b"], [42, 420], [12.3, 45.6])

    process._sf_vectorized_input = pandas.DataFrame


class SumRows:
    def __init__(self) -> None:
        self.sum = None

    def process(self, df):
        if self.sum is None:
            self.sum = df
        else:
            self.sum += df
        return df

    def end_partition(self):
        return self.sum

    process._sf_vectorized_input = pandas.DataFrame
    process._sf_max_batch_size = 1


class BatchSize:
    @vectorized(input=pandas.DataFrame, max_batch_size=4)
    def process(self, df):
        return ([len(df)] * len(df),)
