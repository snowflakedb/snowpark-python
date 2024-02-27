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
# class Process
