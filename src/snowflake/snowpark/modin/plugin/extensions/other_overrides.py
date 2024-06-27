#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

from modin.pandas.series_utils import DatetimeProperties, StringMethods

from snowflake.snowpark.modin.pandas import Series

# We need to override some methods on DatetimeProperties, defined in upstream Modin, in order to
# return the correct Series/DT class.

del DatetimeProperties._Series
DatetimeProperties._Series = Series

del StringMethods._Series
StringMethods._Series = Series


# TODO: SNOW-1504302: Modin upgrade - use Snowpark pandas DataFrame for isocalendar
# OSS Modin's DatetimeProperties frontend class wraps the returned query compiler with `modin.pandas.DataFrame`.
# Since we currently replace `pd.DataFrame` with our own Snowpark pandas DataFrame object, this causes errors
# since OSS Modin explicitly imports its own DataFrame class here. This override can be removed once the frontend
# DataFrame class is removed from our codebase.
def isocalendar(self):  # type: ignore
    from snowflake.snowpark.modin.pandas import DataFrame

    return DataFrame(query_compiler=self._query_compiler.dt_isocalendar())


DatetimeProperties.isocalendar = isocalendar
