#
# Copyright (c) 2012-2024 Snowflake Computing Inc. All rights reserved.
#

import datetime
from typing import List, Any, Tuple
from snowflake.connector.options import pandas as pd


def data_source_data_to_pandas_df(
    data: List[Any], schema: Tuple[Any], connection_type: str
) -> pd.DataFrame:
    columns = [col[0] for col in schema]
    df = pd.DataFrame.from_records(data, columns=columns)

    # convert timestamp and date to string to work around SNOW-1911989
    df = df.map(
        lambda x: x.isoformat()
        if isinstance(x, (datetime.datetime, datetime.date))
        else x
    )
    # convert binary type to object type to work around SNOW-1912094
    df = df.map(lambda x: x.hex() if isinstance(x, (bytearray, bytes)) else x)
    if "pyodbc" in connection_type.lower():
        return df
    elif "oracledb" in connection_type.lower():
        clob_data = []
        for i, col in enumerate(schema):
            if col[1].lower() in ["clob", "nclob"]:
                clob_data.append(i)
        for column in clob_data:
            df[column] = df[column].apply(lambda x: x.read())
    else:
        raise NotImplementedError(
            f"currently supported drivers are pyodbc and oracledb, got: {connection_type}"
        )
    return df
