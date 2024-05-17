# Snowflake Snowpark Python and Snowpark pandas APIs

[![Build and Test](https://github.com/snowflakedb/snowpark-python/actions/workflows/precommit.yml/badge.svg)](https://github.com/snowflakedb/snowpark-python/actions/workflows/precommit.yml)
[![codecov](https://codecov.io/gh/snowflakedb/snowpark-python/branch/main/graph/badge.svg)](https://codecov.io/gh/snowflakedb/snowpark-python)
[![PyPi](https://img.shields.io/pypi/v/snowflake-snowpark-python.svg)](https://pypi.org/project/snowflake-snowpark-python/)
[![License Apache-2.0](https://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![Codestyle Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

The Snowpark library provides intuitive APIs for querying and processing data in a data pipeline.
Using this library, you can build applications that process data in Snowflake without having to move data to the system where your application code runs.

[Source code][source code] | [Snowpark Python developer guide][Snowpark Python developer guide] | [Snowpark Python API reference][Snowpark Python api references] | [Snowpark pandas devleoper guide][Snowpark pandas developer guide] | [Snowpark pandas API reference][Snowpark pandas api references] | [Product documentation][snowpark] | [Samples][samples]

## Getting started

### Have your Snowflake account ready
If you don't have a Snowflake account yet, you can [sign up for a 30-day free trial account][sign up trial].

### Create a Python virtual environment
You can use [miniconda][miniconda], [anaconda][anaconda], or [virtualenv][virtualenv]
to create a Python 3.8, 3.9, 3.10 or 3.11 virtual environment.

For Snowpark pandas, only Python 3.9, 3.10, or 3.11 is supported.

To have the best experience when using it with UDFs, [creating a local conda environment with the Snowflake channel][use snowflake channel] is recommended.

### Install the library to the Python virtual environment
```bash
pip install snowflake-snowpark-python
```
Optionally, you need to install pandas in the same environment if you want to use pandas-related features:
```bash
pip install "snowflake-snowpark-python[pandas]"
```
Optionally, you need to install Modin in the same environment if you want to use Snowpark pandas features:
```bash
pip install "snowflake-snowpark-python[modin]"
```

### Create a session and use the Snowpark Python APIs
```python
from snowflake.snowpark import Session

connection_parameters = {
  "account": "<your snowflake account>",
  "user": "<your snowflake user>",
  "password": "<your snowflake password>",
  "role": "<snowflake user role>",
  "warehouse": "<snowflake warehouse>",
  "database": "<snowflake database>",
  "schema": "<snowflake schema>"
}

session = Session.builder.configs(connection_parameters).create()
df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]) # Create a Snowpark dataframe
df = df.filter(df.a > 1)
df.show()
pandas_df = df.to_pandas()  # this requires pandas installed in the Python environment
result = df.collect()
```

### Create a session and use the Snowpark pandas APIs
```python
import modin.pandas as pd
import snowflake.snowpark.modin.plugin
from snowflake.snowpark import Session

CONNECTION_PARAMETERS = {
    'account': '<myaccount>',
    'user': '<myuser>',
    'password': '<mypassword>',
    'role': '<myrole>',
    'database': '<mydatabase>',
    'schema': '<myschema>',
    'warehouse': '<mywarehouse>',
}
session = Session.builder.configs(CONNECTION_PARAMETERS).create()

# Create a Snowpark pandas dataframe out of a Snowflake table.
df = pd.read_snowflake('pandas_test')

df
# COL_STR  COL_FLOAT  COL_INT
# 0       a        2.0      1.0
# 1       b        4.0      2.0
# 2       c        6.0      NaN

df.shape
# (3, 3)

df.head(2)
# COL_STR  COL_FLOAT  COL_INT
# 0       a        2.0        1
# 1       b        4.0        2

df.dropna(subset=["COL_INT"], inplace=True)

df
# COL_STR  COL_FLOAT  COL_INT
# 0       a        2.0        1
# 1       b        4.0        2

df.shape
# (2, 3)

df.head(2)
# COL_STR  COL_FLOAT  COL_INT
# 0       a        2.0        1
# 1       b        4.0        2

# Save the result back to Snowflake with a row_pos column.
df.reset_index(drop=True).to_snowflake('pandas_test2', index=True, index_label=['row_pos'])
```

## Samples
The [Snowpark Python developer guide][Snowpark Python developer guide], [Snowpark Python API references][Snowpark Python api references], [Snowpark pandas developer guide][Snowpark pandas developer guide], and [Snowpark pandas api references][Snowpark pandas api references] have basic sample code.
[Snowflake-Labs][snowflake lab sample code] has more curated demos.

## Logging
Configure logging level for `snowflake.snowpark` for Snowpark Python API logs.
Snowpark uses the [Snowflake Python Connector][python connector].
So you may also want to configure the logging level for `snowflake.connector` when the error is in the Python Connector.
For instance,
```python
import logging
for logger_name in ('snowflake.snowpark', 'snowflake.connector'):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.DEBUG)
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)
    ch.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
    logger.addHandler(ch)
```

## Contributing
Please refer to [CONTRIBUTING.md][contributing].

[add other sample code repo links]: # (Developer advocacy is open-sourcing a repo that has excellent sample code. The link will be added here.)

[Snowpark Python developer guide]: https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html
[Snowpark Python api references]: https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/index.html
[Snowpark pandas developer guide]: https://docs.snowflake.com/LIMITEDACCESS/snowpark-pandas
[Snowpark pandas api references]: https://docs.snowflake.com/en/LIMITEDACCESS/snowpark-pandas-api/reference/index.html
[snowpark]: https://www.snowflake.com/snowpark
[sign up trial]: https://signup.snowflake.com
[source code]: https://github.com/snowflakedb/snowpark-python
[miniconda]: https://docs.conda.io/en/latest/miniconda.html
[anaconda]: https://www.anaconda.com/
[virtualenv]: https://docs.python.org/3/tutorial/venv.html
[config pycharm interpreter]: https://www.jetbrains.com/help/pycharm/configuring-python-interpreter.html
[python connector]: https://pypi.org/project/snowflake-connector-python/
[use snowflake channel]: https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#local-development-and-testing
[snowflake lab sample code]: https://github.com/Snowflake-Labs/snowpark-python-demos
[samples]: https://github.com/snowflakedb/snowpark-python/blob/main/README.md#samples
[contributing]: https://github.com/snowflakedb/snowpark-python/blob/main/CONTRIBUTING.md
