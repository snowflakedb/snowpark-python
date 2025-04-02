# Snowflake Snowpark Python and Snowpark pandas APIs

[![Build and Test](https://github.com/snowflakedb/snowpark-python/actions/workflows/precommit.yml/badge.svg)](https://github.com/snowflakedb/snowpark-python/actions/workflows/precommit.yml)
[![codecov](https://codecov.io/gh/snowflakedb/snowpark-python/branch/main/graph/badge.svg)](https://codecov.io/gh/snowflakedb/snowpark-python)
[![PyPi](https://img.shields.io/pypi/v/snowflake-snowpark-python.svg)](https://pypi.org/project/snowflake-snowpark-python/)
[![License Apache-2.0](https://img.shields.io/:license-Apache%202-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.txt)
[![Codestyle Black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

The Snowpark library provides intuitive APIs for querying and processing data in a data pipeline.
Using this library, you can build applications that process data in Snowflake without having to move data to the system where your application code runs.

[Source code][source code] | [Snowpark Python developer guide][Snowpark Python developer guide] | [Snowpark Python API reference][Snowpark Python api references] | [Snowpark pandas developer guide][Snowpark pandas developer guide] | [Snowpark pandas API reference][Snowpark pandas api references] | [Product documentation][snowpark] | [Samples][samples]

## Getting started

### Have your Snowflake account ready
If you don't have a Snowflake account yet, you can [sign up for a 30-day free trial account][sign up trial].

### Create a Python virtual environment
You can use [miniconda][miniconda], [anaconda][anaconda], or [virtualenv][virtualenv]
to create a Python 3.9, 3.10, 3.11 or 3.12 virtual environment.

For Snowpark pandas, only Python 3.9, 3.10, or 3.11 is supported.

To have the best experience when using it with UDFs, [creating a local conda environment with the Snowflake channel][use snowflake channel] is recommended.

### Install the library to the Python virtual environment
```bash
pip install snowflake-snowpark-python
```
To use the [Snowpark pandas API][Snowpark pandas developer guide], you can optionally install the following, which installs [modin][modin] in the same environment. The Snowpark pandas API provides a familiar interface for pandas users to query and process data directly in Snowflake.
```bash
pip install "snowflake-snowpark-python[modin]"
```

### Create a session and use the Snowpark Python API
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
# Create a Snowpark dataframe from input data
df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"]) 
df = df.filter(df.a > 1)
result = df.collect()
df.show()

# -------------
# |"A"  |"B"  |
# -------------
# |3    |4    |
# -------------
```

### Create a session and use the Snowpark pandas API
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

# Create a Snowpark pandas dataframe from input data
df = pd.DataFrame([['a', 2.0, 1],['b', 4.0, 2],['c', 6.0, None]], columns=["COL_STR", "COL_FLOAT", "COL_INT"])
df
#   COL_STR  COL_FLOAT  COL_INT
# 0       a        2.0      1.0
# 1       b        4.0      2.0
# 2       c        6.0      NaN

df.shape
# (3, 3)

df.head(2)
#   COL_STR  COL_FLOAT  COL_INT
# 0       a        2.0        1
# 1       b        4.0        2

df.dropna(subset=["COL_INT"], inplace=True)

df
#   COL_STR  COL_FLOAT  COL_INT
# 0       a        2.0        1
# 1       b        4.0        2

df.shape
# (2, 3)

df.head(2)
#   COL_STR  COL_FLOAT  COL_INT
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

## Reading and writing to pandas DataFrame

Snowpark Python API supports reading from and writing to a pandas DataFrame via the [to_pandas][to_pandas] and [write_pandas][write_pandas] commands. 

To use these operations, ensure that pandas is installed in the same environment. You can install pandas alongside Snowpark Python by executing the following command:
```bash
pip install "snowflake-snowpark-python[pandas]"
```
Once pandas is installed, you can convert between a Snowpark DataFrame and pandas DataFrame as follows: 
```python
df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
# Convert Snowpark DataFrame to pandas DataFrame
pandas_df = df.to_pandas() 
# Write pandas DataFrame to a Snowflake table and return Snowpark DataFrame
snowpark_df = session.write_pandas(pandas_df, "new_table", auto_create_table=True)
```

Snowpark pandas API also supports writing to pandas: 
```python
import modin.pandas as pd
df = pd.DataFrame([[1, 2], [3, 4]], columns=["a", "b"])
# Convert Snowpark pandas DataFrame to pandas DataFrame
pandas_df = df.to_pandas() 
```

Note that the above Snowpark pandas commands will work if Snowpark is installed with the `[modin]` option, the additional `[pandas]` installation is not required.

## Verifying Package Signatures

To ensure the authenticity and integrity of the Python package, follow the steps below to verify the package signature using `cosign`.

**Steps to verify the signature:**
- Install cosign:
  - This example is using golang installation: [installing-cosign-with-go](https://edu.chainguard.dev/open-source/sigstore/cosign/how-to-install-cosign/#installing-cosign-with-go)
- Download the file from the repository like pypi:
  - https://pypi.org/project/snowflake-snowpark-python/#files
- Download the signature files from the release tag, replace the version number with the version you are verifying:
  - https://github.com/snowflakedb/snowpark-python/releases/tag/v1.22.1
- Verify signature:
  ````bash
  # replace the version number with the version you are verifying
  ./cosign verify-blob snowflake_snowpark_python-1.22.1-py3-none-any.whl  \
  --certificate snowflake_snowpark_python-1.22.1-py3-none-any.whl.crt \
  --certificate-identity https://github.com/snowflakedb/snowpark-python/.github/workflows/python-publish.yml@refs/tags/v1.22.1 \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com \
  --signature snowflake_snowpark_python-1.22.1-py3-none-any.whl.sig
  Verified OK
  ````

## Contributing
Please refer to [CONTRIBUTING.md][contributing].

[add other sample code repo links]: # (Developer advocacy is open-sourcing a repo that has excellent sample code. The link will be added here.)

[Snowpark Python developer guide]: https://docs.snowflake.com/en/developer-guide/snowpark/python/index.html
[Snowpark Python api references]: https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/index.html
[Snowpark pandas developer guide]: https://docs.snowflake.com/developer-guide/snowpark/python/snowpark-pandas
[Snowpark pandas api references]: https://docs.snowflake.com/developer-guide/snowpark/reference/python/latest/modin/index
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
[to_pandas]: https://docs.snowflake.com/developer-guide/snowpark/reference/python/latest/snowpark/api/snowflake.snowpark.DataFrame.to_pandas
[write_pandas]: https://docs.snowflake.com/developer-guide/snowpark/reference/python/latest/snowpark/api/snowflake.snowpark.Session.write_pandas
[modin]: https://github.com/modin-project/modin
