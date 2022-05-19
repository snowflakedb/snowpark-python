# Snowflake Snowpark Python API

The Snowpark library provides intuitive APIs for querying and processing data in a data pipeline.
Using this library, you can build applications that process data in Snowflake without having to move data to the system where your application code runs.

[Source code][source code] | [Developer guide][developer guide] | [API reference][api references] | [Product documentation][snowpark] | [Samples](#samples)

## Getting started

### Have your Snowflake account ready
If you don't have a Snowflake account yet, you can [sign up for a 30-day free trial account][sign up trial].

### Create a Python virtual environment
Python 3.8 is required. You can use [miniconda][miniconda], [anaconda][anaconda], or [virtualenv][virtualenv]
to create a Python 3.8 virtual environment.

To have the best experience when using it with UDFs, [creating a local conda environment with the Snowflake channel][use snowflake channel] is recommended.

### Install the library to the Python virtual environment
```bash
pip install snowflake-snowpark-python
```

### Create a session and use the APIs
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
df = session.create_dataframe([[1, 2], [3, 4]], schema=["a", "b"])
df = df.filter(df.a > 1)
df.show()
pandas_df = df.to_pandas()
result = df.collect()
```
The [Developer Guide][developer guide] and [API references][api references] have more information and sample code.

## Contributing to the Snowpark Python API

### Clone the repository

```bash
git clone git@github.com:snowflakedb/snowpark-python.git
cd snowpark-python
```

### Install the library in edit mode and install its dependencies
- Activate the Python virtual environment that you created.
- Go to the cloned repository root folder.
- Install the Snowpark API in edit/development mode.

  - For Linux and Mac:
    ```bash
    python -m pip install -e ".[development, pandas]"
    ```

  - For Windows:
    ```bash
    python -m pip install -e '.[development, pandas]'
    ```
  The `-e` tells `pip` to install the library in edit, or development mode.

### Setup your IDE
You can use Pycharm, VS Code, or any other IDEs.
The following steps assume you use Pycharm. VS Code and other IDEs are similar.
#### Download and install Pycharm
Download the newest community version of [Pycharm](https://www.jetbrains.com/pycharm/download/)
and follow the [installation instructions](https://www.jetbrains.com/help/pycharm/installation-guide.html).

#### Setup project
Open project and browse to the cloned git directory. Then right-click the directory `src` in Pycharm
and "Mark Directory as" -> "Source Root".
VS code doesn't have "Source Root" so you can skip this step if you use VS Code.

#### Setup Python Interpreter
[Configure Pycharm interpreter][config pycharm interpreter] to use the previously created Python virtual environment.

## Tests
The [README under tests folder](tests/README.md) tells you how to set up to run tests.

## Logging
Configure logging level for `snowflake.snowpark` for Snowpark Python API logs.
Snowpark uses the the [Snowflake Python Connector][python connector].
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

## Samples
The [Developer Guide][developer guide] and [API references][api references] have sample code.

[add other sample code repo links]: # (Developer advocacy is open-sourcing a repo that has excellent sample code. The link will be added here.)

[developer guide]: https://docs.snowflake.com/en/LIMITEDACCESS/snowpark-python.html
[api references]: https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/index.html
[snowpark]: https://www.snowflake.com/snowpark
[sign up trial]: https://signup.snowflake.com
[source code]: https://github.com/snowflakedb/snowpark-python
[miniconda]: https://docs.conda.io/en/latest/miniconda.html
[anaconda]: https://www.anaconda.com/
[virtualenv]: https://docs.python.org/3/tutorial/venv.html
[config pycharm interpreter]: https://www.jetbrains.com/help/pycharm/configuring-python-interpreter.html
[python connector]: https://pypi.org/project/snowflake-connector-python/
[use snowflake channel]: https://docs.snowflake.com/en/LIMITEDACCESS/udf-python-packages.html#local-development-and-testing
