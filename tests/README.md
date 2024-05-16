# Setting up and Running Tests

## Setting up
### Connection parameters

To connect to Snowflake, create a connection parameters file under `tests/parameters.py`, and add the
following code snippet with your parameters:
```python
CONNECTION_PARAMETERS = {
    'account': '<account>',
    'user': '<user>',
    'password': '<password>',
    'role': '<role>',
    'database': '<database>',
    'schema': '<schema>',
    'warehouse': '<warehouse>',
}
```
Snowpark pandas API also offers the convenience of implicit session creation from a configuration file.
This eliminates the need to explicitly create a Snowpark session in your code, allowing you to write your pandas code just as you would normally.
To achieve this, you'll need to create a configuration file located at `~/.snowflake/connections.toml`. The contents of this configuration file should be as follows (following [TOML](https://toml.io/en/) file format):

```python
default_connection_name = "default"

[default]
account = "<myaccount>"
user = "<myuser>"
password = "<mypassword>"
role="<myrole>"
database = "<mydatabase>"
schema = "<myschema>"
warehouse = "<mywarehouse>"
```

The value of `default_connection_name` points to a configuration inside the TOML file, which will be used as the default configuration.
Note that keys of a configuration (`account`, `user`) are the same as keys of connection parameters we use in `tests/parameters.py` and values of a configuration should be double quoted.

Note that these files will be ignored by git, so you do not need to worry about check in your secret.

## Running Tests

### Running tests with Pycharm

As you configure the Pycharm correctly, you can simply run the test by clicking the play button on
Pycharm.

### Running tests with pytest

You can run a test on a terminal using `pytest`, as the example below:
``` bash
pytest tests/integ
pytest tests/integ/test_dataframe.py
pytest tests/integ/test_dataframe.py -k "test_filter"
```

### Running tests with tox

Install tox within your environment
```bash
pip install tox
```
You can run the linter, all integration and unit tests as well as generate a code coverage report
with tox. To run all three functions run the following command:
```bash
python -m tox
```
**Note:** To view a human-readable code coverage output, open up the html code coverage output in
your web browser. This will be located in `<project_dir>/.tox/htmlcov`

To just run the all Snowpark tests, run the following command:
```bash
# Must have Python 3.8 for the tests to run
python -m tox -e py38
```
### Running doctests

#### Snowpark Python Doctests
In order to run doctests contained within a file that make use of shared objects, use:
```bash
pytest -rP src/snowflake/snowpark/functions.py --log-cli-level=INFO
```

#### Snowpark pandas Doctests
In order to run Snowpark pandas doctests, use:

```bash
pytest -rP src/snowflake/snowpark/modin/pandas --log-cli-level=INFO
```
