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
Snowpark also offers the convenience of implicit session creation from a configuration file.
This feature is particularly useful to Snowpark pandas users, because you can write your Snowpark pandas code almost as you would normally write pandas code.
To achieve this, you'll need to create a configuration file located at `~/.snowflake/connections.toml`. For more details, refer to the docs [here][implicit session documentation].


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
# Must have Python 3.9 for the tests to run
python -m tox -e py39
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

[implicit session documentation]: https://docs.snowflake.com/en/developer-guide/python-connector/python-connector-connect#setting-a-default-connection
