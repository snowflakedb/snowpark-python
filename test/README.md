# Setting up and Running Tests

### Connection parameter

To connect to Snowflake, create a connection parameter under `test/parameters.py`, and add the
following code snippet with your parameters:
```python
#!/usr/bin/env python3
CONNECTION_PARAMETERS = {
    'host': '<host>',
    'port': '<port>',
    'account': '<account>',
    'user': '<user>',
    'password': '<password>',
    'protocol': '<protocol>',
    'role': '<role>',
    'database': '<database>',
    'schema': '<schema>',
    'warehouse': '<warehouse>',
}
```
Note that this file will be ignored by git, so you do not need to worry about check in your secret.

### Running test with Pycharm

As you configure the Pycharm correctly, you can simply run the test by clicking the play button on
Pycharm. Or you can run a test on a terminal using `pytest`, as the example below:
``` bash
pytest test/integ
pytest test/integ/test_dataframe.py
pytest test/integ/test_dataframe.py -k "test_filter"
```
