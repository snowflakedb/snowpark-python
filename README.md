# Snowflake Snowpark Python API

The Snowpark library provides an intuitive API for querying and processing data in a data pipeline.
Using this library, you can build applications that process data in Snowflake without having to move data to the system where your application code runs.

[Source code][source code] | [User guide][user guide] | [API docs][api docs] | [Product documentation][snowpark] | [Samples](#samples)

## To contribute to the Snowpark Python API

### Have your Snowflake account ready
If you don't have a Snowflake account yet, you can [sign up for a 30-day free trial account][sign up trial].

### Create a Python virtual environment
Python 3.8 is required. You can use [miniconda][miniconda], [anaconda][anaconda]), or [virtualenv][virtualenv]
to create a Python 3.8 virtual environment.

3.9, 3.10 and newer versions will be supported in the future.

### Clone the repository

```bash
git clone git@github.com:snowflakedb/snowpark-python.git
cd snowpark-python
```

### Install the library in edit mode and install its dependencies
Activate the Python virtual environment that you created.
Go to the cloned repository root folder.
Then install the Snowpark API in edit/development mode.

For Linux and Mac:
```bash
python -m pip install -e ".[development, pandas]"
```

For Windows:
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


## Samples
The [User Guide][user guide] and [API docs][api docs] have sample code.

[comment]: # (Developer advocacy is open-sourcing a repo that has excellent sample code. The link will be added here.)

[user guide]: https://docs.snowflake.com/en/LIMITEDACCESS/snowpark-python.html
[api docs]: https://docs.snowflake.com/en/developer-guide/snowpark/reference/python/index.html
[snowpark]: https://www.snowflake.com/snowpark
[sign up trial]: https://signup.snowflake.com
[source code]: https://github.com/snowflakedb/snowpark-python
[miniconda]: https://docs.conda.io/en/latest/miniconda.html
[anaconda]: https://www.anaconda.com/
[virtualenv]: https://docs.python.org/3/tutorial/venv.html
[config pycharm interpreter]: https://www.jetbrains.com/help/pycharm/configuring-python-interpreter.html
