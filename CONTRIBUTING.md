# Contributing to snowflake-snowpark-python

Hi, thank you for taking the time to improve Snowflake's Snowpark Python API!

## I have a feature request, or a bug report to submit

Many questions can be answered by checking our [docs](https://docs.snowflake.com/) or looking for already existing bug reports and enhancement requests on our [issue tracker](https://github.com/snowflakedb/snowpark-python/issues).

Please start by checking these first!

## Nobody else had my idea/issue

In that case we'd love to hear from you!
Please [open a new issue](https://github.com/snowflakedb/snowpark-python/issues/new/choose) to get in touch with us.

## I'd like to contribute the bug fix or feature myself

We encourage everyone to first open an issue to discuss any feature work or bug fixes with one of the maintainers.
The following should help guide contributors through potential pitfalls.

### Setup a development environment

#### Clone the repository

```bash
git clone git@github.com:snowflakedb/snowpark-python.git
cd snowpark-python
```

#### Install the library in edit mode and install its dependencies
- Create and activate a new Python virtual environment with any Python version that we support.
- Go to the cloned repository root folder.
- Install the Snowpark API in edit/development mode.
    ```bash
    python -m pip install -e ".[development, pandas]"
    ```
  The `-e` tells `pip` to install the library in [edit, or development mode](https://pip.pypa.io/en/stable/cli/pip_install/#editable-installs).

#### Setup your IDE
You can use Pycharm, VS Code, or any other IDEs.
The following steps assume you use Pycharm. VS Code and other IDEs are similar.
##### Download and install Pycharm
Download the newest community version of [Pycharm](https://www.jetbrains.com/pycharm/download/)
and follow the [installation instructions](https://www.jetbrains.com/help/pycharm/installation-guide.html).

##### Setup project
Open project and browse to the cloned git directory. Then right-click the directory `src` in Pycharm
and "Mark Directory as" -> "Source Root".
VS code doesn't have "Source Root" so you can skip this step if you use VS Code.

##### Setup Python Interpreter
[Configure Pycharm interpreter][config pycharm interpreter] to use the previously created Python virtual environment.

## Tests
The [README under tests folder](tests/README.md) tells you how to set up to run tests.

## Why do my PR tests fail?
If this happens to you do not panic! Any PRs originating from a fork will fail some automated tests. This is because
forks do not have access to our repository's secrets. A maintainer will manually review your changes then kick off
the rest of our testing suite. Feel free to tag [@snowflakedb/snowpark-python-api](https://github.com/orgs/snowflakedb/teams/snowpark-python-api)
if you feel like we are taking too long to get to your PR.
