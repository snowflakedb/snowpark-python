# Contributing to snowflake-snowpark-python

Hi, thank you for taking the time to improve Snowflake's Snowpark Python API!

## I have a feature request, or a bug report to submit

Many questions can be answered by checking our [docs](https://docs.snowflake.com/) or looking for already existing bug reports and enhancement requests on our [issue tracker](https://github.com/snowflakedb/snowpark-python/issues).

Please start by checking these first!

## Nobody else had my idea/issue

In that case we'd love to hear from you!
Please [open a new issue](https://github.com/snowflakedb/snowpark-python/issues/new/choose) to get in touch with us.

## I'd like to contribute the bug fix or feature myself

We encourage everyone to first [open a new issue](https://github.com/snowflakedb/snowpark-python/issues/new/choose) to discuss any feature work or bug fixes with one of the maintainers.
The following should help guide contributors through potential pitfalls.

### Setup a development environment

#### Fork the repository and then clone the forked repo

```bash
git clone <YOUR_FORKED_REPO>
cd snowpark-python
```

#### Install the library in edit mode and install its dependencies

- Create a new Python virtual environment with any Python version that we support. Currently supported version is **Python 3.8**. Support for Python 3.9 and 3.10 is available as a preview feature to all accounts. For example,

  ```bash
  conda create --name snowpark-dev python=3.8
  ```

- Activate the new Python virtual environment. For example,

  ```bash
  conda activate snowpark-dev
  ```

- Go to the cloned repository root folder.
- Install the Snowpark API in edit/development mode.

    ```bash
    python -m pip install -e ".[development, pandas]"
    ```

  The `-e` tells `pip` to install the library in [edit, or development mode](https://pip.pypa.io/en/stable/cli/pip_install/#editable-installs).

#### Setup your IDE

You can use PyCharm, VS Code, or any other IDE.
The following steps assume you use PyCharm, VS Code or any other similar IDE.

##### Download and install PyCharm

Download the newest community version of [PyCharm](https://www.jetbrains.com/pycharm/download/)
and follow the [installation instructions](https://www.jetbrains.com/help/pycharm/installation-guide.html).

##### Download and install VS Code

Downlaod and install the latest version of [VS Code](https://code.visualstudio.com/download)

##### Setup project

Open project and browse to the cloned git directory. Then right-click the directory `src` in PyCharm
and "Mark Directory as" -> "Source Root". **NOTE**: VS Code doesn't have "Source Root" so you can skip this step if you use VS Code.

##### Setup Python Interpreter

[Configure PyCharm interpreter][config pycharm interpreter] or [Configure VS Code interpreter][config vscode interpreter] to use the previously created Python virtual environment.

## Tests

The [README under tests folder](tests/README.md) tells you how to set up to run tests.

## Why do my PR tests fail?

If this happens to you do not panic! Any PRs originating from a fork will fail some automated tests. This is because
forks do not have access to our repository's secrets. A maintainer will manually review your changes then kick off
the rest of our testing suite. Feel free to tag [@snowflakedb/snowpark-python-api](https://github.com/orgs/snowflakedb/teams/snowpark-python-api)
if you feel like we are taking too long to get to your PR.

[config pycharm interpreter]: https://www.jetbrains.com/help/pycharm/configuring-python-interpreter.html
[config vscode interpreter]: https://code.visualstudio.com/docs/python/environments#_manually-specify-an-interpreter
