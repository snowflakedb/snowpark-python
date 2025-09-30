# Contributing to snowflake-snowpark-python

Hi, thank you for taking the time to improve Snowflake's Snowpark Python or Snowpark pandas APIs!

## I have a feature request, or a bug report to submit

Many questions can be answered by checking our [docs](https://docs.snowflake.com/) or looking for already existing bug reports and enhancement requests on our [issue tracker](https://github.com/snowflakedb/snowpark-python/issues).

Please start by checking these first!

## Nobody else had my idea/issue

In that case we'd love to hear from you!
Please [open a new issue](https://github.com/snowflakedb/snowpark-python/issues/new/choose) to get in touch with us.

## I'd like to contribute the bug fix or feature myself

We encourage everyone to first [open a new issue](https://github.com/snowflakedb/snowpark-python/issues/new/choose) to discuss any feature work or bug fixes with one of the maintainers.
The following should help guide contributors through potential pitfalls.

## Contributor License Agreement ("CLA")

We require our contributors to sign a CLA, available at https://github.com/snowflakedb/CLA/blob/main/README.md. A Github Actions bot will assist you when you open a pull request.

### Setup a development environment

#### Fork the repository and then clone the forked repo

```bash
git clone <YOUR_FORKED_REPO>
cd snowpark-python
```

#### Install the library in edit mode and install its dependencies

- Create a new Python virtual environment with any Python version that we support.
  - The Snowpark Python API supports **Python 3.9, Python 3.10, Python 3.11 and Python 3.12**.
  - The Snowpark pandas API supports **Python 3.9, Python 3.10, and Python 3.11**. Additionally, Snowpark pandas requires **Modin 0.36.x or 0.37.x**, and **pandas 2.2.x or 2.3.x**.

    ```bash
    conda create --name snowpark-dev python=3.9
    ```

- Activate the new Python virtual environment. For example,

  ```bash
  conda activate snowpark-dev
  ```

- Go to the cloned repository root folder.
  - To install the Snowpark Python API in edit/development mode, use:

      ```bash
      python -m pip install -e ".[development, pandas]"
      ```
  - To install the Snowpark pandas API in edit/development mode, use:
      ```bash
      python -m pip install -e ".[modin-development]"
      ```

  The `-e` tells `pip` to install the library in [edit, or development mode](https://pip.pypa.io/en/stable/cli/pip_install/#editable-installs).

#### Setup your IDE

You can use PyCharm, VS Code, or any other IDE.
The following steps assume you use PyCharm, VS Code or any other similar IDE.

##### Download and install PyCharm

Download the newest community version of [PyCharm](https://www.jetbrains.com/pycharm/download/)
and follow the [installation instructions](https://www.jetbrains.com/help/pycharm/installation-guide.html).

##### Download and install VS Code

Download and install the latest version of [VS Code](https://code.visualstudio.com/download)

##### Setup project

Open project and browse to the cloned git directory. Then right-click the directory `src` in PyCharm
and "Mark Directory as" -> "Source Root". **NOTE**: VS Code doesn't have "Source Root" so you can skip this step if you use VS Code.

##### Setup Python Interpreter

[Configure PyCharm interpreter][config pycharm interpreter] or [Configure VS Code interpreter][config vscode interpreter] to use the previously created Python virtual environment.

### Thread-safe development

This section covers guidelines for developers that wish to contribute code to `Session`, `ServerConnection`, `MockServerConnection` and other related objects that are critical to correct functionality of `snowpark-python`.

#### Add Config Parameter to `Session`

1. If the config parameter is set once during initialization and never changed, it is safe to add the parameter to the `Session` object.
2. If the config parameter can be updated by the user, and the update has side-effects during compilation i.e. `analyzer.analyze()`, `analyzer.resolve()` etc, add a warning at config update using `warn_session_config_update_in_multithreaded_mode`.

#### Adding a new localized or shared component

Once you have decided that the new component being added with required protection during concurrent access, following can be used:

- `Session._thread_store`, `ServerConnection._thread_store` are `threading.local()` objects which can be used to store a per-thread instance of the component. The python connector cursor object is an example of this.
- `Session._lock` and `ServerConnection._lock` are `RLock` objects which can be used to serialize access to shared resources. `Session.query_tag` is an example of this.
- `Session._package_lock` is a `RLock` object which can be used to protect `packages` and `imports` for stored procedures and user defined functions.
- `Session._plan_lock` is a `RLock` object which can be used to serialize `SnowflakePlan` and `Selectable` method calls. `SnowflakePlan.plan_state` is an example.
- `QueryHistory(session, include_thread_id=True)` can be used to log the query history with thread id.

An example PR to make auto temp table cleaner thread-safe can be found [here](https://github.com/snowflakedb/snowpark-python/pull/2309).

### AST (Abstract Syntax Tree) Support in Snowpark

If you are an open-source developer modifying existing Snowpark APIs (such as by adding a parameter to a `Dataframe` API), or creating new Snowpark APIs in your PR, please request a review from the `snowpark-ir` [team](https://github.com/orgs/snowflakedb/teams/snowpark-ir) and add the `snowpark-ast` [label](https://github.com/snowflakedb/snowpark-python/labels/snowpark-ast). You can also raise an issue on our [issue tracker](https://github.com/snowflakedb/snowpark-python/issues) with the `snowpark-ast` label and assign it to the `snowpark-ir` team to request a review. We will add code to support detailed logging of the usage of your modified or newly created API if relevant to your PR. After we do so, we will also update your PR description by completing the required AST support acknowledgement checkbox.

If you are an internal developer, please ensure you complete the PR checklist for AST support found in the [Snowpark Python AST developer guide](https://docs.google.com/document/d/16K9jBv0pT6SkYbFTxNIQT9-bJjemE4niZTfc9mie6RQ/edit?tab=t.0), before completing the AST support acknowledgement checkbox.

## Tests

The [README under tests folder](tests/README.md) tells you how to set up to run tests.

## Why do my PR tests fail?

If this happens to you do not panic! Any PRs originating from a fork will fail some automated tests. This is because
forks do not have access to our repository's secrets. A maintainer will manually review your changes then kick off
the rest of our testing suite. Feel free to tag [@snowflakedb/snowpark-python-api](https://github.com/orgs/snowflakedb/teams/snowpark-python-api)
or [@snowflakedb/snowpark-pandas-api](https://github.com/orgs/snowflakedb/teams/snowpark-pandas-api)
if you feel like we are taking too long to get to your PR.

[config pycharm interpreter]: https://www.jetbrains.com/help/pycharm/configuring-python-interpreter.html
[config vscode interpreter]: https://code.visualstudio.com/docs/python/environments#_manually-specify-an-interpreter

## Snowpark pandas Folder structure
Following tree diagram shows the high-level structure of the Snowpark pandas.
```
snowflake
└── snowpark
    └── modin
        └── pandas                ← pandas API frontend layer
        └── core
            ├── dataframe         ← folder containing abstraction
            │                       for Modin frontend to DF-algebra
            │── execution         ← additional patching for I/O
        └── plugin
            ├── _interal          ← Snowflake specific internals
            ├── io                ← Snowpark pandas IO functions
            ├── compiler          ← query compiler, Modin -> Snowpark pandas DF
            └── utils             ← util classes from Modin, logging, …
```
