## Developer setup for SnowPandas
This guide is based on https://github.com/snowflakedb/snowpark-python/blob/main/CONTRIBUTING.md, yet modified so it's possible to develop on both Snowpandas and Snowpark-Python in parallel.
This is necessary due to version conflicts (i.e., the pandas/Arrow versions used).

First, create new environment for SnowPandas

```bash
conda create --name snowpandas-dev python=3.9
```

Activate the environment via
```bash
conda activate snowpark-dev
```

Then install all dependencies via (from snowpark root!)
```bash
python -m pip install -e ".[development, modin-development]"
pip3 install psutil

# for demo
pip install jupyter
pip install matplotlib seaborn
```

## Folder structure
Following tree diagram shows the high-level structure of the Snowpark pandas.
```bash
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

## Doctests for Modin
Modin uses a decorator `_inherit_docstrings` to equip functions with the original pandas' docstrings that my contain tests. By simply adding an import statement, these doctests can be run through the modin shim.
Yet, not all tests pass currently which can be either due to missing pandas functionality within Modin or formatting errors between expected output and received output.

For this reason, within `src/conftest.py` all Modin doctests have been deactivated. To activate them, simply comment the `pytest_ignore_collect` function. Modin doctests can be run from the repo root dir via
```bash
pytest -rP src/snowflake/snowpark/modin/pandas --log-cli-level=INFO
```

## Configuration file for connecting to Snowflake
Snowflake Python Connector and Snowpark Python API now support creating a connection/session from a configuration file.
Snowpark pandas API also offers the convenience of implicit session creation from a configuration file.
This eliminates the need to explicitly create a Snowpark session in your code, allowing you to write your pandas code just as you would normally.
To achieve this, you'll need to create a configuration file located at `~/.snowflake/connections.toml`.
The contents of this configuration file should be as follows (following [TOML](https://toml.io/en/) file format):

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

## Git setup
To setup a development version for Snowpark pandas API, run the following git commands:
```
git clone git@github.com:snowflakedb/snowpandas.git
cd snowpandas
git remote add upstream git@github.com:snowflakedb/snowpark-python.git
git remote set-url --push upstream DISABLE

# This should be the output when invoking the following command:
# origin	git@github.com:snowflakedb/snowpandas.git (fetch)
# origin	git@github.com:snowflakedb/snowpandas.git (push)
# upstream	git@github.com:snowflakedb/snowpark-python.git (fetch)
# upstream	DISABLE (push)
git remote -v
```
### Branch
- `pandas-main` is the local main branch which will have all changes for Snowpark pandas API.

### Incorporate changes from the `upstream` Snowpark Python repo
Assume on `pandas-main` branch
```
git checkout -b <your_branch>
git fetch upstream
git merge upstream/main
git push
```
Submit a PR to merge your branch to `pandas-main` branch. This should be done regular or there are important changes from Snowpark.


### Before PuPr
When releasing Snowpark pandas API, merging this branch with the main via a PR should allow for a clean history.
