# running demos

1. run

```bash
conda create --name=hybrid-demo --force python=3.9 --y
conda activate hybrid-demo
pip install "snowflake-snowpark-python[modin] @ git+https://github.com/snowflakedb/snowpark-python.git@modin-hybrid-client"
pip install ipywidgets ipython ipykernel jupyter
```

2. You MAY need to restart vscode to make the progress bars show up.

3. run the notebook in this directory.

# making a wheel

1. edit version in [version.py]( src/snowflake/snowpark/version.py). Using 9.x.0; starting with 9.9.0. Now at 9.10.0
1. set a particular modin main commit in [setup.py](https://github.com/snowflakedb/snowpark-python/blob/cd9c37406abb3915e661fb5c3b1da6b91aed5862/setup.py#L43)
1. `python setup.py  bdist_wheel`
1. test that the wheel built in `dist/` works by using the command below, then trying out pandas in ipython.
1. check out a release branch, and name it like `modin-hybrid-client-release-9.9.0`
1. `git commit -nsam` to the release branch and push it.

# using built wheel

```bash
conda create --name=hybrid-wheel-test python=3.9 --y --force
conda activate hybrid-wheel-test
pip install "$WHEEL_FILE[modin]"
pip install ipywidgets ipython ipykernel jupyter
```

# notes:

DO NOT USE A VPN. downloads possibly way too slow on my VPN. Initially I tried on VPN + sfctest0 and hybrid experience was not fun.

- writing a python-backed DF back to Snowflake:
    - snowflake bug prevents us from writing to sql with to_sql: https://stackoverflow.com/questions/78168268/using-pandas-to-sql-and-getting-typeerror-not-all-arguments-converted-during
    - pd.session.write_pandas works but seems to be too slow
- pandas storage is much less efficient than snowflake's? SAMPLE_DATA.TPCH_SF10.LINEITEM is 1 GB in snowflake but ~60 GB in pandas. would duckdb be better?
    - what if hybrid execution is instead pandas on duckdb on hybrid-snowflake?
