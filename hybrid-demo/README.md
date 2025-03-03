# running demos

1. run

```bash
conda create --name=hybrid-demo --force python=3.9 --y
conda activate hybrid-demo
pip install "snowflake-snowpark-python[modin] @ git+https://github.com/snowflakedb/snowpark-python.git@mvashishtha/hybrid-execution/manual-no-subclasses"
pip install ipywidgets ipython ipykernel
```

2. You MAY need to restart vscode to make the progress bars show up.

3. run the notebook in this directory.

- downloads possibly way too slow on my VPN. Initially I tried on VPN + sfctest0

# other notes:

- writing a python-backed DF back to Snowflake:
    - snowflake bug prevents us from writing to sql with to_sql: https://stackoverflow.com/questions/78168268/using-pandas-to-sql-and-getting-typeerror-not-all-arguments-converted-during
    - pd.session.write_pandas works but seems to be too slow
- pandas storage is much less efficient than snowflake's? SAMPLE_DATA.TPCH_SF10.LINEITEM is 1 GB in snowflake but ~60 GB in pandas. would duckdb be better?
    - what if hybrid execution is instead pandas on duckdb on hybrid-snowflake?
