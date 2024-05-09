## README for pex / TCM

### Requirements
Install `pex` via `pip install pex`. Alternatively, you can run `python -m pip install -e ".[server, development, modin-development]"`.

### Hello world in IR land
Run following to create pex file to invoke a hello-world scenario for the TCM.
```
pex . -v --disable-cache -e snowflake.snowpark._internal.eval:main -o dist/snowflake-snowpark-python-1.15.0.pex && ./dist/snowflake-snowpark-python-1.15.0.pex "942e040f-208d-4efe-9c08-910c044c56df" "CgIIKhIICgYKBAgDEAkaGQoXCAESAggBGg/KCgwiCnRlc3RfdGFibGUaKwopCAISAggCGiHSBh4SB9oBBBICCAEaE6oCEBIOU1RSIExJS0UgJyVlJScaEQoPCAMSAggDGgeqCAQSAggCGggSBggEEgIIAw=="
```
This is based on the following example data
```
# requestId b64_encoded_ast
"942e040f-208d-4efe-9c08-910c044c56df" "CgIIKhIICgYKBAgDEAkaGQoXCAESAggBGg/KCgwiCnRlc3RfdGFibGUaKwopCAISAggCGiHSBh4SB9oBBBICCAEaE6oCEBIOU1RSIExJS0UgJyVlJScaEQoPCAMSAggDGgeqCAQSAggCGggSBggEEgIIAw=="
```
which can be obtained by running `tests/thin-client/steel-thread.py`.

For development purposes, the entry point for the TCM is in `src/snowflake/snowpark/_internal/eval.py`. Simply executing this module with the required parameters gives a debuggable instance locally.


---
(c) 2024 Snowflake Inc.
