## README for pex

Run following to create pex file and invoke hello-world.
```
pex . -v --disable-cache -e snowflake.snowpark._internal.eval:main -o dist/snowflake-snowpark-python-1.15.0.pex && ./dist/snowflake-snowpark-python-1.15.0.pex
```
