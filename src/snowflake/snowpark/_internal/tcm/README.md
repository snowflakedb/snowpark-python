## README for pex

```
# pex . -v --disable-cache -e snowflake.snowpark._internal.tcm.eval:main -o dist/snowflake-snowpark-python-1.15.0.pex

pex . -v --disable-cache -e snowflake.snowpark._internal.eval:main -o dist/snowflake-snowpark-python-1.15.0.pex


```

Entry points can also take the form package:target, such as sphinx:main or fabric.main:main for
Sphinx and Fabric respectively. This is roughly equivalent to running a script 
that does import sys, from package import target; sys.exit(target())
