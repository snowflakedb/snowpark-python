[comment]: <> (TODO SNOW-1764181 update readme to match with current test pratice)

## AST Tests

This driver enables testing of the AST generation that will be used in the server-side Snowpark implementation, starting with Phase 0.

All generated AST should be tested using this mechanism. To add a test, create a new file under `tests/ast/data`. Files look like the following example. The test driver sets up the session and looks at the accumulated lazy values in the resulting environment.

N.B. No eager evaluation is permitted, as any intermediate batches will not be observed. This can easily be changed if necessary, however.

The src line no information is only captured when run with python 3.11+ because of bug fix/differences in frame line no across earlier python versions.

```python
## TEST CASE

df = session.table(tables.table1)
df = df.filter("STR LIKE '%e%'")

## EXPECTED ENCODED AST

[...]

## EXPECTED UNPARSER OUTPUT

res1 = session.table('table1')

res2 = res1.filter('STR LIKE '%e%'')
```

To generate the expected output the first time the test is run, or when the AST generation changes, run:
```bash
pytest --update-expectations tests/ast
```

For these tests to work, the Unparser must be built in the monorepo:
```bash
cd my-monorepo-path
cd Snowflake/unparser
sbt assembly
```

The location of the Unparser can be set either via the environment variable `SNOWPARK_UNPARSER_JAR` or via the _pytest_ commandline argument `--unparser-jar=<path>`.
