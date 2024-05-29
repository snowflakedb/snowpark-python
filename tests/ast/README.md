## AST Tests.

Enables testing of the AST generation that will be used in server-side Snowpark implementation, starting with Phase 0.

All generated AST should be tested using this mechanism. To add a test, create a new file under `tests/ast/data`. Files look like the following example. The test driver sets up the session and looks at the value of `df` in the resulting environment.

```python
## TEST CASE

df = session.table("test_table")
df = df.filter("STR LIKE '%e%'")

## EXPECTED OUTPUT

res1 = session.table('test_table')

res2 = res1.filter('STR LIKE '%e%'')
```

To generate the expected output the first time the test is run, or when the AST generation changes, run:
```bash
pytest -vv --update-expectations tests/ast
```
