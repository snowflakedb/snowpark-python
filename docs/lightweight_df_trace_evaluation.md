# Lightweight DataFrame trace: can it replace the AST for the debug features?

Experimental. Branch `feature/lightweight-df-trace`. Related: SNOW-4328272 (AST dependency
decision), SNOW-4412779 (test plan).

## Question

`context.configure_development_features(enable_dataframe_trace_on_error=..., enable_trace_sql_errors_to_dataframe=...)`
needs the Python source line of the DataFrame operations behind a failing SQL. Today it gets them from
the AST (`_ast_batch._bind_stmt_cache`, `df_ast_ids`), which makes the feature process-wide, forces
`ast_enabled` on and disables the temporary table cleaner. Is that dependency necessary?

## What the change does

- `_internal/df_trace.py`: `SrcPosition`, `TraceNode(src, parents)`, `capture_user_src_position()`
  (the same frame walk as `with_src_position`, without reading source files), a per-session bounded
  `ObjectCreationRegistry` (for the "already exists" context), and the `@traces_inputs` decorator.
- Every place that carried `df_ast_ids` (`SnowflakePlan`, `Selectable` and subclasses, copy and
  deepcopy sites, `get_plan_from_line_numbers`) now also carries `df_trace_nodes`.
  `DataFrame.__init__` creates one node per DataFrame when a tracing flag is on; `_with_plan` links the
  receiver; `@traces_inputs` links dataframe arguments of join, union family, intersect, except, natural
  join, lateral join, cross join, cache_result, `Table.sample`; `RelationalGroupedDataFrame` is a node
  between the dataframe and its aggregate.
- `publicapi` hands each public call to `df_trace.trace_public_call` while a tracing flag is on (one
  flag check otherwise). For the outermost call it links the returned dataframes to what the column
  arguments reference, and keeps the plan lists to the user's operations (see the gaps below).
- UDF, UDTF and UDAF objects get a node at registration; the expression (or `TableFunctionCall`) that
  applies them carries it.
- `debug_utils`, `wrap_exception`, `context` and `Session.__init__` no longer reference the AST.
- The trace is not built for local testing sessions (out of scope).

Everything the AST code path does stays as it is; the debug features just no longer read it.

## Evidence

All on SFCTEST0, Python 3.11 and 3.10.

| Question | Test | Result |
|---|---|---|
| Same output as the AST backend? | `test_df_trace_parity.py`: 132 workloads x 4 settings (simplifier on/off, CTE on/off, multiline on/off) against a frozen copy of the original `debug_utils` (`tests/utils_debug/debug_ast_reference.py`) on the same plan | 527 pass, 1 skip, no xfail |
| Does the corpus cover the public API? | `test_df_trace_coverage.py`: a profiler records which public methods of DataFrame, Table, RelationalGroupedDataFrame, Na/Stat functions, Reader, Writer, UDF/UDTF/UDAF registration and the Session factories the corpus calls; any other must be in `EXCLUDED` with a reason, and stale or redundant exclusions fail | pass |
| Does it work with the AST off? | the existing `test_df_debug_trace.py` and `test_trace_sql_errors_to_df.py` re-run unchanged with the AST disabled (`*_without_ast.py`), plus static checks that `debug_utils`, `df_trace` and `configure_development_features` do not touch the AST | pass |
| Do the existing tests still pass? | `test_df_debug_trace`, `test_trace_sql_errors_to_df`, `test_query_line_intervals`, `test_eager_schema_validation`, `test_context` | pass, assertions unchanged |
| No behavior change for working programs? | `test_df_trace_no_behavior_change.py`: 23 pipelines (including subqueries, UDFs, describe, self-union) x simplifier on/off: rows, schema and SQL identical with the features off and on (SQL equal modulo the line-marker comments the SQL error tracing has always added); process and session state snapshot restored | pass |
| Graph shape | `test_df_trace_graph.py`: chains, joins, diamond, grouped, subquery edge only on the call that uses it, UDF edges, UDF registered before enabling, internal dataframes not listed, call depth back to 0 after errors, lifetime | pass |
| Units | `tests/unit/test_df_trace.py` (no session): capture equals `with_src_position`, expression scan (nested, CASE WHEN branches, shared subtrees), argument scan, call depth per thread and after exceptions, internal-node stripping, registry bound, thread safety, no AST in the debug path | 25 pass |

All suites above together: 812 passed, 1 skipped on Python 3.11; parity and graph on Python 3.10: 568
passed. Unit and mock suites: no new failures against `origin/main`. Existing join, set-operation,
group-by, pivot and UDF integ tests: 243 passed; the one failure (`test_register_artifact_repository_negative`)
fails the same way on `origin/main` (the account has no test artifact repository).

Workloads: 16 sources, 48 operations, 11 shapes, 21 actions, 8 error kinds, 9 expression-subquery
workloads, 13 UDF/UDTF/UDAF workloads (lambda, decorator, named register, vectorized, from file, used
twice, nested, UDTF in join_table_function and session.table_function, UDAF in agg), 6 CTE reuse
workloads.

## Differences from the AST backend

Output equality is checked for the lineage text, the SQL-line to Python-line mapping, the missing
object context and the existing object context.

The three gaps of the first version are closed:

- GAP-1, dataframes inside column expressions (`col.isin(df)`, nested in boolean logic, in CASE WHEN,
  in select, sort and join conditions): the column arguments of the outermost public call are scanned
  for `ScalarSubquery` plans, so the subquery dataframe is an input of the call that passed it, and not
  of later operations, as in the AST.
- GAP-2, function registration lines: UDF/UDTF/UDAF objects carry a registration node; the expression
  (or the table function call, which is an operation of its own as in the AST) that applies them
  carries it into the dataframe's inputs.
- GAP-3, candidate lines under CTE optimization for reused plans: the plan lists keep only the user's
  operations. Nodes of dataframes an API creates internally are removed when the outermost call
  returns, and the lists seeded while a source API (`create_dataframe`, `table`, `sql`, readers)
  builds its dataframe are undone, which is what the AST does because it attaches the id of a source
  after creation. Seeds of calls that raise are kept, so construction-time error context is unchanged.

Scope: by the design documents, only GAP-1 is within the B1 definition ("the chain of DataFrame
operations that could have produced the failing plan"). GAP-2 (functions are not DataFrame
operations) and GAP-3 (CTE line mapping is a documented coarse limitation) were closed for parity.

Workloads without an oracle: `udf.describe` and `udaf.describe` raise missing-AST errors when the AST is
enabled, so they are checked with the lightweight trace alone (the error carries a trace).

Differences where the lightweight output is *more* (asserted as supersets):

- Actions that run an internal DataFrame (`count`, `show`, `first`, writers, stat functions) are a node
  in the lineage; the AST has none for the internal DataFrame (its lineage is empty).
- `random_split` results are linked to their input.
- Same-layer order: the AST visits the dataframes of one level in the iteration order of a `set` of
  bind ids (arbitrary). The lightweight trace uses creation order. Where every level has one node the
  order is asserted equal.

Behavior changes (intended):

- The features no longer enable the AST and no longer disable the temporary table cleaner; turning them
  off restores nothing because nothing was changed. `session.ast_enabled` is the user's.
- Debug output works with the AST disabled and in processes where it cannot be enabled.
- DataFrames created, and functions registered, before the features were turned on have no source
  position (the same as the AST).
- Trace state lives on the DataFrame, its plans and its function objects (freed with them) plus a
  bounded per-session registry of object-creating calls (`session.sql`, `save_as_table`, views), instead
  of the process-wide bind cache.
- Internal API: `DataFrameTraceNode`, `get_df_transform_trace_message` and
  `_get_df_transform_trace` take a `TraceNode` instead of an AST id and statement cache; `df_trace_nodes`
  is a new attribute next to `df_ast_ids`. Two unit tests that built the node from AST objects, and
  mocks that set `df_ast_ids = None`, were adapted.

## Cost

300 x (`with_column` + `filter`) on SFCTEST0, plan building only:

| Mode | per operation | retained per operation |
|---|---|---|
| features off | 1426 us | 1672 B |
| lightweight trace | 1553 us (+9%) | +331 B, freed with the DataFrame |
| AST (previous implementation, measured before this change) | 2512 us (+82%) | +674 B, and 4207 bind statements kept in the session |

With the features off the public API wrapper adds one flag check per call.

## Conclusion

For the line-number features, the AST is not required. The lightweight trace reproduces the AST output
for all 132 workloads in all four settings: exactly, or as a documented superset for 13 (12 action
workloads and `random_split`), with no remaining gap. What it needs that the AST gave for free is
explicit input edges for the operations that take other dataframes (about 10 decorated methods), one
argument scan in the public API wrapper and nodes on function objects, kept honest by the enforced
coverage test. In exchange it removes the process-wide AST requirement and the cleaner side effect, at
a lower cost.

Not evaluated: Snowpark pandas / modin dataframes, async job paths (`collect_nowait`: the error is
raised by the connector), stored procedures (not DataFrame inputs), thread-concurrent use of one session
while toggling the flags.
