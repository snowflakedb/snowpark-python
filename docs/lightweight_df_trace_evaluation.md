# Lightweight DataFrame trace: can it replace the AST for the debug features?

Experimental. Branch `yuwang/SNOW-4328272-lite-df-trace`. Related: SNOW-4328272 (AST dependency
decision), SNOW-4412779 (test plan).

## Question

`context.configure_development_features(enable_dataframe_trace_on_error=..., enable_trace_sql_errors_to_dataframe=...)`
needs the Python source line of the DataFrame operations behind a failing SQL. Today it gets them from
the AST (`_ast_batch._bind_stmt_cache`, `df_ast_ids`), which makes the feature process-wide, forces
`ast_enabled` on and disables the temporary table cleaner. Is that dependency necessary?

## What the PR does

- `_internal/df_trace.py`: `SrcPosition`, `TraceNode(src, parents)`, `capture_user_src_position()`
  (the same frame walk as `with_src_position`, without reading source files), a per-session bounded
  `ObjectCreationRegistry` (for the "already exists" context), and the `@traces_inputs` decorator.
- Every place that carried `df_ast_ids` (`SnowflakePlan`, `Selectable` and subclasses, copy and
  deepcopy sites, mock plans, `get_plan_from_line_numbers`) now also carries `df_trace_nodes`.
  `DataFrame.__init__` creates one node per DataFrame when a tracing flag is on; `_with_plan` links the
  receiver; `@traces_inputs` links dataframe arguments of join, union family, intersect, except, natural
  join, lateral join, cross join, cache_result, `Table.sample`; `RelationalGroupedDataFrame` is a node
  between the dataframe and its aggregate.
- `debug_utils`, `wrap_exception`, `context` and `Session.__init__` no longer reference the AST.

Everything the AST code path does stays as it is; the debug features just no longer read it.

## Evidence

All on SFCTEST0, Python 3.11 and 3.10.

| Question | Test | Result |
|---|---|---|
| Same output as the AST backend? | `test_df_trace_parity.py`: 104 workloads x 4 settings (simplifier on/off, CTE on/off, multiline on/off) against a frozen copy of the original `debug_utils` (`tests/utils_debug/debug_ast_reference.py`) on the same plan | 406 pass, 10 expected gaps (strict xfail), 1 skip |
| Does the corpus cover the public API? | `test_df_trace_coverage.py`: a profiler records which public methods of DataFrame, Table, RelationalGroupedDataFrame, Na/Stat functions, Reader, Writer and the Session factories the corpus calls; any other must be in `EXCLUDED` with a reason, and stale or redundant exclusions fail | pass |
| Does it work with the AST off? | the existing `test_df_debug_trace.py` and `test_trace_sql_errors_to_df.py` re-run unchanged with the AST disabled (`*_without_ast.py`), plus static checks that `debug_utils`, `df_trace` and `configure_development_features` do not touch the AST | 58 pass |
| Do the existing tests still pass? | `test_df_debug_trace`, `test_trace_sql_errors_to_df`, `test_query_line_intervals`, `test_eager_schema_validation` | 68 pass, assertions unchanged |
| No behavior change for working programs? | `test_df_trace_no_behavior_change.py`: 19 pipelines x simplifier on/off: rows, schema and SQL identical with the features off and on (SQL equal modulo the line-marker comments the SQL error tracing has always added); process and session state snapshot restored | 41 pass |
| Units | `tests/unit/test_df_trace.py`: capture equals `with_src_position`, graph shape, diamond, grouped, mixed-age dataframes, lifetime (nodes freed with the DataFrame), registry bound, thread safety | 27 pass |

Workloads: 16 sources, 48 operations, 11 shapes (50-step chain, diamond, self join, loop, multi-line
call, helper functions, wide projection), 21 actions (including writers and views), 8 error kinds
(typing error with line, invalid identifier, missing object, already exists).

Unit and mock suites: no new failures against `origin/main`.

## Differences from the AST backend

Output equality is checked for the lineage text, the SQL-line to Python-line mapping, the missing
object context and the existing object context.

Known gaps (the lightweight output is *less* than the AST output):

- GAP-1: a dataframe used inside a column expression (`col.isin(other_df)`) is not an input of the
  operation. The AST records it because the expression references the statement. Fixable by recording
  dataframe references in `Column` construction.
- GAP-2: the registration line of a UDF used by the operation is a dependency in the AST lineage.
  Fixable by giving UDF objects a node and linking it when the UDF is applied.
- GAP-3: with CTE optimization, for the leaf plan of a self-reused dataframe
  (`a.union_all(a)`, `describe()`), the candidate Python lines of an SQL error are the leaf's own, the
  AST list also has the operation that consumed it. The AST gets this from the moment its ids are attached
  (after `__init__` for some APIs); the lightweight node exists from `__init__`, so the
  construction-time back-propagation (needed by the eager validation tests) also reaches the leaf.
  Could be matched by tagging nodes created inside another API call as "internal".

Differences where the lightweight output is *more* (all asserted as supersets):

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
- DataFrames created before the features were turned on have no source position (the same as the AST).
- Trace state lives on the DataFrame and its plans (freed with them) plus a bounded per-session registry
  of object-creating calls (`session.sql`, `save_as_table`, views), instead of the process-wide bind
  cache.
- Internal API: `DataFrameTraceNode`, `get_df_transform_trace_message` and
  `_get_df_transform_trace` take a `TraceNode` instead of an AST id and statement cache; `df_trace_nodes`
  is a new attribute next to `df_ast_ids`. Two unit tests that built the node from AST objects, and
  mocks that set `df_ast_ids = None`, were adapted.

## Cost

300 x (`with_column` + `filter`) on SFCTEST0, plan building only:

| Mode | per operation | retained per operation |
|---|---|---|
| features off | 1382 us | 1672 B |
| lightweight trace | 1471 us (+6%) | +331 B, freed with the DataFrame |
| AST (previous implementation) | 2512 us (+82%) | +674 B, and 4207 bind statements kept in the session |

## Conclusion

For the line-number features, the AST is not required. The lightweight trace reproduces the AST output
for 100 of 104 workloads in all four settings: exactly for 87, and as a documented superset for 13
(12 action workloads and `random_split`). Four workloads are affected by three small, understood gaps
(GAP-1..3) that are within the lightweight trace's reach and not caused by missing information. What
it needs that the AST gave for free is explicit input edges for the operations that take other
dataframes (about 10 decorated methods), kept honest by the enforced coverage test. In exchange it
removes the process-wide AST requirement, the cleaner side effect and about 75 us per operation of
overhead.

Not evaluated: Snowpark pandas / modin dataframes, async job paths (`collect_nowait`: the error is
raised by the connector), UDF/stored procedure registration lineage (GAP-2), thread-concurrent use of
one session while toggling the flags.
