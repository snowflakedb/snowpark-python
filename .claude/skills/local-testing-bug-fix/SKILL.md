---
name: local-testing-bug-fix
description: Fix a GitHub Local Testing issue
---

Fix GitHub issue $ARGUMENTS (provide a GitHub issue URL or number) following our coding standards.

## Prerequisites

Before using this skill, ensure the following are set up:

1. **`gh` CLI** — must be installed and authenticated (`gh auth status`) to fetch issue details
2. **`tests/parameters.py`** — must exist (gitignored, create it manually) with your Snowflake credentials:
   ```python
   CONNECTION_PARAMETERS = {
       "account": "your_account",
       "user": "your_user",
       "password": "your_password",
       "database": "your_db",
       "schema": "public",
   }
   ```
   Required only for running live integ tests. Mock-only tests (`tests/mock/`) do not need credentials.

1. Fetch and read the issue description (use `gh issue view <number>` or the URL)
2. Understand the requirements and reproduce the problem mentally
3. Determine if the fix is feasible (see Known Limitations below)
4. Find the relevant code in `src/snowflake/snowpark/_internal/mock/`
5. Implement the fix
6. Write or update tests (see Test approach below)
7. Update `CHANGELOG.md` (see Changelog below)
8. Create a commit with message format: `SNOW-XXXXXXX: <description>`

## Codebase layout

- Local testing implementation: `src/snowflake/snowpark/_internal/mock/`
- Mock-only tests (local testing exclusive): `tests/mock/`
- Shared integ tests (run against both live and local): `tests/integ/`
  - Tests unsupported in local testing are explicitly skipped with the marker below

## Skipping a test for local testing

When a test in `tests/integ/` cannot run in local testing mode, mark it:

```python
@pytest.mark.skipif(
    "config.getoption('local_testing_mode', default=False)",
    reason="<explain why it is not supported>",
)
def test_something(session):
    ...
```

## Test approach

Validate by comparing live session output to local testing session output:

```python
from snowflake.snowpark import Session

# live session — tries ~/.snowflake/connections.toml first, falls back to tests/parameters.py
try:
    session = Session.builder.getOrCreate()
except Exception:
    from tests.parameters import CONNECTION_PARAMETERS
    session = Session.builder.configs(CONNECTION_PARAMETERS).create()

# local testing session
local_session = Session.builder.config("local_testing", True).create()

df = session...              # DF operation
local_df = local_session...  # same DF operation

df.print_schema()
df.show()

local_df.print_schema()
local_df.show()

# compare the output, fix
```

Run integ tests in local testing mode:
```bash
pytest tests/integ/<test_file>.py --local_testing_mode
```

Run mock-only tests:
```bash
pytest tests/mock/<test_file>.py
```

## Changelog

Add an entry under the **top-most unreleased version** in `CHANGELOG.md`, in the `### Snowpark Local Testing Updates > #### Bug Fixes` section:

```markdown
## x.y.z (TBD)

### Snowpark Local Testing Updates

#### Bug Fixes

- Fixed a bug where <description of the fix>.
```

If the `### Snowpark Local Testing Updates` section doesn't exist yet in the TBD block, add it after `### Snowpark Python API Updates`.

## Known Limitations

1. Local Testing does not support raw SQL
2. If the issue requires a feature that is fundamentally server-side (e.g., raw SQL execution, file stages), it may not be feasible — explain this to the user instead of partially implementing
