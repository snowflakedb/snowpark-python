#!/bin/bash
set -e

# Run each AutoSwitchBackend test in a separate pytest process because each
# test tests global imports and/or makes global configuration changes.
# Set TEST_SNOWPARK_PANDAS_AUTO_SWITCH_BACKEND=True to opt into running
# those tests, and use --noconftest because conftest files import
# modules of interest before the test cases begin.
echo "Running AutoSwitchBackend tests..."
for test_file in tests/integ/modin/hybrid/auto_switch_backend/test_*.py; do
    TEST_SNOWPARK_PANDAS_AUTO_SWITCH_BACKEND=True pytest --noconftest ${SNOWFLAKE_PYTEST_VERBOSITY} ${SNOWFLAKE_PYTEST_COV_CMD} --durations=20 "$@" "$test_file"
done

echo "Running main modin tests..."
${MODIN_PYTEST_CMD} --durations=20 -m "${SNOWFLAKE_TEST_TYPE}" "$@" ${SNOW_1314507_WORKAROUND_RERUN_FLAGS} tests/unit/modin tests/integ/modin tests/integ/test_df_to_snowpark_pandas.py
