#!/usr/bin/env bash

#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
# this script is called in daily jenkins job SnowparkPythonSnowPandasDailyRegressRunner, it runs all snowpandas
# tests under tests/integ/modin and tests/unit/modin
# it is also called in SnowparkPandasPandasVersionsRunner, which runs the tests with the specified version
# of native pandas installed

set -euxo pipefail

# decrypt profile
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output "tests/parameters.py" $@

# Install tox, which is by default not present in the environment.
python -m pip install tox

# Run snowpandas tests
if [[ -z "${MODIN_PANDAS_PATCH_VERSION+x}" ]]; then
    TOX_ENV="snowparkpandasjenkins-modin"
else
    # If $MODIN_PANDAS_VERSION env variable is set, tell tox to install it
    TOX_ENV="modin_pandas_version-snowparkpandasjenkins-modin"
fi
python -m tox -c $WORKING_DIR -e $TOX_ENV
