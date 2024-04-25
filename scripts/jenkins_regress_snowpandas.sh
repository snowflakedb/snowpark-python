#!/usr/bin/env bash

#
# Copyright (c) 2012-2023 Snowflake Computing Inc. All rights reserved.
#
# this script is called in daily jenkins job SnowparkPythonSnowPandasDailyRegressRunner, it runs all snowpandas
# tests under tests/integ/modin and tests/unit/modin

set -euxo pipefail

# decrypt profile
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output "tests/parameters.py" $@

# Run snowpandas tests
python -m tox -c $WORKING_DIR -e snowparkpandasjenkins-modin
