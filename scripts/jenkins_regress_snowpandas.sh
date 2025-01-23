#!/usr/bin/env bash

#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#
# this script is called in daily jenkins job SnowparkPythonSnowPandasDailyRegressRunner, it runs all snowpandas
# tests under tests/integ/modin and tests/unit/modin

set -euxo pipefail

# decrypt profile
gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output "tests/parameters.py" $@

# Install tox, which is by default not present in the environment.
python -m pip install tox

# Install protoc
pip install protoc-wheel-0==21.1 mypy-protobuf

# Run snowpandas tests
python -m tox -c $WORKING_DIR -e snowparkpandasjenkins-modin
