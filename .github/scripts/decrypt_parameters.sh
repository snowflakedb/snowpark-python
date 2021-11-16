#!/usr/bin/env bash

gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETER_PASSWORD" --output tests/parameters.py .github/workflows/parameters/parameters_${CLOUD_PROVIDER}.py.gpg
