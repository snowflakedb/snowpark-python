#!/usr/bin/env bash

gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETER_PASSWORD" --output tests/parameters.py .github/workflows/parameters/parameters_${CLOUD_PROVIDER}.py.gpg
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETER_PASSWORD" .github/workflows/parameters/parameters_dbapi.py.gpg >> tests/parameters.py
