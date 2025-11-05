#!/usr/bin/env bash

gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETER_PASSWORD" .github/workflows/parameters/rsa_keys/rsa_key_${CLOUD_PROVIDER}.p8.gpg >> tests/rsa_key_${CLOUD_PROVIDER}.p8
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETER_PASSWORD" .github/workflows/parameters/parameters_${CLOUD_PROVIDER}.py.gpg >> tests/parameters.py
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETER_PASSWORD" .github/workflows/parameters/parameters_dbapi.py.gpg >> tests/parameters.py
