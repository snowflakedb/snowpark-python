#!/usr/bin/env bash

gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETER_PASSWORD" .github/workflows/parameters/rsa_keys/rsa_key_${CLOUD_PROVIDER}.p8.gpg >> tests/rsa_key_${CLOUD_PROVIDER}.p8
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETER_PASSWORD" .github/workflows/parameters/parameters_${CLOUD_PROVIDER}.py.gpg >> tests/parameters.py
gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETER_PASSWORD" .github/workflows/parameters/parameters_dbapi.py.gpg >> tests/parameters.py

# Mask the decrypted secrets (raw and the hex Snowpark inlines into SQL) so they
# are redacted even when a failing test prints the query. Keep last so a failure
# fails the step; never add `set -x` here (a trace would print values unmasked).
if command -v python >/dev/null 2>&1; then PY=python; else PY=python3; fi
"$PY" .github/scripts/mask_parameters.py tests/parameters.py
