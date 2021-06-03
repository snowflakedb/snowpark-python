#!/usr/bin/env bash

gpg --quiet --batch --yes --decrypt --passphrase="$PARAMETER_PASSWORD" --output test/parameters.py parameters.py.gpg
