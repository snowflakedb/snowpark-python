#!/bin/bash -e
#
# Test Snowflake Connector
# Note this is the script that test_docker.sh runs inside of the docker container
#
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# shellcheck disable=SC1090
SNOWPARK_DIR="$( dirname "${THIS_DIR}")"
SNOWPARK_WHL="$(ls $SNOWPARK_DIR/dist/*.whl | sort -r | head -n 1)"

python3.8 -m venv fips_env
source fips_env/bin/activate
pip install -U setuptools pip
pip install "${SNOWPARK_WHL}[pandas,secure-local-storage,development]"
pip install "cryptography<3.3.0" --force-reinstall --no-binary cryptography

echo "!!! Environment description !!!"
echo "Default installed OpenSSL version"
openssl version
python -c "import ssl; print('Python openssl library: ' + ssl.OPENSSL_VERSION)"
python -c  "from cryptography.hazmat.backends.openssl import backend;print('Cryptography openssl library: ' + backend.openssl_version_text())"
python -c "import hashlib;print(hashlib.md5(b'G').digest());"
pip freeze

cd $SNOWPARK_DIR
pytest -vvv --cov=snowflake.snowpark --cov-report=xml:coverage.xml -m "(unit or integ) or udfs" tests

deactivate
