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
export OPENSSL_FIPS=1
pip install -U setuptools pip
pip install "${SNOWPARK_WHL}[pandas,secure-local-storage,development]" "cryptography<3.3.0" --force-reinstall --no-binary cryptography
pip install "pytest-timeout"

echo "!!! Environment description !!!"
echo "Default installed OpenSSL version"
openssl version
openssl md5 <<< "12345" || echo "OpenSSL md5 test fails (this is GOOD)"
python -c "import ssl; print('Python openssl library: ' + ssl.OPENSSL_VERSION)"
python -c "from cryptography.hazmat.backends.openssl import backend; print('Cryptography openssl library: ' + backend.openssl_version_text())"
python -c "import hashlib; print(hashlib.md5('test_str'.encode('utf-8')).hexdigest());"
pip freeze

cd $SNOWPARK_DIR
pytest -vvv --cov=snowflake.snowpark --cov-report=xml:coverage.xml -m "(unit or integ) or udfs" tests

deactivate
