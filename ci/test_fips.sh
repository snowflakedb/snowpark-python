#!/bin/bash -e
#
# Test Snowflake Connector
# Note this is the script that test_docker.sh runs inside of the docker container
#
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# shellcheck disable=SC1090
SNOWPARK_DIR="$( dirname "${THIS_DIR}")"
SNOWPARK_WHL="$(ls $SNOWPARK_DIR/dist/*.whl | sort -r | head -n 1)"

FIPS_LD="/usr/local/lib64:/usr/local/lib"

openssl_fips() {
    env LD_LIBRARY_PATH="${FIPS_LD}${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}" PATH="/usr/local/bin:$PATH" \
        openssl "$@"
}

python3.11 -m venv fips_env
source fips_env/bin/activate

# Install dependencies against the system OpenSSL (3.5 on Rocky 9). Python 3.11's
# _hashlib is linked against OPENSSL_3.4.0 symbols and cannot load OpenSSL 3.0.0
# from /usr/local via LD_LIBRARY_PATH.
pip install -U setuptools pip
pip install protoc-wheel-0==21.1
pip install "${SNOWPARK_WHL}[pandas,secure-local-storage,development,opentelemetry]"
pip install "pytest-timeout"

echo "!!! Environment description !!!"
echo "Custom FIPS OpenSSL CLI version"
openssl_fips version
openssl_fips md5 <<< "12345" || echo "OpenSSL md5 test fails (this is GOOD)"
python -c "import ssl; print('Python openssl library: ' + ssl.OPENSSL_VERSION)"
python -c "from cryptography.hazmat.backends.openssl import backend; print('Cryptography openssl library: ' + backend.openssl_version_text())"
python -c "import hashlib; print(hashlib.md5('test_str'.encode('utf-8')).hexdigest());"
pip freeze

cd $SNOWPARK_DIR
pytest -vvv -n 48 --cov=snowflake.snowpark --cov-report=xml:coverage.xml -m "(unit or integ) or udfs" tests --ignore=src/snowflake/snowpark/modin --ignore=tests/integ/modin --ignore=tests/unit/modin

deactivate
