#!/bin/bash -e
#
# Test Snowflake Connector
# Note this is the script that test_docker.sh runs inside of the docker container
#
THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
# shellcheck disable=SC1090
SNOWPARK_DIR="$( dirname "${THIS_DIR}")"
SNOWPARK_WHL="$(ls $SNOWPARK_DIR/dist/*.whl | sort -r | head -n 1)"

python3.9 -m venv fips_env
source fips_env/bin/activate
export PATH=/usr/local/bin:$PATH
export LD_LIBRARY_PATH=/usr/local/lib64/:/usr/local/lib/:$LD_LIBRARY_PATH

pip install -U setuptools pip
pip install protoc-wheel-0==21.1
pip install "${SNOWPARK_WHL}[pandas,secure-local-storage,development,opentelemetry]"
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
pytest -vvv -n 48 --cov=snowflake.snowpark --cov-report=xml:coverage.xml -m "(unit or integ) or udfs" tests --ignore=src/snowflake/snowpark/modin --ignore=tests/integ/modin --ignore=tests/unit/modin

deactivate
