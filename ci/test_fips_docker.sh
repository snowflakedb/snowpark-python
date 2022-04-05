#!/bin/bash -x

THIS_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
SNOWPARK_DIR="$( dirname "${THIS_DIR}")"
# In case this is not run locally and not on Jenkins

cd $THIS_DIR/docker/snowpark_test_fips

CONTAINER_NAME=test_fips_snowpark

echo "[Info] Start building docker image"
docker build -t ${CONTAINER_NAME}:1.0 -f Dockerfile .

user_id=$(id -u $USER)
docker run --network=host \
    -e LANG=en_US.UTF-8 \
    -e TERM=vt102 \
    -e SF_USE_OPENSSL_ONLY=True \
    -e PIP_DISABLE_PIP_VERSION_CHECK=1 \
    -e LOCAL_USER_ID=$user_id \
    -e CRYPTOGRAPHY_ALLOW_OPENSSL_102=1 \
    -e AWS_ACCESS_KEY_ID \
    -e AWS_SECRET_ACCESS_KEY \
    -e SF_REGRESS_LOGS \
    -e SF_PROJECT_ROOT \
    -e cloud_provider \
    -e PYTEST_ADDOPTS \
    --mount type=bind,source="${SNOWPARK_DIR}",target=/home/user/snowpark-python \
    ${CONTAINER_NAME}:1.0 \
    /home/user/snowpark-python/ci/test_fips.sh $1
