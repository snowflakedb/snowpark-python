#!/usr/bin/env bash
#
# Copyright (c) 2024 Snowflake Computing Inc. All rights reserved.
#

# This script is used to install protoc on Github actions.
# Alternatively, we can use third party action like below but it requires a IT update
# - name: Install Protoc
#  uses: arduino/setup-protoc@v3

set -eu

SCRIPT_NAME="$(basename "$0")"

echo "${SCRIPT_NAME} is running... "

PROTOC_VERSION=3.20.1
PROTOC_OS_ARCH=""
PROTOC_ZIP=""

buildProtocZIPName() {
  PROTOC_ZIP=protoc-${PROTOC_VERSION}-${PROTOC_OS_ARCH}.zip
}

getOSNameAndArch(){
  KERNEL_TYPE=$(uname -s | tr '[:upper:]' '[:lower:]')
  ARCH=$(uname -m)
  echo "Your OS is ${KERNEL_TYPE} and arch is ${ARCH}"

  case "${KERNEL_TYPE}" in
      linux)
        PROTOC_OS_ARCH="linux-x86_64"
        ;;
      darwin)
        PROTOC_OS_ARCH="osx-x86_64"
        ;;
      mingw64* | msys* | cygwin*)
        PROTOC_OS_ARCH="win64"
        ;;
      * )
        echo "Your Operating System ${KERNEL_TYPE} -> ITS NOT SUPPORTED"
        exit 1
      ;;
  esac
}


downloadProtoc() {
  URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${PROTOC_ZIP}"

  echo "Downloading ${PROTOC_ZIP} at ${URL}"

  mkdir ${HOME}/local

  curl -L -o "${PROTOC_ZIP}" "${URL}"
  unzip -o "${PROTOC_ZIP}" -d  ${HOME}/local
  echo "$HOME/local/bin" >> $GITHUB_PATH
}


install() {
  getOSNameAndArch

  buildProtocZIPName

  downloadProtoc

  export PATH="$HOME/local/bin:$PATH"
  echo "Protoc version: $(protoc --version)"
}

install

# mypy-protobuf is used to generated typed Python code from protobuf
python -m pip install uv
uv pip install mypy-protobuf --system
echo "mypy-protobuf version: $(protoc-gen-mypy --version)"

echo "${SCRIPT_NAME} done."
