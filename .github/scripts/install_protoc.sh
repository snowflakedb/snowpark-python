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
PROTOC_SHA256=""
DOWNLOAD_ATTEMPTS=3

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
        PROTOC_SHA256="3a0e900f9556fbcac4c3a913a00d07680f0fdf6b990a341462d822247b265562"
        ;;
      darwin)
        PROTOC_OS_ARCH="osx-x86_64"
        PROTOC_SHA256="b4f36b18202d54d343a66eebc9f8ae60809a2a96cc2d1b378137550bbe4cf33c"
        ;;
      mingw64* | msys* | cygwin*)
        PROTOC_OS_ARCH="win64"
        PROTOC_SHA256="897bf86b9c989f91c4171c7f99e3886fedfceb077a94dd150f1401cfe922cd46"
        ;;
      * )
        echo "Your Operating System ${KERNEL_TYPE} -> ITS NOT SUPPORTED"
        exit 1
      ;;
  esac
}

sha256Of() {
  # sha256sum on Linux and Git Bash, shasum on macOS.
  if command -v sha256sum > /dev/null 2>&1; then
    sha256sum "$1" | cut -d ' ' -f 1
  else
    shasum -a 256 "$1" | cut -d ' ' -f 1
  fi
}

downloadProtoc() {
  URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${PROTOC_ZIP}"

  echo "Downloading ${PROTOC_ZIP} at ${URL}"

  mkdir -p "${HOME}/local"

  # The release is fetched unauthenticated, so GitHub throttles it per runner IP
  # and answers with a short error body instead of the archive. Without --fail
  # curl stores that body as the .zip and the run dies later in unzip, so the
  # download is both status-checked and digest-checked before it is trusted.
  ATTEMPT=1
  while true; do
    rm -f "${PROTOC_ZIP}"

    if curl --fail --location --silent --show-error \
            --retry 5 --retry-delay 5 --retry-connrefused \
            --connect-timeout 30 --max-time 300 \
            -o "${PROTOC_ZIP}" "${URL}"; then
      ACTUAL_SHA256=$(sha256Of "${PROTOC_ZIP}")
      if [ "${ACTUAL_SHA256}" = "${PROTOC_SHA256}" ]; then
        break
      fi
      echo "Checksum mismatch for ${PROTOC_ZIP} ($(wc -c < "${PROTOC_ZIP}" | tr -d ' ') bytes)"
      echo "  expected ${PROTOC_SHA256}"
      echo "  actual   ${ACTUAL_SHA256}"
    fi

    if [ "${ATTEMPT}" -ge "${DOWNLOAD_ATTEMPTS}" ]; then
      echo "Could not download a valid ${PROTOC_ZIP} after ${DOWNLOAD_ATTEMPTS} attempts"
      exit 1
    fi

    ATTEMPT=$((ATTEMPT + 1))
    echo "Retrying download (attempt ${ATTEMPT} of ${DOWNLOAD_ATTEMPTS})..."
    sleep $((ATTEMPT * 5))
  done

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
# Exact-version pinned (CWE-829): protoc-gen-mypy is executed while building the
# officially signed release, so an unpinned resolve would let a newly published
# mypy-protobuf run arbitrary code in the release job.
MYPY_PROTOBUF_VERSION="5.1.0"
if command -v uv &> /dev/null; then
    echo "Using uv to install mypy-protobuf"
    uv pip install "mypy-protobuf==${MYPY_PROTOBUF_VERSION}" --system
else
    echo "uv not available, using pip to install mypy-protobuf"
    pip install "mypy-protobuf==${MYPY_PROTOBUF_VERSION}"
fi
echo "mypy-protobuf version: $(protoc-gen-mypy --version)"

echo "${SCRIPT_NAME} done."
