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
# Name of the protoc executable inside the downloaded archive for this platform.
PROTOC_BIN=""
# Expected SHA-256 digests for this platform, resolved by getOSNameAndArch.
EXPECTED_ZIP_SHA256=""
EXPECTED_BIN_SHA256=""

# CWE-494: this archive is fetched over the network and its bytes are then
# executed by privileged jobs. `.github/workflows/python-publish.yml` runs this
# script on `release: published` with `contents: write`, `id-token: write` and
# the PyPI token in scope, and protoc executes during codegen *before* the
# artifacts are signed with sigstore -- so a poisoned protoc would produce a
# validly signed, poisoned wheel. TLS authenticates GitHub's endpoint but
# cannot detect a release asset swapped through a compromised upstream account,
# so the bytes themselves must be pinned.
#
# We pin the digest of the whole ZIP and verify it *before* extracting. That
# covers the protoc executable, the bundled `include/google/protobuf/*.proto`
# descriptors that feed codegen for the shipped package, and it closes the
# zip-slip window that comes with unpacking an unverified archive. The
# extracted executable is then re-verified as a defence-in-depth check on the
# unzip step itself.
#
# To bump PROTOC_VERSION, run
#
#     .github/scripts/install_protoc.sh --print-digests <new-version>
#
# and paste the emitted block over the constants below. The helper downloads
# every supported platform archive and hashes both the ZIP and the executable
# inside it, so the pins cannot drift out of sync with each other.
PROTOC_ZIP_SHA256_linux_x86_64="3a0e900f9556fbcac4c3a913a00d07680f0fdf6b990a341462d822247b265562"
PROTOC_ZIP_SHA256_osx_x86_64="b4f36b18202d54d343a66eebc9f8ae60809a2a96cc2d1b378137550bbe4cf33c"
PROTOC_ZIP_SHA256_win64="897bf86b9c989f91c4171c7f99e3886fedfceb077a94dd150f1401cfe922cd46"

PROTOC_BIN_SHA256_linux_x86_64="4231ac3fdd77303614df205612445f222fc30ec282002543d9541a2ed75257fe"
PROTOC_BIN_SHA256_osx_x86_64="e2ad4603337242b6996e7f63821ff5b824908c93e03653d4292f982dd9afb526"
PROTOC_BIN_SHA256_win64="7be0d7163d4bc97fde49df00aa789bf386a8d56eab7a394d684c6cf5dfd50030"

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
        PROTOC_BIN="protoc"
        EXPECTED_ZIP_SHA256="${PROTOC_ZIP_SHA256_linux_x86_64}"
        EXPECTED_BIN_SHA256="${PROTOC_BIN_SHA256_linux_x86_64}"
        ;;
      darwin)
        PROTOC_OS_ARCH="osx-x86_64"
        PROTOC_BIN="protoc"
        EXPECTED_ZIP_SHA256="${PROTOC_ZIP_SHA256_osx_x86_64}"
        EXPECTED_BIN_SHA256="${PROTOC_BIN_SHA256_osx_x86_64}"
        ;;
      mingw64* | msys* | cygwin*)
        PROTOC_OS_ARCH="win64"
        PROTOC_BIN="protoc.exe"
        EXPECTED_ZIP_SHA256="${PROTOC_ZIP_SHA256_win64}"
        EXPECTED_BIN_SHA256="${PROTOC_BIN_SHA256_win64}"
        ;;
      * )
        echo "Your Operating System ${KERNEL_TYPE} -> ITS NOT SUPPORTED"
        exit 1
      ;;
  esac
}


# Print the SHA-256 digest of "$1" as a bare lowercase hex string.
# Linux runners ship sha256sum, macOS ships shasum, and all of those plus
# git-bash ship openssl. Fail closed if none of them is available rather than
# silently skipping verification.
computeSha256() {
  local target="$1"

  if command -v sha256sum > /dev/null 2>&1; then
    sha256sum "${target}" | awk '{print $1}'
  elif command -v shasum > /dev/null 2>&1; then
    shasum -a 256 "${target}" | awk '{print $1}'
  elif command -v openssl > /dev/null 2>&1; then
    openssl dgst -sha256 "${target}" | awk '{print $NF}'
  else
    echo "ERROR: no SHA-256 utility found (need sha256sum, shasum or openssl)." >&2
    return 1
  fi
}

# verifySha256 <file> <expected-digest> [<label>]
# Returns 0 only when <file> exists and its SHA-256 digest matches
# <expected-digest> exactly. Every other outcome -- missing file, empty or
# unconfigured expected digest, no digest tool, mismatch -- returns non-zero so
# that `set -e` aborts the install before the bytes are trusted.
verifySha256() {
  local target="$1"
  local expected="$2"
  local label="${3:-${1}}"
  local actual

  if [ -z "${expected}" ]; then
    echo "ERROR: no expected SHA-256 digest is pinned for ${label}." >&2
    return 1
  fi

  if [ ! -f "${target}" ]; then
    echo "ERROR: cannot verify ${label}, file not found: ${target}" >&2
    return 1
  fi

  if ! actual="$(computeSha256 "${target}")"; then
    echo "ERROR: could not compute a SHA-256 digest for ${label}." >&2
    return 1
  fi

  if [ "${actual}" != "${expected}" ]; then
    echo "ERROR: SHA-256 integrity check FAILED for ${label}" >&2
    echo "  expected: ${expected}" >&2
    echo "  actual:   ${actual}" >&2
    echo "Refusing to extract or execute unverified protoc bytes (possible tampering)." >&2
    return 1
  fi

  echo "SHA-256 integrity check passed for ${label} (${actual})"
}


# printDigests [version]
# Developer helper for bumping PROTOC_VERSION. Downloads every supported
# platform archive for <version> (default: the pinned PROTOC_VERSION), hashes
# both the ZIP and the executable inside it, and prints the constant block to
# paste above. Installs nothing and touches neither PATH nor GITHUB_PATH.
printDigests() {
  local version="${1:-${PROTOC_VERSION}}"
  local workdir
  workdir="$(mktemp -d)"
  # shellcheck disable=SC2064
  trap "rm -rf '${workdir}'" EXIT

  local zip_lines=""
  local bin_lines=""
  local entry os_arch bin_name suffix zip_name url zip_digest bin_digest

  # "<os-arch> <executable-name> <constant-suffix>"
  for entry in "linux-x86_64 protoc linux_x86_64" \
               "osx-x86_64 protoc osx_x86_64" \
               "win64 protoc.exe win64"; do
    # shellcheck disable=SC2086
    set -- ${entry}
    os_arch="$1"
    bin_name="$2"
    suffix="$3"

    zip_name="protoc-${version}-${os_arch}.zip"
    url="https://github.com/protocolbuffers/protobuf/releases/download/v${version}/${zip_name}"
    echo "Fetching ${url}" >&2

    curl -fsSL -o "${workdir}/${zip_name}" "${url}"
    zip_digest="$(computeSha256 "${workdir}/${zip_name}")"

    rm -rf "${workdir}/x"
    unzip -qo "${workdir}/${zip_name}" -d "${workdir}/x"
    bin_digest="$(computeSha256 "${workdir}/x/bin/${bin_name}")"

    zip_lines="${zip_lines}PROTOC_ZIP_SHA256_${suffix}=\"${zip_digest}\"
"
    bin_lines="${bin_lines}PROTOC_BIN_SHA256_${suffix}=\"${bin_digest}\"
"
  done

  echo >&2
  echo "# ---- paste over the digest constants in ${SCRIPT_NAME} (protoc ${version}) ----"
  printf '%s' "${zip_lines}"
  echo
  printf '%s' "${bin_lines}"
}


usage() {
  cat <<EOF
Usage: ${SCRIPT_NAME} [--print-digests [version]]

  (no arguments)              Install the pinned protoc ${PROTOC_VERSION} and
                              mypy-protobuf. This is what CI invokes.
  --print-digests [version]   Print the SHA-256 constant block for <version>
                              (default ${PROTOC_VERSION}) and exit without
                              installing anything. Use when bumping protoc.
EOF
}


downloadProtoc() {
  URL="https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/${PROTOC_ZIP}"

  echo "Downloading ${PROTOC_ZIP} at ${URL}"

  mkdir -p "${HOME}/local"

  # -f so an HTML error page is never mistaken for an archive.
  curl -fL -o "${PROTOC_ZIP}" "${URL}"

  # CWE-494: verify the archive BEFORE unzipping it, so neither the executable,
  # nor the bundled .proto includes, nor the zip entry names are ever trusted
  # without a digest match.
  verifySha256 "${PROTOC_ZIP}" "${EXPECTED_ZIP_SHA256}" "${PROTOC_ZIP}"

  unzip -o "${PROTOC_ZIP}" -d "${HOME}/local"

  # Defence in depth: re-verify the executable that will actually run, before
  # it is placed on PATH or invoked.
  verifySha256 "${HOME}/local/bin/${PROTOC_BIN}" "${EXPECTED_BIN_SHA256}" "bin/${PROTOC_BIN}"

  echo "$HOME/local/bin" >> $GITHUB_PATH
}


install() {
  getOSNameAndArch

  buildProtocZIPName

  downloadProtoc

  export PATH="$HOME/local/bin:$PATH"
  echo "Protoc version: $(protoc --version)"
}

# Only install when executed directly. Sourcing this script exposes the
# verification helpers (for the security regression tests) without downloading
# or installing anything.
if [ "${BASH_SOURCE[0]:-$0}" = "$0" ]; then
  # CI invokes this script with no arguments, which installs as before.
  case "${1:-}" in
    --print-digests)
      printDigests "${2:-${PROTOC_VERSION}}"
      exit 0
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    "")
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      usage >&2
      exit 1
      ;;
  esac

  install

  # mypy-protobuf is used to generated typed Python code from protobuf
  if command -v uv &> /dev/null; then
      echo "Using uv to install mypy-protobuf"
      uv pip install mypy-protobuf --system
  else
      echo "uv not available, using pip to install mypy-protobuf"
      pip install mypy-protobuf
  fi
  echo "mypy-protobuf version: $(protoc-gen-mypy --version)"

  echo "${SCRIPT_NAME} done."
fi
