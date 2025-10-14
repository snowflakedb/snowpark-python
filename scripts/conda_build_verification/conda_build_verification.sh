#!/bin/bash

# Docker-based Snowflake Snowpark Python conda package verification
# Supports both x86_64 and aarch64 architectures using continuumio/miniconda3
# Tests all Python versions (3.9-3.13) automatically
#
# Usage: ./conda_build_verification.sh [architecture...]
#
# Examples:
#   ./conda_build_verification.sh                    # Test all Python versions on noarch only
#   ./conda_build_verification.sh linux-64           # Test all Python versions on linux-64 only
#   ./conda_build_verification.sh linux-64 noarch linux-aarch64  # Test multiple architectures

set -e

# Parse architecture arguments
if [ $# -gt 0 ]; then
    # User provided architecture arguments
    PACKAGE_DIRS=("$@")
    echo "User specified architectures: ${PACKAGE_DIRS[*]}"
else
    # Default: test noarch only
    PACKAGE_DIRS=("noarch")
    echo "Using default architecture: noarch"
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" &> /dev/null && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

echo "Testing Python versions: 3.9, 3.10, 3.11, 3.12, 3.13"
echo "Testing architectures: ${PACKAGE_DIRS[*]}"
echo "Script directory: $SCRIPT_DIR"
echo "Project root: $PROJECT_ROOT"

# Docker image - continuumio/miniconda3 supports both x86_64 and aarch64
DOCKER_IMAGE="continuumio/miniconda3:latest"

# Function to check if packages exist in directory
check_packages() {
    local dir="$1"
    local package_path="${SCRIPT_DIR}/package/${dir}"

    if [ ! -d "$package_path" ]; then
        return 1
    fi

    # Check if there are any snowflake-snowpark-python packages
    if ls "${package_path}"/snowflake-snowpark-python-*.conda >/dev/null 2>&1 || \
       ls "${package_path}"/snowflake-snowpark-python-*.tar.bz2 >/dev/null 2>&1; then
        return 0
    fi
    return 1
}

# Function to check if any packages exist in directory (for any Python version)
check_any_packages() {
    local dir="$1"
    local package_path="${SCRIPT_DIR}/package/${dir}"

    if [ ! -d "$package_path" ]; then
        return 1
    fi

    # Check if there are any snowflake-snowpark-python packages for any Python version
    if ls "${package_path}"/snowflake-snowpark-python-*.conda >/dev/null 2>&1 || \
       ls "${package_path}"/snowflake-snowpark-python-*.tar.bz2 >/dev/null 2>&1; then
        return 0
    fi
    return 1
}

# Function to run Docker verification for an architecture
run_docker_verification() {
    local arch_dir="$1"

    echo "=== Running verification for $arch_dir ==="

    # Check if any packages exist for this architecture
    if ! check_any_packages "$arch_dir"; then
        echo "No packages found in $arch_dir, skipping..."
        return 0
    fi

    echo "Found packages in $arch_dir, will test all Python versions (3.9-3.13)"

    # Create container name
    local container_name="snowpark-verify-${arch_dir}-$(date +%s)"

    # Determine Docker platform based on architecture
    local platform=""
    case "$arch_dir" in
        "linux-64")
            platform="--platform linux/amd64"
            ;;
        "linux-aarch64")
            platform="--platform linux/arm64"
            ;;
        "noarch")
            # For noarch, use x86_64 as default (most compatible)
            platform="--platform linux/amd64"
            ;;
        *)
            echo "Warning: Unknown architecture $arch_dir, using host platform"
            platform=""
            ;;
    esac

    # Prepare Docker run command
    local docker_cmd="docker run --rm --name $container_name $platform"

    # Mount necessary files and directories
    docker_cmd="$docker_cmd -v ${SCRIPT_DIR}:/verification"
    docker_cmd="$docker_cmd -v ${SCRIPT_DIR}/package/${arch_dir}:/packages"

    # Add environment variables
    docker_cmd="$docker_cmd -e ARCH_DIR=$arch_dir"

    # Use the Docker image
    docker_cmd="$docker_cmd $DOCKER_IMAGE"

    # Run the verification script inside the container
    docker_cmd="$docker_cmd bash /verification/docker_verify.sh"

    echo "Running Docker command for $arch_dir..."
    if eval "$docker_cmd"; then
        echo "✅ Verification successful for $arch_dir"
    else
        echo "❌ Verification failed for $arch_dir"
        return 1
    fi
}

# Global variable to track if parameters.py was decrypted
parameters_decrypted=false

# Cleanup function for decrypted parameters.py
cleanup_decrypted_parameters() {
    if [ "$parameters_decrypted" = true ] && [ -f "${SCRIPT_DIR}/parameters.py" ]; then
        rm -f "${SCRIPT_DIR}/parameters.py"
        echo "Removed decrypted parameters.py"
    fi
}

# Set up trap to ensure cleanup on script exit or interruption
trap cleanup_decrypted_parameters EXIT INT TERM

# Main execution
main() {
    local verification_failed=false

    # Check for Docker
    if ! command -v docker &> /dev/null; then
        echo "Error: Docker is not installed or not in PATH"
        exit 1
    fi

    # Check for GPG_KEY environment variable and decrypt parameters.py if available
    if [ -n "$GPG_KEY" ] && [ ! -f "${SCRIPT_DIR}/parameters.py" ]; then
        echo "GPG_KEY found, attempting to decrypt parameters.py..."
        if [ -f "${PROJECT_ROOT}/scripts/parameters.py.gpg" ]; then
            if gpg --quiet --batch --yes --decrypt --passphrase="$GPG_KEY" --output "${SCRIPT_DIR}/parameters.py" "${PROJECT_ROOT}/scripts/parameters.py.gpg"; then
                echo "✅ Successfully decrypted parameters.py"
                parameters_decrypted=true
            else
                echo "❌ Error: Failed to decrypt parameters.py.gpg"
                echo "Please check your GPG_KEY environment variable or create parameters.py manually"
                exit 1
            fi
        else
            echo "❌ Error: parameters.py.gpg not found at ${PROJECT_ROOT}/scripts/parameters.py.gpg"
            exit 1
        fi
    elif [ -n "$GPG_KEY" ] && [ -f "${SCRIPT_DIR}/parameters.py" ]; then
        echo "GPG_KEY found but parameters.py already exists, using existing file"
    fi

    # Check for required parameters.py file
    if [ ! -f "${SCRIPT_DIR}/parameters.py" ]; then
        echo "❌ Error: Required file 'parameters.py' not found in ${SCRIPT_DIR}/"
        echo ""
        echo "The verification requires a parameters.py file with Snowflake connection parameters."
        echo "Please create ${SCRIPT_DIR}/parameters.py with the following format:"
        echo ""
        echo "CONNECTION_PARAMETERS = {"
        echo "    \"user\": \"your_username\","
        echo "    \"password\": \"your_password\","
        echo "    \"account\": \"your_account\","
        echo "    \"warehouse\": \"your_warehouse\","
        echo "    \"database\": \"your_database\","
        echo "    \"schema\": \"your_schema\""
        echo "}"
        echo ""
        exit 1
    fi

    echo "✅ Found parameters.py file"
    echo "Starting Docker-based conda package verification..."

    # Check each architecture directory with fallback logic
    for dir in "${PACKAGE_DIRS[@]}"; do
        local test_dir=""

        if check_any_packages "$dir"; then
            echo "Found packages in $dir"
            test_dir="$dir"
        else
            echo "❌ Error: No packages found in requested architecture: $dir"
            echo "Available packages can be found in:"
            for check_dir in "linux-64" "linux-aarch64" "noarch"; do
                if check_any_packages "$check_dir"; then
                    echo "  - $check_dir"
                fi
            done
            echo "❌ Exiting due to no packages found in requested architecture"
            exit 1
        fi

        # Run verification for the selected directory
        if ! run_docker_verification "$test_dir"; then
            verification_failed=true
        fi
    done

    # Remove any dangling containers
    docker container prune -f >/dev/null 2>&1 || true

    if [ "$verification_failed" = true ]; then
        echo "❌ Some verifications failed"
        exit 1
    else
        echo "✅ All verifications completed successfully"
    fi
}

# Run main function
main "$@"
