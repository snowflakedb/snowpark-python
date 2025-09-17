#!/bin/bash

# Docker container verification script for Snowflake Snowpark Python packages
# This script runs inside the continuumio/miniconda3 container to verify conda packages

set -e

echo "=== Docker Container Verification Script ==="
echo "Python version: ${PYTHON_VERSION:-3.10}"
echo "Architecture directory: ${ARCH_DIR}"
echo "Container: $(uname -a)"
echo "Conda version: $(conda --version)"

# Function to test a package
test_package() {
    local package_path="$1"
    local package_type="$2"
    local python_version="$3"

    if [ -z "$package_path" ] || [ ! -f "$package_path" ]; then
        echo "Package not found: $package_path"
        return 1
    fi

    echo "Testing $package_type package: $(basename "$package_path")"

    # Ensure conda is properly set up for this shell
    source /opt/conda/etc/profile.d/conda.sh

    # Create test environment
    local env_name="test-${package_type}-$$"
    echo "Creating conda environment: $env_name"
    conda create -n "$env_name" python="$python_version" -y
    conda activate "$env_name"

    # Install required dependencies first
    echo "Installing dependencies..."
    pip install "cloudpickle<=3.1.1" pyyaml snowflake-connector-python tzlocal python-dateutil protobuf

    # Install the package
    echo "Installing package: $(basename "$package_path")"
    if [ "$package_type" = "conda" ]; then
        conda install "$package_path" -y
    else
        conda install "$package_path" -y
    fi

    # Run smoke test and capture output
    echo "Running smoke test..."
    cd /verification

    # Capture both stdout and stderr, and the exit code
    local smoke_test_output
    local smoke_test_exit_code

    smoke_test_output=$(python smoke_test.py 2>&1)
    smoke_test_exit_code=$?

    if [ $smoke_test_exit_code -eq 0 ]; then
        echo "✅ Smoke test successful"
        echo "$smoke_test_output"

        # Cleanup environment
        conda deactivate
        conda env remove -n "$env_name" -y

        echo "✅ Package test completed successfully: $(basename "$package_path")"
    else
        echo "❌ Smoke test failed with exit code: $smoke_test_exit_code"
        echo "Error output:"
        echo "$smoke_test_output"

        # Cleanup environment even on failure
        conda deactivate || true
        conda env remove -n "$env_name" -y || true

        exit 1
    fi
}

# Function to find packages for the current Python version
find_packages() {
    local python_version="$1"
    # Convert Python version (e.g., 3.10 -> py310)
    local py_version="py$(echo $python_version | tr -d '.')"

    # Find matching packages
    local conda_package=$(find /packages -name "snowflake-snowpark-python-*-${py_version}_*.conda" | head -1)
    local tar_package=$(find /packages -name "snowflake-snowpark-python-*-${py_version}_*.tar.bz2" | head -1)

    echo "$conda_package:$tar_package"
}

# Main verification process
main() {
    local python_version="${PYTHON_VERSION:-3.10}"

    # Initialize conda for this shell session
    echo "Initializing conda..."
    source /opt/conda/etc/profile.d/conda.sh
    echo "✅ Conda initialized successfully"

    # Find packages to test
    local packages=$(find_packages "$python_version")
    local conda_package=$(echo "$packages" | cut -d':' -f1)
    local tar_package=$(echo "$packages" | cut -d':' -f2)

    local test_failed=false

    # Test .conda package if available
    if [ -n "$conda_package" ] && [ -f "$conda_package" ]; then
        if ! test_package "$conda_package" "conda" "$python_version"; then
            echo "❌ .conda package test failed"
            test_failed=true
        fi
    else
        echo "No .conda package found for Python $python_version"
    fi

    # Test .tar.bz2 package if available
    if [ -n "$tar_package" ] && [ -f "$tar_package" ]; then
        if ! test_package "$tar_package" "tar.bz2" "$python_version"; then
            echo "❌ .tar.bz2 package test failed"
            test_failed=true
        fi
    else
        echo "No .tar.bz2 package found for Python $python_version"
    fi

    # Final result
    if [ "$test_failed" = true ]; then
        echo "❌ Some package tests failed"
        exit 1
    else
        echo "✅ All package tests completed successfully"
    fi
}

# Run main function
main "$@"
