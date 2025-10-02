# Conda Build Verification

This directory contains scripts for Docker-based verification of Snowflake Snowpark Python conda packages across different architectures using the `continuumio/miniconda3` Docker image.

## Overview

The verification system automatically tests conda packages (.conda and .tar.bz2 formats) for **all Python versions 3.9-3.13** on both x86_64 and aarch64 architectures by:

1. Scanning the `package/` directory for conda packages in `linux-64`, `linux-aarch64`, and `noarch` subdirectories
2. Launching Docker containers using `continuumio/miniconda3` (supports both architectures)
3. Installing packages and their dependencies
4. Running smoke tests with actual Snowflake connection to verify full functionality

**Note**: A valid `parameters.py` file with Snowflake connection parameters is required for all testing.

## Files

- `conda_build_verification.sh` - Main orchestration script
- `docker_verify.sh` - Internal script that runs inside Docker containers
- `smoke_test.py` - Smoke test script for package verification
- `parameters.py` - **Required** Snowflake connection parameters - must be created manually
- `package/` - Directory containing conda packages organized by architecture

## Package Structure

The verification expects packages to be organized as follows:
```
package/
├── linux-64/                    # x86_64 packages
│   ├── snowflake-snowpark-python-1.38.0-py39_0.conda
│   ├── snowflake-snowpark-python-1.38.0-py310_0.conda
│   ├── snowflake-snowpark-python-1.38.0-py311_0.conda
│   ├── snowflake-snowpark-python-1.38.0-py312_0.conda
│   ├── snowflake-snowpark-python-1.38.0-py313_0.conda
│   └── ... (corresponding .tar.bz2 files)
├── linux-aarch64/               # aarch64 packages  
│   ├── snowflake-snowpark-python-1.38.0-py39_0.conda
│   ├── snowflake-snowpark-python-1.38.0-py310_0.conda
│   └── ... (all Python versions 3.9-3.13)
└── noarch/                      # Architecture-independent packages
    ├── snowflake-snowpark-python-1.39.0-py39_0.conda
    ├── snowflake-snowpark-python-1.39.0-py310_0.conda
    └── ... (all Python versions 3.9-3.13)
```

## Usage

### Basic Usage

**Prerequisites**: Ensure you have created `parameters.py` with valid Snowflake connection parameters (see Required Setup section above).

```bash
# Test all Python versions (3.9-3.13) on noarch only
./conda_build_verification.sh

# Test all Python versions on specific architecture(s)
./conda_build_verification.sh linux-64
./conda_build_verification.sh linux-64 noarch
./conda_build_verification.sh linux-64 linux-aarch64 noarch
```

### Required Setup

**Before running any verification**, you need a `parameters.py` file with Snowflake connection parameters. You have two options:

#### Option 1: Automatic GPG Decryption (Recommended for CI/CD)
If you have access to the encrypted parameters file and GPG key:
```bash
export GPG_KEY="your-gpg-passphrase"
./conda_build_verification.sh  # Automatically decrypts parameters.py.gpg
```

#### Option 2: Manual Creation
Manually create a `parameters.py` file in the `conda_build_verification/` directory:

```python
# parameters.py
CONNECTION_PARAMETERS = {
    "user": "your_username",
    "password": "your_password",
    "account": "your_account",
    "warehouse": "your_warehouse",
    "database": "your_database",
    "schema": "your_schema"
}
```

Then run verification:

```bash
# Run verification on default architecture (noarch) for all Python versions
./conda_build_verification.sh

# Run verification on specific architectures for all Python versions
./conda_build_verification.sh linux-64 noarch
```

### Command Line Syntax

```bash
./conda_build_verification.sh [architecture...]
```

**Parameters:**
- `architecture...` (optional): One or more architecture directories to test
  - Available: `linux-64`, `linux-aarch64`, `noarch`
  - Default: `noarch` only if no architectures specified
- **Python versions**: Automatically tests all Python versions 3.9-3.13

**Examples:**
- `./conda_build_verification.sh` → Test all Python versions (3.9-3.13) on noarch
- `./conda_build_verification.sh linux-64` → Test all Python versions on linux-64 only
- `./conda_build_verification.sh linux-64 noarch` → Test all Python versions on both linux-64 and noarch
- `./conda_build_verification.sh linux-64 linux-aarch64 noarch` → Test all Python versions on all architectures

### Environment Variables

- `GPG_KEY` - (Optional) Passphrase for automatically decrypting `scripts/parameters.py.gpg`

## Docker Requirements

- Docker must be installed and accessible
- Docker daemon must be running
- Internet access for pulling `continuumio/miniconda3` image

## How It Works

1. **Prerequisites Validation**: 
   - Checks for Docker availability
   - Attempts GPG decryption of `parameters.py.gpg` if `GPG_KEY` environment variable is set
   - Validates `parameters.py` file existence and provides helpful error messages if missing
   - Fails fast before any Docker operations begin

2. **Package Discovery**: Scans `package/linux-64`, `package/linux-aarch64`, and `package/noarch` for packages across all Python versions (3.9-3.13)
   - **Strict Validation**: If packages are not found in any requested architecture directory, the script errors out immediately

3. **Docker Container Launch**: For each architecture with packages:
   - Launches `continuumio/miniconda3` container with appropriate `--platform` flag
   - **Cross-platform testing**: Can test aarch64 packages on x86_64 hosts and vice versa
   - Mounts verification scripts and package directory  
   - Runs `docker_verify.sh` inside the container

4. **Package Testing**: Inside each container, for each Python version (3.9-3.13):
   - Creates isolated conda environment for the specific Python version
   - Installs package dependencies
   - Installs the conda package (.conda and .tar.bz2 formats) if available for that version
   - Runs smoke tests for that version
   - Reports results per Python version

5. **Smoke Testing**:
   - Connects to Snowflake using the `parameters.py` configuration
   - Runs a test query (`SELECT 1 as A`) to verify full functionality
   - Validates that the installed package can successfully interact with Snowflake

6. **Cleanup**: Removes containers and temporary files

## Architecture Support

The `continuumio/miniconda3` Docker image supports both:
- **x86_64** (Intel/AMD 64-bit)
- **aarch64** (ARM 64-bit, Apple Silicon, ARM servers)

Docker uses the `--platform` flag to specify the target architecture:
- `linux-64` → `--platform linux/amd64` (x86_64)  
- `linux-aarch64` → `--platform linux/arm64` (aarch64)
- `noarch` → `--platform linux/amd64` (x86_64 for compatibility)

## Example Output

```
Testing Python versions: 3.9, 3.10, 3.11, 3.12, 3.13
Using default architecture: noarch
Testing architectures: noarch
Script directory: /path/to/scripts/conda_build_verification
Project root: /path/to/snowpark-python
GPG_KEY found, attempting to decrypt parameters.py...
✅ Successfully decrypted parameters.py
Starting Docker-based conda package verification...
Found packages in noarch
=== Running verification for noarch ===
Found packages in noarch, will test all Python versions (3.9-3.13)
Running Docker command for noarch...

=== Testing Python 3.9 ===
Testing conda package: snowflake-snowpark-python-1.39.0-py39_0.conda
✅ Package test completed successfully: snowflake-snowpark-python-1.39.0-py39_0.conda
Testing tar.bz2 package: snowflake-snowpark-python-1.39.0-py39_0.tar.bz2
✅ Package test completed successfully: snowflake-snowpark-python-1.39.0-py39_0.tar.bz2
✅ All tests passed for Python 3.9

=== Testing Python 3.10 ===
Testing conda package: snowflake-snowpark-python-1.39.0-py310_0.conda
✅ Package test completed successfully: snowflake-snowpark-python-1.39.0-py310_0.conda
Testing tar.bz2 package: snowflake-snowpark-python-1.39.0-py310_0.tar.bz2
✅ Package test completed successfully: snowflake-snowpark-python-1.39.0-py310_0.tar.bz2
✅ All tests passed for Python 3.10

... (similar output for Python 3.11, 3.12, 3.13)

=== Final Results ===
✅ All package tests completed successfully across all Python versions

✅ Verification successful for noarch
Cleaning up...
Removed decrypted parameters.py
✅ All verifications completed successfully
```

## Troubleshooting

### Common Issues

1. **Docker not found**
   ```
   Error: Docker is not installed or not in PATH
   ```
   Install Docker and ensure it's in your PATH.

2. **No packages found**
   ```
   ❌ Error: No packages found in requested architecture: linux-aarch64
   Available packages can be found in:
     - linux-64
     - noarch
   ```
   This error occurs when the script cannot find packages for a requested architecture directory. 
   
   **To fix**: Ensure packages are in the correct `package/` subdirectories with proper naming patterns (e.g., `snowflake-snowpark-python-*-py39_*.conda`), or use one of the available architectures listed in the error message.

3. **GPG decryption fails**
   ```
   ❌ Error: Failed to decrypt parameters.py.gpg
   Please check your GPG_KEY environment variable or create parameters.py manually
   ```
   This occurs when GPG decryption fails. Possible causes:
   - Incorrect `GPG_KEY` passphrase
   - GPG not installed or configured
   - Corrupted `.gpg` file
   
   **To fix**: Verify the GPG_KEY value or create `parameters.py` manually.

4. **Missing parameters.py.gpg file**
   ```
   ❌ Error: parameters.py.gpg not found at /path/to/scripts/parameters.py.gpg
   ```
   This occurs when `GPG_KEY` is set but the encrypted file doesn't exist.
   
   **To fix**: Ensure the encrypted file exists at `scripts/parameters.py.gpg` or create `parameters.py` manually.

5. **Container startup fails**
   Check Docker daemon is running and you have internet access to pull the image.

6. **Missing parameters.py file**
   ```
   ❌ Error: Required file 'parameters.py' not found in /path/to/conda_build_verification/
   
   The verification requires a parameters.py file with Snowflake connection parameters.
   Please create /path/to/conda_build_verification/parameters.py with the following format:
   
   CONNECTION_PARAMETERS = {
       "user": "your_username",
       "password": "your_password",
       "account": "your_account",
       "warehouse": "your_warehouse",
       "database": "your_database",
       "schema": "your_schema"
   }
   ```
   
   This error occurs when `parameters.py` is missing from the verification directory. The script now checks for this file early and fails fast.
   
   **To fix**: Create a `parameters.py` file in the `conda_build_verification/` directory with your connection parameters:
   ```python
   CONNECTION_PARAMETERS = {
       "user": "your_username",
       "password": "your_password", 
       "account": "your_account",
       "warehouse": "your_warehouse",
       "database": "your_database",
       "schema": "your_schema"
   }
   ```

### Manual Testing

To manually test packages for all Python versions:

```bash
# Run container interactively
docker run -it --rm \
  -v $(pwd):/verification \
  -v $(pwd)/package/linux-64:/packages \
  -e ARCH_DIR=linux-64 \
  continuumio/miniconda3:latest \
  bash

# Inside container, run the verification script (tests all Python versions 3.9-3.13)
bash /verification/docker_verify.sh
```

## Dependencies

The verification installs these dependencies inside each test environment:
- cloudpickle<=3.1.1
- pyyaml
- snowflake-connector-python
- tzlocal
- python-dateutil
- protobuf

These match the dependencies expected by Snowflake Snowpark Python.
