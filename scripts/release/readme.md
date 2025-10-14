# Snowpark Python Release Preparation Script

This script automates the process of preparing a release branch for Snowpark Python. It supports both **interactive** and **non-interactive** modes, making it perfect for both manual releases and CI/CD automation. The script handles version updates, changelog management, and test file modifications in a single automated workflow.

## What It Does

The `prepare_release_branch.py` script performs the following tasks:

1. **Validates Environment**: Ensures you're running from within a snowpark-python directory
2. **Mode Detection**: Automatically detects interactive vs non-interactive mode based on command-line arguments
3. **Creates Release Branch**: Creates a new git branch from a specified base (commit, branch, or origin/main)
4. **Updates Version Files**:
   - Updates `src/snowflake/snowpark/version.py` with the new VERSION tuple
   - Updates `recipe/meta.yaml` with the new version string
5. **Updates Changelog**: Replaces the first version entry in `CHANGELOG.md` with the new version and specified/today's date
6. **Updates Test Files**: Updates all `.test` and `.test.DISABLED` files in `tests/ast/data/` with the correct `client_version` format

## Prerequisites

- You must be in a snowpark-python project directory
- Git must be available and the repository must be initialized
- Python 3.x with standard library modules

## Usage

The script supports both **interactive** and **non-interactive** modes with automatic detection:

- **Interactive Mode**: Run without arguments - prompts for all inputs
- **Non-Interactive Mode**: Provide any command-line arguments - uses CLI values and smart defaults

### Interactive Mode

```bash
python ./scripts/release/prepare_release_branch.py
```

### Non-Interactive Mode

```bash
# View help and examples
python ./scripts/release/prepare_release_branch.py --help

# Minimal usage (uses today's date and origin/main as defaults)
python ./scripts/release/prepare_release_branch.py --version 1.40.0

# Full customization
python ./scripts/release/prepare_release_branch.py --version 1.40.0 --release-date 2025-01-15 --base-ref origin/main

# Use specific commit as base
python ./scripts/release/prepare_release_branch.py --version 1.40.0 --base-ref abc1234

# Custom date only (still requires --version in non-interactive mode)
python ./scripts/release/prepare_release_branch.py --version 1.40.0 --release-date 2025-12-31
```

### Command-Line Arguments

- `--version VERSION`: Release version in format x.y.z (e.g., 1.39.0). **Required in non-interactive mode**.
- `--release-date RELEASE_DATE`: Release date in format YYYY-MM-DD (default: today)
- `--base-ref BASE_REF`: Base reference for release branch - commit ID, branch name, or origin/main (default: origin/main)
- `--help`: Show help message with examples

### Interactive Mode Prompts

When running in interactive mode (no arguments), you'll be prompted for:

#### 1. Version Input
```
Enter the next release version (format: x.y.z, e.g., 1.39.0)
Version: _
```
- Enter the version in `x.y.z` format (e.g., `1.40.0`, `2.1.3`)
- The script validates the format and determines if it's a patch release (patch > 0) or major/minor release (patch = 0)

#### 2. Release Date (Optional)
```
Optional: Specify a release date (format: YYYY-MM-DD)
- Press Enter to use today's date (default)
Release date (default: today): _
```

#### 3. Base Reference (Optional)
```
Optional: Specify a base for the release branch
- Enter a commit ID (e.g., abc1234)
- Enter a local branch name (e.g., feature-branch)  
- Press Enter to use origin/main (default)
Base reference (default: origin/main): _
```

**Options:**
- **Press Enter**: Uses `origin/main` (fetches latest and creates branch from it)
- **Commit ID**: Enter a 7-40 character hex string (e.g., `abc1234`, `1a2b3c4d5e6f7890`)
- **Branch Name**: Enter any local branch name (e.g., `feature-branch`, `bugfix-123`)

## Examples

### Interactive Mode Examples

#### Example 1: Standard Release from origin/main
```bash
./scripts/release/prepare_release_branch.py

# Prompts:
Version: 1.40.0
Release date (default: today): [Press Enter]
Base reference (default: origin/main): [Press Enter]

# Creates: release-v1.40.0 branch from latest origin/main with today's date
```

#### Example 2: Patch Release from Specific Commit
```bash
./scripts/release/prepare_release_branch.py

# Prompts:
Version: 1.39.1
Release date (default: today): [Press Enter]
Base reference (default: origin/main): abc123def

# Creates: release-v1.39.1 branch from commit abc123def with today's date
```

#### Example 3: Release from Feature Branch with Custom Date
```bash
./scripts/release/prepare_release_branch.py

# Prompts:
Version: 2.0.0
Release date (default: today): 2025-12-31
Base reference (default: origin/main): feature-new-api

# Creates: release-v2.0.0 branch from feature-new-api branch with specified date
```

### Non-Interactive Mode Examples

#### Example 4: Quick Release (Minimal Arguments)
```bash
./scripts/release/prepare_release_branch.py --version 1.40.0

# Uses defaults:
# - Release date: today's date
# - Base reference: origin/main
# Creates: release-v1.40.0 branch from latest origin/main with today's date
```

#### Example 5: Full Automation (All Arguments)
```bash
./scripts/release/prepare_release_branch.py \
  --version 1.40.0 \
  --release-date 2025-01-15 \
  --base-ref origin/main

# Creates: release-v1.40.0 branch from origin/main with specified date
```

#### Example 6: CI/CD Pipeline Usage
```bash
# Perfect for automation and CI/CD
./scripts/release/prepare_release_branch.py \
  --version 1.39.1 \
  --base-ref abc123def

# Uses today's date, creates release-v1.39.1 from specific commit
```

## What Gets Updated

### 1. Version Files
- **`src/snowflake/snowpark/version.py`**: Updates the `VERSION = (major, minor, patch)` tuple
- **`recipe/meta.yaml`**: Updates the `{% set version = "x.y.z" %}` line

### 2. Changelog
- **`CHANGELOG.md`**: Finds the first version entry and updates it with the new version and today's date
- Example: `## 1.39.0 (YYYY-MM-DD)` → `## 1.40.0 (2025-09-10)`

### 3. AST Test Files
Updates all `.test` and `.test.DISABLED` files in `tests/ast/data/` with the correct `client_version` format:

**For Major/Minor Releases (patch = 0):**
```
client_version {
  major: 1
  minor: 40
}
```

**For Patch Releases (patch > 0):**
```
client_version {
  major: 1
  minor: 39
  patch: 1
}
```

## Sample Output

### Interactive Mode Output
```
Snowpark Python Release Preparation Script
==========================================

Enter the next release version (format: x.y.z, e.g., 1.39.0)
Version: 1.40.0

Optional: Specify a release date (format: YYYY-MM-DD)
- Press Enter to use today's date (default)
Release date (default: today): 

Release date: 2025-09-12

Preparing release for version 1.40.0
Major: 1, Minor: 40, Patch: 0
Release type: Major/Minor

Optional: Specify a base for the release branch
- Enter a commit ID (e.g., abc1234)
- Enter a local branch name (e.g., feature-branch)
- Press Enter to use origin/main (default)
Base reference (default: origin/main): 

🔀 Creating release branch: release-v1.40.0 from origin/main
Fetching latest origin/main...

📝 Updating version files...
✓ Updated src/snowflake/snowpark/version.py
✓ Updated recipe/meta.yaml  
✓ Updated CHANGELOG.md
✓ Updated 124 .test and .test.DISABLED files in tests/ast/data

🎉 Release preparation completed successfully!

Next steps:
1. Sync the dependency updates in version.py and meta.yaml if they are inconsistent
2. Review the changes with: git diff
3. Commit the changes with: git commit -am 'Prepare release v1.40.0'
4. Push the branch with: git push origin release-v1.40.0
```

### Non-Interactive Mode Output
```
Snowpark Python Release Preparation Script (Non-Interactive Mode)
==================================================================

Preparing release for version 1.40.0
Major: 1, Minor: 40, Patch: 0
Release type: Major/Minor

🔀 Creating release branch: release-v1.40.0 from origin/main
Fetching latest origin/main...

📝 Updating version files...
✓ Updated src/snowflake/snowpark/version.py
✓ Updated recipe/meta.yaml  
✓ Updated CHANGELOG.md
✓ Updated 124 .test and .test.DISABLED files in tests/ast/data

🎉 Release preparation completed successfully!

Next steps:
1. Sync the dependency updates in version.py and meta.yaml if they are inconsistent
2. Review the changes with: git diff
3. Commit the changes with: git commit -am 'Prepare release v1.40.0'
4. Push the branch with: git push origin release-v1.40.0
```

### Help Output
```bash
$ python ./scripts/release/prepare_release_branch.py --help

usage: prepare_release_branch.py [-h] [--version VERSION]
                                 [--release-date RELEASE_DATE]
                                 [--base-ref BASE_REF]

Snowpark Python Release Preparation Script

options:
  -h, --help            show this help message and exit
  --version VERSION     Release version in format x.y.z (e.g., 1.39.0).
                        Required in non-interactive mode.
  --release-date RELEASE_DATE
                        Release date in format YYYY-MM-DD (default: today)
  --base-ref BASE_REF   Base reference for release branch - commit ID, branch
                        name, or origin/main (default: origin/main)

Examples:
  # Interactive mode (no arguments - prompts for input)
  python prepare_release_branch.py
  
  # Non-interactive mode (any arguments provided - requires --version)
  python prepare_release_branch.py --version 1.39.0 --release-date 2024-12-31 --base-ref origin/main
  
  # Non-interactive mode with minimal options (uses defaults for date and base-ref)
  python prepare_release_branch.py --version 1.39.0
  
  # Use a specific commit as base
  python prepare_release_branch.py --version 1.39.0 --base-ref abc1234
  
  # Use custom release date with version
  python prepare_release_branch.py --version 1.39.0 --release-date 2024-12-31
```

## Error Handling

The script will exit with an error if:
- Not run from a snowpark-python directory
- Invalid version format provided (must be x.y.z format)
- Invalid date format provided (must be YYYY-MM-DD format)
- Invalid date value provided (e.g., 2025-13-45)
- `--version` is missing when using non-interactive mode (any CLI arguments provided)
- Required files are missing (`version.py`, `meta.yaml`, `CHANGELOG.md`)
- Git commands fail (e.g., branch already exists, uncommitted changes, invalid base reference)
- Test directory doesn't exist (`tests/ast/data/`)

## Next Steps After Running

After the script completes successfully:

1. **Review Changes**: `git diff`
2. **Commit Changes**: `git commit -am 'Prepare release v1.40.0'`  
3. **Push Branch**: `git push origin release-v1.40.0`
4. Create pull request for the release branch
5. Follow your standard release process

## Notes

### Mode Detection
- **Interactive mode**: Triggered when no command-line arguments are provided
- **Non-interactive mode**: Automatically triggered when any CLI arguments are provided  
- No need to specify `--non-interactive` flag - the script detects mode automatically

### Smart Defaults (Non-Interactive Mode)
- **Release date**: Uses today's date if not specified
- **Base reference**: Uses `origin/main` if not specified  
- **Version**: Always required in non-interactive mode

### Technical Details
- The script uses only Python standard library modules (no external dependencies)
- All print statement linter warnings are suppressed with `# noqa: T201`
- The script creates a new branch and doesn't modify your current working branch until checkout
- If a release branch with the same name already exists, git will show an error
- Perfect for CI/CD automation with the non-interactive mode

### Automation-Friendly
The non-interactive mode makes this script perfect for:
- **CI/CD Pipelines**: Automated release preparation
- **Batch Processing**: Multiple releases with different parameters
- **Shell Scripts**: Integration with larger automation workflows
- **Scheduled Releases**: Cron jobs or scheduled tasks

## Future Improvements

- Auto sync dependency updates
- Use Python based unparser to update ast code
- Support for pre-release versions (e.g., 1.40.0-rc1)
- Integration with GitHub Actions for automated PRs
