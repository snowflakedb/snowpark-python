# Snowpark Python Release Preparation Script

This script automates the process of preparing a release branch for Snowpark Python. It handles version updates, changelog management, and test file modifications in a single automated workflow.

## What It Does

The `prepare_release_branch.py` script performs the following tasks:

1. **Validates Environment**: Ensures you're running from within a snowpark-python directory
2. **Creates Release Branch**: Creates a new git branch from a specified base (commit, branch, or origin/main)
3. **Updates Version Files**:
   - Updates `src/snowflake/snowpark/version.py` with the new VERSION tuple
   - Updates `recipe/meta.yaml` with the new version string
4. **Updates Changelog**: Replaces the first version entry in `CHANGELOG.md` with the new version and today's date
5. **Updates Test Files**: Updates all `.test` and `.test.DISABLED` files in `tests/ast/data/` with the correct `client_version` format

## Prerequisites

- You must be in a snowpark-python project directory
- Git must be available and the repository must be initialized
- Python 3.x with standard library modules

## Usage

### Basic Usage

```bash
python ./scripts/release/prepare_release_branch.py
```

### What You'll Be Prompted For

#### 1. Version Input
```
Enter the next release version (format: x.y.z, e.g., 1.39.0)
Version: _
```
- Enter the version in `x.y.z` format (e.g., `1.40.0`, `2.1.3`)
- The script validates the format and determines if it's a patch release (patch > 0) or major/minor release (patch = 0)

#### 2. Base Reference (Optional)
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

### Example 1: Standard Release from origin/main
```bash
./scripts/release/prepare_release_branch.py

# Prompts:
Version: 1.40.0
Base reference (default: origin/main): [Press Enter]

# Creates: release-v1.40.0 branch from latest origin/main
```

### Example 2: Patch Release from Specific Commit
```bash
./scripts/release/prepare_release_branch.py

# Prompts:
Version: 1.39.1
Base reference (default: origin/main): abc123def

# Creates: release-v1.39.1 branch from commit abc123def
```

### Example 3: Release from Feature Branch
```bash
./scripts/release/prepare_release_branch.py

# Prompts:
Version: 2.0.0
Base reference (default: origin/main): feature-new-api

# Creates: release-v2.0.0 branch from feature-new-api branch
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

```
Snowpark Python Release Preparation Script
==========================================

Enter the next release version (format: x.y.z, e.g., 1.39.0)
Version: 1.40.0

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
1. Review the changes with: git diff
2. Commit the changes with: git commit -am 'Prepare release v1.40.0'
3. Push the branch with: git push origin release-v1.40.0
```

## Error Handling

The script will exit with an error if:
- Not run from a snowpark-python directory
- Invalid version format provided
- Required files are missing (`version.py`, `meta.yaml`, `CHANGELOG.md`)
- Git commands fail
- Test directory doesn't exist

## Next Steps After Running

After the script completes successfully:

1. **Review Changes**: `git diff`
2. **Commit Changes**: `git commit -am 'Prepare release v1.40.0'`  
3. **Push Branch**: `git push origin release-v1.40.0`
4. Create pull request for the release branch
5. Follow your standard release process

## Notes

- The script uses only Python standard library modules (no external dependencies)
- All print statement linter warnings are suppressed with `# noqa: T201`
- The script creates a new branch and doesn't modify your current working branch until checkout
- If a release branch with the same name already exists, git will show an error
