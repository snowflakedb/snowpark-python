#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import sys
import re
import subprocess
from datetime import date
import glob


def check_snowpark_directory():
    """Check if we're in a snowpark-python directory"""
    current_dir = os.getcwd()
    if "snowpark-python" not in current_dir:
        print(  # noqa: T201
            "Error: This script must be run from within a snowpark-python directory"
        )
        sys.exit(1)


def get_version_input():
    """Get version input from user and validate format"""
    print("Enter the next release version (format: x.y.z, e.g., 1.39.0)")  # noqa: T201
    version_str = input("Version: ").strip()

    # Validate version format (x.y.z)
    if not re.match(r"^\d+\.\d+\.\d+$", version_str):
        print("Error: Version must be in format x.y.z (e.g., 1.39.0)")  # noqa: T201
        sys.exit(1)

    parts = version_str.split(".")
    major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])

    return version_str, major, minor, patch


def get_base_reference():
    """Get optional base commit/branch for the release branch"""
    print("\nOptional: Specify a base for the release branch")  # noqa: T201
    print("- Enter a commit ID (e.g., abc1234)")  # noqa: T201
    print("- Enter a local branch name (e.g., feature-branch)")  # noqa: T201
    print("- Press Enter to use origin/main (default)")  # noqa: T201
    base_ref = input("Base reference (default: origin/main): ").strip()

    if not base_ref:
        return "origin/main", "origin/main"
    elif re.match(r"^[a-f0-9]{7,40}$", base_ref):
        return base_ref, f"commit {base_ref}"
    else:
        return base_ref, f"branch {base_ref}"


def run_git_command(command):
    """Run git command and handle errors"""
    try:
        result = subprocess.run(
            command, shell=True, check=True, capture_output=True, text=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Git command failed: {command}")  # noqa: T201
        print(f"Error: {e.stderr}")  # noqa: T201
        sys.exit(1)


def update_version_py(version_str, major, minor, patch):
    """Update src/snowflake/snowpark/version.py"""
    version_file = "src/snowflake/snowpark/version.py"

    if not os.path.exists(version_file):
        print(f"Error: {version_file} not found")  # noqa: T201
        sys.exit(1)

    with open(version_file) as f:
        content = f.read()

    # Replace VERSION tuple
    new_content = re.sub(
        r"VERSION = \(\d+, \d+, \d+\)",
        f"VERSION = ({major}, {minor}, {patch})",
        content,
    )

    with open(version_file, "w") as f:
        f.write(new_content)

    print(f"✓ Updated {version_file}")  # noqa: T201


def update_meta_yaml(version_str):
    """Update recipe/meta.yaml"""
    meta_file = "recipe/meta.yaml"

    if not os.path.exists(meta_file):
        print(f"Error: {meta_file} not found")  # noqa: T201
        sys.exit(1)

    with open(meta_file) as f:
        content = f.read()

    # Replace version string
    new_content = re.sub(
        r'{% set version = "[^"]*" %}',
        f'{{% set version = "{version_str}" %}}',
        content,
    )

    with open(meta_file, "w") as f:
        f.write(new_content)

    print(f"✓ Updated {meta_file}")  # noqa: T201


def update_changelog(version_str):
    """Update CHANGELOG.md by replacing the version date with today's date"""
    changelog_file = "CHANGELOG.md"
    today = date.today().strftime("%Y-%m-%d")

    if not os.path.exists(changelog_file):
        print(f"Error: {changelog_file} not found")  # noqa: T201
        sys.exit(1)

    with open(changelog_file) as f:
        content = f.read()

    # Find the first version line and replace it with the new version and today's date
    # Pattern matches the first: ## x.y.z (any_date)
    first_version_pattern = r"## \d+\.\d+\.\d+ \([^)]+\)"
    replacement = f"## {version_str} ({today})"

    new_content = re.sub(first_version_pattern, replacement, content, count=1)

    if new_content == content:
        print(f"Warning: No version entry found in {changelog_file}")  # noqa: T201
    else:
        with open(changelog_file, "w") as f:
            f.write(new_content)
        print(f"✓ Updated {changelog_file}")  # noqa: T201


def update_test_files(major, minor, patch):
    """Update all .test and .test.DISABLED files in tests/ast/data"""
    test_dir = "tests/ast/data"

    if not os.path.exists(test_dir):
        print(f"Error: {test_dir} not found")  # noqa: T201
        sys.exit(1)

    # Get both .test and .test.DISABLED files
    test_files = glob.glob(os.path.join(test_dir, "*.test"))
    test_files.extend(glob.glob(os.path.join(test_dir, "*.test.DISABLED")))

    if not test_files:
        print(  # noqa: T201
            f"Warning: No .test or .test.DISABLED files found in {test_dir}"
        )
        return

    # Determine client_version format based on patch version
    if patch == 0:
        # Major/minor release - only major and minor fields
        client_version_replacement = f"""client_version {{
  major: {major}
  minor: {minor}
}}"""
    else:
        # Patch release - major, minor, and patch fields
        client_version_replacement = f"""client_version {{
  major: {major}
  minor: {minor}
  patch: {patch}
}}"""

    updated_count = 0

    for test_file in test_files:
        with open(test_file) as f:
            content = f.read()

        # Replace client_version blocks
        # This regex handles both formats (with and without patch)
        pattern = r"client_version\s*\{\s*major:\s*\d+\s*minor:\s*\d+\s*(?:patch:\s*\d+\s*)?\}"

        if re.search(pattern, content):
            new_content = re.sub(pattern, client_version_replacement, content)

            if new_content != content:
                with open(test_file, "w") as f:
                    f.write(new_content)
                updated_count += 1

    print(  # noqa: T201
        f"✓ Updated {updated_count} .test and .test.DISABLED files in {test_dir}"
    )


def main():
    print("Snowpark Python Release Preparation Script")  # noqa: T201
    print("==========================================")  # noqa: T201

    # Check if we're in the right directory
    check_snowpark_directory()

    # Get version input
    version_str, major, minor, patch = get_version_input()

    print(f"\nPreparing release for version {version_str}")  # noqa: T201
    print(f"Major: {major}, Minor: {minor}, Patch: {patch}")  # noqa: T201

    is_patch_release = patch != 0
    print(  # noqa: T201
        f"Release type: {'Patch' if is_patch_release else 'Major/Minor'}"
    )

    # Get base reference for release branch
    base_ref, base_description = get_base_reference()

    # Checkout release branch
    branch_name = f"release-v{version_str}"
    print(  # noqa: T201
        f"\n🔀 Creating release branch: {branch_name} from {base_description}"
    )

    if base_ref == "origin/main":
        # Fetch origin/main and create branch from it
        print("Fetching latest origin/main...")  # noqa: T201
        run_git_command("git fetch origin main")
        run_git_command(f"git checkout -b {branch_name} origin/main")
    else:
        # Create branch from specified commit or branch
        run_git_command(f"git checkout -b {branch_name} {base_ref}")

    print("\n📝 Updating version files...")  # noqa: T201

    # Update files
    update_version_py(version_str, major, minor, patch)
    update_meta_yaml(version_str)
    update_changelog(version_str)
    update_test_files(major, minor, patch)

    print("\n🎉 Release preparation completed successfully!")  # noqa: T201
    print("\nNext steps:")  # noqa: T201
    print("1. Review the changes with: git diff")  # noqa: T201
    print(  # noqa: T201
        f"2. Commit the changes with: git commit -am 'Prepare release v{version_str}'"
    )
    print(f"3. Push the branch with: git push origin {branch_name}")  # noqa: T201


if __name__ == "__main__":
    main()
