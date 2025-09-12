#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

import os
import sys
import re
import subprocess
import argparse
from datetime import date, datetime
import glob


def setup_argument_parser():
    """Set up command line argument parser"""
    parser = argparse.ArgumentParser(
        description="Snowpark Python Release Preparation Script",
        epilog="""
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
        """,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--version",
        help="Release version in format x.y.z (e.g., 1.39.0). Required in non-interactive mode.",
        type=str,
    )

    parser.add_argument(
        "--release-date",
        help="Release date in format YYYY-MM-DD (default: today)",
        type=str,
    )

    parser.add_argument(
        "--base-ref",
        help="Base reference for release branch - commit ID, branch name, or origin/main (default: origin/main)",
        type=str,
        default="origin/main",
    )

    return parser


def check_snowpark_directory():
    """Check if we're in a snowpark-python directory"""
    current_dir = os.getcwd()
    if "snowpark-python" not in current_dir:
        print("Error: This script must be run from within a snowpark-python directory")
        sys.exit(1)


def validate_version(version_str):
    """Validate version format and return parsed components"""
    if not version_str:
        return None

    # Validate version format (x.y.z)
    if not re.match(r"^\d+\.\d+\.\d+$", version_str):
        print("Error: Version must be in format x.y.z (e.g., 1.39.0)")
        sys.exit(1)

    parts = version_str.split(".")
    major, minor, patch = int(parts[0]), int(parts[1]), int(parts[2])

    return version_str, major, minor, patch


def validate_date(date_str):
    """Validate date format and return formatted date"""
    if not date_str:
        return date.today().strftime("%Y-%m-%d")

    # Validate date format (YYYY-MM-DD)
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date_str):
        print("Error: Date must be in format YYYY-MM-DD (e.g., 2024-12-31)")
        sys.exit(1)

    # Validate that it's a valid date
    try:
        datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        print("Error: Invalid date. Please enter a valid date in format YYYY-MM-DD")
        sys.exit(1)

    return date_str


def validate_base_ref(base_ref):
    """Validate and format base reference"""
    if not base_ref or base_ref == "origin/main":
        return "origin/main", "origin/main"
    elif re.match(r"^[a-fA-F0-9]{7,40}$", base_ref):
        return base_ref, f"commit {base_ref}"
    else:
        return base_ref, f"branch {base_ref}"


def get_version_input(args=None, is_interactive=True):
    """Get version input from CLI args or user and validate format"""
    if args and args.version:
        return validate_version(args.version)

    if not is_interactive:
        # This shouldn't happen as we validate --version is required in non-interactive mode
        print("Error: Version is required in non-interactive mode")
        sys.exit(1)

    print("Enter the next release version (format: x.y.z, e.g., 1.39.0)")
    version_str = input("Version: ").strip()
    return validate_version(version_str)


def get_base_reference(args=None, is_interactive=True):
    """Get optional base commit/branch for the release branch from CLI args or user input"""
    if args and args.base_ref:
        return validate_base_ref(args.base_ref)

    if not is_interactive:
        # Use default value in non-interactive mode
        return validate_base_ref("origin/main")

    print("\nOptional: Specify a base for the release branch")
    print("- Enter a commit ID (e.g., abc1234)")
    print("- Enter a local branch name (e.g., feature-branch)")
    print("- Press Enter to use origin/main (default)")
    base_ref = input("Base reference (default: origin/main): ").strip()

    return validate_base_ref(base_ref)


def get_release_date_input(args=None, is_interactive=True):
    """Get optional release date from CLI args or user input and validate format"""
    if args and args.release_date:
        return validate_date(args.release_date)

    if not is_interactive:
        # Use today's date as default in non-interactive mode
        return validate_date(None)  # None will default to today

    print("\nOptional: Specify a release date (format: YYYY-MM-DD)")
    print("- Press Enter to use today's date (default)")
    date_str = input("Release date (default: today): ").strip()

    return validate_date(date_str)


def run_git_command(command):
    """Run git command and handle errors"""
    try:
        result = subprocess.run(
            command, shell=True, check=True, capture_output=True, text=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Git command failed: {command}")
        print(f"Error: {e.stderr}")
        sys.exit(1)


def update_version_py(version_str, major, minor, patch):
    """Update src/snowflake/snowpark/version.py"""
    version_file = "src/snowflake/snowpark/version.py"

    if not os.path.exists(version_file):
        print(f"Error: {version_file} not found")
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

    print(f"✓ Updated {version_file}")


def update_meta_yaml(version_str):
    """Update recipe/meta.yaml"""
    meta_file = "recipe/meta.yaml"

    if not os.path.exists(meta_file):
        print(f"Error: {meta_file} not found")
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

    print(f"✓ Updated {meta_file}")


def update_changelog(version_str, release_date):
    """Update CHANGELOG.md by replacing the version date with the specified date"""
    changelog_file = "CHANGELOG.md"

    if not os.path.exists(changelog_file):
        print(f"Error: {changelog_file} not found")
        sys.exit(1)

    with open(changelog_file) as f:
        content = f.read()

    # Find the first version line and replace it with the new version and specified date
    # Pattern matches the first: ## x.y.z (any_date)
    first_version_pattern = r"## \d+\.\d+\.\d+ \([^)]+\)"
    replacement = f"## {version_str} ({release_date})"

    new_content = re.sub(first_version_pattern, replacement, content, count=1)

    if new_content == content:
        print(f"Warning: No version entry found in {changelog_file}")
    else:
        with open(changelog_file, "w") as f:
            f.write(new_content)
        print(f"✓ Updated {changelog_file}")


def update_test_files(major, minor, patch):
    """Update all .test and .test.DISABLED files in tests/ast/data"""
    test_dir = "tests/ast/data"

    if not os.path.exists(test_dir):
        print(f"Error: {test_dir} not found")
        sys.exit(1)

    # Get both .test and .test.DISABLED files
    test_files = glob.glob(os.path.join(test_dir, "*.test"))
    test_files.extend(glob.glob(os.path.join(test_dir, "*.test.DISABLED")))

    if not test_files:
        print(f"Warning: No .test or .test.DISABLED files found in {test_dir}")
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

    print(f"✓ Updated {updated_count} .test and .test.DISABLED files in {test_dir}")


def main():
    # Parse command line arguments
    parser = setup_argument_parser()
    args = parser.parse_args()

    # Determine if we're in interactive or non-interactive mode
    # Non-interactive mode: any command-line arguments provided
    # Interactive mode: no command-line arguments (just script name)
    is_interactive = len(sys.argv) == 1

    # In non-interactive mode, --version is required
    if not is_interactive and not args.version:
        print("Error: --version is required when using non-interactive mode")
        sys.exit(1)

    if is_interactive:
        print("Snowpark Python Release Preparation Script")
        print("==========================================")
    else:
        print("Snowpark Python Release Preparation Script (Non-Interactive Mode)")
        print("==================================================================")

    # Check if we're in the right directory
    check_snowpark_directory()

    # Get version input
    version_result = get_version_input(args, is_interactive)
    if not version_result:
        print("Error: Version is required")
        sys.exit(1)
    version_str, major, minor, patch = version_result

    # Get release date
    release_date = get_release_date_input(args, is_interactive)
    if is_interactive or args.release_date:
        print(f"\nRelease date: {release_date}")

    print(f"\nPreparing release for version {version_str}")
    print(f"Major: {major}, Minor: {minor}, Patch: {patch}")

    is_patch_release = patch != 0
    print(f"Release type: {'Patch' if is_patch_release else 'Major/Minor'}")

    # Get base reference for release branch
    base_ref, base_description = get_base_reference(args, is_interactive)

    # Checkout release branch
    branch_name = f"release-v{version_str}"
    print(f"\n🔀 Creating release branch: {branch_name} from {base_description}")

    if base_ref == "origin/main":
        # Fetch origin/main and create branch from it
        print("Fetching latest origin/main...")
        run_git_command("git fetch origin main")
        run_git_command(f"git checkout -b {branch_name} origin/main")
    else:
        # Create branch from specified commit or branch
        run_git_command(f"git checkout -b {branch_name} {base_ref}")

    print("\n📝 Updating version files...")

    # Update files
    update_version_py(version_str, major, minor, patch)
    update_meta_yaml(version_str)
    update_changelog(version_str, release_date)
    update_test_files(major, minor, patch)

    print(
        f"\n🎉 Release preparation completed successfully!"
        f"\n\nNext steps:"
        f"\n1. Sync the dependency updates in version.py and meta.yaml if they are inconsistent"
        f"\n2. Review the changes with: git diff"
        f"\n3. Commit the changes with: git commit -am 'Prepare release v{version_str}'"
        f"\n4. Push the branch with: git push origin {branch_name}"
    )


if __name__ == "__main__":
    main()
