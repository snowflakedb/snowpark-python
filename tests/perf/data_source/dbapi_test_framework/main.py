#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Main entry point for DBAPI ingestion tests.

Usage:
    # Run a single test
    python main.py

    # Run the full test matrix
    python main.py --matrix
"""

import sys

# Support both direct execution and module import
try:
    from . import config
    from .runner import run_test, run_test_matrix
except ImportError:
    import config
    from runner import run_test, run_test_matrix


def main():
    """Main entry point."""

    # Check if running test matrix
    if "--matrix" in sys.argv:
        # Run all tests in TEST_MATRIX
        run_test_matrix(config.TEST_MATRIX)
    else:
        # Run single test
        result = run_test(config.SINGLE_TEST_CONFIG)
        print(f"\nTest completed: {result['status']}")

    print("\nDone!")


if __name__ == "__main__":
    main()
