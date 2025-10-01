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
        # Run all tests in TEST_MATRIX/TEST_MATRIX_LARGE_QUERY
        run_test_matrix(
            config.TEST_MATRIX_LARGE_QUERY
        )  # CHANGE ME TO RUN THE TEST MATRIX YOU WANT, full list: TEST_MATRIX, TEST_MATRIX_LARGE_QUERY
    else:
        # Run single test
        result = run_test(config.SINGLE_TEST_CONFIG)
        print(f"\nTest completed: {result['status']}")

        # Export single result to CSV if configured
        if config.EXPORT_RESULTS_TO_CSV:
            from runner import export_results_to_csv

            csv_path = export_results_to_csv([result])
            print(f"✓ Results exported to: {csv_path}")

    print("\nDone!")


if __name__ == "__main__":
    main()
