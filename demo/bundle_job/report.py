import logging
import os


def main():
    logging.warning("report for %s", os.environ.get("REPORT_NAME", "nobody"))


if __name__ == "__main__":
    main()
