import logging
import sys


def main():
    logging.warning("summary of %s", ", ".join(sys.argv[1:]) or "nothing")


if __name__ == "__main__":
    main()
