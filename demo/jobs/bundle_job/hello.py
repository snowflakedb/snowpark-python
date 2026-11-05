import logging
import sys


def main():
    # Python logging reaches the event table on a warehouse; print() does not.
    logging.warning("hello from a code bundle, args=%s", sys.argv[1:])


# the guard also keeps test collection (pytest --doctest-modules) from running it
if __name__ == "__main__":
    main()
