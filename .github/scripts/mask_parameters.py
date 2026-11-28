#!/usr/bin/env python
"""Mask decrypted test-parameter secrets in GitHub Actions logs.

Loads tests/parameters.py (written by decrypt_parameters.sh) and emits an
``::add-mask::`` for each credential-like string value -- both raw and as the
lowercase hex that Snowpark inlines into generated SQL via ``bytes.fromhex``
(udf_utils ``pickled_func.hex()``), which GitHub's own masking does not catch.
No value is printed outside the ``::add-mask::`` command, which the runner
consumes without echoing.
"""
import re
import runpy
import sys

SENSITIVE_KEY = re.compile(
    r"pass|pwd|token|secret|private_key|credential", re.IGNORECASE
)
# Keys that only point at a secret (a file/path/name) rather than hold one.
NON_SECRET_SUFFIX = ("_file", "_path", "_name")
MIN_LEN = 6


def is_sensitive(key):
    k = key.lower()
    return not k.endswith(NON_SECRET_SUFFIX) and SENSITIVE_KEY.search(k) is not None


def collect(obj, key, out):
    if isinstance(obj, dict):
        for k, v in obj.items():
            collect(v, str(k), out)
    elif isinstance(obj, (list, tuple, set)):
        for v in obj:
            collect(v, key, out)  # inherit the container's key
    elif isinstance(obj, str) and key and is_sensitive(key) and len(obj) >= MIN_LEN:
        out.add(obj)


def emit_masks(value):
    count = 0
    for line in value.splitlines() or [value]:  # a command is single-line
        if len(line) >= MIN_LEN:
            sys.stdout.write("::add-mask::" + line + "\n")
            count += 1
    hexed = value.encode("utf-8").hex()  # contiguous even across newlines
    if len(hexed) >= MIN_LEN:
        sys.stdout.write("::add-mask::" + hexed + "\n")
        count += 1
    return count


def main(path):
    try:
        namespace = runpy.run_path(path)
    except BaseException as exc:  # never surface file contents
        sys.stderr.write(f"mask_parameters: cannot load {path}: {type(exc).__name__}\n")
        return 1

    secrets = set()
    for name, value in namespace.items():
        if not name.startswith("__"):
            collect(value, name, secrets)

    masks = sum(emit_masks(v) for v in secrets)
    sys.stderr.write(
        f"mask_parameters: {len(secrets)} values, {masks} masks registered\n"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1] if len(sys.argv) > 1 else "tests/parameters.py"))
