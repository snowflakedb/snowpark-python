#!/usr/bin/env python3
#
# Copyright (c) 2012-2025 Snowflake Computing Inc. All rights reserved.
#

"""
Generate large CSV test data for the CSVReader UDTF POC.

Default output size is approximately 1 GiB.
Supports both plain CSV and gzip CSV output:
- .csv    -> plain text CSV
- .csv.gz -> gzip-compressed CSV
"""

import argparse
import csv
import gzip
import os
import random
import string
from datetime import datetime, timedelta


def _rand_word(rng: random.Random, min_len: int = 4, max_len: int = 16) -> str:
    length = rng.randint(min_len, max_len)
    return "".join(rng.choice(string.ascii_lowercase) for _ in range(length))


def _rand_text_with_optional_comma(rng: random.Random) -> str:
    base = f"{_rand_word(rng)} {_rand_word(rng)} {_rand_word(rng)}"
    # Add commas in some rows to exercise CSV quoting behavior.
    if rng.random() < 0.35:
        return f"{base}, {_rand_word(rng)}"
    return base


def generate_csv(
    output_path: str,
    target_size_bytes: int,
    seed: int,
    include_header: bool,
    flush_every_rows: int,
) -> None:
    rng = random.Random(seed)
    start_ts = datetime(2023, 1, 1)
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)

    is_gzip_output = output_path.lower().endswith(".gz")
    open_func = gzip.open if is_gzip_output else open
    # target_size_bytes applies to the on-disk output file size:
    # compressed size for .gz, plain size for .csv.
    with open_func(output_path, "wt", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=",", quotechar='"', quoting=csv.QUOTE_MINIMAL)

        if include_header:
            writer.writerow(
                [
                    "id",
                    "event_ts",
                    "category",
                    "score",
                    "notes",
                    "country",
                    "active",
                    "payload",
                ]
            )

        row_id = 1
        size_check_every_rows = 5000
        while True:
            ts = start_ts + timedelta(seconds=row_id % 31_536_000)
            category = f"cat_{rng.randint(1, 50)}"
            score = f"{rng.random() * 1000:.6f}"
            notes = _rand_text_with_optional_comma(rng)
            country = rng.choice(
                ["US", "CA", "DE", "IN", "JP", "AU", "GB", "FR", "SG", "BR"]
            )
            active = "true" if (row_id % 2 == 0) else "false"
            payload = (
                f"key={_rand_word(rng, 3, 8)};"
                f"value={_rand_word(rng, 10, 24)};"
                f"tag={rng.randint(1, 9999)}"
            )

            writer.writerow(
                [row_id, ts.isoformat(sep=" "), category, score, notes, country, active, payload]
            )
            row_id += 1

            if flush_every_rows > 0 and row_id % flush_every_rows == 0:
                f.flush()
            if row_id % size_check_every_rows == 0:
                f.flush()
                if os.path.getsize(output_path) >= target_size_bytes:
                    break

        f.flush()

    size = os.path.getsize(output_path)
    print(f"Generated: {output_path}")
    print(f"Rows: {row_id - 1}")
    print(f"Size bytes: {size}")
    print(f"Size GiB: {size / (1024**3):.3f}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Generate a large CSV test file (.csv or .csv.gz)."
    )
    parser.add_argument(
        "--output",
        default="poc_files/csv_reader_test_1gb.csv.gz",
        help="Output path. Use .csv for plain CSV or .gz for gzip CSV (default: poc_files/csv_reader_test_1gb.csv.gz)",
    )
    parser.add_argument(
        "--target-size-gb",
        type=float,
        default=1.0,
        help="Approximate target output file size in GiB (default: 1.0)",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=42,
        help="Random seed for reproducible data (default: 42)",
    )
    parser.add_argument(
        "--no-header",
        action="store_true",
        help="Do not include header row.",
    )
    parser.add_argument(
        "--flush-every-rows",
        type=int,
        default=100000,
        help="Flush output every N rows (default: 100000).",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    target_size_bytes = int(args.target_size_gb * (1024**3))
    generate_csv(
        output_path=args.output,
        target_size_bytes=target_size_bytes,
        seed=args.seed,
        include_header=not args.no_header,
        flush_every_rows=max(0, args.flush_every_rows),
    )


if __name__ == "__main__":
    main()
