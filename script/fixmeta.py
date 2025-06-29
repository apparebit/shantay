#!.venv/bin/python

# Generate db.json in staging based on the first category-specific metadata
# file.

import argparse
from pathlib import Path
import sys
from typing import Any

sys.path.insert(0, '')

import polars as pl

from shantay.metadata import Metadata
from shantay.model import ConfigError
from shantay.schema import is_category_file


def configure(argv: list[str]) -> Any:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--staging",
        type=Path,
        help="the staging directory (optional)"
    )
    parser.add_argument(
        "--store",
        action="store_true",
        help="store updated summary statistics and metadata"
    )

    options = parser.parse_args(argv)
    if options.staging is None:
        options.staging = Path.cwd() / "dsa-db-staging"

    pl.Config.set_tbl_cols(20)
    pl.Config.set_tbl_rows(500)
    pl.Config.set_thousands_separator(",")

    return options


def main(argv: list[str]) -> int:
    # Determine configuration, instantiate metadata and summary statistics
    try:
        options = configure(argv)
    except ConfigError as x:
        print(x.args[0])
        return 1

    if not options.store:
        print("performing dry run; use --store to persist changes")

    metadata = None
    for file in options.staging.glob("*.json"):
        if file.name == "db.json" or not is_category_file(file.stem):
            continue

        metadata = Metadata.read_json(file)
        break

    if metadata is None:
        print("ERROR: could not find category-specific metadata in staging")
        return 1

    metadata._category = None
    for value in metadata._releases.values():
        if "batch_rows" in value:
            del value["batch_rows"]
        if "batch_rows_with_keywords" in value:
            del value["batch_rows_with_keywords"]
        if "sha256" in value:
            del value["sha256"]

    if options.store:
        metadata.write_json(options.staging / "db.json", sort_keys=True)
        print(f"wrote {options.staging / 'db.json'}")

    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
