#!.venv/bin/python

# Prepare metadata .json and summary statistics .parquet files for v0.4 by
# removing batch_memory entries. Process the staging, archive, and extract
# root directories.

import argparse
import datetime as dt
import itertools
from pathlib import Path
import sys
from typing import Any

sys.path.insert(0, '')

import polars as pl

from shantay.metadata import Metadata
from shantay.model import ConfigError, Storage
from shantay.schema import is_category_file


def configure(argv: list[str]) -> tuple[Storage, Any]:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--archive",
        type=Path,
        help="the archive directory (required)",
    )
    parser.add_argument(
        "--extract",
        type=Path,
        help="the extract directory (required)",
    )
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

    if options.archive is None:
        raise ConfigError("please specify --archive directory")
    if options.extract is None:
        raise ConfigError("please specify --extract directory")

    storage = Storage(
        archive_root=options.archive,
        extract_root=options.extract,
        staging_root=options.staging or Path.cwd() / "dsa-db-staging"
    )

    pl.Config.set_tbl_cols(20)
    pl.Config.set_tbl_rows(500)
    pl.Config.set_thousands_separator(",")

    return storage, options


def main(argv: list[str]) -> int:
    # Determine configuration, instantiate metadata and summary statistics
    try:
        storage, options = configure(argv)
    except ConfigError as x:
        print(x.args[0])
        return 1

    if not options.store:
        print("performing dry run; use --store to persist changes")

    is_all_ok = True
    for file in itertools.chain(
        storage.staging_root.glob("*.parquet"),
        [storage.the_archive_root / "db.parquet"],
        storage.the_extract_root.glob("*.parquet"),
    ):
        if file.name != "db.parquet" and not is_category_file(file.stem):
            continue

        frame = pl.read_parquet(file)
        original_height = frame.height
        days = frame.select(
            pl.col("end_date").max() - pl.col("start_date").min() + dt.timedelta(days=1)
        ).item().days

        frame = frame.filter(
            pl.col("column").ne("batch_memory")
        )
        updated_height = frame.height

        if original_height - days != updated_height:
            is_all_ok = False
            print(
                f"ERROR: {original_height:10,} - {days:5,} "
                f"!= {updated_height:10,} for {file}"
            )
        elif options.store:
            tmp = file.with_suffix(".tmp.parquet")
            frame.write_parquet(tmp)
            tmp.replace(file)
            print(f"fixed {file}")
        else:
            print(f"processed {file}")

    for file in itertools.chain(
        storage.staging_root.glob("*.json"),
        [storage.the_archive_root / "db.json"],
        storage.the_extract_root.glob("*.json"),
    ):
        if not file.exists() or (
            file.name != "db.json" and not is_category_file(file.stem)
        ):
            continue

        metadata = Metadata.read_json(file)
        for value in metadata._releases.values():
            if "batch_memory" in value:
                del value["batch_memory"]

        if options.store:
            metadata.write_json(file, sort_keys=True)
            print(f"fixed {file}")
        else:
            print(f"processed {file}")

    return not is_all_ok


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
