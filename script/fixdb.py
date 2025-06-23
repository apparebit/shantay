#!.venv/bin/python

import argparse
import datetime as dt
from pathlib import Path
import shutil
import sys
from typing import Any, cast

sys.path.insert(0, '')

import polars as pl

from shantay.dsa_sor import StatementsOfReasons
from shantay.metadata import Metadata
from shantay.model import ConfigError, Coverage, Daily, file_stem_for, Release, Storage
from shantay.processor import Processor
from shantay.progress import Progress
from shantay.stats import Statistics


def configure(argv: list[str]) -> tuple[Storage, Metadata, Any]:
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
        "--check",
        action="store_true",
        help="check counts",
    )
    parser.add_argument(
        "--print",
        action="store_true",
        help="print 500 relevant rows of the updated summary statistics"
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
        staging_root=Path.cwd() / "dsa-db-staging"
    )

    try:
        metapath = Metadata.find_file(storage.the_extract_root)
        metadata = Metadata.read_json(metapath)
    except FileNotFoundError:
        raise ConfigError("--extract directory does not contain metadata")

    if metadata.category is None:
        raise ConfigError("metadata in --extract directory has no category")

    pl.Config.set_tbl_cols(20)
    pl.Config.set_tbl_rows(500)
    pl.Config.set_thousands_separator(",")

    return storage, metadata, options


def recompute(storage: Storage, release: Daily) -> tuple[int, int, int, int]:
    dataset = StatementsOfReasons()
    coverage = Coverage(release, release, None)
    metadata = Metadata()
    progress = Progress()
    processor = Processor(
        dataset=dataset,
        storage=storage,
        coverage=coverage,
        metadata=metadata,
        offline=True,
        progress=progress,
    )

    processor.stage_archive(release)
    filenames = processor.list_archived_files(storage.staging_root, release)
    progress.activity(
        f"process batches from release {release.id}",
        f"process {release.id}",
        "batch",
        with_rate=False,
    )
    progress.start(len(filenames))

    total_rows1 = total_rows2 = 0
    total_rows_with_keywords1 = total_rows_with_keywords2 = 0

    for index, name in enumerate(filenames):
        progress.step(index, "unarchive data")
        processor.unarchive_file(storage.staging_root, release, index, name)

        frame = dataset.ingest_database_data(
            root=storage.staging_root,
            release=release,
            index=index,
            name=name,
            progress=progress,
        )

        total_rows1 += frame.height
        total_rows_with_keywords1 += frame.select(
            pl.col("category_specification").is_not_null().sum()
        ).item()

        path = storage.staging_root / release.temp_directory
        csv_files = f"{path}/sor-global-{release.id}-full-{index:05}-*.csv"
        ttl, kw = dataset.get_total_row_counts(csv_files, index, name)

        total_rows2 += ttl
        total_rows_with_keywords2 += kw

        shutil.rmtree(storage.staging_root / release.temp_directory)

    shutil.rmtree(storage.staging_root / release.parent_directory)
    progress.perform("")

    return (
        total_rows1, total_rows_with_keywords1, total_rows2, total_rows_with_keywords2
    )


def main(argv: list[str]) -> int:
    # Determine configuration, instantiate metadata and summary statistics
    try:
        storage, metadata, options = configure(argv)
    except ConfigError as x:
        print(x.args[0])
        return 1

    frame = pl.read_parquet(storage.the_archive_root / "db.parquet")

    # Limit summary statistics to intersection with metadata
    range = metadata.range
    frame = frame.filter(
        (range.first <= pl.col("start_date")) & (pl.col("end_date") <= range.last)
    )

    # Determine indices of relevant counts
    index_matrix = frame.select(
        pl.col("column").eq("batch_count").arg_true().alias("batch_count"),
        pl.col("column").eq("batch_rows").arg_true().alias("batch_rows"),
        pl.col("column").eq("batch_rows_with_keywords").arg_true()
        .alias("batch_rows_with_keywords"),
        pl.col("column").eq("total_rows").arg_true().alias("total_rows"),
        pl.col("column").eq("total_rows_with_keywords").arg_true()
        .alias("total_rows_with_keywords"),
    )

    # Process indices
    is_data_ok = True
    for index_row in index_matrix.iter_rows():
        # Get counts and metadata entry
        date = frame[index_row[0], "start_date"]
        print(f"{date}")

        batch_count = frame[index_row[0], "count"]
        batch_rows = frame[index_row[1], "count"]
        batch_rows_with_keywords = frame[index_row[2], "count"]
        total_rows = frame[index_row[3], "count"]
        total_rows_with_keywords = frame[index_row[4], "count"]
        md_entry = metadata[date]

        # Check consistency of summary statistics and metadata
        assert batch_count == 0
        assert total_rows == 0
        assert total_rows_with_keywords == 0
        assert md_entry.get("total_rows") == batch_rows

        # If inconsistent, recompute counts from CSV and frame
        kw = batch_rows_with_keywords
        kw2 = md_entry.get("total_rows_with_keywords")
        if kw != kw2:
            print(f"    stats={kw:>10,}   meta={kw2:>10,}")

            if not options.check:
                is_data_ok = False
            else:
                ttl1, kw3, ttl2, kw4 = recompute(storage, cast(Daily, Release.of(date)))
                assert ttl1 == batch_rows
                assert ttl2 == batch_rows

                # Check counts
                if kw != kw3:
                    is_data_ok = False
                    print(f"    stats={kw:>10,}  frame={kw3:>10,}")
                if kw != kw4:
                    print(f"    stats={kw:>10,}    csv={kw4:>10,}")#

        if is_data_ok:
            # Patch summary statistics
            frame[index_row[0], "count"] = md_entry["batch_count"]
            frame[index_row[3], "count"] = batch_rows
            frame[index_row[4], "count"] = batch_rows_with_keywords

            # Patch metdata
            md_entry["total_rows_with_keywords"] = kw

    if options.print:
        print(frame.filter(
            pl.col("column").is_in([
                "batch_count",
                "batch_rows", "batch_rows_with_keywords",
                "total_rows", "total_rows_with_keywords",
            ])
        ).select(
            pl.col("start_date", "end_date", "tag", "platform", "column", "count")
        ))

    if not is_data_ok:
        if options.check:
            msg = "recomputed counts also are inconsistent"
        else:
            msg = "did not recompute counts"
        if options.store:
            msg += "; cannot store data"
        else:
            msg = "; nothing else to do"
        print(msg)
    elif options.store:
        assert metadata.category is not None
        stem = file_stem_for(metadata.category)

        # Save frame
        path = storage.staging_root / "db.parquet"
        tmp = path.with_suffix(".tmp.parquet")
        frame.rechunk().write_parquet(tmp)
        tmp.replace(path)
        Statistics.copy("db.parquet", storage.staging_root, storage.the_archive_root)

        # Save metadata
        source = storage.staging_root / f"{stem}.json"
        target = storage.the_extract_root / f"{stem}.json"
        metadata.write_json(source)
        metadata.copy_json(source, target)

        print(f"wrote updated db.parquet and {stem}.json!")

    return not is_data_ok

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
