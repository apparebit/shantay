import datetime as dt
import logging
from pathlib import Path

import polars as pl

from shantay.digest import validate_digests
from shantay.metadata import Metadata
from shantay.model import (
    Daily, DateRange, DIGEST_FILE, Filter, FilterKind, ReleaseRange, Storage
)


_logger = logging.getLogger(__package__)


class Repair:
    def __init__(
        self,
        storage: Storage,
        stem: None | str = None,
        filter: None | Filter = None,
    ) -> None:
        self._storage = storage
        self._stem = stem
        self._filter = filter
        self._error_list = []
        self._rearchive = []
        self._redistill = []

    @property
    def error_count(self) -> int:
        return len(self._error_list)

    def error(self, msg, *args) -> None:
        _logger.error(msg, *args)
        self._error_list.append(msg % args)

    def recreate_extract_metadata(self) -> None | Metadata:
        archive_metadata = Metadata.read_json(
            self._storage.the_archive_root / "db.json"
        )
        metadata_file = f"{self._stem}.json"
        assert self._stem is not None
        extract_metadata = Metadata(self._stem, self._filter)

        release_range = self.read_release_directory_range()
        if release_range is None:
            return None
        _logger.info(
            'extract root %s contains releases from %s to %s',
            self._storage.extract_root, *release_range
        )

        for release in release_range:
            if release not in archive_metadata:
                self.error('archive metadata does not include release %s', release)
                self._redistill.append(release)
                self._rearchive.append(release)
                continue

            directory = self._storage.the_extract_root / release.directory
            if not directory.exists():
                self.error('release directory "%s" is missing', directory)
                self._redistill.append(release)
                continue

            md_entry = archive_metadata[release].copy()
            try:
                md_entry["sha256"] = validate_digests(
                    directory,
                    release.batch_glob,
                    directory / DIGEST_FILE,
                    restore=True,
                )
            except Exception as x:
                self.error(x.args[0])
                self._redistill.append(release)

            batch_count = batch_rows = batch_kw_rows = 0
            for path in directory.glob(release.batch_glob):
                rows, kw_rows = pl.scan_parquet(path).select(
                    pl.len()
                        .alias("rows"),
                    pl.col("category_specification")
                        .is_not_null()
                        .sum()
                        .alias("kw_rows"),
                ).collect().row(0)

                batch_count += 1
                batch_rows += rows
                batch_kw_rows += kw_rows

            md_entry["batch_count"] = batch_count
            md_entry["batch_rows"] = batch_rows
            md_entry["batch_rows_with_keywords"] = batch_kw_rows
            extract_metadata[release] = md_entry
            extract_metadata.write_json(self._storage.staging_root / metadata_file)
            _logger.info(
                'release %s has %d batches with %d rows and %d rows with keywords',
                release, batch_count, batch_rows, batch_kw_rows
            )

        return extract_metadata

    def read_release_directory_range(self) -> None | ReleaseRange[Daily]:
        root = self._storage.the_extract_root

        limits = DateRange(
            dt.date(2023, 9, 25),
            dt.date.today() - dt.timedelta(days=2)
        ).dailies()

        first = limits.first
        while first <= limits.last and not (root / first.directory).exists():
            first = first.next()

        if limits.last < first:
            self.error('unable to locate data in extract root "%s"', root)
            return None

        last = limits.last
        while first <= last and not (root / last.directory).exists():
            last = last.previous()

        return ReleaseRange(first, last)


if __name__ == "__main__":
    digest = validate_digests(
        Path("/Volumes/dsa/protection-of-minors/2023/09/25"),
        "*.parquet",
        Path("/Volumes/dsa/protection-of-minors/2023/09/25/sha256.txt"),
    )

    print(digest)

    # repair = Repair(
    #     storage=Storage(
    #         Path("/Volumes/dsa/archive"),
    #         Path("/Volumes/dsa/protection-of-minors"),
    #         Path("dsa-db-staging"),
    #     ),
    #     stem="protection-of-minors",
    #     filter=Filter(FilterKind.CATEGORY, "STATEMENT_CATEGORY_PROTECTION_OF_MINORS"),
    # )

    # repair.recreate_extract_metadata()
    # if repair.error_count == 0:
    #     print("Happy, happy, joy, joy!")
    # else:
    #     print("\n".join(repair._error_list))
