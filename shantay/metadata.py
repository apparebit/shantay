from collections.abc import Iterator
import datetime as dt
import hashlib
import json
import logging
from pathlib import Path
import re
import shutil
from typing import Callable, cast, Self

# from .framing import below within method
from .model import (
    Coverage, DateRange, DIGEST_FILE, FullMetadataEntry, META_FILE, MetadataConflict,
    MetadataEntry, Release
)
from .progress import NO_PROGRESS, Progress


_logger = logging.getLogger(__spec__.parent)


class Metadata[R: Release]:

    __slots__ = ("_filter", "_releases")

    def __init__(
        self,
        filter: None | str = None,
        releases: None | dict[str, MetadataEntry] = None,
    ) -> None:
        self._filter = filter
        self._releases = releases or {}

    @property
    def filter(self) -> None | str:
        """Get the filter for the working set."""
        return self._filter

    @property
    def records(self) -> Iterator[FullMetadataEntry]:
        """Get an iterator over the release records."""
        for release, entry in self._releases.items():
            yield cast(FullMetadataEntry, dict(release=release, **entry))

    @property
    def range(self) -> DateRange:
        """Get the date for the first and last release."""
        if len(self._releases) == 0:
            raise ValueError("no coverage available")
        releases = sorted(self._releases)
        return DateRange(
            dt.date.fromisoformat(releases[0]),
            dt.date.fromisoformat(releases[-1])
        )

    @property
    def coverage(self) -> Coverage:
        range = self.range
        return Coverage(Release.of(range.first), Release.of(range.last), self.filter)

    def set_filter(self, filter: str) -> None:
        """Set the not yet configured category."""
        if self._filter is None:
            self._filter = filter
        elif self._filter != filter:
            raise MetadataConflict(f"categories {self._filter} and {filter} differ")

    def batch_count(self, release: str | R) -> int:
        """Get the batch count for the given release."""
        return self._releases[str(release)]["batch_count"]

    def __contains__(self, key: R) -> bool:
        """Determine whether the given release has an entry."""
        return str(key) in self._releases

    def __getitem__(self, key: R) -> MetadataEntry:
        """Get the entry for the given release."""
        return self._releases[str(key)]

    def __setitem__(self, key: R, value: MetadataEntry) -> None:
        """Set the entry for the given release."""
        self._releases[str(key)] = value

    def __len__(self) -> int:
        """Get the number of releases covered."""
        return len(self._releases)

    @classmethod
    def merge(cls, *sources: Path, not_exist_ok: bool = False) -> Self:
        """Merge the metadata from the given directories."""
        merged = cls()
        for source in sources:
            if not_exist_ok and not (source / META_FILE).exists():
                continue
            source_data = cls.read_json(source)
            merged._merge_filter(source_data._filter)
            merged._merge_releases(source_data._releases)
        return merged

    def merge_with(self, other: Self) -> Self:
        """Merge with the other metadata."""
        merged = type(self)(self._filter, dict(self._releases))
        merged._merge_filter(other._filter)
        merged._merge_releases(other._releases)
        return merged

    def _merge_filter(self, other: None | str) -> None:
        if other is None:
            pass
        elif self._filter is None or self._filter == other:
            self._filter = other
        else:
            raise MetadataConflict(f"divergent categories {self._filter} and {other}")

    def _merge_releases(self, other: dict[str, MetadataEntry]) -> None:
        for release, entry2 in other.items():
            if release not in self._releases:
                self._releases[release] = entry2
                continue

            mismatch = False
            entry1 = self._releases[release]
            for key in (
                "batch_count",
                "total_rows",
                "total_rows_with_keywords",
                "batch_rows",
                "batch_rows_with_keywords",
                "batch_memory",
                "sha256",
            ):
                # Copy over missing fields, check existing fields for consistency
                if key not in entry1 and key in entry2:
                    entry1[key] = entry2[key] # type: ignore
                elif key == "batch_memory":
                    # Don't compare for equality since only an estimate
                    pass
                elif key in entry1 and key in entry2 and entry1[key] != entry2[key]: # type: ignore
                    mismatch = True

            if mismatch:
                raise MetadataConflict(f"divergent metadata for release {release}")

    @classmethod
    def read_json(cls, root: Path) -> Self:
        with open(root / META_FILE, mode="r", encoding="utf8") as file:
            data = json.load(file)
        filter = data["filter"]
        releases = data["releases"]
        return cls(filter, releases)

    def write_json(self, root: Path, *, sort_keys: bool = False) -> None:
        path = root / META_FILE
        tmp = path.with_suffix(".tmp.json")
        with open(tmp, mode="w", encoding="utf8") as file:
            json.dump({
                "filter": self._filter,
                "releases": self._releases
            }, file, indent=2, sort_keys=sort_keys)
        tmp.replace(path)

    @classmethod
    def copy_json(cls, source: Path, target: Path) -> None:
        """Copy the metadata in JSON format from source to target directory."""
        path = target / META_FILE
        tmp = path.with_suffix(".tmp.json")
        shutil.copy(source / META_FILE, tmp)
        tmp.replace(path)

    def __repr__(self) -> str:
        return f"Metadata({self._filter}, {len(self._releases):,} releases)"


def read_digest_file(directory: Path) -> None | dict[str, str]:
    """Read the text file with a list of batchfile digests."""
    digests = {}

    try:
        with open(directory / DIGEST_FILE, mode="r", encoding="utf8") as file:
            for line in file.readlines():
                digest, batchfile = line.strip().split(" ")
                digests[batchfile] = digest
        return digests
    except FileNotFoundError:
        return None


def write_digest_file(directory: Path, digests: dict[str, str]) -> None:
    """Write the text file with the list of batchfile digests."""
    path = directory / DIGEST_FILE
    tmp = path.with_suffix(".tmp.txt")

    with open(tmp, mode="w", encoding="utf8") as file:
        for batchfile, digest in digests.items():
            file.write(f"{digest} {batchfile}\n")

    tmp.replace(path)


def compute_digest(path: Path) -> str:
    """Compute the batchfile digest."""
    with open(path, mode="rb") as file:
        return hashlib.file_digest(file, "sha256").hexdigest()


def fsck(
    root: Path,
    *,
    progress: Progress = NO_PROGRESS,
) -> Metadata:
    """
    Validate the directory hierarchy at the given root.

    This function validates the directory hierarchy at the given root by
    checking the following properties:

      - Directories representing years have consecutive four digit names and
        are, in fact, directories
      - Directories representing months have consecutive two digit names between
        1 and 12 and are, in fact, directories
      - Directories representing days have consecutive two digit names between 1
        and the number of days for that particular month and are, in fact,
        directories
      - At most one monthly directory starts with a day other than 01
      - At most one monthly directory ends with a day other than that month's
        number of days.
      - A day's parquet files are, in fact, files and have consecutive indexes
        starting with 0.
      - The number of parquet files matches the `batch_count` property of that
        day's metadata record. If missing, it is automatically filled in.
      - The list of SHA-256 hashes for a day's parquet files matches the files'
        actual SHA-256 hashes. If missing, the list is automatically created.
    """
    return _Fsck(root, progress=progress).run()


_TWO_DIGITS = re.compile(r"^[0-9]{2}$")
_FOUR_DIGITS = re.compile(r"^[0-9]{4}$")
_BATCH_FILE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{5}.parquet$")

class _Fsck:
    """Validate a directory hierarchy of parquet files."""

    def __init__(
        self,
        root: Path,
        *,
        progress: Progress = NO_PROGRESS,
    ) -> None:
        self._root = root
        self._first_date = None
        self._last_date = None
        self._errors = []
        self._progress = progress
        self._throttle = 0

    def error(self, msg: str) -> None:
        """Record an error."""
        self._errors.append(ValueError(msg))
        _logger.error(msg)
        self._progress.perform(f"ERROR: {msg}")

    def run(self) -> Metadata:
        """Run the file system analysis."""
        try:
            self._metadata = Metadata.read_json(self._root)
        except FileNotFoundError:
            self._metadata = Metadata()

        years = self.scandir(self._root, "????", _FOUR_DIGITS)
        self.check_children(self._root, years, 1800, 3000, int)

        for year in years:
            if not self.check_is_directory(year):
                continue

            year_no = int(year.name)
            months = self.scandir(year, "??", _TWO_DIGITS)
            self.check_children(year, months, 1, 12, int)

            for month in months:
                if not self.check_is_directory(month):
                    continue

                month_no = int(month.name)
                days_in_month = _get_days_in_month(year_no, month_no)

                days = self.scandir(month, "??", _TWO_DIGITS)
                self.check_children(month, days, 1, days_in_month, int)

                for day in days:
                    if not self.check_is_directory(day):
                        continue

                    self.check_batch_files(day)

        # If there were no errors, save metadata and be done.
        if len(self._errors) == 0:
            self._metadata.write_json(self._root)
            self._progress.perform(
                f'wrote "meta.json" with updated metadata to "{self._root}"'
            )
            print()
            return self._metadata

        # There were errors. Metadata may still be useful, so save under another name.
        with open(Path.cwd() / "fsck.json", mode="w", encoding="utf8") as file:
            json.dump({
                "filter": self._metadata._filter,
                "releases": self._metadata._releases
            }, file, indent=2)

        self._progress.perform(
            'wrote "fsck.json" with recovered metadata to current directory'
        )
        print()

        raise ExceptionGroup(
            f'working data in "{self._root}" has problems', self._errors
        )

    def scandir(self, path: Path, glob: str, pattern: re.Pattern) -> list[Path]:
        """Scan the given directory with the glob and file name pattern."""
        children = sorted(p for p in path.glob(glob) if pattern.match(p.name))
        if len(children) == 0:
            self.error(f'directory "{path}" is empty')
        return children

    def check_children(
        self,
        path: Path,
        children: list[Path],
        min_value: int,
        max_value: int,
        extract: Callable[[str], int],
    ) -> None:
        """Check that children are indexed correctly."""
        index = None

        for child in children:
            current = extract(child.name)
            if not min_value <= current <= max_value:
                self.error(f'"{child}" has out-of-bounds index')
            if index is None and min_value == 0 and current != 0:
                # Only batch files have a min index of 0 and always start with it.
                self.error(f'"{child}" has non-zero index')
            if index is not None and current != index:
                self.error(f'"{child}" has non-consecutive index {current}')
            index = current + 1

    def check_is_directory(self, path: Path) -> bool:
        """Validate path is directory."""
        if path.is_dir():
            return True

        self.error(f'"{path}" is not a directory')
        return False

    def check_is_file(self, path: Path) -> bool:
        """Validate path is file."""
        if path.is_file():
            return True

        self.error(f'"{path}" is not a file')
        return False

    def check_batch_files(self, day: Path) -> None:
        # Determine error count so far.
        error_count = len(self._errors)

        batches = self.scandir(day, "*.parquet", _BATCH_FILE)
        self.check_children(day, batches, 0, 99_999, lambda n: int(n[-13:-8]))

        expected_digests = read_digest_file(day)
        actual_digests = {}

        batch_no = 0
        for batch in batches:
            if not self.check_is_file(batch):
                continue

            batch_no += 1

            actual_digests[batch.name] = actual = compute_digest(batch)
            if expected_digests is None:
                pass
            elif batch.name not in expected_digests:
                self.error(f'digest for "{batch}" is missing')
                expected_digests[batch.name] = actual
            elif expected_digests[batch.name] != actual:
                self.error(f'digests for "{batch}" don\'t match')

            self._throttle += 1
            if self._throttle % 47 == 0:
                self._progress.perform(f"scanned {batch}")

        if error_count == len(self._errors) and expected_digests is None:
            # Only write a new digest file if there were no errors and no file.
            write_digest_file(day, actual_digests)

        if self._metadata._filter is None and 0 < batch_no:
            self.update_filter(f"{day}/*.parquet")

        digest_of_digests = None
        if (day / DIGEST_FILE).exists():
            digest_of_digests = compute_digest(day / DIGEST_FILE)

        year_no = int(day.parent.parent.name)
        month_no = int(day.parent.name)
        day_no = int(day.name)
        self.update_batch_count(year_no, month_no, day_no, batch_no, digest_of_digests)

        _logger.info('checked batch-count=%d directory="%s"', batch_no, day)

    def update_filter(self, glob: str) -> None:
        """
        Scan data frames matching glob to extract only category name. If the
        frames do not have a unique category name, do nothing.
        """
        from .framing import extract_category_from_parquet
        category = extract_category_from_parquet(glob)
        if category:
            self._metadata._filter = category

    def update_batch_count(
        self,
        year: int,
        month: int,
        day: int,
        batch_count: int,
        digest_of_digests: None | str,
    ) -> None:
        """Update the batch count for a given release."""
        if batch_count == 0:
            return

        current = dt.date(year, month, day)
        if self._first_date is None:
            self._first_date = current

        if self._last_date is None:
            pass
        elif self._last_date + dt.timedelta(days=1) != current:
            self.error(
                f'daily releases between {self._last_date} and {current} (exclusive) are missing'
            )
        self._last_date = current

        key = f"{year}-{month:02}-{day:02}"
        if key not in self._metadata:
            # Just create entry from scratch.
            self._metadata[key] = {
                "batch_count": batch_count,
                "sha256": digest_of_digests
            }
            return

        # Entry exists: Validate existing properties and update missing ones.
        entry = self._metadata[key]
        for key, value in [
            ("batch_count", batch_count),
            ("sha256", digest_of_digests),
        ]:
            if key in entry:
                if entry[key] != value:
                    self.error(
                        f'metadata for {year}-{month:02}-{day:02} has field {key} '
                        f'with {value}, but was {entry[key]}'
                    )
            else:
                entry[key] = value


def _get_days_in_month(year, month) -> int:
    month += 1
    if month == 13:
        year += 1
        month = 1
    return (dt.date(year, month, 1) - dt.timedelta(days=1)).day


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("ERROR: invoke as `python -m shantay.metadata <directory-to-scan>`")
    else:
        fsck(Path(sys.argv[1]))
