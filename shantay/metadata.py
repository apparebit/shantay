from collections.abc import Iterator
import datetime as dt
import json
import logging
from pathlib import Path
import re
import shutil
from typing import Callable, cast, Self

# from .framing import below within method
from .digest import compute_digest, read_digest_file, write_digest_file
from .model import (
    Coverage, DateRange, DIGEST_FILE, FullMetadataEntry, MetadataConflict,
    MetadataEntry, Release
)
from .progress import NO_PROGRESS, Progress
from .schema import is_category_file


_logger = logging.getLogger(__spec__.parent)


class Metadata[R: Release]:

    __slots__ = ("_category", "_releases")

    def __init__(
        self,
        category: None | str = None,
        releases: None | dict[str, MetadataEntry] = None,
    ) -> None:
        self._category = category
        self._releases = releases or {}

    @property
    def category(self) -> None | str:
        """Get the category for the corresponding extract."""
        return self._category

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
        return Coverage(Release.of(range.first), Release.of(range.last), self.category)

    def set_category(self, category: str) -> None:
        """Set the not yet configured category."""
        if self._category is None:
            self._category = category
        elif self._category != category:
            raise MetadataConflict(f"categories {self._category} and {category} differ")

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

    def without_category(self) -> Self:
        """Create a stripped down version suitable for the archive root."""
        def strip(data: MetadataEntry) -> MetadataEntry:
            return {
                "batch_count": data["batch_count"],
                "total_rows": data.get("total_rows"),
                "total_rows_with_keywords": data.get("total_rows_with_keywords"),
            }

        return type(self)(
            None,
            {
                k: strip(v)
                for k, v in self._releases.items()
            }
        )

    @classmethod
    def merge(cls, *sources: None | Path, not_exist_ok: bool = False) -> Self:
        """Merge the metadata from the given metadata files."""
        merged = cls()
        for source in sources:
            if source is None:
                continue
            try:
                source_data = cls.read_json(source)
            except FileNotFoundError:
                if not_exist_ok:
                    continue
                raise
            merged._merge_category(source_data._category)
            merged._merge_releases(source_data._releases)
        return merged

    def merge_with(self, other: Self) -> Self:
        """Merge with the other metadata."""
        merged = type(self)(self._category, dict(self._releases))
        merged._merge_category(other._category)
        merged._merge_releases(other._releases)
        return merged

    def _merge_category(self, other: None | str) -> None:
        if other is None:
            pass
        elif self._category is None or self._category == other:
            self._category = other
        else:
            raise MetadataConflict(f"divergent categories {self._category} and {other}")

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
                "sha256",
            ):
                # Copy over missing fields, check existing fields for consistency
                if key not in entry1 and key in entry2:
                    entry1[key] = entry2[key] # type: ignore
                elif key in entry1 and key in entry2 and entry1[key] != entry2[key]: # type: ignore
                    mismatch = True

            if mismatch:
                raise MetadataConflict(f"divergent metadata for release {release}")

    @classmethod
    def find_file(cls, directory: Path) -> Path:
        """
        Find the JSON file with metadata. This method considers all files that
        are named after a statement category, ignoring case and allowing dashes
        or spaces instead of underscores, followed by the ".json" extension. It
        does *not* recognize "db.json". It is an error if the directory contains
        more or less than one file matching the criteria.
        """
        files = []
        for file in directory.glob("*.json"):
            if is_category_file(file.stem):
                files.append(file)

        if len(files) == 0:
            raise FileNotFoundError(
                f'directory "{directory}" does not contain metadata file'
            )
        if len(files) != 1:
            raise FileNotFoundError(
                f'directory "{directory}" contains more than one metadata file:\n'
                f'{", ".join(f.stem for f in files)}'
            )

        return files[0]

    @classmethod
    def read_json(cls, file: Path) -> Self:
        """Read the given file as metadata."""
        with open(file, mode="r", encoding="utf8") as stream:
            data = json.load(stream)
        category = data["category"]
        releases = data["releases"]
        return cls(category, releases)

    def write_json(self, file: Path, *, sort_keys: bool = False) -> None:
        """Write the metadata to the given file."""
        tmp = file.with_suffix(".tmp.json")
        with open(tmp, mode="w", encoding="utf8") as handle:
            json.dump({
                "category": self._category,
                "releases": self._releases
            }, handle, indent=2, sort_keys=sort_keys)
        tmp.replace(file)

    @classmethod
    def copy_json(cls, source: Path, target: Path) -> None:
        """Copy the metadata from source to target files."""
        tmp = target.with_suffix(".tmp.json")
        shutil.copy(source, tmp)
        tmp.replace(target)

    def __repr__(self) -> str:
        return f"Metadata({self._category}, {len(self._releases):,} releases)"


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
            path = Metadata.find_file(self._root)
            self._metadata = Metadata.read_json(path)
        except FileNotFoundError:
            self._metadata = Metadata()

        _logger.info('scanning root directory="%s"', self._root)
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
            path = self._root / "fsck.json"
            _logger.info('wrote result of successful scan to file="%s"', path)
            self._metadata.write_json(path)
            self._progress.perform(
                f'wrote "fsck.json" with updated metadata to "{self._root}"'
            )
            print()
            return self._metadata

        # There were errors. Metadata may still be useful, so save under another name.

        with open(Path.cwd() / "bad-fsck.json", mode="w", encoding="utf8") as file:
            json.dump({
                "category": self._metadata._category,
                "releases": self._metadata._releases
            }, file, indent=2)

        self._progress.perform(
            'wrote "bad-fsck.json" with recovered metadata to current directory'
        )
        print()

        raise ExceptionGroup(
            f'category-specific extract in "{self._root}" has problems', self._errors
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

        try:
            expected_digests = read_digest_file(day / DIGEST_FILE)
        except FileNotFoundError:
            expected_digests = None
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
            write_digest_file(day / DIGEST_FILE, actual_digests)

        if self._metadata._category is None and 0 < batch_no:
            self.update_category(f"{day}/*.parquet")

        digest_of_digests = None
        if (day / DIGEST_FILE).exists():
            digest_of_digests = compute_digest(day / DIGEST_FILE)

        year_no = int(day.parent.parent.name)
        month_no = int(day.parent.name)
        day_no = int(day.name)
        self.update_batch_count(year_no, month_no, day_no, batch_no, digest_of_digests)

        _logger.info('checked batch-count=%d, directory="%s"', batch_no, day)

    def update_category(self, glob: str) -> None:
        """
        Scan data frames matching glob to extract only category name. If the
        frames do not have a unique category name, do nothing.
        """
        from .framing import distill_category_from_parquet
        category = distill_category_from_parquet(glob)
        if category:
            self._metadata._category = category

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
