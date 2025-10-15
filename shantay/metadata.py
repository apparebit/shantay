from collections.abc import Iterator
import datetime as dt
import json
import logging
from pathlib import Path
import re
import shutil
from typing import Callable, cast, Self

from .digest import compute_digest, read_digest_file, write_digest_file
from .model import (
    Daily, DateRange, DIGEST_FILE, file_stem_for, Filter, FullMetadataEntry,
    MetadataConflict, MetadataEntry, Release, ReleaseRange
)
from .progress import NO_PROGRESS, Progress


JSON_SCHEMA_ID = "https://apparebit.com/schema/shantay-metadata.schema.json"

_FILE_TYPE = re.compile(
    br'^\{\s*"@schema":\s*"https://apparebit.com/schema/shantay-metadata\.schema\.json",',
    re.DOTALL
)
_IS_FILE_BUFFER_LENGTH = 128
_logger = logging.getLogger(__spec__.parent)


class Metadata[R: Release]:
    """
    Meta data about (an extract from) the EU's DSA transparency database.

    Meta data comprises the file stem, an optional filter, and a map of covered
    releases. The filter can be a statement category name or an arbitrary
    Pola.rs expression. Stem and filter must be consistent for statement
    categories.

    The implementation internally maintains a mapping from releases as ISO date
    strings to metadata entries, without release properties. Methods that merge
    entries only preserve well-known properties in canonical order.
    """

    __slots__ = ("_stem", "_filter", "_releases")

    def __init__(
        self,
        stem: str,
        filter: None | Filter = None,
        releases: None | dict[str, MetadataEntry] = None,
    ) -> None:
        self._stem = stem
        self._filter = filter
        self._releases = releases or {}

        if stem in ("builtin", "db"):
            if filter is not None:
                raise ValueError("metadata for full database has filter")
        else:
            if filter is None:
                raise ValueError("metadata for database extract has no filter")

    def with_stem_and_filter(self) -> Self:
        """Create a new metadata instance that has the same stem and filter."""
        return type(self)(self._stem, self._filter)

    def with_release_only(self, release: Release) -> Self:
        """Create a new metadata instance that is limited to the given release."""
        r = str(release)
        return type(self)(
            self._stem,
            self._filter,
            {r: cast(MetadataEntry, dict(self._releases[r]))},
        )

    @property
    def stem(self) -> str:
        """Get the file name stem for this meta data instance."""
        return self._stem

    @property
    def filter(self) -> None | Filter:
        """Get the filter for a data extract."""
        return self._filter

    def tag(self) -> None | str:
        """Get a tag for this filter."""
        return None if self.filter is None else self.filter.tag()

    @property
    def records(self) -> Iterator[FullMetadataEntry]:
        """Get an iterator over the release records."""
        for release, entry in self._releases.items():
            yield fill_entry(release, entry) # type: ignore

    def _label_range(self) -> None | tuple[str, str]:
        if len(self._releases) == 0:
            return None

        first = last = None
        for release in self._releases.keys():
            if first is None or release < first:
                first = release
            if last is None or last < release:
                last = release

        assert first is not None
        assert last is not None
        return first, last

    @property
    def release_range(self) -> None | ReleaseRange[Daily]:
        """Get the range of releases as textual labels."""
        range = self._label_range()
        if range is None:
            return None

        return ReleaseRange(
            cast(Daily, Release.of(range[0])),
            cast(Daily, Release.of(range[1])),
        )

    @property
    def date_range(self) -> None | DateRange:
        """Get the date for the first and last release."""
        range = self._label_range()
        if range is None:
            return None

        return DateRange(
            dt.date.fromisoformat(range[0]),
            dt.date.fromisoformat(range[1]),
        )

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

    def without_filter(self) -> Self:
        """
        Create a stripped down version of the metadata suitable for the full
        database. The stem is `db` and the filter is none.
        """
        def strip(data: MetadataEntry) -> MetadataEntry:
            return {
                "batch_count": data["batch_count"],
                "total_rows": data.get("total_rows"),
                "total_rows_with_keywords": data.get("total_rows_with_keywords"),
                "extract_rows": data.get("total_rows"),
                "extract_rows_with_keywords": data.get("total_rows_with_keywords"),
            }

        return type(self)("db", None, {k: strip(v) for k, v in self._releases.items()})

    @classmethod
    def merge(cls, *sources: Path) -> Self:
        """
        Merge the metadata from given source paths. All files must have the same
        stem and filter. This method ignores if some source paths do not exist.
        However, if all source paths do not exist, this method raises a
        `FileNotFoundError`.
        """
        assert 0 < len(sources), "no source paths given"

        merged = None
        for source in sources:
            try:
                source_data = cls.read_json(source)
            except FileNotFoundError:
                continue
            if merged is None:
                merged = source_data
            else:
                merged._merge_stem(source_data._stem)
                merged._merge_filter(source_data._filter)
                merged._merge_releases(source_data._releases)

        if merged is None:
            raise FileNotFoundError(*sources)
        else:
            return merged

    def merge_with(self, other: Self) -> Self:
        """
        Create a new metadata instance covering the releases of this and the
        other instances.
        """
        merged = type(self)(self._stem, self._filter, dict(self._releases))
        merged._merge_stem(other._stem)
        merged._merge_filter(other._filter)
        merged._merge_releases(other._releases)
        return merged

    def _merge_stem(self, other: None | str) -> None:
        if self._stem != other:
            raise MetadataConflict(f"divergent stems {self._stem} and {other}")

    def _merge_filter(self, other: None | Filter) -> None:
        if self._filter != other:
            raise MetadataConflict(f"divergent filters {self._filter} and {other}")

    def _merge_releases(self, other: dict[str, MetadataEntry]) -> None:
        for release, entry2 in other.items():
            self.merge_release(release, entry2, strict=True)

    def merge_release(
        self, release: str | R, other: MetadataEntry, strict: bool = True
    ) -> bool:
        """
        Merge the given release metadata entry into this metadata instance.
        Missing properties are copied over. In strict mode, this method checks
        that properties present in both entries have the same value and raises
        an exception otherwise. This method returns a flag indicating whether
        this metadata instance was modified.
        """
        release = str(release)
        this = self._releases.get(release, {})
        result = {}

        updated = False
        mismatched = []

        # Rebuild entry to ensure canonical ordering of properties...
        for key in (
            "batch_count",
            "extract_rows",
            "extract_rows_with_keywords",
            "total_rows",
            "total_rows_with_keywords",
            "sha256",
        ):
            if key in this:
                if strict and key in other and this[key] != other[key]: # type: ignore
                    mismatched.append(key)
                else:
                    result[key] = this[key] # type: ignore
            elif key in other:
                result[key] = other[key] # type: ignore
                updated = True

        if mismatched:
            raise MetadataConflict(
                f"divergent metadata for release {release} "
                f"on field(s) {", ".join(mismatched)}"
            )

        # ... but only update this metadata instance if the entry changed.
        if updated:
            self._releases[release] = result # type: ignore
        return updated

    @classmethod
    def is_file(cls, file: Path) -> bool:
        """
        Determine whether the file contains metadata for Shantay. This method
        checks the `@schema` key and its value without parsing the JSON format.
        """
        if file.suffix != ".json":
            return False
        with open(file, mode="rb") as handle:
            return _FILE_TYPE.match(handle.read(_IS_FILE_BUFFER_LENGTH)) is not None

    @classmethod
    def find_file(cls, directory: Path) -> Path:
        """Find the metadata file in the given directory."""
        files = []
        for file in directory.glob("*.json"):
            if file.stem.endswith(".tmp") or file.stem.endswith(".bak"):
                continue
            if cls.is_file(file):
                files.append(file)

        match len(files):
            case 0:
                raise FileNotFoundError(
                    f'directory "{directory}" does not contain metadata file'
                )
            case 1:
                return files[0]
            case _:
                raise MetadataConflict(
                    f'directory "{directory}" contains more than one metadata file'
                )

    @classmethod
    def read_json(cls, file: Path) -> Self:
        """Read the given file as metadata."""
        with open(file, mode="r", encoding="utf8") as stream:
            data = json.load(stream)

        schema = data.get("@schema")
        if schema != JSON_SCHEMA_ID:
            raise ValueError(f'"{file}" is not a valid metadata file for Shantay')

        config = data["config"]
        f = config["filter"]
        filter = None if f is None else Filter.from_json(f)
        stem = config["stem"]

        releases = data["releases"]

        return cls(stem, filter, releases)

    def write_json(self, file: Path) -> None:
        """Write the metadata to the given file."""
        if self._stem != file.stem:
            raise ValueError(
                f"inconsistent stems {file} in path and {self._stem} in data"
            )

        tmp = file.with_suffix(".tmp.json")
        with open(tmp, mode="w", encoding="utf8") as handle:
            json.dump({
                "@schema": JSON_SCHEMA_ID,
                "config": {
                    "stem": self._stem,
                    "filter": None if self._filter is None else self._filter.to_json(),
                },
                "releases": dict(sorted(self._releases.items())),
            }, handle, indent=2)
            handle.write("\n")
        tmp.replace(file)

    @classmethod
    def copy_json(cls, source: Path, target: Path) -> None:
        """Copy the metadata from source to target files."""
        tmp = target.with_suffix(".tmp.json")
        shutil.copy(source, tmp)
        tmp.replace(target)

    def __repr__(self) -> str:
        return f"Metadata({self._stem}, {len(self._releases):,} releases)"


def fill_entry(release: str | Daily, entry: MetadataEntry) -> FullMetadataEntry:
    """
    Fill in the release. If the metadata entry does not yet have a `release`
    property, this function creates a new entry with the release and the all
    other properties of the given entry. If the entry already has a `release`
    property, this function checks that both releases are the same before
    returning the entry unmodified.
    """
    release = str(release)
    raw_entry = cast(dict[str, object], entry)
    if "release" in raw_entry:
        if raw_entry["release"] != release:
            raise MetadataConflict(
                f"metadata release {raw_entry["release"]} does not match {release}"
            )
        return cast(FullMetadataEntry, raw_entry)

    return cast(FullMetadataEntry, {"release": release} | raw_entry)


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
            self._metadata = Metadata("fsck")

        _logger.info('scanning root directory="%s"', self._root)
        years = self.scandir(self._root, "????", _FOUR_DIGITS)
        self.check_children(self._root, years, 2000, 2100, int)

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

        path = self._root / "fsck.json"
        self._metadata.write_json(path)
        _logger.info('wrote result of scan to file="%s"', path)
        print(f'saved results of scan to "{path}"')
        if len(self._errors) == 0:
            return self._metadata

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

        digest_of_digests = None
        if (day / DIGEST_FILE).exists():
            digest_of_digests = compute_digest(day / DIGEST_FILE)

        year_no = int(day.parent.parent.name)
        month_no = int(day.parent.name)
        day_no = int(day.name)
        self.update_batch_count(year_no, month_no, day_no, batch_no, digest_of_digests)

        _logger.info('checked batch-count=%d, directory="%s"', batch_no, day)

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
