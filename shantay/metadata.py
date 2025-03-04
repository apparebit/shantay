from collections.abc import Iterator
import datetime as dt
import json
from pathlib import Path
import re
import shutil
from typing import Callable, Required, Self, TypedDict

import polars as pl


from .model import MetadataConflict, Release


class Entry(TypedDict, total=False):
    batch_count: Required[int]


class FullEntry(Entry):
    release: str


class Metadata[R: Release]:

    FILENAME = "meta.json"

    __slots__ = ("_filter", "_releases")

    def __init__(
        self,
        filter: None | str = None,
        releases: None | dict[str, Entry] = None,
    ) -> None:
        self._filter = filter
        self._releases = releases or {}

    @property
    def filter(self) -> None | str:
        """Get the filter for the working set."""
        return self._filter

    @property
    def records(self) -> Iterator[FullEntry]:
        """Get an iterator over the release records."""
        return (dict(release=k) | v for k, v in self._releases.items())

    @property
    def range(self) -> tuple[dt.date, dt.date]:
        """Get the date for the first and last release."""
        if len(self._releases) == 0:
            raise ValueError("no coverage available")
        releases = sorted(self._releases)
        return releases[0].date, releases[-1].date

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

    def __getitem__(self, key: R) -> Entry:
        """Get the entry for the given release."""
        return self._releases[str(key)]

    def __setitem__(self, key: R, value: Entry) -> None:
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
            if not_exist_ok and not (source / cls.FILENAME).exists():
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

    def _merge_releases(self, other: dict[str, Entry]) -> None:
        for release, entry2 in other.items():
            if release not in self._releases:
                self._releases[release] = entry2
                continue

            entry1 = self._releases[release]
            if entry1["batch_count"] == entry2["batch_count"]:
                if 1 == len(entry1) and 1 == len(entry2):
                    continue
                if 1 == len(entry1) and 1 < len(entry2):
                    self._releases[release] = entry2
                    continue
                elif 1 < len(entry1) and 1 == len(entry2):
                    continue
                elif all(
                    entry1.get(k) == entry2.get(k) for k in (
                        "total_rows",
                        "total_rows_with_keywords",
                        "batch_rows",
                        "batch_rows_with_keywords"
                    )
                ):
                    continue

            raise MetadataConflict(f"divergent metadata for release {release}")

    @classmethod
    def read_json(cls, root: Path) -> Self:
        with open(root / cls.FILENAME, mode="r", encoding="utf8") as file:
            data = json.load(file)
        return cls(data["filter"], data["releases"])

    def write_json(self, root: Path, *, sort_keys: bool = False) -> None:
        path = root / self.FILENAME
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
        path = target / cls.FILENAME
        tmp = path.with_suffix(".tmp.json")
        shutil.copy(source / cls.FILENAME, tmp)
        tmp.replace(path)

    @classmethod
    def recover(cls, root: Path, *, verbose: bool = False) -> Self:
        """
        Recover the batch_count data for all daily releases stored under the
        root directory.

        This method inspects all directories and files matching the naming
        convention for storing daily releases, i.e.,
        "YYYY/mm/dd/YYYY-mm-dd-nnnnn.parquet", with the one-based months and
        days always two decimal digits and the zero-based sequence numbers in
        batch files always five decimal digits. It reports file system entities
        that are files but should be directories and vice versa, empty
        directories, month and day numbers that are out of range (accounting for
        different months having different numbers of days, including February in
        leap years), as well as missing month, day, and sequence numbers.
        """
        return _DailyFileSystemScan(root, cls(), verbose=verbose).run()


_TWO_DIGITS = re.compile(r"^[0-9]{2}$")
_FOUR_DIGITS = re.compile(r"^[0-9]{4}$")
_BATCH_FILE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}-[0-9]{5}.parquet$")

class _DailyFileSystemScan:
    def __init__(self, root: Path, metadata: Metadata, *, verbose: bool = False) -> None:
        self._root = root
        self._first_date = None
        self._last_date = None
        self._metadata = metadata
        self._errors = []
        self._verbose = verbose

    def error(self, msg: str) -> None:
        self._errors.append(msg)
        if self._verbose:
            print(f"ERROR: {msg}")

    def signal(self, msg: None | str = None) -> None:
        if msg:
            self.error(msg)
        if 0 < len(self._errors):
            raise ValueError("\n".join(self._errors))

    def run(self) -> Metadata:
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

                    day_no = int(day.name)
                    batches = self.scandir(day, "*.parquet", _BATCH_FILE)
                    self.check_children(day, batches, 0, 99_999, lambda n: int(n[-13:-8]))

                    batch_no = 0
                    for batch in batches:
                        if self.check_is_file(batch):
                            batch_no += 1

                    if self._metadata._filter is None and 0 < batch_no:
                        self.update_filter(day)
                    self.update_batch_count(year_no, month_no, day_no, batch_no)

        self.signal()
        return self._metadata

    def scandir(self, path: Path, glob: str, pattern: re.Pattern) -> list[Path]:
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
        index = None
        for child in children:
            current = extract(child.name)
            if not min_value <= current <= max_value:
                self.signal(f'"{child}" has invalid index')
            if index is None and min_value == 0 and current != 0:
                self.error(f'"{child}" has index other than 0')
            if index is not None and current != index:
                self.error(f'entries of "{path}" are not consecutively numbered')
            index = current + 1

    def check_is_directory(self, path: Path) -> bool:
        if path.is_dir():
            return True

        self.error(f'"{path}" is not a directory')
        return False

    def check_is_file(self, path: Path) -> bool:
        if path.is_file():
            return True

        self.error(f'"{path}" is not a file')
        return False

    def update_filter(self, path: Path) -> None:
        self._metadata._filter = (
            pl.scan_parquet(path)
            .select(
                pl.col("filter")
                .value_counts(sort=True)
                .first()
                .struct.field("filter")
            )
            .collect()
            .item()
        )

    def update_batch_count(
        self,
        year: int,
        month: int,
        day: int,
        batch_count: int,
    ) -> None:
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
        self._metadata[key] = { "batch_count": batch_count }
        if self._verbose:
            print(f"{key}: {batch_count:6,d}")


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
        Metadata.recover(Path(sys.argv[1]), verbose=True)
