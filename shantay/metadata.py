from collections.abc import Iterator
import datetime as dt
import json
from pathlib import Path
import re
import shutil
from typing import Callable, Required, Self, TypedDict

from .release import Release


class MetadataConflict(Exception):
    """Exception to indicate that metadata records for the same release differ."""


def _as_key(key: str | Release) -> str:
    return key if isinstance(key, str) else key.id


class Entry(TypedDict, total=False):
    batch_count: Required[int]


class FullEntry(Entry):
    release: str


def _get_days_in_month(year, month) -> int:
    month += 1
    if month == 13:
        year += 1
        month = 1
    return (dt.date(year, month, 1) - dt.timedelta(days=1)).day


class Metadata:

    FILENAME = "meta.json"

    __slots__ = ("_data",)

    def __init__(self, data: None | dict[str, Entry] = None) -> None:
        self._data = {} if data is None else data

    def batch_count(self, release: str | Release) -> int:
        return self[_as_key(release)]["batch_count"]

    def __contains__(self, key: str | Release) -> bool:
        return _as_key(key) in self._data

    def __getitem__(self, key: str | Release) -> Entry:
        return self._data[_as_key(key)]

    def __setitem__(self, key: str | Release, value: Entry) -> None:
        self._data[_as_key(key)] = value

    def __len__(self) -> int:
        return len(self._data)

    def records(self) -> Iterator[FullEntry]:
        return (dict(release=k) | v for k, v in self._data.items() if k != "meta")

    @classmethod
    def read_json(cls, root: Path) -> Self:
        with open(root / cls.FILENAME, mode="r", encoding="utf8") as file:
            return cls(json.load(file))

    def write_json(self, root: Path, *, sort_keys: bool = False) -> None:
        path = root / self.FILENAME
        tmp = path.with_suffix(".tmp.json")
        with open(tmp, mode="w", encoding="utf8") as file:
            json.dump(self._data, file, indent=2, sort_keys=sort_keys)
        tmp.replace(path)

    @classmethod
    def setup(cls, staging: Path, batches: Path) -> Self:
        # Read metadata
        staged_md = None
        batched_md = None
        if (staging / cls.FILENAME).exists():
            staged_md = cls.read_json(staging)
        if (batches / cls.FILENAME).exists():
            batched_md = cls.read_json(batches)

        # Merge metadata
        metadata = None
        if staged_md is None:
            if batched_md is None:
                metadata = cls()
            else:
                metadata = batched_md
        else:
            if batched_md is None:
                metadata = staged_md
            else:
                metadata = cls.merge(staging, batches)

        # Save best metadata in staging and return that metadata
        metadata.write_json(staging)
        return metadata

    @classmethod
    def copy_json(cls, source: Path, target: Path) -> None:
        path = target / cls.FILENAME
        tmp = path.with_suffix(".tmp.json")
        shutil.copy(source / cls.FILENAME, tmp)
        tmp.replace(path)

    @classmethod
    def merge(cls, *sources: Path) -> Self:
        merged = None
        for source in sources:
            source_data = cls.read_json(source)
            if merged is None:
                merged = source_data
                continue

            for k, v2 in source_data._data.items():
                if k not in merged:
                    merged[k] = v2
                    continue

                v1 = merged[k]
                if v1 == v2:
                    continue
                if v1["batch_count"] == v2["batch_count"]:
                    if 1 == len(v1) and 1 < len(v2):
                        merged[k] = v2
                        continue
                    if 1 < len(v1) and 1 == len(v2):
                        continue

                raise MetadataConflict(f"divergent metadata for {k.id}")

        return merged

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

                    for batch in batches:
                        self.check_is_file(batch)

                    self.update_batch_count(year_no, month_no, day_no, len(batches))

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


if __name__ == "__main__":
    import sys

    if len(sys.argv) != 2:
        print("ERROR: invoke as `python -m shantay.metadata <directory-to-scan>`")
    else:
        Metadata.recover(Path(sys.argv[1]), verbose=True)
