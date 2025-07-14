from abc import abstractmethod, ABCMeta
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
import datetime as dt
from pathlib import Path
import re
from typing import (
    Callable, cast, Literal, Optional, overload, Protocol, Required, Self, TypedDict
)

from .progress import NO_PROGRESS, Progress


# The model does touch upon Pola.rs data frames. Define the necessary types
# here, but also delete the package reference thereafter. We want data wrangling
# logic to be contained.
import polars
type DataFrameType = polars.DataFrame
del polars


# ================================================================================================
# Release, Daily, Monthly


class Period(metaclass=ABCMeta):
    """
    The abstract base for all entities with specific start and end dates.
    Concrete subclasses include daily and monthly releases, release ranges, and
    date ranges.
    """

    @property
    @abstractmethod
    def start_date(self) -> dt.date: ...

    @property
    @abstractmethod
    def end_date(self) -> dt.date: ...

    @property
    def date(self) -> None | dt.date:
        """Get the only date, if this period covers just one day. Otherwise,
        return None."""
        return self.start_date if self.start_date == self.end_date else None

    def date_range(self) -> "DateRange":
        """Get this period's date range."""
        return DateRange(self.start_date, self.end_date)


_RELEASE = re.compile(r"(?P<year>[0-9]{4})-(?P<month>[0-9]{2})(?:-(?P<day>[0-9]{2}))?")

class Release(Period):

    @overload
    @staticmethod
    def of(year: int, month: int, day: int, /) -> "Daily": ...
    @overload
    @staticmethod
    def of(year: int, month: int, /) -> "Monthly": ...
    @overload
    @staticmethod
    def of(release: str, /) -> "Release": ...
    @overload
    @staticmethod
    def of(date: dt.date, /) -> "Daily": ...
    @staticmethod
    def of(
        year: int | str | dt.date,
        month: None | int = None,
        day: None | int = None,
        /
    ) -> "Release":
        if isinstance(year, dt.date):
            return Daily(year.year, year.month, year.day)
        if isinstance(year, int):
            assert month is not None
            if day is None:
                return Monthly(year, month)
            else:
                return Daily(year, month, day)
        match = _RELEASE.match(year)
        if match is None:
            raise ValueError(f'"{year}" does not denote a daily or monthly release')
        year = int(match.group("year"))
        month = int(match.group("month"))
        if match.group("day") is None:
            return Monthly(year, month)
        else:
            return Daily(year, month, int(match.group("day")))

    @property
    @abstractmethod
    def id(self) -> str:
        """The ID."""

    @property
    @abstractmethod
    def frequency(self) -> Literal["daily", "monthly"]:
        """The release frequency."""

    @property
    @abstractmethod
    def parent_directory(self) -> Path:
        """The parent directory"""

    @property
    @abstractmethod
    def directory(self) -> Path:
        """The directory for per-"""

    @property
    @abstractmethod
    def temp_directory(self) -> Path: ...

    def batch_file(self, index: int) -> str:
        """Get the name for the batch file with the given index."""
        if not 0 <= index <= 99_999:
            raise ValueError(f"batch {index} is out of permissible range")
        return f"{self.id}-{index:05}.parquet"

    @property
    @abstractmethod
    def batch_glob(self) -> str: ...

    @abstractmethod
    def next(self) -> Self: ...

    @abstractmethod
    def __sub__(self, other: object) -> int: ...

    @abstractmethod
    def __eq__(self, other: object) -> bool: ...
    @abstractmethod
    def __lt__(self, other: object) -> bool: ...
    @abstractmethod
    def __le__(self, other: object) -> bool: ...
    @abstractmethod
    def __gt__(self, other: object) -> bool: ...
    @abstractmethod
    def __ge__(self, other: object) -> bool: ...

    def __str__(self) -> str:
        return self.id


@dataclass(frozen=True, slots=True, eq=True, order=True)
class Daily(Release):

    year: int # type: ignore
    month: int # type: ignore
    day: int

    def __post_init__(self) -> None:
        assert 1600 <= self.year <= 3000
        assert 1 <= self.month <= 12
        assert 1 <= self.day <= _days_in_month(self.year, self.month)

    @property
    def id(self) -> str:
        """The ID."""
        return f"{self.year}-{self.month:02}-{self.day:02}"

    @property
    def frequency(self) -> Literal["daily", "monthly"]:
        return "daily"

    @property
    def start_date(self) -> dt.date:
        return dt.date(self.year, self.month, self.day)

    @property
    def end_date(self) -> dt.date:
        return dt.date(self.year, self.month, self.day)

    @property
    def date(self) -> None | dt.date:
        return dt.date(self.year, self.month, self.day)

    @property
    def parent_directory(self) -> Path:
        """The directory for monthly artifacts."""
        return Path(f"{self.year}") / f"{self.month:02}"

    @property
    def directory(self) -> Path:
        """The directory for daily artifacts."""
        return Path(f"{self.year}") / f"{self.month:02}" / f"{self.day:02}"

    @property
    def temp_directory(self) -> Path:
        """A temporary directory for grouping *per* period files."""
        return Path(f"{self.year}") / f"{self.month:02}" / f"{self.day:02}.tmp"

    @property
    def batch_glob(self) -> str:
        """Get a glob for all batch files for the release."""
        return f"{self.id}-?????.parquet"

    def to_first_full_month(self) -> "Monthly":
        monthly = Monthly(self.year, self.month)
        if self.day != 1:
            monthly = monthly.next()
        return monthly

    def to_last_full_month(self) -> "Monthly":
        monthly = Monthly(self.year, self.month)
        if self.day != _days_in_month(self.year, self.month):
            monthly = monthly.previous()
        return monthly

    def __sub__(self, other: object) -> int:
        if type(other) is Daily:
            return (
                dt.date(self.year, self.month, self.day)
                - dt.date(other.year, other.month, other.day)
            ).days
        return NotImplemented

    def previous(self) -> Self:
        year = self.year
        month = self.month
        day = self.day - 1
        if day == 0:
            month -= 1
            day = _days_in_month(year, month)
            if month == 0:
                year -= 1
                month = 12
        return type(self)(year, month, day)

    def next(self) -> Self:
        year = self.year
        month = self.month
        day = self.day + 1
        if _days_in_month(year, month) < day:
            month += 1
            day = 1
            if 12 < month:
                year += 1
                month = 1
        return type(self)(year, month, day)


@dataclass(frozen=True, slots=True, eq=True, order=True)
class Monthly(Release):

    year: int
    month: int

    def __post_init__(self) -> None:
        assert 1600 <= self.year <= 3000
        assert 1 <= self.month <= 12

    @property
    def id(self) -> str:
        return f"{self.year}-{self.month:02}"

    @property
    def frequency(self) -> Literal["daily", "monthly"]:
        return "monthly"

    @property
    def start_date(self) -> dt.date:
        return dt.date(self.year, self.month, 1)

    @property
    def end_date(self) -> dt.date:
        return dt.date(self.year, self.month, _days_in_month(self.year, self.month))

    @property
    def date(self) -> None | dt.date:
        return None

    @property
    def parent_directory(self) -> Path:
        """The directory for monthly artifacts."""
        return Path(f"{self.year}")

    @property
    def directory(self) -> Path:
        """The directory for daily artifacts."""
        return Path(f"{self.year}") / f"{self.month:02}"

    @property
    def temp_directory(self) -> Path:
        """A temporary directory for grouping *per* period files."""
        return Path(f"{self.year}") / f"{self.month:02}.tmp"

    @property
    def batch_glob(self) -> str:
        """Get a glob for all batch files for the release."""
        return f"{self.year}/{self.month:02}/??/{self.year}-{self.month:02}-??-?????.parquet"

    def previous(self) -> Self:
        year = self.year
        month = self.month - 1
        if month == 0:
            year -= 1
            month = 12

        return type(self)(year, month)

    def next(self) -> Self:
        year = self.year
        month = self.month + 1
        if 12 < month:
            year += 1
            month = 1

        return type(self)(year, month)

    def __sub__(self, other: object) -> int:
        if type(other) == Monthly:
            return (self.year - other.year) * 12 + self.month - other.month

        return NotImplemented


@dataclass(frozen=True, slots=True)
class ReleaseRange[R: Release](Period):

    first: R
    last: R

    def __post_init__(self) -> None:
        assert self.first <= self.last, "first must come before last"

    @property
    def duration(self) -> int:
        return self.last - self.first + 1

    @property
    def start_date(self) -> dt.date:
        return self.first.start_date

    @property
    def end_date(self) -> dt.date:
        return self.last.end_date

    def __iter__(self) -> Iterator[R]:
        cursor = self.first
        last = self.last
        while True:
            yield cursor
            if cursor == last:
                break
            cursor = cursor.next()


@dataclass(frozen=True, slots=True)
class DateRange(Period):

    first: dt.date
    last: dt.date

    @property
    def start_date(self) -> dt.date:
        """
        Return the first date. This alias exists to turn date ranges into
        periods.
        """
        return self.first

    @property
    def end_date(self) -> dt.date:
        """
        Return the last date. This alias exists to turn date ranges into
        periods.
        """
        return self.last

    # Explicitly passing empty_ok=False buys us a tighter return type
    @overload
    def intersection(self, other: Self, *, empty_ok: Literal[False]) -> Self:
        ...
    @overload
    def intersection(self, other: Self, *, empty_ok: bool = ...) -> None | Self:
        ...
    def intersection(self, other: Self, *, empty_ok: bool = True) -> None | Self:
        """
        Compute the intersection between two date ranges. The result is `None`,
        if the two ranges do not overlap,
        """
        result = type(self)(max(self.first, other.first), min(self.last, other.last))
        if result.first <= result.last:
            return result
        elif empty_ok:
            return None
        else:
            raise ValueError(
                f"intersection of date ranges {self} and {other} is empty"
            )

    def union(self, other: Self) -> Self:
        """Compute the union between two date ranges."""
        return type(self)(min(self.first, other.first), max(self.last, other.last))

    def uncovered_near_past(self) -> None | Self:
        """
        Compute the date range following this date range up to two days before
        today.
        """
        first = self.last + dt.timedelta(days=1)
        last = dt.date.today() - dt.timedelta(days=3)
        return type(self)(first, last) if first <= last else None

    def to_limits(self) -> tuple[dt.date, dt.date]:
        """Convert the date range to a tuple of the first and last dates."""
        return self.first, self.last

    def dailies(self) -> ReleaseRange[Daily]:
        """Convert to the corresponding daily release range."""
        return ReleaseRange(Daily.of(self.first), Daily.of(self.last))

    def monthlies(self) -> ReleaseRange[Monthly]:
        """Convert to a monthly release range with fully covered months."""
        return ReleaseRange(
            Daily.of(self.first).to_first_full_month(),
            Daily.of(self.last).to_last_full_month(),
        )

    def __and__(self, other: object) -> None | Self:
        if isinstance(other, type(self)):
            return self.intersection(other)
        return NotImplemented

    def __or__(self, other: object) -> Self:
        if isinstance(other, type(self)):
            return self.union(other)
        return NotImplemented

    def __len__(self) -> int:
        return (self.last - self.first).days + 1

    def __iter__(self) -> Iterator[dt.date]:
        cursor = self.first
        while cursor <= self.last:
            yield cursor
            cursor += dt.timedelta(days=1)

    def __str__(self) -> str:
        return f"{self.first.isoformat()}-{self.last.isoformat()}"


def _days_in_month(year: int, month: int) -> int:
    if month in (4, 6, 9, 11):
        return 30
    elif month == 2:
        return 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28
    else:
        return 31


# ================================================================================================
# Dataset, Coverage


DIGEST_FILE = "sha256.txt"


def file_stem_for(entity: str) -> str:
    if entity.startswith("STATEMENT_CATEGORY_"):
        entity = entity[len("STATEMENT_CATEGORY_"):]
    return entity.lower().replace("_", "-").replace(" ", "-")


class MetadataEntry(TypedDict, total=False):
    batch_count: Required[int]
    total_rows: Optional[int]
    batch_rows: Optional[int]
    sha256: Optional[str]

    # Specific to DSA SoR DB
    total_rows_with_keywords: Optional[int]
    batch_rows_with_keywords: Optional[int]


class FullMetadataEntry(MetadataEntry):
    release: str


@dataclass(frozen=True, slots=True)
class Coverage[R: Release](Period):
    """The matter of interest."""

    first: R
    last: R
    category: None | str

    @classmethod
    def of(cls, range: ReleaseRange, category: None | str = None) -> Self:
        return cls(range.first, range.last, category)

    def __post_init__(self) -> None:
        assert self.first <= self.last

    @property
    def start_date(self) -> dt.date:
        return self.first.start_date

    @property
    def end_date(self) -> dt.date:
        return self.last.end_date

    def frequency(self) -> Literal["daily", "monthly"]:
        assert self.first.frequency == self.last.frequency
        return self.first.frequency

    def __iter__(self) -> Iterator[R]:
        cursor = self.first
        while True:
            yield cursor
            if cursor == self.last:
                break
            cursor = cursor.next()

    def __len__(self) -> int:
        return self.last - self.first + 1

    def stem(self) -> str:
        return "db" if self.category is None else file_stem_for(self.category)


class CollectorProtocol(Protocol):
    """The protocol for incremental data frame generation."""

    # The name of the main statistics frame.
    STATISTICS = "stats"

    def collect(
        self,
        release: Release,
        frame: DataFrameType,
        tag: None | str = None,
        metadata_entry: None | MetadataEntry = None,
    ) -> None:
        """
        Collect summary statistics for the data frame.

        This method should collect the standard statistics for the given data
        frame. For a frame with data from the full database, the tag and
        metadata should be omitted. For a frame with category-specific data,
        the tag should be the category and the metadata should be the result
        of extracting the category-specific data.
        """

    def frame(self, validate: bool = False) -> DataFrameType:
        """
        Combine all summary statistics collected so far into one data frame.
        Optionally validate the summary statistics. Also, optionally group by
        day or month. It is an error to try grouping-by-day summary statistics
        collected at monthly granularity.
        """
        ...


class Dataset(metaclass=ABCMeta):
    """A specific dataset."""

    @property
    @abstractmethod
    def name(self) -> str:
        """The dataset name."""

    @abstractmethod
    def url(self, filename: str) -> str:
        """The URL for the release."""

    @abstractmethod
    def archive_name(self, release: Daily) -> str:
        """The archive file name for the release."""

    @abstractmethod
    def digest_name(self, release: Daily) -> str:
        """The digest file name for the release."""

    @abstractmethod
    def ingest_database_data(
        self,
        *,
        root: Path,
        release: Daily,
        index: int,
        name: str,
        progress: Progress = NO_PROGRESS,
    ) -> tuple[Counter, DataFrameType]:
        """Ingest unfiltered, uncompressed data."""

    @abstractmethod
    def distill_category_data(
        self,
        *,
        root: Path,
        release: Daily,
        index: int,
        name: str,
        category: str,
        progress: Progress = NO_PROGRESS,
    ) -> tuple[str, Counter]:
        """Extract category-specific data from uncompressed data."""

    @abstractmethod
    def summarize_release(
        self,
        root: Path,
        release: Daily,
        category: str,
        metadata_entry: MetadataEntry,
        collector: CollectorProtocol
    ) -> None:
        """
        Analyze a release's data. The release period need not be the original
        release period and, in fact, is likely to be coarser.
        """


# ================================================================================================
# Storage


_TWO_DIGITS = re.compile(r"^[0-9]{2}$")
_FOUR_DIGITS = re.compile(r"^[0-9]{4}$")
_ARCHIVE = re.compile(r"^sor-global-[0-9]{4}-[0-9]{2}-[0-9]{2}-full\.zip$")


def _archive_as_number(path: Path) -> int:
    return int(path.name[-11:-9])


def _file_as_number(path: Path) -> int:
    return int(path.name)


def _select_dir_entries(
    directory: Path, pattern: re.Pattern, key: Callable[[Path], int]
) -> list[Path]:
    return sorted(
        (p for p in directory.glob("*") if pattern.match(p.name)),
        key=key
    )


def _find_coverage(directory: Path, is_extract: bool) -> None | DateRange:
    day_pattern = _TWO_DIGITS if is_extract else _ARCHIVE
    day_key = _file_as_number if is_extract else _archive_as_number

    years = _select_dir_entries(directory, _FOUR_DIGITS, _file_as_number)
    year_number = len(years)
    if year_number == 0:
        return None
    months = _select_dir_entries(years[0], _TWO_DIGITS, _file_as_number)
    month_number = len(months)
    if month_number == 0:
        return None
    days = _select_dir_entries(months[0], day_pattern, day_key)
    if len(days) == 0:
        return None

    first = dt.date(
        _file_as_number(years[0]),
        _file_as_number(months[0]),
        day_key(days[0])
    )

    if 1 < year_number:
        months = _select_dir_entries(years[-1], _TWO_DIGITS, _file_as_number)
        if len(months) == 0:
            return None
    if 1 < year_number or 1 == year_number and 1 < month_number:
        days = _select_dir_entries(months[-1], day_pattern, day_key)
        if len(days) == 0:
            return None

    last = dt.date(
        _file_as_number(years[-1]),
        _file_as_number(months[-1]),
        day_key(days[-1])
    )

    return DateRange(first, last)


@dataclass(frozen=True, slots=True)
class Storage:
    """The current storage locations."""

    archive_root: None | Path
    extract_root: None | Path
    staging_root: Path

    def isolate(self, worker: int) -> Self:
        """Isolate the work by assigning a unique staging root."""
        return type(self)(
            self.archive_root,
            self.extract_root,
            self.staging_root.with_suffix(f".{worker}")
        )

    @property
    def the_archive_root(self) -> Path:
        """
        The non-null archive root. If the archive root is null, the
        implementation throws an exception.
        """
        if self.archive_root is None:
            raise ValueError('no extract root available')
        return self.archive_root

    @property
    def the_extract_root(self) -> Path:
        """
        The non-null extract root. If the extract root is null, the
        implementation throws an exception.
        """
        if self.extract_root is None:
            raise ValueError('no extract root available')
        return self.extract_root

    @property
    def best_available_root(self) -> Path:
        if self.extract_root is not None:
            return self.extract_root
        if self.archive_root is not None:
            return self.archive_root
        return self.staging_root

    def coverage_of_archive(self) -> None | DateRange:
        """Determine the date coverage of the archive based on directory names."""
        if self.archive_root is None:
            return None
        return _find_coverage(self.archive_root, False)

    def coverage_of_extract(self) -> None | DateRange:
        """Determine the date coverage of the extract based on directory names."""
        if self.extract_root is None:
            return None
        return _find_coverage(self.extract_root, True)


# ================================================================================================
# Exceptions


class ConfigError(Exception):
    """An invalid configuration option."""


from ._platform import DownloadFailed as DownloadFailed


class MetadataConflict(Exception):
    """Inconsistent metadata while merging."""


class StagingIsBusy(Exception):
    """The staging directory is already in use."""
