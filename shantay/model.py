from abc import abstractmethod, ABCMeta
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
import datetime as dt
from pathlib import Path
import re
from typing import (
    cast, Literal, Optional, overload, Protocol, Required, Self, TypedDict
)

from .progress import NO_PROGRESS, Progress


# The model does touch upon Pola.rs data frames. Define the necessary types
# here, but also delete the package reference thereafter. We want data wrangling
# logic to be contained.
import polars
type DataFrameType = polars.DataFrame
type LazyFrameType = polars.LazyFrame
type QueryExpression = polars.Expr
del polars


# ================================================================================================
# Release, Daily, Monthly


class Period(metaclass=ABCMeta):

    @property
    @abstractmethod
    def start_date(self) -> dt.date: ...

    @property
    @abstractmethod
    def end_date(self) -> dt.date: ...


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
    def date(self) -> None | dt.date:
        """The only date if this release is a daily one. Otherwise `None`."""

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
    def to_monthly(self) -> "Monthly": ...

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
        return f"{self.year}/{self.month:02}/{self.day:02}/{self.id}-?????.parquet"

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

    def to_monthly(self) -> "Monthly":
        return Monthly(self.year, self.month)

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

    def to_monthly(self) -> Self:
        return self

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
        assert self.first <= self.last

    @property
    def duration(self) -> int:
        return self.last - self.first + 1

    @property
    def start_date(self) -> dt.date:
        return self.first.start_date

    @property
    def end_date(self) -> dt.date:
        return self.last.end_date

    def date_range(self) -> 'DateRange':
        return DateRange(self.start_date, self.end_date)

    def to_monthly(self) -> "ReleaseRange[Monthly]":
        if isinstance(self.first, Monthly):
            return cast(ReleaseRange[Monthly], self)
        else:
            return ReleaseRange(self.first.to_monthly(), self.last.to_monthly())

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

    def __and__(self, other: object) -> None | Self:
        if isinstance(other, type(self)):
            return self.intersection(other)
        return NotImplemented

    def __or__(self, other: object) -> Self:
        if isinstance(other, type(self)):
            return self.union(other)
        return NotImplemented

    def uncovered_near_past(self) -> None | Self:
        """
        Compute the date range following this date range up to two days before
        today.
        """
        first = self.last + dt.timedelta(days=1)
        last = dt.date.today() - dt.timedelta(days=2)
        return type(self)(first, last) if first <= last else None

    def dailies(self) -> ReleaseRange[Daily]:
        """Convert to the corresponding daily release range."""
        return ReleaseRange(Daily.of(self.first), Daily.of(self.last))

    def monthlies(self) -> ReleaseRange[Monthly]:
        """Convert to a monthly release range with fully covered months."""
        return ReleaseRange(
            Daily.of(self.first).to_first_full_month(),
            Daily.of(self.last).to_last_full_month(),
        )

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


META_FILE = "meta.json"
DIGEST_FILE = "sha256.txt"


class MetadataEntry(TypedDict, total=False):
    batch_count: Required[int]
    total_rows: Optional[int]
    batch_rows: Optional[int]
    batch_memory: Optional[int]
    sha256: Optional[str]

    # Specific to DSA SoR DB
    total_rows_with_keywords: Optional[int]
    batch_rows_with_keywords: Optional[int]


class FullMetadataEntry(MetadataEntry):
    release: str


@dataclass(frozen=True, slots=True)
class Coverage[R: Release]:
    """The matter of interest."""

    first: R
    last: R
    filter: None | str | QueryExpression

    def __post_init__(self) -> None:
        assert self.first <= self.last

    def __iter__(self) -> Iterator[R]:
        cursor = self.first
        while True:
            yield cursor
            if cursor == self.last:
                break
            cursor = cursor.next()

    def __len__(self) -> int:
        return self.last - self.first + 1

    def to_date_range(self) -> DateRange:
        return DateRange(self.first.start_date, self.last.end_date)


type StatSource = Literal[None, "archive", "working"]


class CollectorProtocol[R: Release](Protocol):
    """The protocol for incremental data frame generation."""

    # The name of the main statistics frame.
    STATISTICS = "stats"

    def collect(
        self,
        release: Release,
        frame: DataFrameType | LazyFrameType,
        tag: None | str = None,
        metadata: None | DataFrameType = None,
    ) -> None:
        """
        Collect summary statistics for the data frame.

        This method should collect the standard statistics for the given data
        frame. If the tag is none, it should also collect statistics about the
        relationship between the data frame and the complete data set. In
        particular, if no metadata is provided, this method should assume that
        the frame is part of the complete data set. If, however, metadata is
        provided, the frame contains working data only.
        """

    def frame(
        self, validate: bool = False, group_by: None | Literal["day", "month"] = None
    ) -> DataFrameType:
        """
        Combine all summary statistics collected so far into one data frame.
        Optionally validate the summary statistics. Also, optionally group by
        day or month. It is an error to try grouping-by-day summary statistics
        collected at monthly granularity.
        """
        ...


class Dataset[R: Release](metaclass=ABCMeta):
    """A specific dataset."""

    @property
    @abstractmethod
    def name(self) -> str:
        """The dataset name."""

    @abstractmethod
    def url(self, filename: str) -> str:
        """The URL for the release."""

    @abstractmethod
    def archive_name(self, release: R) -> str:
        """The archive file name for the release."""

    @abstractmethod
    def digest_name(self, release: R) -> str:
        """The digest file name for the release."""

    @abstractmethod
    def ingest_file_data(
        self,
        *,
        root: Path,
        release: R,
        index: int,
        name: str,
        progress: Progress = NO_PROGRESS,
    ) -> DataFrameType:
        """Ingest unfiltered, uncompressed data."""

    @abstractmethod
    def extract_file_data(
        self,
        *,
        root: Path,
        release: R,
        index: int,
        name: str,
        filter: str | QueryExpression,
        progress: Progress = NO_PROGRESS,
    ) -> tuple[str, Counter]:
        """Extract working data from an uncompressed data."""

    @abstractmethod
    def analyze_release(
        self,
        root: Path,
        release: Release,
        metadata: DataFrameType,
        collector: CollectorProtocol
    ) -> None:
        """
        Analyze a release's data. The release period need not be the original
        release period and, in fact, is likely to be coarser.
        """

    @abstractmethod
    def combine_releases[T: Release](
        self,
        root: Path,
        coverage: Coverage[T],
        collector: CollectorProtocol,
    ) -> DataFrameType:
        """
        Combine the analysis results. The release period is the same as for
        analysis. This method may return more than one named data frame.
        """


# ================================================================================================
# Storage


@dataclass(frozen=True, slots=True)
class Storage:
    """The current storage locations."""

    archive_root: Path
    working_root: Path
    staging_root: Path

    def isolate(self, worker: int) -> Self:
        """Isolate the work by assigning a unique staging root."""
        return type(self)(
            self.archive_root,
            self.working_root,
            self.staging_root.with_suffix(f".{worker}")
        )


# ================================================================================================
# Exceptions


class ConfigError(Exception):
    """An invalid configuration option."""


class DownloadFailed(Exception):
    """A download ended in a status code other than 200."""


class MetadataConflict(Exception):
    """Inconsistent metadata while merging."""
