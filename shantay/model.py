from abc import abstractmethod, ABCMeta
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from typing import Any, Optional, Required, Self, TypedDict


from .collector import Collector
from .progress import NO_PROGRESS, Progress


# ================================================================================================
# Release, Daily, Monthly


class Release(metaclass=ABCMeta):

    @property
    @abstractmethod
    def id(self) -> str:
        """The ID."""

    @property
    @abstractmethod
    def first_daily(self) -> "Daily":
        """The first day."""

    @property
    @abstractmethod
    def last_daily(self) -> "Daily":
        """The last day."""

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

    @property
    @abstractmethod
    def monthly(self) -> "Monthly": ...

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


@dataclass(frozen=True, slots=True, eq=True, order=True)
class Daily(Release):

    year: int # type: ignore
    month: int # type: ignore
    day: int

    def __post_init__(self) -> None:
        assert 1600 <= self.year <= 3000
        assert 1 <= self.month <= 12
        assert 1 <= self.day <= _days_in_month(self.year, self.month)

    @classmethod
    def of(cls, date: str | dt.date | dt.datetime) -> Self:
        """
        Create a new daily occurrence from the given string, date, or date
        time.
        """
        if isinstance(date, str):
            date = dt.date.fromisoformat(date)
        return cls(date.year, date.month, date.day)

    @property
    def id(self) -> str:
        """The ID."""
        return f"{self.year}-{self.month:02}-{self.day:02}"

    # @property
    # def ymd(self) -> tuple[int, int, int]:
    #     """Get the year, month, and day as a tuple."""
    #     return self.year, self.month, self.day

    @property
    def first_daily(self) -> "Daily":
        return self

    @property
    def last_daily(self) -> "Daily":
        return self

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
        return Path(f"{self.year}") / f"{self.month:02}" / "{self.day:02}.tmp"

    @property
    def batch_glob(self) -> str:
        """Get a glob for all batch files for the release."""
        return f"{self.year}/{self.month:02}/{self.day:02}/{self.id}-?????.parquet"

    def to_full_first_month(self) -> "Monthly":
        monthly = Monthly(self.year, self.month)
        if self.day != 1:
            monthly = monthly.next()
        return monthly

    def to_full_last_month(self) -> "Monthly":
        monthly = Monthly(self.year, self.month)
        if self.day != _days_in_month(self.year, self.month):
            monthly = monthly.previous()
        return monthly

    def to_date(self) -> dt.date:
        return dt.date(self.year, self.month, self.day)

    @property
    def monthly(self) -> "Monthly":
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
    def first_daily(self) -> "Daily":
        return Daily(self.year, self.month, 1)

    @property
    def last_daily(self) -> "Daily":
        return Daily(self.year, self.month, _days_in_month(self.year, self.month))

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

    @property
    def monthly(self) -> Self:
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
class DailyRange:

    first: Daily
    last: Daily

    def __post_init__(self) -> None:
        assert self.first <= self.last

    def __iter__(self) -> Iterator[Daily]:
        cursor = self.first
        last = self.last
        while True:
            yield cursor
            if cursor == last:
                break
            cursor = cursor.next()


@dataclass(frozen=True, slots=True)
class MonthlyRange:

    first: Monthly
    last: Monthly

    def __post_init__(self) -> None:
        assert self.first <= self.last

    def __iter__(self) -> Iterator[Monthly]:
        cursor = self.first
        last = self.last
        while True:
            yield cursor
            if cursor == last:
                break
            cursor = cursor.next()


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
KEYWORDS_FILE = "meta-keywords.parquet"
PLATFORMS_FILE = "meta-platforms.parquet"
STATISTICS_FILE = "meta-statistics.parquet"
DIGEST_FILE = "sha256.txt"


class MetadataEntry(TypedDict, total=False):
    batch_count: Required[int]
    total_rows: Optional[int]
    total_rows_with_keywords: Optional[int]
    batch_rows: Optional[int]
    batch_rows_with_keywords: Optional[int]
    batch_memory: Optional[int]
    sha256: Optional[str]


class FullMetadataEntry(MetadataEntry):
    release: str


@dataclass(frozen=True, slots=True)
class Coverage[R: Release]:
    """The matter of interest."""

    first: R
    last: R
    # Really: str | pl.Expr
    filter: str | object

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

    @property
    @abstractmethod
    def extract_data_step_count(self) -> int:
        """The number of steps for extracting data."""

    @abstractmethod
    def extract_file_data(
        self,
          *,
        root: Path,
        release: R,
        index: int,
        name: str,
        filter: str | Any,
        progress: Progress = NO_PROGRESS,
    ) -> tuple[str, Counter]:
        """Extract working data from an uncompressed data."""

    @abstractmethod
    def analyze_release(
        self, root: Path, release: Release, metadata: MetadataEntry, collector: Collector
    ) -> None:
        """
        Analyze a release's data. The release may have a different type than the
        dataset's native release.
        """

    @abstractmethod
    def combine_releases[T: Release](
        self,
        root: Path,
        coverage: Coverage[T],
        collector: Collector,
    ) -> Any:
        """
        Combine the analysis results. The release may have a different type than
        the dataset's native release.
        """


# ================================================================================================
# Storage


@dataclass(frozen=True, slots=True)
class Storage:
    """The current storage locations."""

    archive_root: Path
    working_root: Path
    staging_root: Path


# ================================================================================================
# Storage


class ConfigError(Exception):
    """An invalid configuration option."""


class DownloadFailed(Exception):
    """A download ended in a status code other than 200."""


class MetadataConflict(Exception):
    """Inconsistent metadata while merging."""
