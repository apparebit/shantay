from abc import abstractmethod, ABCMeta
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from typing import Any, Self


from .collector import Collector
from .progress import NO_PROGRESS, Progress


class Release(metaclass=ABCMeta):
    """
    A release capturing the periodical aspects of a dataset.

    # Daily vs Monthly Releases

    To better adjust to working set size and statistical requirements, the
    period for analysis can be adjusted between, for now, daily and monthly
    releases. All code works the same, only the start and stop releases change
    granularities.

    To make this as seamless as possible, monthly releases are second-class.
    They are always backed by a daily release, only the monthly facade blocks
    out access to the day. That way, it is possible to to convert a daily
    release to a monthly one and back again without loss of information: The
    final daily release is the same as the original daily release.

    Alas, loss of information may still occur when converting a daily release
    for, say, the 31st of January, March, May, August, October, or December to a
    monthly release and then accessing the next month before converting back to
    a daily release. Since the second month is shorter, the logic incrementing
    the month also adjusts the day of the backing daily release, decrementing it
    to 28, 29, or 30, depending on the month and leap year.

    This approach can be easily extended to cover weeks and quarters, too. If
    you need them for your analysis, [please file an
    issue](https://github.com/apparebit/shantay/issues/new/choose).
    """

    @property
    @abstractmethod
    def id(self) -> str:
        """
        A unique identifier for this release that can be used in a file name,
        e.g., "2000-03-30" for a daily release.
        """

    @property
    @abstractmethod
    def parent_directory(self) -> Path:
        """
        A directory for grouping files at release granularity, e.g., "2000/03"
        for a daily release on any day in March 2000.
        """

    @property
    @abstractmethod
    def directory(self) -> Path:
        """
        A directory for grouping *per* release files, e.g., "2000/03/30" for a
        daily release on March 30, 2000.
        """

    @property
    def temp_directory(self) -> Path:
        """A temporary directory for grouping *per* period files."""
        return self.directory.with_suffix(".tmp")

    def batch_file(self, index: int) -> str:
        if not 0 <= index <= 99_999:
            raise ValueError(f"batch {index} is out of permissible range")
        return f"{self.id}-{index:05}.parquet"

    @property
    def batch_glob(self) -> str:
        year = self.year
        month = self.month
        return f"{year}/{month:02}/??/{year}-{month:02}-??-*.parquet"

    @property
    @abstractmethod
    def ymd(self) -> tuple[int, int, int]: ...

    @property
    def daily(self) -> Self:
        return self

    @property
    def monthly(self) -> Self:
        return self

    def __eq__(self, other: object) -> bool:
        if isinstance(other, Release):
            y1, m1, d1 = self.ymd
            y2, m2, d2 = other.ymd
            return y1 == y2 and m1 == m2 and d1 == d2

        return NotImplemented

    def __ne__(self, other: object) -> bool:
        if isinstance(other, Release):
            y1, m1, d1 = self.ymd
            y2, m2, d2 = other.ymd
            return y1 != y2 and m1 != m2 and d1 != d2

        return NotImplemented

    def __lt__(self, other: object) -> bool:
        if isinstance(other, Release):
            y1, m1, d1 = self.ymd
            y2, m2, d2 = other.ymd
            return y1 < y2 or (y1 == y2 and m1 < m2 or m1 == m2 and d1 < d2)

        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, Release):
            y1, m1, d1 = self.ymd
            y2, m2, d2 = other.ymd
            return y1 < y2 or (y1 == y2 and m1 < m2 or m1 == m2 and d1 <= d2)

        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, Release):
            y1, m1, d1 = self.ymd
            y2, m2, d2 = other.ymd
            return y1 > y2 or (y1 == y2 and m1 > m2 or m1 == m2 and d1 > d2)

        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, Release):
            y1, m1, d1 = self.ymd
            y2, m2, d2 = other.ymd
            return y1 > y2 or (y1 == y2 and m1 > m2 or m1 == m2 and d1 >= d2)

        return NotImplemented

    @abstractmethod
    def __sub__(self, other: object) -> int: ...

    def __iter__(self) -> Self:
        return self

    @abstractmethod
    def __next__(self) -> Self: ...

    def __str__(self) -> str:
        return self.id


@dataclass(frozen=True, slots=True)
class Daily(Release):

    year: int
    month: int
    day: int

    def __post_init__(self) -> None:
        assert 1 <= self.month <= 12
        assert 1 <= self.day <= _days_in_month(self.year, self.month)

    @classmethod
    def of(
        cls,
        year: dt.date | dt.datetime | str | int,
        month: None | int = None,
        day: None | int = None,
    ) -> Self:
        if isinstance(year, str):
            year = dt.date.fromisoformat(year)
        if isinstance(year, (dt.date, dt.datetime)):
            return cls(year.year, year.month, year.day)

        assert isinstance(year, int) and isinstance(month, int) and isinstance(day, int)
        return cls(year, month, day)

    @property
    def id(self) -> str:
        return f"{self.year}-{self.month:02}-{self.day:02}"

    @property
    def parent_directory(self) -> Path:
        return Path(f"{self.year}") / f"{self.month:02}"

    @property
    def directory(self) -> Path:
        return self.parent_directory / f"{self.day:02}"

    @property
    def ymd(self) -> tuple[int, int, int]:
        return self.year, self.month, self.day

    @property
    def monthly(self) -> Self:
        return Monthly(self)

    def __sub__(self, other) -> int:
        if type(self) is type(other):
            return (dt.date(*other.ymd) - dt.date(*self.ymd)).days

        return NotImplemented

    def __next__(self) -> Self:
        year, month, day = self.ymd
        day += 1
        if _days_in_month(year, month) < day:
            month += 1
            day = 1
        if 12 < month:
            year += 1
            month = 1
        return Daily(year, month, day)


@dataclass(frozen=True, slots=True)
class Monthly(Release):

    inner: Daily

    @property
    def year(self) -> int:
        return self.inner.year

    @property
    def month(self) -> int:
        return self.inner.month

    @property
    def id(self) -> str:
        return f"{self.inner.year}-{self.inner.month:02}"

    @property
    def parent_directory(self) -> Path:
        return Path(f"{self.inner.year}")

    @property
    def directory(self) -> Path:
        return self.inner.parent_directory

    @property
    def ymd(self) -> tuple[int, int, int]:
        return self.inner.ymd

    @property
    def daily(self) -> Self:
        return self.inner

    def __sub__(self, other) -> int:
        if type(self) is type(other):
            return (other.year - self.year) * 12 + other.month - self.month

        return NotImplemented

    def __next__(self) -> Self:
        year, month, day = self.ymd
        month += 1
        if 12 < month:
            year += 1
            month = 1
        max_days = _days_in_month(year, month)
        if max_days < day:
            day = max_days

        return Monthly(Daily(year, month, day))


def _days_in_month(year: int, month: int) -> int:
    if month in (4, 6, 9, 11):
        return 30
    elif month == 2:
        return 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28
    else:
        return 31


@dataclass(frozen=True, slots=True)
class ReleaseRange[R: Release]:
    """A range of releases, inclusive."""

    first: R
    last: R

    def __post_init__(self) -> None:
        assert self.first <= self.last

    def __iter__(self) -> Iterator[R]:
        cursor = self.first
        while True:
            yield cursor
            if cursor == self.last:
                break
            cursor = next(cursor)

    def __len__(self) -> int:
        return self.last - self.first + 1


@dataclass(frozen=True, slots=True)
class Coverage[R: Release]:
    """The matter of interest."""

    first: R
    last: R
    filter: str

    def __post_init__(self) -> None:
        assert self.first <= self.last

    def __iter__(self) -> Iterator[R]:
        cursor = self.first
        while True:
            yield cursor
            if cursor == self.last:
                break
            cursor = next(cursor)

    def __len__(self) -> int:
        return self.last - self.first + 1


class Dataset[R: Release](metaclass=ABCMeta):
    """A specific dataset."""

    @abstractmethod
    def name(self) -> str:
        """The dataset name."""

    @abstractmethod
    def url(self, release: R) -> str:
        """The URL for the release."""

    @abstractmethod
    def archive(self, release: R) -> str:
        """The archive file name for the release."""

    @abstractmethod
    def digest(self, release: R) -> str:
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
        filter: str,
        progress: Progress = NO_PROGRESS,
    ) -> Counter:
        """Extract working data from an uncompressed data."""

    @abstractmethod
    def analyze_release[T: Release](
        self, root: Path, release: T, collector: Collector
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


@dataclass(frozen=True, slots=True)
class Storage:
    """The current storage locations."""

    archive: Path
    working: Path
    staging: Path


class DownloadFailed(Exception):
    """A download ended in a status code other than 200."""


class MetadataConflict(Exception):
    """Inconsistent metadata while merging."""
