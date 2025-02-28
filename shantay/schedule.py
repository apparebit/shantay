from collections.abc import Iterator
from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from typing import Self

from .release import DailyRelease, Release

@dataclass(frozen=True, slots=True, order=True)
class YearMonth:
    """
    A year-month.

    Year-months represent the flow of time at month-granularity. They are
    iterators, with the next month also the __next__ month. They support
    addition and subtraction of integers representing numbers of months,
    yielding year-months. They also support subtraction of year-months, yielding
    numbers of months. A `MonthlySchedule` is the inclusive range of two
    year-months.
    """

    year: int
    month: int

    def __post_init__(self) -> None:
        if not 1 <= self.month <= 12:
            raise ValueError(f"invalid month {self.month}")

    @classmethod
    def of(cls, year: int | str | dt.date, month: None | int = None) -> Self:
        """Create a new year-month from a date or a year and month."""
        if isinstance(year, str):
            year = dt.date.fromisoformat(year)
        if isinstance(year, dt.date):
            return cls(year.year, year.month)
        else:
            assert isinstance(month, int)
            return cls(year, month)

    @property
    def id(self) -> str:
        """Get this year-month's ID."""
        return f"{self.year}-{self.month:02}"

    def daily_glob(self, root: Path) -> str:
        """Get a glob for daily category data files."""
        return f"{root}/{self.year}/{self.month:02}/??/{self.year}-{self.month:02}-??-*.parquet"

    def __iter__(self) -> Self:
        return self

    def __next__(self) -> Self:
        year = self.year
        month = self.month + 1
        if month == 13:
            year += 1
            month = 1
        return type(self)(year, month)

    def __sub__(self, other: object) -> int | Self:
        if isinstance(other, YearMonth):
            return (self.year - other.year) * 12 + self.month - other.month
        if not isinstance(other, int):
            return NotImplemented

        years, months = divmod(other, 12)
        year = self.year - years
        month = self.month - months
        if month < 1:
            year -= 1
            month += 12
        return type(self)(year, month)

    def __add__(self, other: object) -> Self:
        if not isinstance(other, int):
            return NotImplemented

        years, months = divmod(other, 12)
        year = self.year + years
        month = self.month + months
        if 12 < month:
            year += 1
            month -= 12
        return type(self)(year, month)

    def __radd__(self, other: object) -> Self:
        return self.__add__(other)


@dataclass(frozen=True, slots=True)
class MonthlySchedule:
    """A monthly schedule from start to stop (inclusive)."""

    start: YearMonth
    stop: YearMonth

    def __post_init__(self) -> None:
        """Ensure that start is at most stop."""
        assert (
            self.start.year < self.stop.year or
            self.start.year == self.stop.year and self.start.month <= self.stop.month
        ), "start comes before stop"

    @property
    def months(self) -> int:
        """Determine the covered duration in months (inclusive)."""
        return self.stop - self.start + 1

    def __iter__(self) -> Iterator[YearMonth]:
        cursor = self.start
        while True:
            yield cursor
            if cursor == self.stop:
                break
            cursor = next(cursor)


@dataclass(frozen=True, slots=True)
class Schedule[R: Release]:
    """A release schedule."""

    start: R
    stop: R

    def __post_init__(self) -> None:
        """Ensure that start is at most stop."""
        assert self.start <= self.stop, "start comes before stop"

    def to_monthly(self) -> MonthlySchedule:
        """Convert this release-based schedule to a monthly schedule."""
        if isinstance(self.start, DailyRelease):
            return MonthlySchedule(YearMonth.of(self.start.date), YearMonth.of(self.stop.date))
        raise NotImplementedError

    def __iter__(self) -> Iterator[R]:
        cursor = self.start
        while True:
            yield cursor
            if cursor == self.stop:
                break
            cursor = next(cursor)

