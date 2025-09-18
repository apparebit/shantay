import datetime as dt
from typing import cast
import unittest

from shantay.model import Daily, DateRange, Monthly, Release, ReleaseRange


class TestRelease(unittest.TestCase):

    def check_daily(self, release: Release) -> None:
        self.assertIsInstance(release, Daily)
        daily = cast(Daily, release)
        self.assertTupleEqual((daily.year, daily.month, daily.day), (1999, 12, 31))

    def check_monthly(self, release: Release, year: int = 1999, month: int = 12) -> None:
        self.assertIsInstance(release, Monthly)
        monthly = cast(Monthly, release)
        self.assertTupleEqual((monthly.year, monthly.month), (year, month))

    def test_from_string(self) -> None:
        r = Release.of("1999-12-31")
        self.check_daily(r)

    def test_from_date(self) -> None:
        r = Release.of(dt.date(1999, 12, 31))
        self.check_daily(r)

    def test_to_string(self) -> None:
        r = Daily(1999, 12, 31)
        self.assertEqual(str(r), "1999-12-31")

    def test_leap_years(self) -> None:
        self.assertEqual(Daily(1899, 2, 28).next().month, 3)
        self.assertEqual(Daily(1900, 2, 28).next().month, 3)
        self.assertEqual(Daily(2000, 2, 28).next().month, 2)
        self.assertEqual(Daily(2004, 2, 28).next().month, 2)

    def test_range(self) -> None:
        memorial_day = dt.date(2024, 5, 27)
        juneteenth = dt.date(2024, 6, 19)
        independence_day = dt.date(2024, 7, 4)
        labor_day = dt.date(2024, 9, 2)

        date_range = DateRange(memorial_day, independence_day)
        self.assertTrue(juneteenth in date_range)
        self.assertFalse(labor_day in date_range)
        self.assertEqual(str(date_range), "2024-05-27-2024-07-04")

        release_range = date_range.dailies()
        self.assertTrue(Release.of(juneteenth) in release_range)
        self.assertFalse(Release.of(labor_day) in release_range)
        self.assertEqual(str(release_range), "2024-05-27-2024-07-04")

        self.assertEqual(release_range.date_range(), date_range)
        june2024 = Monthly(2024, 6)
        self.assertEqual(date_range.monthlies(), ReleaseRange(june2024, june2024))
        date_range_too = release_range.date_range().monthlies().date_range()
        self.assertEqual(date_range_too.first, dt.date(2024, 6, 1))
        self.assertEqual(date_range_too.last, dt.date(2024, 6, 30))
