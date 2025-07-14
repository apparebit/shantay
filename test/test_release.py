import datetime as dt
from typing import cast
import unittest

from shantay.model import Daily, Monthly, Release


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
