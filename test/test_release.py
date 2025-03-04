import datetime as dt
import unittest

from shantay.model import Daily, Monthly, Release

class TestRelease(unittest.TestCase):

    def check_daily(self, release: Release) -> None:
        self.assertIsInstance(release, Daily)
        self.assertTupleEqual(release.ymd, (1999, 12, 31))

    def test_from_string(self) -> None:
        r = Daily.of("1999-12-31")
        self.check_daily(r)

    def test_from_date(self) -> None:
        r = Daily.of(dt.date(1999, 12, 31))
        self.check_daily(r)

    def test_from_datetime(self) -> None:
        r = Daily.of(dt.datetime(1999, 12, 31, 23, 59, 59))
        self.check_daily(r)

    def test_leap_years(self) -> None:
        self.assertEqual(next(Daily(1899, 2, 28)).month, 3)
        self.assertEqual(next(Daily(1900, 2, 28)).month, 3)
        self.assertEqual(next(Daily(2000, 2, 28)).month, 2)
        self.assertEqual(next(Daily(2004, 2, 28)).month, 2)
