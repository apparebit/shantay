import datetime as dt
from typing import cast
import unittest

import polars as pl

from shantay.framing import Collector
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

    def test_to_monthly(self) -> None:
        r = Daily(1999, 12, 31).to_monthly()
        self.check_monthly(r)

    def test_leap_years(self) -> None:
        self.assertEqual(Daily(1899, 2, 28).next().month, 3)
        self.assertEqual(Daily(1900, 2, 28).next().month, 3)
        self.assertEqual(Daily(2000, 2, 28).next().month, 2)
        self.assertEqual(Daily(2004, 2, 28).next().month, 2)

    def test_collector(self) -> None:
        collector = Collector()
        collector.add_frames(Daily(2000, 1, 1), data=pl.DataFrame([1, 1], ["column"]))
        collector.add_frames(Daily(1999, 12, 31), data=pl.DataFrame([12, 31], ["column"]))

        collection = [*collector.consume_frames()]
        self.assertEqual(len(collection), 1)
        self.assertIsInstance(collection[0], tuple)
        self.assertEqual(len(collection[0]), 2)
        self.assertEqual(collection[0][0], "data")
        self.assertTrue(collection[0][1].equals(pl.DataFrame(
            [12, 31, 1, 1], ["column"]
        )))
