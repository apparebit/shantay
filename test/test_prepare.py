from collections import Counter
from pathlib import Path
import shutil
import unittest

import polars as pl

from shantay.collector import Collector
from shantay.metadata import Metadata
from shantay.runner import Runner
from shantay.schedule import YearMonth
from shantay.sor import DailySoR
from shantay.tool import configure_logging

ROOT = Path(__file__).parent
FIXTURE = ROOT / "fixture"
ARCHIVE = FIXTURE / "archive"

# We never copy the parquet files out of staging.
# So we only need ARCHIVE and STAGING.
STAGING = ROOT / "tmp"
LOGFILE = STAGING / "log.log"

ZIP_FILES = [
    "sor-global-2024-03-14-full-00000.csv.zip",
    "sor-global-2024-03-14-full-00001.csv.zip",
]

CSV_FILES = [
    "sor-global-2024-03-14-full-00000-00000.csv",
    "sor-global-2024-03-14-full-00000-00001.csv",
    "sor-global-2024-03-14-full-00001-00000.csv",
    "sor-global-2024-03-14-full-00001-00001.csv",
]

CATEGORY = "STATEMENT_CATEGORY_PROTECTION_OF_MINORS"

def setUpModule():
    shutil.rmtree(STAGING, ignore_errors=True)
    # We need to recreate STAGING right away,
    # as logging expects the directory to exist.
    STAGING.mkdir()
    configure_logging(str(LOGFILE), verbose=True)

def tearDownModule():
    pass

class TestPrepare(unittest.TestCase):

    def assertFileEqual(self, path1: Path, path2: Path) -> None:
        data1 = path1.read_bytes()
        data2 = path2.read_bytes()
        self.assertEqual(len(data1), len(data2), "file contents must have equal length")
        self.assertEqual(data1, data2, "file contents must be equal")

    def test_extraction(self):
        with self.subTest("set up metadata, runner, and release"):
            metadata = Metadata(CATEGORY, {})
            runner = Runner(
                archive=ARCHIVE,
                batches=STAGING,
                staging=STAGING,
                metadata=metadata,
            )
            release = DailySoR("2024-03-14")

            digest = release.directory / release.digest
            archive = release.directory / release.archive

        with self.subTest("stage archive by copying fixture"):
            self.assertFalse((STAGING / digest).exists())
            self.assertFalse((STAGING / archive).exists())
            runner.stage_archive(release)
            self.assertTrue((STAGING / digest).exists())
            self.assertTrue((STAGING / archive).exists())
            self.assertFileEqual(ARCHIVE / digest, STAGING / digest)
            self.assertFileEqual(ARCHIVE / archive, STAGING / archive)

        with self.subTest("determine archived files"):
            filenames = release.archived_files(STAGING)
            self.assertListEqual(filenames, ZIP_FILES)

        with self.subTest("unarchive first of two CSV files"):
            step_count = release.extract_data_step_count()
            self.assertEqual(step_count, 12)

            workdir = STAGING / release.working_directory
            self.assertFalse(workdir.exists())

            release.unarchive_file(STAGING, 0, ZIP_FILES[0])
            self.assertTrue(workdir.exists())
            self.assertListEqual(sorted(p.name for p in workdir.glob("*")), CSV_FILES[:2])
            self.assertFileEqual(workdir / CSV_FILES[0], FIXTURE / "csv" / CSV_FILES[0])
            self.assertFileEqual(workdir / CSV_FILES[1], FIXTURE / "csv" / CSV_FILES[1])

        with self.subTest("determine row counts"):
            glob = f"{STAGING / release.working_directory}/*.csv"
            count1, count2 = release._extract_row_counts(glob, 0, ZIP_FILES[0])
            self.assertEqual(count1, 100)
            self.assertEqual(count2, 12)

        with self.subTest("extract first batch of category data"):
            frame = release._extract_filtered_rows(glob, 0, ZIP_FILES[0], CATEGORY)
            release._validate_schema(frame)

            counters = Counter(batch_count = 2)
            counters += release._assemble_frame_counters(frame, count1, count2)
            self.assertEqual(counters["batch_count"], 2)
            self.assertEqual(counters["total_rows"], 100)
            self.assertEqual(counters["total_rows_with_keywords"], 12)
            self.assertEqual(counters["batch_rows"], 8)
            self.assertEqual(counters["batch_rows_with_keywords"], 2)

            framedir = STAGING / release.batch_directory
            self.assertFalse(framedir.exists())
            framedir.mkdir(parents=True)
            batch0 = framedir / release.batch(0)
            frame.write_parquet(batch0)

            self.assertFileEqual(batch0, FIXTURE / release.batch(0))

        with self.subTest("extract second batch of category data"):
            batch1 = STAGING / release.batch_directory / release.batch(1)
            self.assertFalse(batch1.exists())

            release.unarchive_file(STAGING, 1, ZIP_FILES[1])
            counters += release.extract_data(STAGING, 1, ZIP_FILES[1], CATEGORY)

            self.assertListEqual(sorted(p.name for p in workdir.glob("*")), CSV_FILES)
            self.assertFileEqual(workdir / CSV_FILES[2], FIXTURE / "csv" / CSV_FILES[2])
            self.assertFileEqual(workdir / CSV_FILES[3], FIXTURE / "csv" / CSV_FILES[3])

            self.assertEqual(counters["batch_count"], 2)
            self.assertEqual(counters["total_rows"], 100 + 100)
            self.assertEqual(counters["total_rows_with_keywords"], 12 + 1)
            self.assertEqual(counters["batch_rows"], 8 + 9)
            self.assertEqual(counters["batch_rows_with_keywords"], 2 + 0)

            memory = round(counters["batch_memory"] / 1_000)
            self.assertTrue(18 <= memory <= 22)

            self.assertTrue(batch1.exists())
            self.assertFileEqual(batch1, FIXTURE / release.batch(1))

        with self.subTest("analyze monthly data"):
            collector = Collector()
            release.analyze_month(STAGING, YearMonth(2024, 3), collector)

            for key, value in collector.consume_frames():
                self.assertIn(key, ("platforms", "platforms_with_keywords"))
                platforms = value.select(
                    pl.col("platform_name").unique()
                ).to_series().to_list()

                if key == "platforms":
                    self.assertListEqual(sorted(platforms), [
                        "Google Shopping", "Snapchat", "TikTok"
                    ])
                elif key == "platforms_with_keywords":
                    self.assertListEqual(platforms, ["Snapchat"])

        with self.subTest("check log file"):
            with LOGFILE.open(mode="r", encoding="utf8") as file:
                lines = file.readlines()

            self.assertEqual(len(lines), 10)
            self.assertIn("staged file", lines[0])
            self.assertIn("validated file", lines[1])
            self.assertIn('unarchived type="nested archive"', lines[2])
            self.assertIn('counted filter="none", rows=100', lines[3])
            self.assertIn('counted filter="with_keywords", rows=12', lines[4])
            self.assertIn('extracted rows=8', lines[5])
            self.assertIn('unarchived type="nested archive"', lines[6])
            self.assertIn('counted filter="none", rows=100', lines[7])
            self.assertIn('counted filter="with_keywords", rows=1', lines[8])
            self.assertIn('extracted rows=9', lines[9])

