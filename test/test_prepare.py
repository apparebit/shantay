from collections import Counter
import datetime as dt
from pathlib import Path
import shutil
from typing import cast
import unittest

import polars as pl

from shantay.collector import Collector
from shantay.dsa_sor import StatementsOfReasons
from shantay.metadata import Metadata
from shantay.model import Coverage, Daily, MetadataEntry, Storage
from shantay.processor import Processor
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

FILTER = "STATEMENT_CATEGORY_PROTECTION_OF_MINORS"

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
            dataset = StatementsOfReasons()
            storage = Storage(
                archive_root=ARCHIVE,
                working_root=STAGING,
                staging_root=STAGING
            )
            release = Daily.of(2024, 3, 14)
            coverage = Coverage(release, release, FILTER)
            metadata = Metadata(FILTER, {})
            processor = Processor(
                dataset=dataset,
                storage=storage,
                coverage=coverage,
                metadata=metadata,
            )

            digest = release.parent_directory / dataset.digest_name(release)
            archive = release.parent_directory / dataset.archive_name(release)

        with self.subTest("stage archive by copying fixture"):
            self.assertFalse((STAGING / digest).exists())
            self.assertFalse((STAGING / archive).exists())
            processor.stage_archive(release)
            self.assertTrue((STAGING / digest).exists())
            self.assertTrue((STAGING / archive).exists())
            self.assertFileEqual(ARCHIVE / digest, STAGING / digest)
            self.assertFileEqual(ARCHIVE / archive, STAGING / archive)

        with self.subTest("determine archived files"):
            filenames = processor.list_archived_files(STAGING, release)
            self.assertListEqual(filenames, ZIP_FILES)

        with self.subTest("unarchive first of two CSV files"):
            step_count = dataset.extract_data_step_count
            self.assertEqual(step_count, 12)

            workdir = STAGING / release.temp_directory
            self.assertFalse(workdir.exists())

            processor.unarchive_file(STAGING, release, 0, ZIP_FILES[0])
            self.assertTrue(workdir.exists())
            self.assertListEqual(sorted(p.name for p in workdir.glob("*")), CSV_FILES[:2])
            self.assertFileEqual(workdir / CSV_FILES[0], FIXTURE / "csv" / CSV_FILES[0])
            self.assertFileEqual(workdir / CSV_FILES[1], FIXTURE / "csv" / CSV_FILES[1])

        with self.subTest("determine row counts"):
            glob = f"{STAGING / release.temp_directory}/*.csv"
            count1, count2 = dataset._extract_row_counts(glob, 0, ZIP_FILES[0])
            self.assertEqual(count1, 100)
            self.assertEqual(count2, 12)

        with self.subTest("extract first batch of category data"):
            frame = dataset._extract_filtered_rows(glob, 0, ZIP_FILES[0], FILTER)
            dataset._validate_schema(frame)

            counters = Counter(batch_count = 2)
            counters += dataset._assemble_frame_counters(frame, count1, count2)
            self.assertEqual(counters["batch_count"], 2)
            self.assertEqual(counters["total_rows"], 100)
            self.assertEqual(counters["total_rows_with_keywords"], 12)
            self.assertEqual(counters["batch_rows"], 8)
            self.assertEqual(counters["batch_rows_with_keywords"], 2)

            framedir = STAGING / release.directory
            self.assertFalse(framedir.exists())
            framedir.mkdir(parents=True)
            batch0 = framedir / release.batch_file(0)
            frame.write_parquet(batch0)

            self.assertFileEqual(batch0, FIXTURE / release.batch_file(0))

        with self.subTest("extract second batch of category data"):
            batch1 = STAGING / release.directory / release.batch_file(1)
            self.assertFalse(batch1.exists())

            processor.unarchive_file(STAGING, release, 1, ZIP_FILES[1])
            digest, more_counters = dataset.extract_file_data(
                root=STAGING,
                release=release,
                index=1,
                name=ZIP_FILES[1],
                filter=FILTER,
            )

            self.assertEqual(digest, "1d58cfeccb6a50b39d3ea72dec8178631b4292d5b4856b2e5c9272eb94b2c1f0")

            self.assertListEqual(sorted(p.name for p in workdir.glob("*")), CSV_FILES)
            self.assertFileEqual(workdir / CSV_FILES[2], FIXTURE / "csv" / CSV_FILES[2])
            self.assertFileEqual(workdir / CSV_FILES[3], FIXTURE / "csv" / CSV_FILES[3])

            counters += more_counters
            self.assertEqual(counters["batch_count"], 2)
            self.assertEqual(counters["total_rows"], 100 + 102)
            self.assertEqual(counters["total_rows_with_keywords"], 12 + 1)
            self.assertEqual(counters["batch_rows"], 8 + 9)
            self.assertEqual(counters["batch_rows_with_keywords"], 2 + 0)

            memory = round(counters["batch_memory"] / 1_000)
            self.assertTrue(18 <= memory <= 22)

            self.assertTrue(batch1.exists())
            self.assertFileEqual(batch1, FIXTURE / release.batch_file(1))

        with self.subTest("analyze release data"):
            collector = Collector()
            metadata_entry = cast(MetadataEntry, {"batch_count": 2})
            dataset.analyze_release(STAGING, release.monthly, metadata_entry, collector)

            for key, value in collector.consume_frames():
                if key == "stats":
                    self.assertDictEqual(
                        value.to_dict(as_series=False),
                        {
                            "batch_count": [2],
                            "first_day": [dt.date(2024, 3, 1)],
                            "last_day": [dt.date(2024, 3, 31)],
                            "total_rows": [None],
                            "total_rows_with_keywords": [None],
                            "rows": [17],
                            "keywords": [2],
                            "rows_with_keywords": [2],
                            "max_keywords_per_row": [1],
                        }
                    )
                elif key == "keywords":
                    self.assertListEqual(
                        value.rows(),
                        [
                            ("KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL", 1),
                            ("KEYWORD_GROOMING_SEXUAL_ENTICEMENT_MINORS", 1),
                        ]
                    )
                elif key in ("platforms", "platforms_with_keywords"):
                    platforms = value.select(
                        pl.col("platform_name").unique()
                    ).to_series().to_list()

                    if key == "platforms":
                        self.assertListEqual(sorted(platforms), [
                            "Google Shopping", "Snapchat", "TikTok"
                        ])
                    elif key == "platforms_with_keywords":
                        self.assertListEqual(platforms, ["Snapchat"])
                else:
                    self.assertIn(
                        key, ("stats", "keywords", "platforms", "platforms_with_keywords")
                    )

        with self.subTest("check log file"):
            with LOGFILE.open(mode="r", encoding="utf8") as file:
                lines = file.readlines()

            self.assertEqual(len(lines), 33)
            self.assertIn("staged file", lines[0])
            self.assertIn("validated file", lines[1])
            self.assertIn('unarchived type="nested archive"', lines[2])
            self.assertIn('counted filter="none", rows=100', lines[3])
            self.assertIn('counted filter="with_keywords", rows=12', lines[4])
            self.assertIn('extracted rows=8', lines[5])
            self.assertIn('unarchived type="nested archive"', lines[6])
            self.assertIn('counted filter="none", rows=102', lines[7])
            self.assertIn('counted filter="with_keywords", rows=1', lines[8])
            # Trying to parse both CSV files in one Pola.rs operation fails:
            self.assertIn('[WARNING] failed to read CSV using="Pola.rs with glob"', lines[9])
            self.assertTrue(lines[10].startswith('Traceback'))
            self.assertTrue(lines[11].startswith('  File'))
            self.assertTrue(lines[12].startswith('    ).collect()'))
            self.assertTrue(lines[13].startswith('      ^^^^^^^'))
            self.assertTrue(lines[14].startswith('  File'))
            self.assertTrue(lines[15].startswith('    return wrap_df(ldf'))
            self.assertTrue(lines[16].startswith('                   ^^^'))
            self.assertTrue(lines[17].startswith('polars.exceptions.ComputeError: could not parse'))
            self.assertTrue(lines[18].startswith(''))
            self.assertTrue(lines[19].startswith('The current offset in the file is 131 bytes'))
            self.assertTrue(lines[20].startswith(''))
            self.assertTrue(lines[21].startswith('You might want to try'))
            self.assertTrue(lines[22].startswith('- increasing'))
            self.assertTrue(lines[23].startswith('- specifying'))
            self.assertTrue(lines[24].startswith('- setting'))
            self.assertTrue(lines[25].startswith('- adding'))
            self.assertTrue(lines[26].startswith(''))
            self.assertTrue(lines[27].startswith('Original error: ```invalid csv file'))
            self.assertTrue(lines[28].startswith(''))
            self.assertTrue(lines[29].startswith('Field `"Napodobňovanie'))
            # Parsing the first CSV file by itself with Pola.rs works:
            self.assertIn('extracted rows=8, using="Pola.rs"', lines[30])
            # Parsing the second CSV file by itself with Pola.rs fails:
            self.assertIn('failed to read CSV using="Pola.rs"', lines[31])
            # Parsing the second CSV fail by itself with Python's csv works:
            self.assertIn('extracted rows=1, using="Python\'s CSV module"', lines[32])
