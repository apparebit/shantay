from collections import Counter
import datetime as dt
from pathlib import Path
import shutil
import unittest

import polars as pl

from shantay.dsa_sor import StatementsOfReasons
from shantay.framing import Collector
from shantay.metadata import Metadata
from shantay.model import Coverage, Daily, Storage
from shantay.processor import Processor
from shantay.tool import configure_logging

ROOT = Path(__file__).parent
FIXTURE = ROOT / "fixture"
ARCHIVE = FIXTURE / "archive"

# We never copy the parquet files out of staging.
# So we only need ARCHIVE and STAGING.
STAGING = ROOT / "tmp"
LOGFILE = STAGING / "log.log"
SENTINEL = STAGING / "prepare.run"

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
    # Since the staging directory and log file are shared across test modules,
    # we use per-test-module sentinel files to detect new runs.
    if SENTINEL.exists():
        shutil.rmtree(STAGING)
    STAGING.mkdir(exist_ok=True)
    SENTINEL.write_text(f"{dt.datetime.now()}\n")

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
            release = Daily(2024, 3, 14)
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
            frame = dataset._extract_filtered_rows(
                csv_files=glob,
                release=release,
                index=0,
                name=ZIP_FILES[0],
                filter=FILTER,
            )
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

            self.assertEqual(digest, "f8b9a455d5521a41280c67eab737ff003ff8f65b5b038b1fc8e844cd418572b5")

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
            release_metadata = pl.DataFrame({
                "batch_count": [2],
                "total_rows": [665],
                "total_rows_with_keywords": [212],
            })
            dataset.analyze_release(STAGING, release.to_monthly(), release_metadata, collector)

            for key, value in collector.consume_frames():
                if key == "stats":
                    self.assertDictEqual(
                        value.to_dict(as_series=False),
                        {
                            "account_decision_only": [1],
                            "account_suspended": [1],
                            "account_suspended_null_date": [1],
                            "account_suspended_until_date": [0],
                            "account_terminated": [0],
                            "account_terminated_null_date": [0],
                            "account_terminated_until_date": [0],
                            "account_type_business": [0],
                            "account_type_private": [0],
                            "all_kinds_decision": [0],
                            "automated_decision_fully": [13],
                            "automated_decision_not_automated": [4],
                            "automated_decision_partially": [0],
                            "automated_detection_no": [1],
                            "automated_detection_yes": [16],
                            "batch_count": [2],
                            "content_age_restricted": [4],
                            "content_demoted": [0],
                            "content_disabled": [0],
                            "content_interaction_restricted": [0],
                            "content_labeled": [0],
                            "content_removed": [3],
                            "content_type_app": [0],
                            "content_type_audio": [0],
                            "content_type_image": [2],
                            "content_type_other": [1],
                            "content_type_product": [0],
                            "content_type_synthetic_media": [0],
                            "content_type_text": [2],
                            "content_type_values": [18],
                            "content_type_video": [13],
                            "csam": [1],
                            "csam_account_decision_only": [0],
                            "csam_account_suspended": [0],
                            "csam_account_suspended_null_date": [0],
                            "csam_account_suspended_until_date": [0],
                            "csam_account_terminated": [0],
                            "csam_account_terminated_null_date": [0],
                            "csam_account_terminated_until_date": [0],
                            "csam_account_type_business": [0],
                            "csam_account_type_private": [0],
                            "csam_all_kinds_decision": [0],
                            "csam_automated_decision_fully": [1],
                            "csam_automated_decision_not_automated": [0],
                            "csam_automated_decision_partially": [0],
                            "csam_automated_detection_no": [0],
                            "csam_automated_detection_yes": [1],
                            "csam_content_age_restricted": [0],
                            "csam_content_demoted": [0],
                            "csam_content_disabled": [0],
                            "csam_content_interaction_restricted": [0],
                            "csam_content_labeled": [0],
                            "csam_content_removed": [1],
                            "csam_content_type_app": [0],
                            "csam_content_type_audio": [0],
                            "csam_content_type_image": [1],
                            "csam_content_type_other": [0],
                            "csam_content_type_product": [0],
                            "csam_content_type_synthetic_media": [0],
                            "csam_content_type_text": [0],
                            "csam_content_type_values": [1],
                            "csam_content_type_video": [0],
                            "csam_illegal_content": [0],
                            "csam_incompatible_content": [1],
                            "csam_incompatible_content_illegal_no": [0],
                            "csam_incompatible_content_illegal_yes": [1],
                            "csam_max_content_types_per_row": [1],
                            "csam_max_visibility_per_row": [1],
                            "csam_monetary_account_decision": [0],
                            "csam_monetary_decision_only": [0],
                            "csam_monetary_decision_only": [0],
                            "csam_monetary_other": [0],
                            "csam_monetary_provision_account_decision": [0],
                            "csam_monetary_provision_decision": [0],
                            "csam_monetary_suspension": [0],
                            "csam_monetary_termination": [0],
                            "csam_null_account_decision": [1],
                            "csam_null_account_type": [1],
                            "csam_null_automated_decision": [0],
                            "csam_null_automated_detection": [0],
                            "csam_null_content_types": [0],
                            "csam_null_decision": [0],
                            "csam_null_decision_ground": [0],
                            "csam_null_incompatible_content_illegal": [0],
                            "csam_null_monetary_decision": [1],
                            "csam_null_provision_decision": [1],
                            "csam_null_source_type": [0],
                            "csam_null_visibility_decision": [0],
                            "csam_other_visibility": [0],
                            "csam_provision_account_decision": [0],
                            "csam_provision_decision_only": [0],
                            "csam_provision_partial_suspension": [0],
                            "csam_provision_partial_termination": [0],
                            "csam_provision_total_suspension": [0],
                            "csam_provision_total_termination": [0],
                            "csam_rows_with_content_type": [1],
                            "csam_rows_with_visibility": [1],
                            "csam_source_article_16": [0],
                            "csam_source_other_notification": [0],
                            "csam_source_trusted_flagger": [0],
                            "csam_source_voluntary": [1],
                            "csam_visibility_account_decision": [0],
                            "csam_visibility_decision_only": [1],
                            "csam_visibility_monetary_account_decision": [0],
                            "csam_visibility_monetary_decision": [0],
                            "csam_visibility_monetary_provision_decision": [0],
                            "csam_visibility_provision_account_decision": [0],
                            "csam_visibility_provision_decision": [0],
                            "csam_visibility_values": [1],
                            "end_date": [dt.date(2024, 3, 31)],
                            "illegal_content": [0],
                            "incompatible_content": [17],
                            "incompatible_content_illegal_no": [0],
                            "incompatible_content_illegal_yes": [2],
                            "keywords": [2],
                            "max_content_types_per_row": [2],
                            "max_keywords_per_row": [1],
                            "max_visibility_per_row": [1],
                            "monetary_account_decision": [0],
                            "monetary_decision_only": [0],
                            "monetary_other": [0],
                            "monetary_provision_account_decision": [0],
                            "monetary_provision_decision": [0],
                            "monetary_suspension": [0],
                            "monetary_termination": [0],
                            "null_account_decision": [16],
                            "null_account_type": [17],
                            "null_automated_decision": [0],
                            "null_automated_detection": [0],
                            "null_content_types": [0],
                            "null_decision": [0],
                            "null_decision_ground": [0],
                            "null_incompatible_content_illegal": [15],
                            "null_monetary_decision": [17],
                            "null_provision_decision": [17],
                            "null_source_type": [0],
                            "null_visibility_decision": [1],
                            "other_visibility": [9],
                            "provision_account_decision": [0],
                            "provision_decision_only": [0],
                            "provision_partial_suspension": [0],
                            "provision_partial_termination": [0],
                            "provision_total_suspension": [0],
                            "provision_total_termination": [0],
                            "rows": [17],
                            "rows_with_content_type": [17],
                            "rows_with_keywords": [2],
                            "rows_with_visibility": [16],
                            "source_article_16": [1],
                            "source_other_notification": [0],
                            "source_trusted_flagger": [0],
                            "source_voluntary": [16],
                            "start_date": [dt.date(2024, 3, 1)],
                            "total_rows": [665],
                            "total_rows_with_keywords": [212],
                            "visibility_account_decision": [0],
                            "visibility_decision_only": [16],
                            "visibility_monetary_account_decision": [0],
                            "visibility_monetary_decision": [0],
                            "visibility_monetary_provision_decision": [0],
                            "visibility_provision_account_decision": [0],
                            "visibility_provision_decision": [0],
                            "visibility_values": [16]
                        }
                    )

                elif key == "keywords":
                    self.assertListEqual(
                        # start_date, end_date, keyword, count
                        value.sort("keyword").rows(),
                        [
                            (
                                dt.date(2024, 3, 1), dt.date(2024, 3, 31),
                                "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL", 1,

                            ),
                            (
                                dt.date(2024, 3, 1), dt.date(2024, 3, 31),
                                "KEYWORD_GROOMING_SEXUAL_ENTICEMENT_MINORS", 1,
                            ),
                        ]
                    )

                elif key == "platforms":
                    platforms = value.select(
                        pl.col("platform").unique()
                    ).to_series().to_list()
                    self.assertListEqual(sorted(platforms), [
                        "Google Shopping", "Snapchat", "TikTok"
                    ])

                else:
                    self.assertIn(
                        key, ("stats", "keywords", "platforms", "platforms_with_keywords")
                    )

        with self.subTest("check log file"):
            lines = LOGFILE.read_text("utf8").splitlines(keepends=True)

            offset = -1
            for offset, line in enumerate(lines):
                if 'staged file="sor-global-2024-03-14-full.zip"' in line:
                    break

            self.assertNotEqual(offset, -1)
            self.assertTrue(offset + 33 <= len(lines))
            self.assertIn("staged file", lines[offset + 0])
            self.assertIn("validated file", lines[offset + 1])
            self.assertIn('unarchived type="nested archive"', lines[offset + 2])
            self.assertIn('counted filter="none", rows=100', lines[offset + 3])
            self.assertIn('counted filter="with_keywords", rows=12', lines[offset + 4])
            self.assertIn('extracted rows=8', lines[offset + 5])
            self.assertIn('unarchived type="nested archive"', lines[offset + 6])
            self.assertIn('counted filter="none", rows=102', lines[offset + 7])
            self.assertIn('counted filter="with_keywords", rows=1', lines[offset + 8])
            # Trying to parse both CSV files in one Pola.rs operation fails:
            self.assertIn('shantay︙WARNING︙failed to read CSV with strategy=1, using="globbing Pola.rs"', lines[offset + 9])
            self.assertTrue(lines[offset + 10].startswith('Traceback'))
            self.assertTrue(lines[offset + 11].startswith('  File'))
            self.assertTrue(lines[offset + 12].startswith('    ).collect()'))
            self.assertTrue(lines[offset + 13].startswith('      ^^^^^^^'))
            self.assertTrue(lines[offset + 14].startswith('  File'))
            self.assertTrue(lines[offset + 15].startswith('    return wrap_df(ldf'))
            self.assertTrue(lines[offset + 16].startswith('                   ^^^'))
            self.assertTrue(lines[offset + 17].startswith('polars.exceptions.ComputeError: could not parse'))
            self.assertTrue(lines[offset + 18].startswith(''))
            self.assertTrue(lines[offset + 19].startswith('The current offset in the file is 131 bytes'))
            self.assertTrue(lines[offset + 20].startswith(''))
            self.assertTrue(lines[offset + 21].startswith('You might want to try'))
            self.assertTrue(lines[offset + 22].startswith('- increasing'))
            self.assertTrue(lines[offset + 23].startswith('- specifying'))
            self.assertTrue(lines[offset + 24].startswith('- setting'))
            self.assertTrue(lines[offset + 25].startswith('- adding'))
            self.assertTrue(lines[offset + 26].startswith(''))
            self.assertTrue(lines[offset + 27].startswith('Original error: ```invalid csv file'))
            self.assertTrue(lines[offset + 28].startswith(''))
            self.assertTrue(lines[offset + 29].startswith('Field `"Napodobňovanie'))
            # Parsing the first CSV file by itself with Pola.rs works:
            self.assertIn('extracted rows=8, strategy=2, using="Pola.rs"', lines[offset + 30])
            # Parsing the second CSV file by itself with Pola.rs fails:
            self.assertIn('failed to read CSV with strategy=2, using="Pola.rs"', lines[offset + 31])
            # Parsing the second CSV fail by itself with Python's csv works:
            self.assertIn('extracted rows=1, strategy=3, using="Python\'s CSV module"', lines[offset + 32])
