from pathlib import Path
import shutil
import unittest

import polars as pl

from shantay.dsa_sor import StatementsOfReasons
from shantay.metadata import Metadata
from shantay.model import Coverage, Daily, Storage
from shantay.processor import Processor
from shantay.schema import StatementCategoryProtectionOfMinors
from shantay.stats import Statistics


ROOT = Path(__file__).parent
FIXTURE = ROOT / "fixture"

STAGING = ROOT / "tmp"
ARCHIVE = STAGING / "summarize-archive"
EXTRACT = STAGING / "summarize-extract"


class TestSummarize(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        shutil.copytree(FIXTURE / "archive", ARCHIVE, dirs_exist_ok=True)

    @classmethod
    def tearDownClass(cls):
        pass

    def assertFileEqual(self, path1: Path, path2: Path) -> None:
        data1 = path1.read_bytes()
        data2 = path2.read_bytes()
        self.assertEqual(len(data1), len(data2), "file contents must have equal length")
        self.assertEqual(data1, data2, "file contents must be equal")

    def assertFrameEqual(self, frame1: pl.DataFrame, frame2: pl.DataFrame) -> None:
        self.assertSetEqual(set(frame1.columns), set(frame2.columns))
        for column in frame1.columns:
            series1 = frame1.get_column(column)
            series2 = frame2.get_column(column)

            self.assertEqual(series1.dtype, series2.dtype)
            self.assertEqual(series1.len(), series2.len())
            for it1, it2 in zip(series1, series2):
                self.assertEqual(it1, it2)

    def test_summarize_db(self):
        dataset = StatementsOfReasons()
        storage = Storage(
            archive_root=ARCHIVE, extract_root=None, staging_root=STAGING
        )
        release = Daily(2024, 3, 14)
        coverage = Coverage(release, release, None)
        metadata = Metadata(None, {})
        processor = Processor(
            dataset=dataset,
            storage=storage,
            coverage=coverage,
            metadata=metadata,
            offline=True,
        )

        processor.run("summarize-all")

        self.assertFileEqual(STAGING / "db.json", FIXTURE / "db.json")
        self.assertFileEqual(ARCHIVE / "db.json", FIXTURE / "db.json")

        frame1 = pl.read_parquet(STAGING / "db.parquet")
        frame2 = pl.read_parquet(ARCHIVE / "db.parquet")
        self.assertFrameEqual(frame1, frame2)

        # By indirecting through Statistics.read, we ensure that the type of the
        # fixture's platform column is up to date.s
        frame2 = Statistics.read(FIXTURE / "db.parquet").frame()
        self.assertFrameEqual(frame1, frame2)

        self.assertFileEqual(STAGING / "db.parquet", ARCHIVE / "db.parquet")

    def test_summarize_category(self):
        dataset = StatementsOfReasons()
        storage = Storage(
            archive_root=ARCHIVE, extract_root=EXTRACT, staging_root=STAGING
        )
        release = Daily(2024, 3, 14)
        coverage = Coverage(release, release, StatementCategoryProtectionOfMinors)
        metadata = Metadata(StatementCategoryProtectionOfMinors, {})
        processor = Processor(
            dataset=dataset,
            storage=storage,
            coverage=coverage,
            metadata=metadata,
            offline=True,
        )

        processor.run("summarize-category")

        meta_json = FIXTURE / "protection-of-minors.json"
        self.assertFileEqual(STAGING / "protection-of-minors.json", meta_json)
        self.assertFileEqual(EXTRACT / "protection-of-minors.json", meta_json)

        frame1 = pl.read_parquet(STAGING / "protection-of-minors.parquet")
        frame2 = pl.read_parquet(EXTRACT / "protection-of-minors.parquet")
        self.assertFrameEqual(frame1, frame2)

        # By indirecting through Statistics.read, we ensure that the type of the
        # fixture's platform column is up to date.s
        frame2 = Statistics.read(FIXTURE / "protection-of-minors.parquet").frame()
        self.assertFrameEqual(frame1, frame2)

        self.assertFileEqual(
            STAGING / "protection-of-minors.parquet",
            EXTRACT / "protection-of-minors.parquet",
        )
