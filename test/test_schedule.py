import logging
from pathlib import Path
import shutil

import polars as pl

from .runtime import TestCase

from shantay.dsa_sor import StatementsOfReasons
from shantay.log import LogEntry
from shantay.logutil import log_rule, Size
from shantay.metadata import Metadata
from shantay.model import Config, Daily, ReleaseRange, Storage
from shantay.multiprocessor import Multiprocessor
from shantay.stats import Statistics


_logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent
FIXTURE = ROOT / "fixture"

TMP = ROOT / "tmp"
LOGFILE = TMP / "log.log"
STAGING = TMP / "schedule-staging"
ARCHIVE = TMP / "schedule-archive"


class TestRenew(TestCase):

    WITH_STAGING = "schedule"

    def setUp(self):
        shutil.copytree(FIXTURE / "archive" / "2025", ARCHIVE / "2025")

    def tearDown(self):
        pass

    def test_renew(self):
        log_rule(Size.M)
        _logger.info('testing runner="Multiprocessor", task="summarize-all"')

        # Prepare for processing
        dataset = StatementsOfReasons()
        storage = Storage(
            archive_root=ARCHIVE, extract_root=None, staging_root=STAGING
        )
        coverage = ReleaseRange(Daily(2025, 8, 5), Daily(2025, 8, 14))
        metadata = Metadata("db")
        metadata.write_json(storage.staging_root / f"{metadata.stem}.json")

        processor = Multiprocessor(
            dataset=dataset,
            storage=storage,
            coverage=coverage,
            config=Config(progress=False, workers=2, max_tasks=2),
            metadata=metadata,
        )

        # Process the releases
        processor.run("summarize-all")

        # Validate the results
        self.assertFileEqual(STAGING / "db.json", FIXTURE / "schedule.json")
        self.assertFileEqual(ARCHIVE / "db.json", FIXTURE / "schedule.json")

        frame1 = pl.read_parquet(STAGING / "db.parquet")
        frame2 = pl.read_parquet(ARCHIVE / "db.parquet")
        self.assertFrameEqual(frame1, frame2)

        frame2 = Statistics.read(FIXTURE / "schedule.parquet").frame()
        self.assertFrameEqual(frame1, frame2)

        self.assertFileEqual(STAGING / "db.parquet", ARCHIVE / "db.parquet")

        # Also validate the log
        workers = set()
        skip = True

        for entry in LogEntry.parse_file(LOGFILE):
            if skip:
                if entry.module == "test.test_schedule":
                    self.assertTrue(
                        entry.message.has("runner", "task", prefix="testing")
                    )
                    skip = False
                continue

            if entry.message.has(
                "pid", "max_tasks", "pool", prefix="initialized worker process"
            ):
                pid = entry.message.props["pid"]
                workers.add(entry.message.props["pid"])
                self.assertEqual(entry.message.props["max_tasks"], 2)
            elif entry.message.has("pid", "pool", prefix="retiring worker"):
                pid = entry.message.props["pid"]
                self.assertIn(pid, workers)
                workers.remove(pid)

        _logger.info('completed test for runner="Multiprocessor", task="summarize-all"')
