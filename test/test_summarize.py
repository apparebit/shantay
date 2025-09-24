import logging
from pathlib import Path
import shutil

import polars as pl

from .runtime import TestCase

from shantay.dsa_sor import StatementsOfReasons
from shantay.logutil import log_rule, Size
from shantay.metadata import Metadata
from shantay.model import Config, Daily, ReleaseRange, Storage
from shantay.multiprocessor import Multiprocessor
from shantay.processor import Processor
from shantay.schema import StatementCategoryProtectionOfMinors
from shantay.stats import Statistics


_logger = logging.getLogger(__name__)

ROOT = Path(__file__).parent
FIXTURE = ROOT / "fixture"

TMP = ROOT / "tmp"
STAGING = TMP / "summarize-staging"
ARCHIVE = TMP / "summarize-archive"
EXTRACT = TMP / "summarize-extract"


class TestSummarize(TestCase):

    WITH_STAGING = "summarize"

    def setUp(self):
        shutil.rmtree(STAGING, ignore_errors=True)
        shutil.rmtree(EXTRACT, ignore_errors=True)
        shutil.rmtree(ARCHIVE, ignore_errors=True)
        STAGING.mkdir(parents=True)
        shutil.copytree(FIXTURE / "archive", ARCHIVE)

    def tearDown(self):
        pass

    def test_summarize_db(self):
        log_rule(Size.M)
        _logger.info('testing runner="Processor", task="summarize-all"')

        dataset = StatementsOfReasons()
        storage = Storage(
            archive_root=ARCHIVE, extract_root=None, staging_root=STAGING
        )
        release = Daily(2024, 3, 14)
        coverage = ReleaseRange(release, release)
        metadata = Metadata("db")
        metadata.write_json(storage.staging_root / f"{metadata.stem}.json")

        processor = Processor(
            dataset=dataset,
            storage=storage,
            coverage=coverage,
            config=Config(),
            metadata=metadata,
        )

        processor.run("summarize-all")

        # Preserve a copy to simplify updating the fixture when warranted
        shutil.copy(storage.staging_root / "db.parquet", TMP)

        self.validate_db_summary()
        _logger.info('completed test for runner="Processor", task="summarize-all"')

    def test_summarize_db_multiproc(self):
        log_rule(Size.S)
        _logger.info('testing runner="Multiprocessor", task="summarize-all"')

        dataset = StatementsOfReasons()
        storage = Storage(
            archive_root=ARCHIVE, extract_root=None, staging_root=STAGING
        )
        release = Daily(2024, 3, 14)
        coverage = ReleaseRange(release, release)
        metadata = Metadata("db")
        metadata.write_json(storage.staging_root / f"{metadata.stem}.json")

        processor = Multiprocessor(
            dataset=dataset,
            storage=storage,
            coverage=coverage,
            config=Config(workers=1),
            metadata=metadata,
        )

        processor.run("summarize-all")

        self.validate_db_summary()
        _logger.info('completed test for runner="Multiprocessor", task="summarize-all"')

    def validate_db_summary(self):
        self.assertFileEqual(STAGING / "db.json", FIXTURE / "db.json")
        self.assertFileEqual(ARCHIVE / "db.json", FIXTURE / "db.json")

        frame1 = pl.read_parquet(STAGING / "db.parquet")
        frame2 = pl.read_parquet(ARCHIVE / "db.parquet")
        self.assertFrameEqual(frame1, frame2)

        frame2 = Statistics.read(FIXTURE / "db.parquet").frame()
        self.assertFrameEqual(frame1, frame2)

        self.assertFileEqual(STAGING / "db.parquet", ARCHIVE / "db.parquet")

    def test_summarize_extract(self):
        log_rule(Size.M)
        _logger.info('testing runner="Processor", task="summarize-extract"')

        dataset = StatementsOfReasons()
        storage = Storage(
            archive_root=ARCHIVE, extract_root=EXTRACT, staging_root=STAGING
        )
        release = Daily(2024, 3, 14)
        coverage = ReleaseRange(release, release)
        metadata = Metadata.for_category(StatementCategoryProtectionOfMinors)
        metadata.write_json(storage.staging_root / f"{metadata.stem}.json")

        processor = Processor(
            dataset=dataset,
            storage=storage,
            coverage=coverage,
            config=Config(),
            metadata=metadata,
        )

        processor.run("summarize-extract")

        # Preserve a copy to simplify updating the fixture when warranted
        shutil.copy(storage.staging_root / "protection-of-minors.parquet", TMP)

        self.validate_extract_summary()
        _logger.info('completed test for runner="Processor", task="summarize-extract"')

    def test_summarize_extract_multiproc(self):
        log_rule(Size.S)
        _logger.info('testing runner="Multiprocessor", task="summarize-extract"')

        dataset = StatementsOfReasons()
        storage = Storage(
            archive_root=ARCHIVE, extract_root=EXTRACT, staging_root=STAGING
        )
        release = Daily(2024, 3, 14)
        coverage = ReleaseRange(release, release)
        metadata = Metadata.for_category(StatementCategoryProtectionOfMinors)
        metadata.write_json(storage.staging_root / f"{metadata.stem}.json")

        processor = Multiprocessor(
            dataset=dataset,
            storage=storage,
            coverage=coverage,
            config=Config(workers=1),
            metadata=metadata,
        )

        processor.run("summarize-extract")

        self.validate_extract_summary()
        _logger.info(
            'completed test for runner="Multiprocessor", task="summarize-extract"'
        )

    def validate_extract_summary(self):
        self.assertFileEqual(
            STAGING / "protection-of-minors.json",
            EXTRACT / "protection-of-minors.json"
        )

        # The per-release digests may differ between from the fixture since the
        # schema may incorporate additional platforms. Hence, we need to compare
        # ignoring the digests.
        self.assertMetaDataEqual(
            Metadata.read_json(STAGING / "protection-of-minors.json"),
            Metadata.read_json(FIXTURE / "protection-of-minors.json")
        )

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


def load_frame() -> pl.DataFrame:
    pl.Config.set_tbl_rows(20)
    pl.Config.set_tbl_cols(10)
    pl.Config.set_thousands_separator(",")

    dataset = StatementsOfReasons()
    release = Daily(2024, 3, 14)

    frames = []
    for index in range(2):
        frames.append(dataset._read_rows(
            csv_files=f"{FIXTURE}/csv/sor-global-{release.id}-full-{index:05}-*.csv",
            release=release,
            index=0,
            name="archive",
            filter=None,
        ))

    return pl.concat(frames, how="vertical")
