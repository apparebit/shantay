from collections import Counter
import enum
import logging
from pathlib import Path
import shutil
from typing import Any

from .metadata import Metadata, MetadataConflict
from .progress import Progress
from .release import DownloadFailed, Release
from .schedule import MonthlySchedule, Schedule, YearMonth
from .sor import Collector


_logger = logging.getLogger("shantay")


class Task(enum.StrEnum):
    PREPARE = "prepare"
    ANALYZE = "analyze"


class Runner:
    def __init__(
        self,
        *,
        archive: Path,
        batches: Path,
        progress: Progress,
        id: int = 0,
    ) -> None:
        self._id = id
        self._staging = Path.cwd() / (
            "dsa-db-staging" if id == 0 else f"dsa-db-staging-{id:03}"
        )
        self._archive = archive
        self._batches = batches

        self._metadata = None
        self._progress = progress

    @property
    def staging(self) -> Path:
        return self._staging

    @property
    def archive(self) -> Path:
        return self._archive

    @property
    def batches(self) -> Path:
        return self._batches

    @property
    def progress(self) -> Progress:
        if self._progress is None:
            raise ValueError("no release has been registered")
        return self._progress

    # ----------------------------------------------------------------------------------
    # Startup

    def start(self) -> None:
        _logger.info('runner=%d, key="staging", value="%s"', self._id, self._staging)
        _logger.info('runner=%d, key="archive", value="%s"', self._id, self._archive)
        _logger.info('runner=%d, key="batches", value="%s"', self._id, self._batches)

        self._staging.mkdir(parents=True, exist_ok=True)
        self._metadata = Metadata.setup(self._staging, self._batches)

    # ----------------------------------------------------------------------------------

    def is_archive_downloaded(self, release: Release) -> bool:
        return (self._archive / release.directory / release.archive).exists()

    def download_archive(self, release: Release) -> None:
        if self.is_archive_downloaded(release):
            return

        self._progress.activity(
            f"downloading data for release {release.id}",
            f"downloading {release.id}", "byte", with_rate=True,
        )
        size = release.download_archive(self._staging, self._progress)
        _logger.info('downloaded bytes=%d, file="%s"', size, release.archive)
        self._progress.perform(f"validating release {release.id}")
        release.validate_archive(self._staging)
        _logger.info('validated file="%s"', release.archive)
        self._progress.perform(f"copying release {release.id} to archive")
        release.copy_archive(self._staging, self._archive)
        _logger.info('archived file="%s"', release.archive)

    def is_archive_staged(self, release: Release) -> bool:
        return (self._staging / release.directory / release.archive).exists()

    def stage_archive(self, release: Release) -> None:
        assert self.is_archive_downloaded(release)

        if self.is_archive_staged(release):
            return

        self._progress.perform(f"copying release {release.id} from archive to staging")
        release.copy_archive(self._archive, self._staging)
        _logger.info('staged file="%s"', release.archive)
        self._progress.perform(f"validating release {release.id}")
        release.validate_archive(self._staging)
        _logger.info('validated file="%s"', release.archive)

    def extract_batches(self, category: str, release: Release) -> None:
        assert self.is_archive_staged(release)

        filenames = release.archived_files(self._staging)
        batch_count = len(filenames)
        self._progress.activity(
            f"extracting batches from release {release.id}",
            f"extracting {release.id}", "batch", with_rate=False,
        )
        steps = release.extract_data_step_count() + 1
        self._progress.start(steps * batch_count)

        # Archived files are archives, too. Unarchive one at a time.
        counters = Counter(batch_count=batch_count)
        for index, name in enumerate(filenames):
            self._progress.step(steps * index, "unarchiving data")
            release.unarchive_file(self._staging, index, name)
            counters += release.extract_data(
                self._staging, index, name, category, self._progress
            )

            shutil.rmtree(self._staging / release.working_directory)

        self._progress.perform(f"updating batch metadata for release {release.id}")
        self._metadata[release] = counters
        self._metadata.write_json(self._staging)
        _logger.info('extracted batch_count=%d, file="%s"', batch_count, release.archive)

        self._progress.activity(
            f"copying batches for {release.id} out of staging",
            f"persisting {release.id}", "batch", with_rate=False,
        ).start(batch_count)
        release.copy_extracted_data(self._staging, self._batches, batch_count, self._progress)
        _logger.info('archived batch_count=%d, release="%s"', batch_count, release.id)

    def prepare_batches(self, category: str, release: Release) -> None:
        if (
            release in self._metadata
            and release.extracted_data_exits(self._batches, self._metadata.batch_count(release))
        ):
            return

        _logger.debug('preparing release="%s"', release.id)
        if not self.is_archive_downloaded(release):
            self.download_archive(release)

        self.stage_archive(release)
        self.extract_batches(category, release)

        shutil.rmtree(self._staging / release.directory)
        self._progress.perform(f"done with {release.id}").done()
        return release

    def prepare(self, category: str, schedule: Schedule) -> None:
        for release in schedule:
            try:
                self.prepare_batches(category, release)
            except (DownloadFailed, MetadataConflict) as x:
                raise
            except Exception as x:
                x.add_note(
                    f"WARNING: Artifacts for release {release} may be incomplete or corrupted!"
                )
                raise

    def analyze_month(
        self,
        *,
        month: YearMonth,
        collector: Collector,
        release_type: type,
    ) -> None:
        """
        Analyze the category data for the given month, accumulating intermediate
        results with the collector instance.
        """
        release_type.analyze_month(root=self._batches, month=month, collector=collector)

    def combine_months(
        self,
        *,
        schedule: MonthlySchedule,
        collector: Collector,
        release_type: type,
    ) -> Any:
        """Combine the monthly analysis results into final results."""
        return release_type.combine_months(
            root=self._batches, schedule=schedule, collector=collector
        )

    def analyze(self, schedule: Schedule) -> Any:
        monthly_schedule = schedule.to_monthly()
        release_type = type(schedule.start)

        self._progress.activity(
            "analyzing monthly batches", "analyzing batches", "batch", with_rate=False
        )
        self._progress.start(monthly_schedule.months)

        collector = Collector()
        for index, month in enumerate(monthly_schedule):
            self.analyze_month(month=month, collector=collector, release_type=release_type)
            self._progress.step(index + 1, extra=month.id)

        return self.combine_months(
            schedule=monthly_schedule, collector=collector, release_type=release_type
        )
