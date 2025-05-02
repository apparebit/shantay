from collections import Counter
import datetime as dt
import hashlib
import logging
import os
from pathlib import Path
import shutil
import time
from typing import cast, Literal, NoReturn
from urllib.request import Request, urlopen
import zipfile

from .__init__ import __version__
from .framing import collect_release_metadata, filter_period
from .metadata import compute_digest, Metadata
from .model import (
    CollectorProtocol, Coverage, DataFrameType, Dataset, DIGEST_FILE, DownloadFailed,
    MetadataEntry, Release, StatSource, Storage
)
from .pool import check_not_cancelled
from .progress import NO_PROGRESS, Progress
from .stats import (
    check_new_platform_names, Collector, MissingPlatformError, Statistics,
    update_new_platform_names
)
from .util import annotate_error, scale_time
from .viz import visualize


_logger = logging.getLogger(__spec__.parent)


class Processor[R: Release]:

    CHUNK_SIZE = 64 * 1_024

    def __init__(
        self,
        *,
        dataset: Dataset[R],
        storage: Storage,
        coverage: Coverage[R],
        metadata: Metadata,
        progress: Progress = NO_PROGRESS,
        stat_source: StatSource = None,
    ) -> None:
        self._dataset = dataset
        self._storage = storage
        self._coverage = coverage
        self._metadata = metadata
        self._progress = progress
        self._stat_source: StatSource = stat_source
        self._frequency: Literal["daily", "monthly"]
        self._runtime = 0.0

    @property
    def runtime(self) -> float:
        """The latency of the most recent invocation of run()."""
        return self._runtime

    def run(self, task: str) -> None:
        _logger.info('running processor with pid=%d, task="%s"', os.getpid(), task)
        _logger.info('    key="dataset.name",         value="%s"', self._dataset.name)
        _logger.info('    key="storage.archive_root", value="%s"', self._storage.archive_root)
        _logger.info('    key="storage.working_root", value="%s"', self._storage.working_root)
        _logger.info('    key="storage.staging_root", value="%s"', self._storage.staging_root)
        _logger.info('    key="coverage.filter",      value="%s"', self._coverage.filter)
        _logger.info('    key="coverage.first",       value="%s"', self._coverage.first.id)
        _logger.info('    key="coverage.last",        value="%s"', self._coverage.last.id)

        # Arguably, time.process_time() would be the more accurate time source
        # for measuring latency. However, that may not hold for the parallel
        # version of shantay, as the main process doesn't do much data
        # processing. Hence, to keep any comparisons fair-ish, we use wall clock
        # time.
        start_time = time.time()
        if task == "prepare":
            self.prepare()
        elif task == "summarize":
            self.summarize_archive()
        elif task == "analyze":
            self.analyze_working()
        elif task == "visualize":
            self.visualize()
        else:
            raise ValueError(f'invalid task "{task}"')

        self._runtime = time.time() - start_time
        value, unit = scale_time(self._runtime)
        _logger.info('processing took time=%.3f, unit="%s"', value, unit)

    def prepare(self) -> None:
        for release in self._coverage:
            self.prepare_batches(release)
            # If the working root contains a meta.json, then the staging root's
            # meta.json was created with that file's data. Hence it's perfectly
            # fine to copy back the JSON after each release. In fact, that
            # avoids metadata loss if the prepare task is interrupted.
            Metadata.copy_json(self._storage.staging_root, self._storage.working_root)
            # Meanwhile the archive copy is just that, a copy. It helps simplify
            # the logic of visualization.
            Metadata.copy_json(self._storage.staging_root, self._storage.archive_root)

    def prepare_batches(self, release: R) -> None:
        if (
            release in self._metadata
            and extracted_data_exists(self._storage.working_root, release, self._metadata)
        ):
            return

        _logger.debug('preparing release="%s"', release.id)
        if not self.is_archive_downloaded(release):
            self.download_archive(release)

        self.stage_archive(release)
        try:
            self.extract_batches(release)
        except Exception as x:
            x.add_note(
                f"WARNING: Artifacts for release {release} may be incomplete or corrupted!"
            )
            raise

        shutil.rmtree(self._storage.staging_root / release.parent_directory)
        self._progress.perform(f"done with {release.id}").done()
        return

    def download_archive(self, release: R) -> None:
        if self.is_archive_downloaded(release):
            return

        self._progress.activity(
            f"downloading data for release {release.id}",
            f"downloading {release.id}", "byte", with_rate=True,
        )
        archive = self._dataset.archive_name(release)
        size = self._download_archive(self._storage.staging_root, release)
        _logger.info('downloaded bytes=%d, file="%s"', size, archive)
        self._progress.perform(f"validating release {release.id}")
        self.validate_archive(self._storage.staging_root, release)
        _logger.info('validated file="%s"', archive)
        self._progress.perform(f"copying release {release.id} to archive")
        self.copy_archive(self._storage.staging_root, self._storage.archive_root, release)
        _logger.info('archived file="%s"', archive)

    def is_archive_downloaded(self, release: R) -> bool:
        """Determine whether the archive for the release has been downloaded."""
        return (
            self._storage.archive_root
            / release.parent_directory
            / self._dataset.archive_name(release)
        ).exists()

    @annotate_error(filename_arg="root")
    def _download_archive(self, root: Path, release: R) -> int:
        """Download the release archive and digest."""
        digest = self._dataset.digest_name(release)
        url = self._dataset.url(digest)
        path = root / release.parent_directory

        with urlopen(Request(url, None, {})) as response:
            if response.status != 200:
                self._download_failed("digest", url, response.status)

            path.mkdir(parents=True, exist_ok=True)
            with open(path / digest, mode="wb") as file:
                shutil.copyfileobj(response, file)

        archive = self._dataset.archive_name(release)
        url = self._dataset.url(archive)
        with urlopen(Request(url, None, {})) as response:
            if response.status != 200:
                self._download_failed("archive", url, response.status)

            content_length = response.getheader("content-length")
            content_length = (
                None if content_length is None else int(content_length.strip())
            )
            downloaded = 0

            with open(path / archive, mode="wb") as file:
                self._progress.start(content_length)
                while True:
                    chunk = response.read(self.CHUNK_SIZE)
                    if not chunk:
                        break
                    file.write(chunk)

                    downloaded += len(chunk)
                    self._progress.step(downloaded)

            return downloaded

    def _download_failed(self, artifact: str, url: str, status: int) -> NoReturn:
        """Signal that the download failed."""
        _logger.error(
            'failed to download type="%s", status=%d, url="%s"', artifact, status, url
        )
        raise DownloadFailed(
            f'download of {artifact} "{url}" failed with status {status}'
        )

    @annotate_error(filename_arg="root")
    def validate_archive(self, root: Path, release: R) -> None:
        """Validate the archive stored under the root against its digest."""
        digest = root / release.parent_directory / self._dataset.digest_name(release)
        with open(digest, mode="rt", encoding="ascii") as file:
            expected = file.read().strip()
            expected = expected[:expected.index(" ")]

        algo = digest.suffix[1:]
        archive = root / release.parent_directory / self._dataset.archive_name(release)
        with open(archive, mode="rb") as file:
            actual = hashlib.file_digest(file, algo).hexdigest()

        if expected != actual:
            _logger.error('failed to validate digest=%s, file="%s"', algo, archive)
            raise ValueError(f'digest {actual} does not match {expected}')

    @annotate_error(filename_arg="target")
    def copy_archive(self, source: Path, target: Path, release: R) -> None:
        """
        Copy the archive and digest stored under the source directory to the
        target directory.
        """
        source_dir = source / release.parent_directory
        target_dir = target / release.parent_directory
        digest = self._dataset.digest_name(release)
        archive = self._dataset.archive_name(release)

        target_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(source_dir / digest, target_dir / digest)
        shutil.copy(source_dir / archive, target_dir / archive)

    def stage_archive(self, release: R) -> None:
        """
        Stage the archive for the given release. The archive must have been
        downloaded before.
        """
        assert self.is_archive_downloaded(release)

        if self.is_archive_staged(release):
            return

        archive = self._dataset.archive_name(release)
        self._progress.perform(f"copying release {release.id} from archive to staging")
        self.copy_archive(self._storage.archive_root, self._storage.staging_root, release)
        _logger.info('staged file="%s"', archive)
        self._progress.perform(f"validating release {release.id}")
        self.validate_archive(self._storage.staging_root, release)
        _logger.info('validated file="%s"', archive)

    def is_archive_staged(self, release: R) -> bool:
        """"Determine whether the archive for the given release has been staged."""
        return (
            self._storage.staging_root
            / release.parent_directory
            / self._dataset.archive_name(release)
        ).exists()

    def extract_batches(self, release: R) -> None:
        """Extract the batches for the given release."""
        assert self.is_archive_staged(release)
        assert self._coverage.filter is not None

        filenames = self.list_archived_files(self._storage.staging_root, release)
        batch_count = len(filenames)
        self._progress.activity(
            f"extracting batches from release {release.id}",
            f"extracting {release.id}", "batch", with_rate=False,
        )
        self._progress.start(batch_count)

        # Archived files are archives, too. Unarchive one at a time.
        batch_digests = []
        full_counters = Counter(batch_count=batch_count)
        for index, name in enumerate(filenames):
            self._progress.step(index, "unarchiving data")
            self.unarchive_file(self._storage.staging_root, release, index, name)
            digest, counters = self._dataset.extract_file_data(
                root=self._storage.staging_root,
                release=release,
                index=index,
                name=name,
                filter=self._coverage.filter,
                progress=self._progress
            )
            batch_digests.append(digest)
            full_counters += counters

            shutil.rmtree(self._storage.staging_root / release.temp_directory)

        digest_file = self._storage.staging_root / release.directory / DIGEST_FILE
        with open(digest_file, mode="w", encoding="utf8") as file:
            for index, digest in enumerate(batch_digests):
                file.write(f"{digest} {release.id}-{index:05}.parquet\n")

        self._progress.perform(f"updating batch metadata for release {release.id}")
        meta_data_entry = cast(MetadataEntry, dict(full_counters))
        meta_data_entry["sha256"] = compute_digest(digest_file)
        self._metadata[release] = meta_data_entry
        self._metadata.write_json(self._storage.staging_root)
        _logger.info(
            'extracted batch-count=%d, file="%s"',
            batch_count,
            self._dataset.archive_name(release)
        )

        # It's safe to copy the batches here because each worker has its own,
        # isolated releases. So even if several workers are copying batch files
        # to the working root, they only add subdirectories and files. That does
        # *not* hold for the metadata, which must be merged and written from a
        # single process such as the coordinator.
        self._progress.activity(
            f"copying batches for {release.id} out of staging",
            f"persisting {release.id}", "batch", with_rate=False,
        ).start(batch_count)
        self.copy_extracted_data(
            self._storage.staging_root, self._storage.working_root, release, batch_count
        )
        _logger.info('archived batch-count=%d, release="%s"', batch_count, release.id)

    def list_archived_files(self, root: Path, release: R) -> list[str]:
        """Get the sorted list of files for the archive under the root directory."""
        path = root / release.parent_directory / self._dataset.archive_name(release)
        with zipfile.ZipFile(path) as archive:
            return sorted(archive.namelist())

    @annotate_error(filename_arg="root")
    def unarchive_file(self, root: Path, release: R, index: int, name: str) -> None:
        """
        Unarchive the file with index and name from the archive under the source
        directory into a suitable directory under the target directory.
        """
        input = root / release.parent_directory / self._dataset.archive_name(release)
        with zipfile.ZipFile(input) as archive:
            with archive.open(name) as source_file:
                output = root / release.temp_directory
                output.mkdir(parents=True, exist_ok=True)

                if name.endswith(".zip"):
                    kind = "nested archive"
                    with zipfile.ZipFile(source_file) as nested_archive:
                        nested_archive.extractall(output)
                else:
                    kind = "file"
                    with open(output / name, mode="wb") as target_file:
                        shutil.copyfileobj(source_file, target_file)
                _logger.debug('unarchived type="%s", file="%s"', kind, name)

    def extracted_data_exists(self, root: Path, release: R) -> bool:
        """Determine whether all batch files exist under the given root directory."""
        return extracted_data_exists(root, release, self._metadata)

    @annotate_error(filename_arg="target")
    def copy_extracted_data(
        self, source: Path, target: Path, release: R, count: int
    ) -> None:
        """Copy the batch files between root directories."""
        source_dir = source / release.directory
        target_dir = target / release.directory
        target_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(source_dir / DIGEST_FILE, target_dir / DIGEST_FILE)
        for index in range(count):
            batch = release.batch_file(index)
            shutil.copy(source_dir / batch, target_dir / batch)
            self._progress.step(index)

    def analyze_working(self) -> DataFrameType:
        """Analyze the data extracted into the working root."""
        # Prepare metadata for analysis
        range, metadata = collect_release_metadata(self._metadata.records)
        range = range.intersection(
            self._coverage.to_date_range(), empty_ok=False
        ).monthlies()

        # Prepare progress tracker
        self._progress.activity(
            "analyzing monthly batches", "analyzing batches", "batch", with_rate=False
        )
        self._progress.start(range.last - range.first + 1)

        collector = Collector()

        for index, release in enumerate(range):
            check_not_cancelled()
            self.analyze_working_release(release, metadata, collector)
            self._progress.step(index + 1, extra=release.id)
        return self._dataset.combine_releases(
            self._storage.working_root, self._coverage, collector
        )

    def analyze_working_release(
        self,
        release: Release,
        metadata: DataFrameType,
        collector: CollectorProtocol,
    ) -> None:
        """Analyze the working data for the given release."""
        release_metadata = filter_period(metadata, release)
        self._dataset.analyze_release(
            self._storage.working_root, release, release_metadata, collector
        )

    def summarize_archive(self) -> None:
        """Analyze the full data set."""
        staged = self._storage.staging_root / Statistics.FILE
        archive = self._storage.archive_root / Statistics.FILE

        stats = Statistics.from_storage(
            self._storage.staging_root, self._storage.archive_root
        )

        if not stats.is_empty():
            range = stats.range()
            _logger.info(
                'existing statistics cover start_date="%s", end_date="%s"',
                range.first, range.last
            )

        # Due to variability of daily record numbers and worker process timing,
        # the multiprocessing version of summarize may add daily statistics out
        # of calendar order. By always processing all possible release dates in
        # order, this loop ensures that any holes are filled, making this a
        # robust, self-healing implementation strategy.
        for release in Statistics.DEFAULT_RANGE.dailies():
            if release in stats:
                _logger.debug('summary statistics already cover release="%s"', release)
                continue
            try:
                self.summarize_archived_release(cast(R, release), stats)
            except MissingPlatformError as x:
                # This method is only executed during single-process runs and
                # hence it is safe-ish to update the Python source code.
                update_new_platform_names(x.args[2])
                raise
            _logger.debug('writing summary statistics to file="%s"', staged)
            stats.write(self._storage.staging_root)

        # Rewrite saved statistics after rechunking and copy to persistent root
        _logger.debug('writing rechunked summary statistics to file="%s"', staged)
        stats.write(self._storage.staging_root, rechunk=True)

        _logger.debug('copying summary statistics to archive file="%s"', archive)
        Statistics.copy(self._storage.staging_root, self._storage.archive_root)

    def summarize_archived_release(
        self,
        release: R,
        collector: CollectorProtocol,
    ) -> None:
        """Analyze the full data for the given release."""
        _logger.debug('analyzing release="%s"', release.id)
        if not self.is_archive_downloaded(release):
            self.download_archive(release)

        self.stage_archive(release)

        filenames = self.list_archived_files(self._storage.staging_root, release)
        batch_count = len(filenames)
        self._progress.activity(
            f"analyzing batches from release {release.id}",
            f"analyzing {release.id}", "batch", with_rate=False,
        )
        self._progress.start(batch_count)

        # Archived files are archives, too. Unarchive one at a time.
        for index, name in enumerate(filenames):
            check_not_cancelled()

            self._progress.step(index, "unarchiving data")
            self.unarchive_file(self._storage.staging_root, release, index, name)

            frame = self._dataset.ingest_file_data(
                root=self._storage.staging_root,
                release=release,
                index=index,
                name=name,
                progress=self._progress
            )

            # Proactively check for hereto unknown platform names. Since this
            # method may be executed concurrently by several process pool
            # workers, we only extract new names here but make no updates.
            check_new_platform_names(release.id, index, frame)
            collector.collect(release, frame)

            # A daily release may comprise over 100 GB of uncompressed CSV data.
            # With three concurrent processes, that would be over 300 GB of disk
            # space for staging alone. Hence, we must aggressively clean up
            # temporary files again. This same operation is the last one of the
            # loop in extract_batches(), too.
            shutil.rmtree(self._storage.staging_root / release.temp_directory)

        # The data frame generated by this method is a small one indeed. Hence,
        # there is no need to save it to disk first. We must, however, continue
        # cleaning up aggressively. While not as huge as uncompressed CSV data,
        # the actual release for a 100 GB of CSV data still weighs in at over 8
        # GB. This same operation is the last one of prepare_batches(), too.
        shutil.rmtree(self._storage.staging_root / release.parent_directory)

    def visualize(self) -> None:
        """Visualize the analysis results."""
        visualize(
            self._storage, self._coverage, notebook=False, stat_source=self._stat_source
        )


def extracted_data_exists(root: Path, release: Release, metadata: Metadata) -> bool:
    """Determine whether all batch files exist under the given root directory."""
    path = root / release.directory
    for index in range(metadata.batch_count(release)):
        if not (path / release.batch_file(index)).exists():
            return False
    return True
