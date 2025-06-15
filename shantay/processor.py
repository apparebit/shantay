from collections import Counter
import hashlib
import logging
import os
from pathlib import Path
import platform
import shutil
import time
from typing import cast, NoReturn
from urllib.request import Request, urlopen
import zipfile

from .__init__ import __version__
from .metadata import compute_digest, Metadata
from .model import (
    CollectorProtocol, Coverage, Daily, DataFrameType, Dataset, DateRange, DIGEST_FILE,
    DownloadFailed, MetadataEntry, Release, Storage
)
from .pool import check_not_cancelled
from .progress import NO_PROGRESS, Progress
from .schema import (
    check_db_platforms, MissingPlatformError, update_platforms
)
from .stats import Statistics
from .util import annotate_error, scale_time
from .viz import visualize


_logger = logging.getLogger(__spec__.parent)


class Processor[R: Release]:

    CHUNK_SIZE = 64 * 1_024

    def __init__(
        self,
        *,
        dataset: Dataset,
        storage: Storage,
        coverage: Coverage[Daily],
        metadata: Metadata,
        offline: bool = False,
        progress: Progress = NO_PROGRESS,
    ) -> None:
        self._dataset = dataset
        self._storage = storage
        self._coverage = coverage
        self._metadata = metadata
        self._offline = offline
        self._progress = progress
        self._running_time = 0.0

    @property
    def stats_file(self) -> str:
        return f"{self._coverage.stem()}.parquet"

    @property
    def latency(self) -> float:
        """The latency of the most recent invocation of run()."""
        return self._running_time

    def run(self, task: str) -> None | DataFrameType:
        _logger.info('running processor with pid=%d, task="%s"', os.getpid(), task)
        _logger.info('    key="dataset.name",         value="%s"', self._dataset.name)
        _logger.info('    key="storage.archive_root", value="%s"', self._storage.archive_root)
        _logger.info('    key="storage.extract_root", value="%s"',
            "" if self._storage.extract_root is None else self._storage.extract_root)
        _logger.info('    key="storage.staging_root", value="%s"', self._storage.staging_root)
        _logger.info('    key="coverage.category",    value="%s"', self._coverage.category)
        _logger.info('    key="coverage.first",       value="%s"', self._coverage.first.id)
        _logger.info('    key="coverage.last",        value="%s"', self._coverage.last.id)
        _logger.info('    key="coverage.frequency",   value="%s"', self._coverage.frequency())
        _logger.info('    key="statistics.file",      value="%s"', self.stats_file)
        _logger.info('    key="network.offline",      value="%s"', self._offline)
        _logger.info('    key="pool.size",            value=1')

        # Arguably, time.process_time() would be the more accurate time source
        # for measuring latency. However, that may not hold for the parallel
        # version of shantay, as the main process doesn't do much data
        # processing. Hence, to keep any comparisons fair-ish, we use wall clock
        # time.
        start_time = time.time()
        result = None
        if task == "info":
            result = self.info()
        elif task == "download":
            result = self.download()
        elif task == "distill":
            result = self.distill_category()
        elif task == "summarize-all":
            result = self.summarize_database()
        elif task == "summarize-category":
            result = self.summarize_category()
        elif task == "visualize":
            result = self.visualize()
        else:
            raise ValueError(f'invalid task "{task}"')

        self._running_time = time.time() - start_time
        value, unit = scale_time(self._running_time)
        _logger.info('processing took time=%.3f, unit="%s"', value, unit)

        return result

    def info(self) -> None:
        keys = []
        values = []

        def emit_pair(key, value) -> None:
            keys.append(key)
            values.append(value)

        def emit_rule(strong: bool = False) -> None:
            keys.append(None)
            values.append(2 if strong else 1)

        def emit_range(dirname: str, source: str, range: None | DateRange) -> None:
            first = "n/a" if range is None else range.first.isoformat()
            last = "n/a" if range is None else range.last.isoformat()

            emit_pair(f'{dirname}.date-range.source', source)
            emit_pair(f'{dirname}.date-range.first', first)
            emit_pair(f'{dirname}.date-range.last', last)

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # Shantay, its dependencies, Python, and OS

        emit_rule(strong=True)
        emit_pair("shantay.version", __version__)
        emit_rule()
        import altair
        emit_pair("altair.version", altair.__version__)
        import polars
        emit_pair("polars.version", polars.__version__)
        import pyarrow
        emit_pair("pyarrow.version", pyarrow.__version__)
        emit_rule()
        emit_pair("platform.id", platform.platform())
        emit_pair("python.implementation", platform.python_implementation())
        emit_pair("python.version", platform.python_version())
        emit_pair("os.system", platform.system())
        emit_pair("os.release", platform.release())
        #record("os.version", platform.version())

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # Archive Root

        emit_rule(strong=True)
        emit_pair("archive.path", str(self._storage.archive_root))
        emit_rule()
        emit_range("archive", "file system", self._storage.coverage_of_archive())

        emit_rule()
        try:
            stats = Statistics.read(self._storage.archive_root / "db.parquet")
        except FileNotFoundError:
            stats = None
        emit_range("archive", "db.parquet", None if stats is None else stats.range())

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # Extract Root

        if self._storage.extract_root is not None:
            emit_rule(strong=True)
            emit_pair("extract.path", str(self._storage.extract_root))
            emit_rule()
            emit_range("extract", "file system", self._storage.coverage_of_extract())

            emit_rule()
            try:
                metapath = Metadata.find_file(self._storage.extract_root)
                metadata = Metadata.read_json(metapath)
            except FileNotFoundError:
                metapath = None
                metadata = None
            emit_range(
                "extract",
                "n/a" if metapath is None else metapath.name,
                None if metadata is None else metadata.range
            )

            emit_rule()
            filename = f"{self._coverage.stem()}.parquet"
            try:
                stats = Statistics.read(self._storage.extract_root / filename)
            except FileNotFoundError:
                stats = None
            emit_range("extract", filename, None if stats is None else stats.range())

        emit_rule(strong=True)

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # Actually emit the output

        key_width = max(0 if k is None else len(k) for k in keys)
        value_width = max(0 if v in (1, 2) else len(v) for v in values)
        width = key_width + 3 + value_width + 2

        for key, value in zip(keys, values):
            if key is None:
                if value == 1:
                    line = '─' * width
                else:
                    line = '━' * width
            else:
                line = f'{key:<{key_width}} = "{value}"'

            print(line)
            _logger.debug(line)

    def distill_category(self) -> None:
        for release in self._coverage:
            # Ensure graceful termination in offline mode
            if self._offline and not self.is_archive_downloaded(release):
                _logger.debug(
                    'stopping due to missing archive in offline mode '
                    'for task="distill", release="%s"',
                    release.id
                )
                break

            # Do the distillation
            self.distill_category_release(release)

            # The staging root's category-specific metadata was merged with the
            # extract's metadata during startup. Hence writing it back to the
            # extract directory won't lead to data loss---as long as there are
            # no concurrent writers!
            meta_json = f"{self._coverage.stem()}.json"
            Metadata.copy_json(
                self._storage.staging_root / meta_json,
                self._storage.the_extract_root / meta_json
            )

    def distill_category_release(self, release: Daily) -> None:
        if (
            release in self._metadata
            and distilled_category_exists(
                self._storage.the_extract_root, release, self._metadata
            )
        ):
            return

        _logger.debug('preparing release="%s"', release.id)
        if not self.is_archive_downloaded(release):
            self.download_archive(release)

        self.stage_archive(release)
        try:
            self.actually_distill_category_release(release)
        except Exception as x:
            x.add_note(
                f"WARNING: Artifacts for release {release} may be incomplete or corrupted!"
            )
            raise

        shutil.rmtree(self._storage.staging_root / release.parent_directory)
        self._progress.perform(f"done with {release.id}").done()
        return

    def download(self) -> None:
        if self._offline:
            raise ValueError("can't download daily distributions in offline mode")

        for release in self._coverage:
            if self.is_archive_downloaded(release):
                _logger.debug(
                    'skipping already downloaded release="%s"',
                    release.id
                )

            self.download_archive(release)
            shutil.rmtree(self._storage.staging_root / release.parent_directory)

    def download_archive(self, release: Daily) -> None:
        if self._offline:
            raise ValueError("can't download daily distributions in offline mode")
        if self.is_archive_downloaded(release):
            return

        self._progress.activity(
            f"downloading data for release {release.id}",
            f"downloading {release.id}", "byte", with_rate=True,
        )
        archive = self._dataset.archive_name(release)
        size = self._actually_download_archive(self._storage.staging_root, release)
        _logger.info('downloaded bytes=%d, file="%s"', size, archive)
        self._progress.perform(f"validating release {release.id}")
        self.validate_archive(self._storage.staging_root, release)
        _logger.info('validated file="%s"', archive)
        self._progress.perform(f"copying release {release.id} to archive")
        self.copy_archive(self._storage.staging_root, self._storage.archive_root, release)
        _logger.info('archived file="%s"', archive)

    def is_archive_downloaded(self, release: Daily) -> bool:
        """Determine whether the archive for the release has been downloaded."""
        return (
            self._storage.archive_root
            / release.parent_directory
            / self._dataset.archive_name(release)
        ).exists()

    @annotate_error(filename_arg="root")
    def _actually_download_archive(self, root: Path, release: Daily) -> int:
        """Download the release archive and digest."""
        if self._offline:
            raise ValueError("can't download daily distributions in offline mode")

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
                    check_not_cancelled()
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
    def validate_archive(self, root: Path, release: Daily) -> None:
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
    def copy_archive(self, source: Path, target: Path, release: Daily) -> None:
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

    def stage_archive(self, release: Daily) -> None:
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

    def is_archive_staged(self, release: Daily) -> bool:
        """"Determine whether the archive for the given release has been staged."""
        return (
            self._storage.staging_root
            / release.parent_directory
            / self._dataset.archive_name(release)
        ).exists()

    def actually_distill_category_release(self, release: Daily) -> None:
        """Extract the batches for the given release."""
        assert self.is_archive_staged(release)
        assert self._coverage.category is not None

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
            check_not_cancelled()

            self._progress.step(index, "unarchiving data")
            self.unarchive_file(self._storage.staging_root, release, index, name)
            digest, counters = self._dataset.distill_category_data(
                root=self._storage.staging_root,
                release=release,
                index=index,
                name=name,
                category=self._coverage.category,
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
        self._metadata.write_json(
            self._storage.staging_root / f"{self._coverage.stem()}.json"
        )
        _logger.info(
            'extracted batch-count=%d, file="%s"',
            batch_count,
            self._dataset.archive_name(release)
        )

        # It's safe to copy the batches here because each worker has its own,
        # isolated releases. So even if several workers are copying batch files
        # to the extract root, they only add subdirectories and files. That does
        # *not* hold for the metadata, which must be merged and written from a
        # single process such as the coordinator.
        self._progress.activity(
            f"copying batches for {release.id} out of staging",
            f"persisting {release.id}", "batch", with_rate=False,
        ).start(batch_count)
        self.copy_category_data(
            self._storage.staging_root, self._storage.the_extract_root, release, batch_count
        )
        _logger.info('archived batch-count=%d, release="%s"', batch_count, release.id)

    def list_archived_files(self, root: Path, release: Daily) -> list[str]:
        """Get the sorted list of files for the archive under the root directory."""
        path = root / release.parent_directory / self._dataset.archive_name(release)
        with zipfile.ZipFile(path) as archive:
            return sorted(archive.namelist())

    @annotate_error(filename_arg="root")
    def unarchive_file(self, root: Path, release: Daily, index: int, name: str) -> None:
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

    def category_data_exists(self, root: Path, release: Daily) -> bool:
        """Determine whether all batch files exist under the given root directory."""
        return distilled_category_exists(root, release, self._metadata)

    @annotate_error(filename_arg="target")
    def copy_category_data(
        self, source: Path, target: Path, release: Daily, count: int
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

    def summarize_category(self) -> DataFrameType:
        """Analyze the data extracted into the extract root."""
        # Prepare metadata for analysis
        range = self._metadata.range.intersection(
            self._coverage.to_date_range(), empty_ok=False
        ).dailies()

        # Prepare progress tracker
        self._progress.activity(
            "summarizing category data", "summarizing category", "batch", with_rate=False
        )
        self._progress.start(range.last - range.first + 1)

        stats = Statistics(self.stats_file)

        for index, release in enumerate(range):
            # Ensure graceful termination in offline mode
            if self._offline and not self.is_archive_downloaded(release):
                _logger.debug(
                    'stopping due to missing archive in offline mode '
                    'for task="summarize-category", release="%s"',
                    release.id
                )
                break

            self.distill_category_release(release)
            self.summarize_category_release(release, self._metadata[release], stats)
            # The generation of summary statistics creates a large number of
            # data frames (at least as few hundred), many of which have only one
            # row. That ensures that even daily statistics easily fit into
            # memory. But when there are too many data frames to concatenate,
            # Pola.rs gets stuck. Hence it's a good idea to regularly save the
            # statistics and thereby force reduction to a single data frame.
            # However, saving the statistics for every release noticeably slows
            # down progress. Hence we only save after processing n days worth of
            # data.
            if index % 11 == 0:
                stats.write(self._storage.staging_root)
            self._progress.step(index + 1, extra=release.id)

        return self._dataset.combine_releases(
            self._storage.the_extract_root, self.stats_file, stats
        )

    def summarize_category_release(
        self,
        release: Daily,
        metadata_entry: MetadataEntry,
        collector: CollectorProtocol,
    ) -> None:
        """Analyze the category-specific data for the given release."""
        assert isinstance(self._coverage.category, str)
        self._dataset.summarize_release(
            root=self._storage.the_extract_root,
            release=release,
            category=self._coverage.category,
            metadata_entry=metadata_entry,
            collector=collector
        )

    def summarize_database(self) -> DataFrameType:
        """Analyze the full data set."""
        stats = Statistics.from_storage(
            self.stats_file, self._storage.staging_root, self._storage.archive_root
        )

        staged = self._storage.staging_root / self.stats_file
        archive = self._storage.archive_root / self.stats_file

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
        for release in self._coverage:
            if cast(Daily, release) in stats:
                _logger.debug('summary statistics already cover release="%s"', release)
                continue

            # Ensure graceful termination in offline mode
            if self._offline and not self.is_archive_downloaded(release):
                _logger.debug(
                    'stopping due to missing archive in offline mode '
                    'for task="summarize-all", release="%s"',
                    release.id
                )
                break

            try:
                self.summarize_database_release(release, stats)
            except MissingPlatformError as x:
                # This method is only executed during single-process runs and
                # hence it is safe-ish to update the Python source code.
                update_platforms(x.args[0])
                raise
            _logger.debug('writing summary statistics to file="%s"', staged)
            stats.write(self._storage.staging_root)

        # Rewrite saved statistics after rechunking and copy to persistent root
        _logger.info('writing rechunked summary statistics to file="%s"', staged)
        stats.write(self._storage.staging_root, should_finalize=True)

        _logger.info('copying summary statistics to archive file="%s"', archive)
        Statistics.copy(
            self.stats_file, self._storage.staging_root, self._storage.archive_root
        )
        return stats.frame()

    def summarize_database_release(
        self,
        release: Daily,
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

            frame = self._dataset.ingest_category_data(
                root=self._storage.staging_root,
                release=release,
                index=index,
                name=name,
                progress=self._progress
            )

            # check_frame_platforms probes the data frame for hereto unknown
            # platform names, raises a MissingPlatformError with any unknown
            # names, but does not modify the _platform module. Hence, this
            # method can be safely executed by process pool workers, as long as
            # they communicate the error and its payload to the coordinator.
            check_db_platforms(release.id, index, frame)
            collector.collect(release, frame)

            # A daily release may comprise over 100 GB of uncompressed CSV data.
            # With three concurrent processes, that would be over 300 GB of disk
            # space for staging alone. Hence, we must aggressively clean up
            # temporary files again. This same operation is the last one of the
            # loop in distill_batches(), too.
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
            storage=self._storage,
            coverage=self._coverage,
            notebook=False,
        )


def distilled_category_exists(root: Path, release: Daily, metadata: Metadata) -> bool:
    """Determine whether all batch files exist under the given root directory."""
    path = root / release.directory
    for index in range(metadata.batch_count(release)):
        if not (path / release.batch_file(index)).exists():
            return False
    return True
