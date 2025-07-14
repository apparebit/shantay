from collections import Counter
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
from .digest import (
    compute_digest, read_digest_file, validate_digests, write_digest_file
)
from .metadata import Metadata
from .model import (
    CollectorProtocol, Coverage, Daily, DataFrameType, Dataset, DateRange, DIGEST_FILE,
    DownloadFailed, MetadataEntry, Release, Storage
)
from .pool import check_not_cancelled
from .progress import NO_PROGRESS, Progress
from .schema import (
    check_db_platforms, is_category_file, MissingPlatformError, update_platforms
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
        _logger.info('    key="storage.archive_root", value="%s"',
            "" if self._storage.archive_root is None else self._storage.archive_root)
        _logger.info('    key="storage.extract_root", value="%s"',
            "" if self._storage.extract_root is None else self._storage.extract_root)
        _logger.info('    key="storage.staging_root", value="%s"', self._storage.staging_root)
        _logger.info('    key="coverage.category",    value="%s"',
            "" if self._coverage.category is None else self._coverage.category)
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
        elif task == "summarize-builtin":
            stats = Statistics.builtin()
            stats.write(self._storage.staging_root / self.stats_file)
            result = stats.frame()
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
        emit_pair("archive.path", "<builtin>")
        emit_rule()
        emit_range("archive", "db.parquet", Statistics.builtin().date_range())

        if self._storage.archive_root is not None:
            emit_rule(strong=True)
            emit_pair("archive.path", str(self._storage.archive_root))
            emit_rule()
            emit_range("archive", "<file-system>", self._storage.coverage_of_archive())

            path = self._storage.archive_root / "db.json"
            if path.exists():
                try:
                    metarange = Metadata.read_json(path).range
                except FileNotFoundError | ValueError:
                    metarange = None

                emit_rule()
                emit_range("archive", "db.json", metarange)

            path = self._storage.archive_root / "db.parquet"
            if path.exists():
                try:
                    stats_range = Statistics.read(path).date_range()
                except FileNotFoundError:
                    stats_range = None

                emit_rule()
                emit_range("archive", "db.parquet", stats_range)

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # Extract Root

        if self._storage.extract_root is not None:
            emit_rule(strong=True)
            emit_pair("extract.path", str(self._storage.extract_root))
            emit_rule()
            emit_range("extract", "<file-system>", self._storage.coverage_of_extract())

            try:
                metapath = Metadata.find_file(self._storage.extract_root)
            except FileNotFoundError:
                metapath = None
            if metapath is not None:
                metarange = Metadata.read_json(metapath).range

                emit_rule()
                emit_range("extract", metapath.name, metarange)

            path = self._storage.extract_root / f"{self._coverage.stem()}.parquet"
            if path.exists():
                stats_range = Statistics.read(path).date_range()

                emit_rule()
                emit_range("extract", path.name, stats_range)

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # Staging Root

        emit_rule(strong=True)
        emit_pair("staging.path", str(self._storage.staging_root))

        for file in sorted(self._storage.staging_root.glob("*.json")):
            if file.name != "db.json" and not is_category_file(file.stem):
                continue

            try:
                metarange = Metadata.read_json(file).range
            except ValueError:
                metarange = None

            emit_rule()
            emit_range("staging", file.name, metarange)

        for file in sorted(self._storage.staging_root.glob("*.parquet")):
            if file.name != "db.parquet" and not is_category_file(file.stem):
                continue

            emit_rule()
            emit_range("staging", file.name, Statistics.read(file).date_range())

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # Actually emit the output

        key_width = max(0 if k is None else len(k) for k in keys)
        value_width = max(0 if v in (1, 2) else len(v) for v in values)
        width = key_width + 3 + value_width

        for key, value in zip(keys, values):
            if key is None:
                if value == 1:
                    line = '─' * width
                else:
                    line = '━' * width
            else:
                line = f'{key:<{key_width}} = {value}'

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

    def distill_category_release(self, release: Daily, cleanup: bool = True) -> None:
        if (
            release in self._metadata
            and distilled_category_exists(
                self._storage.the_extract_root, release, self._metadata
            )
        ):
            return

        _logger.debug('distill release="%s"', release.id)
        if not self.is_archive_downloaded(release):
            self.download_archive(release)

        self.stage_archive(release)
        try:
            self._actually_distill_category_release(release)
        except Exception as x:
            x.add_note(
                f"WARNING: Artifacts for release {release} may be incomplete or corrupted!"
            )
            raise

        if cleanup:
            shutil.rmtree(self._storage.staging_root / release.parent_directory)
        self._progress.perform(f"distilled {release.id}").done()
        return

    def download(self) -> None:
        if self._offline:
            raise ValueError("can't download daily distributions in offline mode")

        for release in self._coverage:
            self.download_archive(release)
            shutil.rmtree(self._storage.staging_root / release.parent_directory)

    def download_archive(self, release: Daily) -> None:
        if self._offline:
            raise ValueError("can't download daily distributions in offline mode")
        if self.is_archive_downloaded(release):
            _logger.debug('already downloaded release="%s"', release.id)
            return

        _logger.debug(
            'download release="%s", directory="%s"',
            release.id, self._storage.staging_root
        )
        self._progress.activity(
            f"downloading data for release {release.id}",
            f"downloading {release.id}", "byte", with_rate=True,
        )
        archive = self._dataset.archive_name(release)
        size = self._actually_download_archive(self._storage.staging_root, release)
        _logger.info(
            'downloaded release="%s", size=%d, file="%s"',
            release.id,
            size,
            self._storage.staging_root / release.parent_directory / archive
        )
        self._progress.perform(f"validating release {release.id}")
        self.validate_archive(self._storage.staging_root, release)
        self._progress.perform(f"copying release {release.id} to archive")
        self.copy_archive(
            self._storage.staging_root, self._storage.the_archive_root, release
        )
        _logger.info(
            'archived release="%s", file="%s"',
            release.id,
            self._storage.the_archive_root / release.parent_directory / archive
        )

    def is_archive_downloaded(self, release: Daily) -> bool:
        """Determine whether the archive for the release has been downloaded."""
        return (
            self._storage.the_archive_root
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
        digest = root / release.parent_directory / self._dataset.digest_name(release)
        archive = root / release.parent_directory / self._dataset.archive_name(release)
        _logger.debug('validate release="%s", file="%s"', release.id, archive)

        with open(digest, mode="rt", encoding="ascii") as file:
            expected = file.read().strip()
            expected = expected[:expected.index(" ")]

        algo = digest.suffix[1:]
        if algo not in ("sha1", "sha256"):
            raise ValueError(f"invalid digest algorithm {algo}")

        actual = compute_digest(archive, algo=algo)
        if actual != expected:
            _logger.error(
                'failed to validate file="%s", algo="%s", expected="%s", actual="%s"',
                archive, algo, digest, actual
            )
            raise ValueError(
                f'{archive} should have {algo} digest {digest} but has {actual}'
            )

        _logger.info(
            'validated release="%s", file="%s", digest="%s"',
            release.id, archive, expected
        )

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
        digest_path = target_dir / digest
        archive_path = target_dir / archive

        target_dir.mkdir(parents=True, exist_ok=True)

        if archive_path.exists():
            raise ValueError(f"cannot copy over existing {archive_path}")
        shutil.copy(source_dir / archive, archive_path)

        if digest_path.exists():
            raise ValueError(f"cannot copy over existing {digest_path}")
        shutil.copy(source_dir / digest, digest_path)

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
        self.copy_archive(
            self._storage.the_archive_root, self._storage.staging_root, release
        )
        _logger.info('staged file="%s"', archive)
        self._progress.perform(f"validating release {release.id}")
        self.validate_archive(self._storage.staging_root, release)

    def is_archive_staged(self, release: Daily) -> bool:
        """"Determine whether the archive for the given release has been staged."""
        return (
            self._storage.staging_root
            / release.parent_directory
            / self._dataset.archive_name(release)
        ).exists()

    def _actually_distill_category_release(self, release: Daily) -> None:
        """Distill the batches for the given release."""
        assert self.is_archive_staged(release)
        assert self._coverage.category is not None

        filenames = self.list_archived_files(self._storage.staging_root, release)
        batch_count = len(filenames)
        self._progress.activity(
            f"distilling batches of release {release.id}",
            f"distilling {release.id} ", "batch", with_rate=False,
        )
        self._progress.start(batch_count)

        # Archived files are archives, too. Unarchive one at a time.
        batch_digests = {}
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
            batch_digests[f"{release.id}-{index:05}.parquet"] = digest
            full_counters += counters

            # The complete CSV data may take up 100 GB of disk space. So we need
            # to aggressively reclaim storage to avoid filling the file system
            # with the staging directory.
            shutil.rmtree(self._storage.staging_root / release.temp_directory)

        digest_file = self._storage.staging_root / release.directory / DIGEST_FILE
        write_digest_file(digest_file, batch_digests)

        self._progress.perform(f"updating batch metadata for release {release.id}")
        meta_data_entry = cast(MetadataEntry, dict(full_counters))
        meta_data_entry["sha256"] = compute_digest(digest_file)
        self._metadata[release] = meta_data_entry
        self._metadata.write_json(
            self._storage.staging_root / f"{self._coverage.stem()}.json"
        )
        _logger.info(
            'distilled release="%s", batch-count=%d, category="%s"',
            release.id, batch_count, self._coverage.category
        )

        # It's ok for a worker process to copy the batches to long-term storage
        # because each worker processes different releases. So even if several
        # workers are concurrently copying batches to the extract root, they are
        # only adding new subdirectories and files. That does *not* hold for the
        # metadata, which must be merged and written by the coordinator.
        self._progress.activity(
            f"copying batches for {release.id} out of staging",
            f"persisting {release.id}", "batch", with_rate=False,
        ).start(batch_count)
        self.copy_category_data(
            self._storage.staging_root, self._storage.the_extract_root, release, batch_count
        )
        _logger.info(
            'persisted release="%s", batch-count=%d, category="%s"',
            release.id, batch_count, self._coverage.category
        )

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

            target_path = target_dir / batch
            if target_path.exists():
                raise ValueError(f"cannot copy over existing {target_path}")
            shutil.copy(source_dir / batch, target_path)

            self._progress.step(index)

    def stage_category_data(self, release: Daily) -> None:
        batch_count = self._metadata[release]["batch_count"]
        if self.category_data_exists(self._storage.staging_root, release):
            return

        self.copy_category_data(
            self._storage.the_extract_root,
            self._storage.staging_root,
            release,
            batch_count,
        )
        _logger.info('staged extract for release="%s"', release)

        digest = self._metadata[release].get("sha256")
        if digest is not None:
            try:
                validate_digests(
                    self._storage.staging_root / release.directory,
                    release.batch_glob,
                    self._storage.staging_root / release.directory / DIGEST_FILE,
                    digest,
                )
            except ValueError as x:
                _logger.error(
                    'could not validate extract for release="%s"', release, exc_info=x
                )
                raise

        _logger.info('validated extract for release="%s"', release)

    def summarize_category(self) -> DataFrameType:
        """Analyze the data distilled into the extract root."""
        # Prepare progress tracker
        self._progress.activity(
            "summarizing category data", "summarizing category", "batch", with_rate=False
        )
        self._progress.start(self._coverage.last - self._coverage.first + 1)

        stats = Statistics(self.stats_file)

        for index, release in enumerate(self._coverage):
            # Ensure graceful termination in offline mode
            if self._offline and not self.is_archive_downloaded(release):
                _logger.info(
                    'stopping due to missing archive in offline mode '
                    'for task="summarize-category", release="%s"',
                    release.id
                )
                break

            if not distilled_category_exists(
                self._storage.the_extract_root, release, self._metadata
            ):
                with self._progress.nested():
                    self.distill_category_release(release, cleanup=False)
            self.summarize_category_release(release, self._metadata[release], stats)

            # While collecting summary statistics, Shantay generates hundreds of
            # data frames, many with just one row. However, concatenation in
            # Pola.rs doesn't seem to have linear performance and gets stuck
            # when there are too many frames. Hence, we regularly save
            # statistics, which concatenates the frames. Yet, we need to avoid
            # saving too often, which noticeably slows down Shantay. As a
            # compromise, we only save after processing n=11 days worth of data.
            if index % 11 == 0:
                stats.write(self._storage.staging_root)
            self._progress.step(index + 1, extra=release.id)

        meta_json = f"{self._coverage.stem()}.json"
        Metadata.copy_json(
            self._storage.staging_root / meta_json,
            self._storage.the_extract_root / meta_json
        )

        _logger.info(
            'writing rechunked summary statistics to file="%s"',
            self._storage.staging_root / self.stats_file
        )
        stats.write(self._storage.staging_root, should_finalize=True)

        _logger.info(
            'copying summary statistics to extract file="%s"',
            self._storage.the_extract_root / self.stats_file
        )
        Statistics.copy(
            self.stats_file, self._storage.staging_root, self._storage.the_extract_root
        )
        return stats.frame()

    def summarize_category_release(
        self,
        release: Daily,
        metadata_entry: MetadataEntry,
        collector: CollectorProtocol,
    ) -> None:
        """Analyze the category-specific data for the given release."""
        self.stage_category_data(release)

        assert isinstance(self._coverage.category, str)
        self._dataset.summarize_release(
            root=self._storage.staging_root,
            release=release,
            category=self._coverage.category,
            metadata_entry=metadata_entry,
            collector=collector
        )

        shutil.rmtree(self._storage.staging_root / release.parent_directory)

    def summarize_database(self) -> DataFrameType:
        """Analyze the full data set."""
        stats = Statistics.from_storage(
            self.stats_file, self._storage.staging_root, self._storage.the_archive_root
        )

        staged = self._storage.staging_root / self.stats_file
        archive = self._storage.the_archive_root / self.stats_file

        if not stats.is_empty():
            range = stats.date_range()
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
                # hence it is safe-ish to update the list of platforms here.
                update_platforms(x.args[0])
                raise
            _logger.debug('writing summary statistics to file="%s"', staged)
            stats.write(self._storage.staging_root)

        meta_json = f"{self._coverage.stem()}.json"
        Metadata.copy_json(
            self._storage.staging_root / meta_json,
            self._storage.the_archive_root / meta_json
        )

        # Rewrite saved statistics after rechunking and copy to persistent root
        _logger.info('writing rechunked summary statistics to file="%s"', staged)
        stats.write(self._storage.staging_root, should_finalize=True)

        _logger.info('copying summary statistics to archive file="%s"', archive)
        Statistics.copy(
            self.stats_file, self._storage.staging_root, self._storage.the_archive_root
        )
        return stats.frame()

    def summarize_database_release(
        self,
        release: Daily,
        collector: CollectorProtocol,
    ) -> None:
        """Analyze the full data for the given release."""
        _logger.info('summarizing release="%s"', release.id)
        if not self.is_archive_downloaded(release):
            self.download_archive(release)

        self.stage_archive(release)

        filenames = self.list_archived_files(self._storage.staging_root, release)
        batch_count = len(filenames)
        self._progress.activity(
            f"summarizing batches from release {release.id}",
            f"summarizing {release.id}", "batch", with_rate=False,
        )
        self._progress.start(batch_count)

        full_counts = Counter(batch_count=batch_count)

        # Archived files are archives, too. Unarchive one at a time.
        for index, name in enumerate(filenames):
            check_not_cancelled()

            self._progress.step(index, "unarchiving data")
            self.unarchive_file(self._storage.staging_root, release, index, name)

            counts, frame = self._dataset.ingest_database_data(
                root=self._storage.staging_root,
                release=release,
                index=index,
                name=name,
                progress=self._progress
            )

            full_counts += counts

            # Check_db_platforms only probes the data frame for hereto unknown
            # platform names, raising a MissingPlatformError with such names.
            path = (
                self._storage.staging_root
                / release.parent_directory
                / self._dataset.archive_name(release)
            )
            check_db_platforms(path , frame)

            # We process each batch by itself. When the summary statistics are
            # finalized, those unit counts add up.s
            collector.collect(release, frame, metadata_entry={"batch_count": 1})

            # A daily release may comprise over 100 GB of uncompressed CSV data.
            # With three concurrent processes, that would be over 300 GB of disk
            # space for staging alone. Hence, we must aggressively clean up
            # temporary files again.
            shutil.rmtree(self._storage.staging_root / release.temp_directory)

        self._metadata[release] = cast(MetadataEntry, full_counts)
        self._metadata.write_json(
            self._storage.staging_root / f"{self._coverage.stem()}.json"
        )

        # While not quite as big as the uncompressed data, the zipped release
        # can still weigh 8 GB. Hence we aggressively clean staged releases as
        # well.
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
    batch_count = sum(1 for _ in path.glob(release.batch_glob))
    if batch_count == 0:
        return False
    if not (path / DIGEST_FILE).exists():
        return False
    if batch_count != len(read_digest_file(path / DIGEST_FILE)):
        return False

    return True
