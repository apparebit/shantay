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

from . import __version__
from .digest import (
    compute_digest, read_digest_file, validate_digests, write_digest_file
)
from .logutil import log_max_rss
from .metadata import Metadata
from .model import (
    Config, ConfigError, CollectorProtocol, Daily, DataFrameType, Dataset, DateRange,
    DIGEST_FILE, DownloadFailed, FilterKind, FullMetadataEntry, MetadataEntry,
    ReleaseRange, Storage
)
from .pool import check_not_cancelled
from .progress import NO_PROGRESS, Progress
from .schema import (
    check_db_platforms, MissingPlatformError, update_platforms
)
from .stats import Statistics
from .util import annotate_error, scale_time


_logger = logging.getLogger(__spec__.parent)


class Processor:
    """The single-process implementation of Shantay's tasks."""

    CHUNK_SIZE = 64 * 1_024

    def __init__(
        self,
        *,
        dataset: Dataset,
        storage: Storage,
        coverage: ReleaseRange[Daily],
        config: Config,
        metadata: Metadata,
        progress: Progress = NO_PROGRESS,
    ) -> None:
        self._dataset = dataset
        self._storage = storage
        self._coverage = coverage
        self._config = config
        self._metadata = metadata
        self._progress = progress if config.progress else NO_PROGRESS
        self._running_time = 0.0

    @property
    def stem(self) -> str:
        """The metadata and statistics stem."""
        return self._metadata.stem

    @property
    def latency(self) -> float:
        """The latency of the most recent invocation of run()."""
        return self._running_time

    def run(self, task: str) -> None | DataFrameType:
        """Run the given task."""
        _logger.info('running processor with pid=%d, task="%s"', os.getpid(), task)
        _logger.info('    key="runtime.offline",      value="%s"', self._config.offline)
        _logger.info('    key="runtime.workers",      value=%d', self._config.workers)
        _logger.info('    key="runtime.max_tasks",    value=%s', self._config.max_tasks_str)
        _logger.info('    key="runtime.progress",     value="%s"', self._config.progress)
        _logger.info('    key="dataset.name",         value="%s"', self._dataset.name)
        _logger.info('    key="storage.archive_root", value="%s"', self._storage.archive_root or "")
        _logger.info('    key="storage.extract_root", value="%s"', self._storage.extract_root or "")
        _logger.info('    key="storage.staging_root", value="%s"', self._storage.staging_root)
        _logger.info('    key="coverage.first",       value="%s"', self._coverage.first.id)
        _logger.info('    key="coverage.last",        value="%s"', self._coverage.last.id)
        _logger.info('    key="coverage.frequency",   value="%s"', self._coverage.frequency)
        _logger.info('    key="coverage.filter",      value="%s"', self._metadata.filter or "")
        _logger.info('    key="stratify.category",    value="%s"', self._config.stratify_by_category)
        _logger.info('    key="stratify.all_text",    value="%s"', self._config.stratify_all_text)
        _logger.info('    key="statistics.stem",      value="%s"', self.stem)

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
            result = self.distill()
        elif task == "summarize-builtin":
            stats = Statistics.builtin()
            stats.write(self._storage.staging_root)
            result = stats.frame()
        elif task == "summarize-all":
            result = self.summarize_database()
        elif task == "summarize-extract":
            result = self.summarize_extract()
        elif task == "visualize":
            result = self.visualize()
        else:
            raise ValueError(f'invalid task "{task}"')

        self._running_time = time.time() - start_time
        value, unit = scale_time(self._running_time)
        _logger.info('processing took time=%.3f, unit="%s"', value, unit)

        return result

    def info(self) -> None:
        """Print information about all known root directories."""
        keys = []
        values = []

        def emit_rule(strong: bool = False) -> None:
            keys.append(None)
            values.append(2 if strong else 1)

        def emit_key_value(key, value) -> None:
            keys.append(key)
            values.append(value)

        def emit_range(root: str, range: None | DateRange | ReleaseRange[Daily]) -> None:
            if range is None:
                emit_key_value(f"{root}.range", "null")
                return

            if isinstance(range, DateRange):
                first = range.first.isoformat()
                last = range.last.isoformat()
            else:
                first = range.first.id
                last = range.last.id

            emit_key_value(f"{root}.range.first", first)
            emit_key_value(f"{root}.range.last", last)

        def emit_meta_data(
            root: str, stem: str, metadata: None | Metadata, data: None | Statistics
        ) -> None:
            if metadata is not None:
                emit_key_value(f"{root}.file", f"{stem}.json")
                emit_key_value(f"{root}.stem", metadata.stem)

                filter = metadata.filter
                if filter is None:
                    emit_key_value(f"{root}.filter", "null")
                else:
                    emit_key_value(f"{root}.filter.kind", filter.kind)
                    if filter.kind is FilterKind.PLATFORM:
                        criterion = ", ".join(filter.criterion)
                    else:
                        criterion = filter.criterion
                    emit_key_value(f"{root}.filter.criterion", criterion)

                emit_range(root, metadata.release_range)

            if data is not None:
                if metadata is not None:
                    emit_rule()
                emit_key_value(f"{root}.file", f"{stem}.parquet")

                by_category = str(data.stratify_by_category).lower()
                all_text = str(data.stratify_all_text).lower()
                emit_key_value(f"{root}.stratify_by_category", by_category)
                emit_key_value(f"{root}.stratify_all_text", all_text)

                emit_range(root, data.release_range())

        def emit_directory(root: str, path: Path, strong: bool = False) -> None:
            files = sorted(
                (
                    *(p for p in path.glob("*.json") if p.is_file()),
                    *(p for p in path.glob("*.parquet") if p.is_file())
                ),
            )

            index = 0
            file_count = len(files)

            while index < file_count:
                file = files[index]
                index += 1

                metadata = data = None
                if file.suffix == ".json":
                    metadata = Metadata.read_json(file)
                    if index < file_count and file.stem == files[index].stem:
                        data = Statistics.read(files[index])
                        index += 1
                else:
                    data = Statistics.read(file)

                emit_rule(strong=strong)
                strong = False

                emit_meta_data(root, file.stem, metadata, data)

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # Shantay, its dependencies, Python, and OS

        emit_rule(strong=True)
        emit_key_value("shantay.version", __version__)
        emit_rule()
        import polars
        emit_key_value("polars.version", polars.__version__)
        import pyarrow
        emit_key_value("pyarrow.version", pyarrow.__version__)
        import altair
        emit_key_value("altair.version", altair.__version__)
        emit_rule()
        emit_key_value("platform.id", platform.platform())
        emit_key_value("python.implementation", platform.python_implementation())
        emit_key_value("python.version", platform.python_version())
        emit_key_value("os.system", platform.system())
        emit_key_value("os.release", platform.release())
        #record("os.version", platform.version())

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

        emit_rule(strong=True)
        emit_meta_data("builtin", "db", None, Statistics.builtin())

        if self._storage.archive_root is not None:
            emit_rule(strong=True)
            emit_key_value("archive.fs.root", str(self._storage.archive_root))
            emit_range("archive.fs", self._storage.coverage_of_archive())
            emit_directory("archive", self._storage.archive_root, strong=True)

        if self._storage.extract_root is not None:
            emit_rule(strong=True)
            emit_key_value("extract.fs.root", str(self._storage.extract_root))
            emit_range("extract.fs", self._storage.coverage_of_extract())
            emit_directory("extract", self._storage.extract_root)

        emit_rule(strong=True)
        emit_key_value("staging.fs.root", str(self._storage.staging_root))
        emit_directory("staging", self._storage.staging_root, strong=True)

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # Actually emit the output

        key_width = max(0 if k is None else len(k) for k in keys)
        value_width = max(0 if v in (1, 2) else len(v) for v in values)
        width = key_width + 5 + value_width

        for key, value in zip(keys, values):
            if key is None:
                if value == 1:
                    line = '─' * width
                else:
                    line = '━' * width
            else:
                line = f' {key:<{key_width}} = {value}'

            print(line)
            _logger.debug(line)

    def distill(self) -> None:
        """Filter data for all covered releases."""
        for release in self._coverage:
            # Ensure graceful termination in offline mode
            if self._config.offline and not self.is_archive_downloaded(release):
                _logger.debug(
                    'stopping due to missing archive in offline mode '
                    'for task="distill", release="%s"',
                    release.id
                )
                break

            # Do the distillation
            self.distill_release(release)

        # The staging root's metadata was merged with the extract's metadata
        # during startup. Hence writing it back to the extract directory won't
        # lead to data loss---as long as there are no concurrent writers!
        meta_json = f"{self.stem}.json"
        Metadata.copy_json(
            self._storage.staging_root / meta_json,
            self._storage.the_extract_root / meta_json
        )

    def distill_release(self, release: Daily, cleanup: bool = True) -> None:
        """Filter data for the given release."""
        if (batches := distilled_batch_count(self._storage.the_extract_root, release)):
            maybe_update_metadata(self._storage, release, batches, self._metadata)
            return

        _logger.debug('distill release="%s"', release.id)
        if not self.is_archive_downloaded(release):
            self.download_archive(release)

        self.stage_archive(release)
        try:
            self._actually_distill_release(release)
        except Exception as x:
            x.add_note(
                f"WARNING: Artifacts for release {release} may be incomplete or corrupted!"
            )
            raise

        if cleanup:
            shutil.rmtree(
                self._storage.staging_root / release.parent_directory,
                ignore_errors=True,
            )
        self._progress.perform(f"distilled {release.id}").done()
        return

    def download(self) -> None:
        """Download the archives for this processor's coverage."""
        if self._config.offline:
            raise ValueError("can't download daily distributions in offline mode")

        for release in self._coverage:
            self.download_archive(release)
            shutil.rmtree(
                self._storage.staging_root / release.parent_directory,
                ignore_errors=True
            )

    def download_archive(self, release: Daily) -> None:
        """Download the archive for the given release."""
        if self._config.offline:
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
            self._storage.the_archive_root / self._dataset.archive_path(release)
        ).exists()

    @annotate_error(filename_arg="root")
    def _actually_download_archive(self, root: Path, release: Daily) -> int:
        """Download the release archive and digest."""
        if self._config.offline:
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
        """Validate the SHA1 hash of the downloaded archive."""
        digest = root / release.parent_directory / self._dataset.digest_name(release)
        archive = root / self._dataset.archive_path(release)

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
                archive.name, algo, digest, actual
            )
            raise ValueError(
                f'{archive} should have {algo} digest {digest} but has {actual}'
            )

        _logger.info(
            'validated release="%s", file="%s", digest="%s"',
            release.id, archive.name, expected
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

    def stage_archive(self, release: Daily) -> Path:
        """
        Stage the archive for the given release. The archive must have been
        downloaded before.
        """
        assert self.is_archive_downloaded(release)
        archive_path = self._storage.staging_root / self._dataset.archive_path(release)

        if self.is_archive_staged(release):
            return archive_path

        self._progress.perform(f"copying release {release.id} from archive to staging")
        self.copy_archive(
            self._storage.the_archive_root, self._storage.staging_root, release
        )
        _logger.info('staged file="%s"', archive_path.name)
        self._progress.perform(f"validating release {release.id}")
        self.validate_archive(self._storage.staging_root, release)
        return archive_path

    def is_archive_staged(self, release: Daily) -> bool:
        """"Determine whether the archive for the given release has been staged."""
        return (
            self._storage.staging_root / self._dataset.archive_path(release)
        ).exists()

    def _actually_distill_release(self, release: Daily) -> None:
        """Distill the batches for the given release."""
        assert self.is_archive_staged(release)
        assert self._metadata.filter is not None

        archive_path = self._storage.staging_root / self._dataset.archive_path(release)
        with zipfile.ZipFile(archive_path) as archive:
            filenames = sorted(archive.namelist())
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
                self.unarchive_file(archive, release, index, name)
                digest, counters = self._dataset.distill_release(
                    root=self._storage.staging_root,
                    release=release,
                    index=index,
                    name=name,
                    filter=self._metadata.filter,
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
        self._metadata.write_json(self._storage.staging_root / f"{self.stem}.json")
        _logger.info(
            'distilled release="%s", batch-count=%d, filter="%s"',
            release.id, batch_count, self._metadata.filter or ""
        )

        # It's ok for a worker process to copy the batches to long-term storage
        # because each worker processes different releases. So even if several
        # workers are concurrently copying batches to the extract root, they are
        # only adding new subdirectories and files. That does *not* hold for the
        # metadata, which must be merged and written by the coordinator.
        self.copy_distilled_data(
            self._storage.staging_root, self._storage.the_extract_root, release, batch_count
        )
        _logger.info(
            'persisted release="%s", batch-count=%d, filter="%s"',
            release.id, batch_count, self._metadata.filter or ""
        )

    def list_archived_files(self, root: Path, release: Daily) -> list[str]:
        """Get the sorted list of files for the archive under the root directory."""
        with zipfile.ZipFile(root / self._dataset.archive_path(release)) as archive:
            return sorted(archive.namelist())

    @annotate_error(filename_arg="root")
    def unarchive_file(
        self, archive: zipfile.ZipFile, release: Daily, index: int, name: str
    ) -> None:
        """
        Unarchive the file with index and name from the archive under the source
        directory into a suitable directory under the target directory.
        """
        with archive.open(name) as source_file:
            output = self._storage.staging_root / release.temp_directory
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

    @annotate_error(filename_arg="target")
    def copy_distilled_data(
        self,
        source: Path,
        target: Path,
        release: Daily,
        count: int,
        silent: bool = True,
    ) -> None:
        """Copy the batch files between root directories."""
        source_dir = source / release.directory
        target_dir = target / release.directory
        target_dir.mkdir(parents=True, exist_ok=True)

        if not silent:
            if target == self._storage.staging_root:
                direction = "into"
            else:
                direction = "out of"
            self._progress.activity(
                f"copying batches for {release.id} {direction} staging",
                f"persisting {release.id}", "batch", with_rate=False,
            ).start(count)

        shutil.copy(source_dir / DIGEST_FILE, target_dir / DIGEST_FILE)
        for index in range(count):
            batch = release.batch_file(index)

            target_path = target_dir / batch
            if target_path.exists():
                raise ValueError(f"cannot copy over existing {target_path}")
            shutil.copy(source_dir / batch, target_path)

            if not silent:
                self._progress.step(index)

    def stage_extract_data(self, release: Daily) -> None:
        """Stage the category-specific data for the given release."""
        batch_count = None
        if release in self._metadata:
            batch_count = self._metadata[release]["batch_count"]
        bc2 = distilled_batch_count(self._storage.staging_root, release)
        if batch_count is not None and bc2 != 0 and batch_count != bc2:
            raise ValueError(
                f"metadata batch_count {batch_count} does not match {bc2} files"
            )
        if bc2 != 0:
            return
        if batch_count is None:
            maybe_update_metadata(self._storage, release, bc2, self._metadata)
            batch_count = bc2

        self.copy_distilled_data(
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
                    digest_of_digests=digest,
                )
            except ValueError as x:
                _logger.error(
                    'could not validate extract for release="%s"', release, exc_info=x
                )
                raise

        _logger.info('validated extract for release="%s"', release)

    def summarize_extract(self) -> DataFrameType:
        """Determine summary statistics for the filtered subset."""
        # Prepare progress tracker
        self._progress.activity(
            "summarizing extracted data", "summarizing extract", "batch", with_rate=False
        )
        self._progress.start(self._coverage.duration)

        stats_dir = self._storage.staging_root / f"{self.stem}.stats"
        pre_existing_stats = prepare_statistics(self.stem, self._storage, self._config)

        for index, release in enumerate(self._coverage):
            release_stats_path = stats_dir / f"{release}.parquet"
            release_stats_path_exists = release_stats_path.exists()

            if release in pre_existing_stats or release_stats_path_exists:
                _logger.debug(
                    'summary statistics for extract already cover release="%s"',
                    release
                )
                if not release_stats_path_exists:
                    pre_existing_stats.write_release(release, release_stats_path)
                continue

            # Ensure graceful termination in offline mode
            if self._config.offline and not self.is_archive_downloaded(release):
                _logger.error(
                    'stopping due to missing archive in offline mode '
                    'for task="summarize-extract", release="%s"',
                    release.id
                )
                break

            if distilled_batch_count(self._storage.the_extract_root, release) == 0:
                with self._progress.nested():
                    self.distill_release(release, cleanup=False)

            assert self._metadata.filter is not None
            stats = Statistics(
                f"{release}.parquet",
                stratify_by_category=self._config.stratify_by_category,
                stratify_all_text=self._config.stratify_all_text,
            )
            metadata_entry = self.summarize_release_extract(
                release=release, collector=stats
            )
            stats.write(stats_dir, should_finalize=True)
            stats = None

            if self._metadata.merge_release(release, metadata_entry):
                self._metadata.write_json(
                    self._storage.staging_root / f"{self.stem}.json"
                )

            self._progress.step(index + 1, extra=release.id)

        meta_json = f"{self.stem}.json"
        Metadata.copy_json(
            self._storage.staging_root / meta_json,
            self._storage.the_extract_root / meta_json
        )

        stats_file = f"{self.stem}.parquet"
        _logger.info(
            'combining entity="release statistics", count=%d, glob="%s", file="%s"',
            self._coverage.duration, f"{self.stem}.stats/*.parquet", stats_file
        )
        stats = Statistics.read_all(stats_dir)
        stats.write(self._storage.staging_root, should_finalize=True)

        _logger.info(
            'copying summary statistics to extract root="%s"',
            self._storage.the_extract_root
        )
        Statistics.copy_all(
            self.stem, self._storage.staging_root, self._storage.the_extract_root
        )

        return stats.frame()

    def summarize_release_extract(
        self, release: Daily, collector: CollectorProtocol
    ) -> FullMetadataEntry:
        """Determine summary statistics for the extract of the given release."""
        start_time = time.time()

        self._progress.perform(f"summarizing subset of release {release}")
        self.stage_extract_data(release)

        metadata_entry = self._dataset.summarize_release_extract(
            root=self._storage.staging_root,
            release=release,
            metadata=self._metadata,
            collector=collector,
        )

        shutil.rmtree(self._storage.staging_root / release.parent_directory)

        latency = time.time() - start_time
        _logger.info(
            'summarized release="%s", filter="%s", latency=%.3f, unit="sec"',
            release.id, self._metadata.filter or "", latency
        )

        log_max_rss(release.id)
        return metadata_entry

    def summarize_database(self) -> DataFrameType:
        """Determine summary statistics for the full database."""
        stats_dir = self._storage.staging_root / f"{self.stem}.stats"
        pre_existing_stats = prepare_statistics(
            self.stem, self._storage, self._config
        )

        # Due to variability of daily record numbers and worker process timing,
        # the multiprocessing version of summarize may add daily statistics out
        # of calendar order. By always processing all possible release dates in
        # order, this loop ensures that any holes are filled, making this a
        # robust, self-healing implementation strategy.
        for release in self._coverage:
            release_stats_path = stats_dir / f"{release}.parquet"
            release_stats_path_exists = release_stats_path.exists()

            if release in pre_existing_stats or release_stats_path_exists:
                _logger.debug('summary statistics already cover release="%s"', release)
                if not release_stats_path_exists:
                    pre_existing_stats.write_release(release, stats_dir)
                continue

            # Ensure graceful termination in offline mode
            if self._config.offline and not self.is_archive_downloaded(release):
                _logger.error(
                    'archive is missing while in offline mode '
                    'for task="summarize-all", release="%s"',
                    release.id
                )
                break

            try:
                stats = self.summarize_full_release(release)
            except MissingPlatformError as x:
                # This method is only executed during single-process runs and
                # hence it is safe-ish to update the list of platforms here.
                update_platforms(x.args[0])
                raise

            assert stats is not None
            stats.write(stats_dir, should_finalize=True)
            _logger.debug(
                'saved per-release summary statistics to file="%s", release="%s"',
                stats.file, release
            )
            stats = None

            log_max_rss(release.id)

        meta_json = f"{self.stem}.json"
        Metadata.copy_json(
            self._storage.staging_root / meta_json,
            self._storage.the_archive_root / meta_json
        )

        # Rewrite saved statistics after rechunking and copy to persistent root
        stats_file = f"{self.stem}.parquet"
        _logger.info(
            'combining entity="release statistics", count=%d, glob="%s", file="%s"',
            self._coverage.duration, f"{self.stem}.stats/*.parquet", stats_file
        )
        stats = Statistics.read_all(stats_dir)
        stats.write(self._storage.staging_root, should_finalize=True)

        _logger.info(
            'copying summary statistics to archive file="%s/db.parquet"',
            self._storage.archive_root
        )
        Statistics.copy_all(
            self.stem, self._storage.staging_root, self._storage.the_archive_root
        )
        return stats.frame()

    def summarize_full_release(
        self, release: Daily, metadata_only: bool = False
    ) -> None | Statistics:
        """
        Determine summary statistics for the given release of the full database
        and return the result.
        """
        _logger.info('summarizing release="%s"', release.id)
        if not self.is_archive_downloaded(release):
            self.download_archive(release)

        archive_path = self.stage_archive(release)
        with zipfile.ZipFile(archive_path) as archive:
            filenames = sorted(archive.namelist())
            batch_count = len(filenames)
            self._progress.activity(
                f"summarizing batches from release {release.id}",
                f"summarizing {release.id}", "batch", with_rate=False,
            )
            self._progress.start(batch_count)
            full_counts = Counter(batch_count=batch_count)

            (self._storage.staging_root / release.directory).mkdir(parents=True)

            # Archived files are archives, too. Unarchive one at a time.
            for index, name in enumerate(filenames):
                check_not_cancelled()
                start_time = time.time()

                self._progress.step(index, "unarchiving data")
                self.unarchive_file(archive, release, index, name)

                if metadata_only:
                    self._progress.step(index, "count rows")
                    full_counts += self._dataset.get_batch_row_counts(
                        root=self._storage.staging_root,
                        release=release,
                        index=index,
                    )
                else:
                    counts, frame = self._dataset.ingest_release(
                        root=self._storage.staging_root,
                        release=release,
                        index=index,
                        name=name,
                        progress=self._progress
                    )

                    full_counts += counts

                    # Check_db_platforms only probes the data frame for hereto
                    # unknown platform names, raising a MissingPlatformError
                    # with such names.
                    check_db_platforms(
                        self._storage.staging_root/self._dataset.archive_path(release),
                        frame
                    )

                    # We process each batch by itself. When the summary
                    # statistics are finalized, those unit counts add up.
                    stats = Statistics(
                        f"{release}-{index:05}.parquet",
                        stratify_by_category=self._config.stratify_by_category,
                        stratify_all_text=self._config.stratify_all_text,
                    )
                    stats.collect(release, frame, metadata_entry={"batch_count": 1})
                    stats.write(self._storage.staging_root / release.directory)
                    stats = None

                # A daily release may comprise over 100 GB of uncompressed CSV data.
                # With three concurrent processes, that would be over 300 GB of disk
                # space for staging alone. Hence, we must aggressively clean up
                # temporary files again.
                shutil.rmtree(self._storage.staging_root / release.temp_directory)

                latency = time.time() - start_time
                _logger.info(
                    'summarized batch file="%s", latency=%.3f, unit="sec"',
                    name, latency
                )

        if metadata_only:
            stats = None
        else:
            stats_file = f"{release}.parquet"
            _logger.debug(
                'combining entity="batch statistics", '
                'count=%d, glob="%s-*.parquet", file="%s"',
                batch_count, release, stats_file
            )
            stats = Statistics.read_all(
                self._storage.staging_root / release.directory,
                glob=f"{release}-*.parquet",
                file=stats_file
            )

        self._metadata[release] = cast(MetadataEntry, full_counts)
        self._metadata.write_json(self._storage.staging_root / f"{self.stem}.json")

        # While not quite as big as the uncompressed data, the zipped release
        # can still weigh 8 GB. Hence we aggressively clean staged releases as
        # well.
        shutil.rmtree(self._storage.staging_root / release.parent_directory)

        return stats

    def visualize(self) -> None:
        """Visualize summary statistics."""
        from .viz import Visualizer

        Visualizer(
            storage=self._storage,
            coverage=self._coverage,
            config=self._config,
            metadata=self._metadata,
            with_interaction=self._config.interactive_report,
            with_clamped_outliers=self._config.clamp_outliers,
            with_platforms=self._config.platforms,
            progress=self._progress,
        ).run()


def prepare_statistics(stem: str, storage: Storage, config: Config) -> Statistics:
    """
    Recreate the directory for per-release summary statistics and copy existing
    statistics (if they exist) into it.
    """
    stats_dir = storage.staging_root / f"{stem}.stats"
    stats_dir.mkdir(exist_ok=True)

    file = f"{stem}.parquet"
    stats = Statistics.pick(file, storage.staging_root, storage.best_root)
    if stats is None:
        return Statistics(
            file,
            stratify_by_category=config.stratify_by_category,
            stratify_all_text=config.stratify_all_text,
        )

    if config.stratification != stats.stratification:
        raise ConfigError(
            f'Existing statistics have different options ({stats.stratification}) '
            f'from configuration ({config.stratification}). Please adjust command '
            'line options or move file with statistics data.'
        )

    range = stats.date_range()
    assert range is not None
    _logger.info(
        'existing statistics "%s" cover start_date="%s", end_date="%s"',
        file, range.first, range.last
    )
    return stats


def distilled_batch_count(root: Path, release: Daily) -> int:
    """
    Determine the number of distilled batch files under the given root
    directory. If the directory corresponding directory does not exist, does not
    contain parquet files named after the release, does not contain a digest
    file, or does not provide as many digests as there are batch files, this
    function returns 0. Otherwise it returns the number of distilled batch
    files, which always is at least 1.
    """
    path = root / release.directory
    if not path.exists():
        return 0
    if not (path / DIGEST_FILE).exists():
        return 0
    batch_count = sum(1 for _ in path.glob(release.batch_glob))
    if batch_count == 0:
        return 0
    if batch_count != len(read_digest_file(path / DIGEST_FILE)):
        return 0

    return batch_count


def maybe_update_metadata(
    storage: Storage,
    release: Daily,
    batch_count: int,
    metadata: Metadata
) -> None:
    if release in metadata:
        return

    # While there is no metadata entry, we just determined the
    # critical missing entry, i.e., the batch count. Since data
    # integrity matters, we might as well validate the digests, too.
    digest = validate_digests(
        storage.the_extract_root / release.directory,
        release.batch_glob,
        storage.the_extract_root / release.directory / DIGEST_FILE,
    )

    metadata[release] = cast(MetadataEntry, dict(
        batch_count=batch_count,
        sha256=digest,
    ))

    metadata.write_json(
        storage.staging_root / f"{metadata.stem}.json"
    )
