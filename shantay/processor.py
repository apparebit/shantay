from collections import Counter
import hashlib
import logging
from pathlib import Path
import shutil
from typing import Any, NoReturn
from urllib.request import Request, urlopen
import zipfile

from .collector import Collector
from .metadata import Metadata
from .model import Coverage, Dataset, DownloadFailed, Release, Storage
from .progress import NO_PROGRESS, Progress
from .util import annotate_error


_logger = logging.getLogger(__package__)


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
    ) -> None:
        self._id = 0
        self._dataset = dataset
        self._storage = storage
        self._coverage = coverage
        self._metadata = metadata
        self._progress = progress

    def start(self, task: str) -> None:
        _logger.info('starting runner=%d, task="%s"', self._id, task)
        _logger.info('    key="dataset.name",         value="%s"', self._dataset.name)
        _logger.info('    key="storage.archive_root", value="%s"', self._storage.archive_root)
        _logger.info('    key="storage.working_root", value="%s"', self._storage.working_root)
        _logger.info('    key="storage.staging_root", value="%s"', self._storage.staging_root)
        _logger.info('    key="coverage.filter",      value="%s"', self._coverage.filter)
        _logger.info('    key="coverage.first",       value="%s"', self._coverage.first.id)
        _logger.info('    key="coverage.last",        value="%s"', self._coverage.last.id)

        if task == "prepare":
            self.prepare()
        elif task == "analyze":
            self.analyze()
        else:
            pass

    def prepare(self) -> None:
        for release in self._coverage:
            self.prepare_batches(release)

    def prepare_batches(self, release: R) -> None:
        if (
            release in self._metadata
            and self.extracted_data_exists(self._storage.working_root, release)
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
        return release

    def download_archive(self, release: R) -> None:
        if self.is_archive_downloaded(release):
            return

        self._progress.activity(
            f"downloading data for release {release.id}",
            f"downloading {release.id}", "byte", with_rate=True,
        )
        size = self._download_archive(self._storage.staging_root, release)
        _logger.info('downloaded bytes=%d, file="%s"', size, self._dataset.archive_name(release))
        self._progress.perform(f"validating release {release.id}")
        self.validate_archive(self._storage.staging_root, release)
        _logger.info('validated file="%s"', self._dataset.archive_name(release))
        self._progress.perform(f"copying release {release.id} to archive")
        self.copy_archive(self._storage.staging_root, self._storage.archive_root, release)
        _logger.info('archived file="%s"', self._dataset.archive_name(release))

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

        self._progress.perform(f"copying release {release.id} from archive to staging")
        self.copy_archive(self._storage.archive_root, self._storage.staging_root, release)
        _logger.info('staged file="%s"', self._dataset.archive_name(release))
        self._progress.perform(f"validating release {release.id}")
        self.validate_archive(self._storage.staging_root, release)
        _logger.info('validated file="%s"', self._dataset.archive_name(release))

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

        filenames = self.list_archived_files(self._storage.staging_root, release)
        batch_count = len(filenames)
        self._progress.activity(
            f"extracting batches from release {release.id}",
            f"extracting {release.id}", "batch", with_rate=False,
        )
        steps = self._dataset.extract_data_step_count + 1
        self._progress.start(steps * batch_count)

        # Archived files are archives, too. Unarchive one at a time.
        batch_digests = []
        full_counters = Counter(batch_count=batch_count)
        for index, name in enumerate(filenames):
            self._progress.step(steps * index, "unarchiving data")
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

        digest_file = self._storage.staging_root / release.directory / "digest.txt"
        with open(digest_file, mode="w", encoding="utf8") as file:
            for index, digest in enumerate(batch_digests):
                file.write(f"{digest} {release.id}-{index:05}.parquet\n")

        self._progress.perform(f"updating batch metadata for release {release.id}")
        self._metadata[release] = counters
        self._metadata.write_json(self._storage.staging_root)
        _logger.info('extracted batch_count=%d, file="%s"', batch_count, self._dataset.archive_name(release))

        self._progress.activity(
            f"copying batches for {release.id} out of staging",
            f"persisting {release.id}", "batch", with_rate=False,
        ).start(batch_count)
        self.copy_extracted_data(
            self._storage.staging_root, self._storage.working_root, release, batch_count
        )
        _logger.info('archived batch_count=%d, release="%s"', batch_count, release.id)

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
        path = root / release.directory
        for index in range(self._metadata.batch_count(release)):
            if not (path / release.batch_file(index)).exists():
                return False
        return True

    @annotate_error(filename_arg="target")
    def copy_extracted_data(self, source: Path, target: Path, release: R, count: int) -> None:
        """Copy the batch files between root directories."""
        path = target / release.directory
        path.mkdir(parents=True, exist_ok=True)
        for index in range(count):
            batch = release.batch_file(index)
            shutil.copy(source / release.directory / batch, path / batch)
            self._progress.step(index)

    def analyze(self) -> Any:
        self._progress.activity(
            "analyzing monthly batches", "analyzing batches", "batch", with_rate=False
        )
        self._progress.start(len(self._coverage))

        collector = Collector()
        for index, release in enumerate(self._coverage):
            self.dataset.analyze_release(self._storage.working_root, release, collector)
            self._progress.step(index + 1, extra=release.id)

        return self.dataset.combine_releases(
            self._storage.working_root, self._coverage, collector
        )
