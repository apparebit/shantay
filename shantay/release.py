"""
# Data Releases

A `Release` corresponds to a compressed archive with a cryptographic digest on
the side. It is immutable and has no other state than the entity that versions
releases, such as a `datetime.date` for `DailyRelease`. Releases can be
instantiated with either such a valid or the string representation accessible
through `Release.id`. Since that entity versions the release, it must support
inequality comparisons.
"""
from abc import abstractmethod, ABCMeta
from collections import Counter
from collections.abc import Iterator
from dataclasses import dataclass
import datetime as dt
import hashlib
import logging
from pathlib import Path
import shutil
from typing import NoReturn, Self
from urllib.request import Request, urlopen
import zipfile

from .progress import NO_PROGRESS, Progress
from .util import annotate_error


_logger = logging.getLogger("shantay")


class DownloadFailed(Exception):
    """An exception indicating that a download didn't yield a resource."""
    pass


class Release(metaclass=ABCMeta):
    """
    A data release.

    Releases are distributed in compressed archives and have cryptographic
    digests. They may be distributed on a regular schedule (e.g., daily), or
    irregularly but with version numbers.
    """
    CHUNK_SIZE = 64 * 1_024

    @property
    @abstractmethod
    def id(self) -> str:
        """The unique identifier for the release."""

    @property
    @abstractmethod
    def archive(self) -> str:
        """The file name of the release archive."""

    @property
    @abstractmethod
    def digest(self) -> str:
        """The file name of the digest for the release archive."""

    @abstractmethod
    def batch(self, number: int) -> str:
        """The file name of batch number."""

    @property
    @abstractmethod
    def url(self) -> str:
        """The base URL without archive or digest name."""

    @property
    @abstractmethod
    def directory(self) -> Path:
        """
        The directory storing the release, relative to the storage root. For
        example, "2025/03" might be the directory for storing daily releases
        made during March 2025. As illustrated by the example, the directory is
        likely shared between several releases. If a per-release directory is
        needed, the release ID should be used as name.
        """

    @property
    @abstractmethod
    def working_directory(self) -> Path:
        """The working directory nested inside the directory."""

    @property
    @abstractmethod
    def batch_directory(self) -> Path:
        """The batch directory nested inside the directory."""

    @annotate_error(filename_arg="root")
    def download_archive(
        self,
        root: Path,
        progress: Progress = NO_PROGRESS
    ) -> int:
        """Download the release archive and digest."""
        url = f"{self.url}/{self.digest}"
        with urlopen(Request(url, None, {})) as response:
            if response.status != 200:
                self._download_failed("digest", url, response.status)

            (root / self.directory).mkdir(parents=True, exist_ok=True)
            with open(root / self.directory / self.digest, mode="wb") as file:
                shutil.copyfileobj(response, file)

        url = f"{self.url}/{self.archive}"
        with urlopen(Request(url, None, {})) as response:
            if response.status != 200:
                self._download_failed("archive", url, response.status)

            content_length = response.getheader("content-length")
            content_length = (
                None if content_length is None else int(content_length.strip())
            )
            downloaded = 0

            target = root / self.directory / self.archive
            with open(target, mode="wb") as file:
                progress.start(content_length)
                while True:
                    chunk = response.read(self.CHUNK_SIZE)
                    if not chunk:
                        break
                    file.write(chunk)

                    downloaded += len(chunk)
                    progress.step(downloaded)

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
    def validate_archive(self, root: Path) -> None:
        """Validate the archive stored under the root against its digest."""
        digest = root / self.directory / self.digest
        with open(digest, mode="rt", encoding="ascii") as file:
            expected = file.read().strip()
            expected = expected[:expected.index(" ")]

        algo = digest.suffix[1:]
        archive = root / self.directory / self.archive
        with open(archive, mode="rb") as file:
            actual = hashlib.file_digest(file, algo).hexdigest()

        if expected != actual:
            _logger.error('failed to validate digest=%s, file="%s"', algo, archive)
            raise ValueError(f'digest {actual} does not match {expected}')

    @annotate_error(filename_arg="target")
    def copy_archive(self, source: Path, target: Path) -> None:
        """
        Copy the archive and digest stored under the source directory to the
        target directory.
        """
        source_dir = source / self.directory
        target_dir = target / self.directory
        target_dir.mkdir(parents=True, exist_ok=True)
        shutil.copy(source_dir / self.digest, target_dir / self.digest)
        shutil.copy(source_dir / self.archive, target_dir / self.archive)

    def archived_files(self, root: Path) -> list[str]:
        """Get the sorted list of files for the archive under the root directory."""
        with zipfile.ZipFile(root / self.directory / self.archive) as archive:
            return sorted(archive.namelist())

    @annotate_error(filename_arg="root")
    def unarchive_file(self, root: Path, index: int, name: str) -> None:
        """
        Unarchive the file with index and name from the archive under the source
        directory into a suitable directory under the target directory.
        """
        input = root / self.directory / self.archive
        with zipfile.ZipFile(input) as archive:
            with archive.open(name) as source_file:
                output = root / self.working_directory
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

    @abstractmethod
    def extract_data_step_count(self) -> int:
        """Determine the number of logical steps performed by extract_batch."""

    def extract_data_step(self, index: int, step: int) -> int:
        """Determine the argument for Progress.step()."""
        return (self.extract_data_step_count() + 1) * index + step

    @abstractmethod
    def extract_data(
        self,
        root: Path,
        index: int, # Index of unarchived file
        name: str, # Name of unarchived file
        progress: Progress = NO_PROGRESS,
    ) -> Counter:
        """
        Extract the batch data from the unarchived file and return summary
        statistics.
        """

    def extracted_data_exits(self, root: Path, count: int) -> bool:
        """Determine whether all batch files exist under the given root directory."""
        path = root / self.batch_directory
        for index in range(count):
            if not (path / self.batch(index)).exists():
                return False
        return True

    @annotate_error(filename_arg="target")
    def copy_extracted_data(
        self,
        source: Path,
        target: Path,
        count: int,
        progress: Progress = NO_PROGRESS,
    ) -> None:
        """Copy the batch files between root directories."""
        path = target / self.batch_directory
        path.mkdir(parents=True, exist_ok=True)
        for index in range(count):
            batch = self.batch(index)
            shutil.copy(source / self.batch_directory / batch, path / batch)
            progress.step(index)

    def __hash__(self) -> int:
        return hash(self.id)

    def __eq__(self, other: object) -> bool:
        return type(self) == type(other) and self.id == other.id

    @abstractmethod
    def __lt__(self, other: object) -> bool:
        """
        Determine whether this release comes before the other release. The other
        ordering comparisons are implemented in terms of the less and equal
        comparisons. Unlike equality, which can be implemented in terms of ID,
        this comparison is abstract because IDs are strings and hence are not
        always suitable for establishing order.
        """

    def __le__(self, other: object) -> bool:
        if isinstance(other, type(self)):
            return self < other or self == other
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, type(self)):
            return not self < other
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, type(self)):
            return not self < other and not self == other
        return NotImplemented

    def __iter__(self) -> Self:
        return self

    @abstractmethod
    def __next__(self) -> Self:
        """Get the next release."""

    def __str__(self) -> str:
        return self.id

    def __repr__(self) -> str:
        return f"{type(self).__name__}({self.id})"


class DailyRelease(Release):

    __slots__ = ("_date",)

    """A release made every day."""
    def __init__(self, date: str | dt.date) -> None:
        super().__init__()
        if isinstance(date, str):
            date = dt.date.fromisoformat(date)
        self._date = date

    @property
    def date(self) -> dt.date:
        return self._date

    @property
    def id(self) -> str:
        return f"{self._date.year}-{self._date.month:02}-{self._date.day:02}"

    @property
    def directory(self) -> Path:
        """Get the directory prefix, i.e., 'YYYY/MM'."""
        return Path(f"{self._date.year}") / f"{self._date.month:02}"

    @property
    def working_directory(self) -> Path:
        return self.directory / f"tmp{self._date.day:02}"

    @property
    def batch_directory(self) -> Path:
        return self.directory / f"{self._date.day:02}"

    def __eq__(self, other: object) -> bool:
        if isinstance(other, type(self)):
            return self._date == other._date
        return NotImplemented

    def __lt__(self, other: object) -> bool:
        if isinstance(other, type(self)):
            return self._date < other._date
        return NotImplemented

    def __le__(self, other: object) -> bool:
        if isinstance(other, type(self)):
            return self._date <= other._date
        return NotImplemented

    def __gt__(self, other: object) -> bool:
        if isinstance(other, type(self)):
            return other._date < self._date
        return NotImplemented

    def __ge__(self, other: object) -> bool:
        if isinstance(other, type(self)):
            return other._date <= self._date
        return NotImplemented

    def __next__(self) -> Self:
        return type(self)(self._date + dt.timedelta(days=1))
