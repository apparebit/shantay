from abc import abstractmethod, ABCMeta
from collections import Counter
import datetime as dt
import hashlib
from pathlib import Path
import shutil
from typing import Self
from urllib.request import Request, urlopen
import zipfile

from .progress import Progress
from .util import annotate_error

class DownloadFailed(Exception):
    """An exception indicating that a download didn't yield a resource."""
    pass


CHUNK_SIZE = 64 * 1_024


class Release(metaclass=ABCMeta):
    """
    A data release.

    Releases are distributed in compressed archives and have cryptographic
    digests. They may be distributed on a regular schedule (e.g., daily), or
    irregularly but with version numbers.
    """

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
        progress: None | Progress = None,
    ) -> None:
        """Download the release archive and digest."""
        url = f"{self.url}/{self.digest}"
        with urlopen(Request(url, None, {})) as response:
            if response.status != 200:
                raise DownloadFailed(
                    f"download of release digest {url} failed with status {response.status}"
                )

            (root / self.directory).mkdir(parents=True, exist_ok=True)
            with open(root / self.directory / self.digest, mode="wb") as file:
                shutil.copyfileobj(response, file)

        url = f"{self.url}/{self.archive}"
        with urlopen(Request(url, None, {})) as response:
            if response.status != 200:
                raise DownloadFailed(
                    f"download of release archive {url} failed with status {response.status}"
                )

            content_length = response.getheader("content-length")
            content_length = (
                None if content_length is None else int(content_length.strip())
            )
            downloaded = 0

            with open(root / self.directory / self.archive, mode="wb") as file:
                if progress:
                    progress.start(content_length)
                while True:
                    chunk = response.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    file.write(chunk)

                    downloaded += len(chunk)
                    if progress:
                        progress.step(downloaded)

    @annotate_error(filename_arg="root")
    def validate_archive(self, root: Path) -> None:
        """Validate the archive stored under the root against its digest."""
        digest = root / self.directory / self.digest
        with open(digest, mode="rt", encoding="ascii") as file:
            expected = file.read().strip()
            expected = expected[:expected.index(" ")]

        algo = digest.suffix[1:]
        with open(root / self.directory / self.archive, mode="rb") as file:
            actual = hashlib.file_digest(file, algo).hexdigest()

        if expected != actual:
            raise ValueError(f'digest {actual} does not match {expected}')

    @annotate_error(filename_arg="target")
    def copy_archive(self, source: Path, target: Path) -> None:
        """
        Copy the archive and digest stored under the source directory to the
        target directory.
        """
        path = target / self.directory
        path.mkdir(parents=True, exist_ok=True)
        shutil.copy(source / self.directory / self.digest, path / self.digest)
        shutil.copy(source / self.directory / self.archive, path / self.archive)

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
        with zipfile.ZipFile(root / self.directory / self.archive) as archive:
            with archive.open(name) as source_file:
                output = root / self.working_directory
                output.mkdir(parents=True, exist_ok=True)

                if name.endswith(".zip"):
                    with zipfile.ZipFile(source_file) as nested_archive:
                        nested_archive.extractall(output)
                else:
                    with open(output / name, mode="wb") as target_file:
                        shutil.copyfileobj(source_file, target_file)

    @abstractmethod
    def extract_batch_steps(self) -> int:
        """
        The number of logical steps performed by each invocation of
        extract_batch(). The method should invoke Progress.step() as many times,
        using `index * (steps + 1)` as the first step number.
        """


    @abstractmethod
    def extract_batch(
        self,
        root: Path,
        index: int, # Index of unarchived file
        name: str, # Name of unarchived file
        progress: None | Progress = None,
    ) -> Counter:
        """
        Extract the batch data from the unarchived file and return summary
        statistics.
        """

    def batches_exist(self, root: Path, count: int) -> bool:
        """Determine whether all batch files exist under the given root directory."""
        path = root / self.batch_directory
        for index in range(count):
            if not (path / self.batch(index)).exists():
                return False
        return True

    @abstractmethod
    def process_batch(self, root: Path, index: int) -> None:
        """Process the batch with the given index."""

    @annotate_error(filename_arg="target")
    def copy_batches(
        self,
        source: Path,
        target: Path,
        count: int,
        progress: None | Progress = None,
    ) -> None:
        """Copy the batch files between root directories."""
        path = target / self.batch_directory
        path.mkdir(parents=True, exist_ok=True)
        for index in range(count):
            batch = self.batch(index)
            shutil.copy(source / self.batch_directory / batch, path / batch)
            if progress:
                progress.step(index)

    def __eq__(self, other: object) -> bool:
        """
        Determine whether this release has the same type and ID as the other
        object.
        """
        return type(self) == type(other) and self.id == other.id

    def __iter__(self) -> Self:
        """Get this release as an iterator."""
        return self

    @abstractmethod
    def __next__(self) -> Self:
        """Get the next release."""


class DailyRelease(Release):
    """A release made every day."""
    def __init__(self, date: dt.date) -> None:
        super().__init__()
        self._date = date

    @property
    def date(self) -> dt.date:
        return self._date

    @property
    def id(self) -> str:
        return f"{self.date.year}-{self.date.month:02}-{self.date.day:02}"

    @property
    def directory(self) -> Path:
        """Get the directory prefix, i.e., 'YYYY/MM'."""
        return Path(f"{self.date.year}") / f"{self.date.month:02}"

    @property
    def working_directory(self) -> Path:
        return self.directory / f"tmp{self.date.day:02}"

    @property
    def batch_directory(self) -> Path:
        return self.directory / f"{self.date.day:02}"

    def __next__(self) -> Self:
        """Get the next daily release."""
        return type(self)(self.date + dt.timedelta(days=1))

    def __repr__(self) -> str:
        """Get a debug representation for this release"""
        return f"{type(self).__name__}({self.id})"
