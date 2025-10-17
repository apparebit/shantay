"""
A re-imagined (multi)processor module.

This module approaches the problem of data processing in a data-centric instead
of process-centric fashion and explicitly models the different kinds of data to
be downloaded and processed in some number of steps to drive a visualization. By
doing so, it can automate the tracking of data dependencies, minimize the
incremental computation of missing data, and eliminate much of the book-keeping
code that tracks and logs progress, while also copying data between staging and
persistent root directories. Or so I hope...
"""

from collections import Counter
from collections.abc import Iterator, Sequence
import dataclasses
import datetime as dt
import enum
import logging
from pathlib import Path
import shutil
from typing import Any, cast, ClassVar, final, get_origin
from urllib.request import Request, urlopen
import zipfile

from .digest import compute_digest
from .dsa_sor import StatementsOfReasons
from .metadata import Metadata
from .model import Daily, DownloadFailed, Filter, MetadataEntry
from .pool import check_not_cancelled
from .progress import NO_PROGRESS, Progress
from .schema import check_db_platforms
from .stats import Statistics


_logger = logging.getLogger(__file__)


class Resource:
    """
    An abstract data resource.

    Almost all resources are persistent, i.e., are at least temporarily
    materialized as files in the file system. While a resource may comprise more
    than one file, all files must be stored in the same file system directory.
    If they are not, that's a good indication that you should define several
    resources.

    Each subclass is a dataclass whose fields represent the identifiers of the
    data, e.g., the release date, and the data dependencies, e.g., some extract
    from the release archive. While most data dependencies can be statically
    modelled as resources, some require additional information, e.g., the number
    of batches to use. Resources that require such dynamically computed
    information declare a static dependency on a dynamic variable, i.e.,
    subclass of `DynamicVariable`, and implement `depends_on()` to compute the
    full set of dependencies.

    A resource, e.g., a file containing a SHA256 hash, may optionally serve as
    witness for another resource, e.g., by recomputing the hash and comparing
    with the expected value. Such a resource should implement the `validate()`
    method, which just like `__call__()` accepts a single root path as argument
    and returns nothing.
    """
    @classmethod
    def is_subclass(cls, o: object) -> bool:
        """Determine whether the object is a subclass."""
        return (
            cls.is_subclass(get_origin(o))
            or (isinstance(o, type) and issubclass(o, Resource))
        )

    @final
    def is_dyn_var(self) -> bool:
        """Determine whether this resource is a dynamic variable. Subclasses
        must not override this method."""
        return False

    def directory(self) -> Path:
        """Get the directory containing all of this resource's files. This path
        must be relative off some unspecified root. Subclasses must implement
        this method."""
        raise NotImplementedError

    def file(self) -> str:
        """If this resource always has exactly one file, get the name."""
        return NotImplemented

    def path(self) -> str:
        """Compute the relative path to the resource's only file."""
        return str(self.directory() / self.file())

    def file_pattern(self) -> str:
        """If this resource may have more than one file, get the glob pattern."""
        return NotImplemented

    def depends_on(self) -> "Sequence[Resource]":
        """Compute all of this resource's data dependencies. Before the runtime
        invokes this method, it ensures that the values of all dynamic variables
        have been computed. Subclasses that have a dynamic variable as field
        value must implement this method."""
        return NotImplemented

    def __call__(self, root: Path) -> Any:
        """Compute the data for this resource. Upon invocation by the runtime, all
        direct data dependencies are available in directories off the given
        root. Also, this resource's directory is guaranteed to exist. This
        method may optionally accept a `Progress` argument; it should default to
        `NO_PROGRESS`. Subclasses must implement this method."""
        raise NotImplementedError

    def has_files(self) -> bool:
        """Determine whether this resource has more than one file."""
        if self.file() is NotImplemented == self.file_pattern() is NotImplemented:
            raise AssertionError(
                f'{type(self)} should implement one of `file()` and `file_pattern()`'
            )

        return self.file() is NotImplemented

    def dependencies(self) -> "Sequence[Resource]":
        """Compute the resource's statically known direct data dependencies."""
        assert dataclasses.is_dataclass(self)

        resources = []
        seen_before = set()
        for field in dataclasses.fields(self):
            if not self.is_subclass(field.type):
                continue

            value = getattr(self, field.name)
            if value.is_dyn_var():
                value = value.dependencies()[0]

            if value not in seen_before:
                seen_before.add(value)
                resources.append(value)
        return resources


@dataclasses.dataclass(frozen=True, slots=True)
class ConfigVariable[V: dt.date](Resource):
    """
    A resource depending on external input.

    Subclasses are used to indicate that the dependencies of a regular resource
    cannot be statically computed and that the resource's `depends_on()` method
    must be invoked to dynamically compute the dependencies. In fact, the
    runtime won't materialize the actual value until it is preparing to invoke
    said method.

    Unlike dynamic variables, config variables have no resources as
    dependencies.
    """
    value: V = None # type: ignore

    def is_config_var(self) -> bool: # type: ignore
        return True

    def __call__(self, __ignored: None | Path = None) -> V:
        """Compute this dynamic variable's value."""
        raise NotImplementedError

    def dependencies(self) -> Sequence[Resource]:
        return []


class StartDate(ConfigVariable[dt.date]):

    def __call__(self, __ignored: None | Path = None) -> dt.date:
        return dt.date(2023, 9, 25)

    def to(self, end: ConfigVariable[dt.date]) -> Iterator[dt.date]:
        current = self()
        end_date = end()
        while current <= end_date:
            yield current
            current += dt.timedelta(days=1)


class EndDate(ConfigVariable[dt.date]):

    def __call__(self, __ignored: None | Path = None) -> dt.date:
        return dt.date.today() - dt.timedelta(days=3)


@dataclasses.dataclass(frozen=True, slots=True)
class DynamicVariable[V: int | str](Resource):
    """
    An integer or string that can be computed from some storage resource and is
    required for fully enumerating another resource's dependencies.

    The runtime transparently invokes an instance's `__call__` method with the
    right `root` when necessary and update's the `value` attribute. That implies
    that, technically, each instance violates the typing constraint of the value
    and is updated at most once. The runtime ensures that the invariants are
    observed, as long as client code only accesses the value during an
    invocation of `dyn_depends_on()`.

    To define a dynamic variable, you still need to define a subclass that has a
    meaningful implementation of `__call__`. The subclass should not, however,
    add any more fields.
    """
    resource: Resource
    value: V = None # type: ignore

    def __post_init__(self) -> None:
        if isinstance(self.resource, DynamicVariable):
            raise AssertionError(
                f"the dynamic variable's resource {self.resource} is "
                "another dynamic variable"
            )

    def is_dyn_var(self) -> bool: # type: ignore
        return True

    def __call__(self, root: Path) -> V:
        """Compute this dynamic variable's value."""
        raise NotImplementedError

    def dependencies(self) -> Sequence[Resource]:
        return [self.resource]

# --------------------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True, slots=True)
class ReleaseArchive(Resource):

    CHUNK_SIZE: ClassVar[int] = 64 * 1_024

    date: dt.date

    def release(self) -> str:
        return self.date.isoformat()

    def description(self) -> str:
        return f'downloading entity="release archive", url="{self.url(self.file())}"'

    def url(self, file: str) -> str:
        return f"https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/{file}"

    def directory(self) -> Path:
        return Path(f"{self.date.year}") / f"{self.date.month:02}"

    def file(self) -> str:
        return f"sor-global-{self.release()}-full.zip"

    def __call__(self, root: Path, progress: Progress = NO_PROGRESS) -> int:
        progress.activity(
            f"downloading data for release {self.release()}",
            f"downloading {self.release()}", "byte", with_rate=True,
        )

        url = self.url(self.file())
        with urlopen(Request(url, None, {})) as response:
            if response.status != 200:
                raise DownloadFailed(
                    f'download of release archive "{url}" '
                    f'failed with status {response.status}'
                )

            content_length = response.getheader("content-length")
            content_length = (
                None if content_length is None else int(content_length.strip())
            )
            downloaded = 0

            with open(root / self.path(), mode="wb") as file:
                progress.start(content_length)
                while True:
                    check_not_cancelled()
                    chunk = response.read(self.CHUNK_SIZE)
                    if not chunk:
                        break
                    file.write(chunk)

                    downloaded += len(chunk)
                    progress.step(downloaded)

            return downloaded


@dataclasses.dataclass(frozen=True, slots=True)
class ReleaseDigest(Resource):

    archive: ReleaseArchive

    def release(self) -> str:
        return self.archive.release()

    def description(self) -> str:
        return f'downloading entity="release digest", url="{self.url(self.file())}"'

    def url(self, file: str) -> str:
        return self.archive.url(file)

    def directory(self) -> Path:
        return self.archive.directory()

    def file(self) -> str:
        return f"{self.archive.file()}.sha1"

    def __call__(self, root: Path) -> None:
        url = self.url(self.file())
        with urlopen(Request(url, None, {})) as response:
            if response.status != 200:
                raise DownloadFailed(
                    f'download of release digest "{url}" '
                    f'failed with status {response.status}'
                )

            with open(root / self.file(), mode="wb") as file:
                shutil.copyfileobj(response, file)

    def validate(self, root: Path) -> None:
        with open(root / self.path(), mode="r", encoding="utf8") as file:
            expected = file.read().strip()
            expected = expected[:expected.index(" ")]

        actual = compute_digest(root / self.archive.path(), "sha1")
        if actual != expected:
            raise ValueError(
                f'SHA1 digest for {self.archive.path()} is {actual} and not {expected}'
            )


@dataclasses.dataclass(frozen=True, slots=True)
class BatchCount[int](DynamicVariable):

    def __call__(self, root: Path) -> int:
        with zipfile.ZipFile(root / self.resource.path()) as archive:
            names = sorted(archive.namelist())

        # Validate that the names being counted follow the expected naming convention
        prefix = self.resource.file()[:-4]
        count = len(names)
        for index, name in enumerate(names):
            if name != f"{prefix}-{index:05}.csv.zip":
                raise ValueError(f'unexpected batch name "{name}" for batch #{index}')

        return count # type: ignore


@dataclasses.dataclass(frozen=True, slots=True)
class ReleaseBatch(Resource):

    archive: ReleaseArchive
    index: int

    def date(self) -> dt.date:
        return self.archive.date

    def release(self) -> str:
        return self.archive.release()

    def description(self) -> str:
        return (
            'unarchiving entity="batch archive", '
            f'name="{self.archive.file()[:-4]}-{self.index:05}.csv.zip"'
        )

    def directory(self) -> Path:
        return self.archive.directory() / f"{self.date().day:02}" / "batch"

    def file_pattern(self) -> str:
        return f"{self.archive.file()[:-4]}-{self.index:05}-?????.csv"

    def __call__(self, root: Path) -> None:
        with zipfile.ZipFile(root / self.archive.path()) as archive:
            name = f'{self.archive.file()[:-4]}-{self.index:05}.csv.zip'
            with archive.open(name) as source_file:
                with zipfile.ZipFile(source_file) as nested_archive:
                    nested_archive.extractall(self.directory())


@dataclasses.dataclass(frozen=True, slots=True)
class BatchExtract(Resource):

    batch_data: ReleaseBatch
    filter: Filter

    def date(self) -> dt.date:
        return self.batch_data.date()

    def release(self) -> str:
        return self.batch_data.release()

    def index(self) -> int:
        return self.batch_data.index

    def description(self) -> str:
        return (
            f'distilling batch={self.index()}, '
            f'release="{self.release()}", filter="{self.filter}"'
        )

    def directory(self) -> Path:
        return self.batch_data.directory().parent / "extract"

    def file(self) -> str:
        return f"{self.release()}-{self.index():05}.parquet"


@dataclasses.dataclass(frozen=True, slots=True)
class BatchExtractSummary:

    batch_extract: BatchExtract

    def date(self) -> dt.date:
        return self.batch_extract.date()

    def release(self) -> str:
        return self.batch_extract.release()

    def index(self) -> int:
        return self.batch_extract.index()

    def description(self) -> str:
        return (
            f'computing entity="summary statistics", batch={self.index()}, '
            f'release="{self.release()}", filter="{self.batch_extract.filter}"'
        )

    def directory(self) -> Path:
        return self.batch_extract.directory().with_suffix(".stats")

    def file(self) -> str:
        return f'{self.release()}-{self.index():05}.stats.parquet'

    def __call__(self, root: Path) -> None:
        release = Daily.from_date(self.date())
        stats = Statistics(self.file())

        metadata_entry = StatementsOfReasons().summarize_release_extract(
            root=root,
            release=release,
            metadata=Metadata(
                self.batch_extract.filter.tag(),
                self.batch_extract.filter,
                cast(dict[str, MetadataEntry], {release: {}}),
            ),
            collector=stats,
        )

        # check_db_platforms(root / self.batch_data.archive.path(), frame) ????
        stats.write(root / self.directory())


@dataclasses.dataclass(frozen=True, slots=True)
class BatchSummary(Resource):

    batch_data: ReleaseBatch

    def date(self) -> dt.date:
        return self.batch_data.date()

    def release(self) -> str:
        return self.batch_data.release()

    def index(self) -> int:
        return self.batch_data.index

    def description(self) -> str:
        return (
            f'computing entity="summary statistics", batch={self.index()}, '
            f'release="{self.release()}"'
        )

    def directory(self) -> Path:
        return self.batch_data.directory().with_suffix(".stats")

    def file(self) -> str:
        return f'{self.release()}-{self.index():05}.stats.parquet'

    def __call__(self, root: Path) -> Counter:
        release = Daily.from_date(self.date())

        counters, frame = StatementsOfReasons().ingest_release(
            root=root,
            release=release,
            index=self.index(),
            name=self.batch_data.file(),
        )

        # Check_db_platforms only probes the data frame for hereto
        # unknown platform names, raising a MissingPlatformError
        # with such names.
        check_db_platforms(root / self.batch_data.archive.path(), frame)

        # We process each batch by itself. When the summary
        # statistics are finalized, those unit counts add up.
        stats = Statistics(self.file())
        stats.collect(release, frame, metadata_entry={"batch_count": 1})
        stats.write(root / self.directory())

        return counters


@dataclasses.dataclass(frozen=True, slots=True)
class ReleaseSummary(Resource):
    archive: ReleaseArchive
    batch_count: BatchCount

    def date(self) -> dt.date:
        return self.archive.date

    def release(self) -> str:
        return self.archive.release()

    def directory(self) -> Path:
        return Path("db.stats")

    def file(self) -> str:
        return f'{self.release()}.stats.parquet'

    def depends_on(self) -> Sequence[Resource]:
        return [
            BatchSummary(ReleaseBatch(
                self.archive,
                index
            )) for index in range(self.batch_count.value)
        ]

    def __call__(self, root: Path) -> None:
        stats = Statistics.read_all(
            root / BatchSummary(ReleaseBatch(self.archive, 0)).directory(),
            glob=f'{self.release()}-?????.stats.parquet',
            file=self.file(),
        )
        stats.write(root / self.directory(), should_finalize=True)


class DatabaseSummary(Resource):

    start_date: StartDate
    end_date: EndDate

    def directory(self) -> Path:
        return Path(".")

    def file(self) -> str:
        return "db.stats.parquet"

    def __call__(self, root: Path) -> None:
        stats = Statistics.read_all(
            root,
            glob=f"{root}/db.stats/????-??-??.stats.parquet",
            file=self.file(),
        )
        stats.write(root, should_finalize=True)

    def depends_on(self) -> Sequence[Resource]:
        return [
            ReleaseSummary(archive, BatchCount(archive))
            for date in self.start_date.to(self.end_date)
            for archive in (ReleaseArchive(date),)
        ]
