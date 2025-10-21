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
from collections.abc import Hashable, Iterable, Iterator, Sequence
import dataclasses
import datetime as dt
import enum
import logging
from pathlib import Path
import re
import shutil
from typing import Any, cast, ClassVar, final, get_origin, Self
from urllib.request import Request, urlopen
import zipfile

from .digest import compute_digest
from .dsa_sor import StatementsOfReasons
from .metadata import Metadata
from .model import Daily, DateRange, DownloadFailed, Filter, MetadataEntry, ReleaseRange
from .pool import check_not_cancelled
from .progress import NO_PROGRESS, Progress
from .schema import check_db_platforms
from .stats import Statistics


_logger = logging.getLogger(__file__)


class Resource(Hashable):
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
    def is_config_var(self) -> bool:
        """Determine whether this resource is a configuration variable. Subclasses
        must not override this method."""
        return False

    @final
    def is_dyn_var(self) -> bool:
        """Determine whether this resource is a dynamic variable. Subclasses
        must not override this method."""
        return False

    def is_transient(self) -> bool:
        return False

    def name(self) -> str:
        raise NotImplementedError

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

    def exists(self, root: Path) -> bool:
        """
        Determine whether this resource exists under the given root. This method
        returns a success if at least one of the files identified by the resource
        as persistent exists.

        """
        directory = root / self.directory()
        if (file := self.file()) is NotImplemented:
            return all(f.exists() for f in directory.glob(self.file_pattern()))
        else:
            return (directory / file).exists()

    @final
    def static_dependencies(self) -> "Sequence[Resource]":
        """
        Compute the resource's statically known direct data dependencies. This
        includes the resource's variables, but no resources that can only be
        determined with those variable values.
        """
        assert dataclasses.is_dataclass(self)

        return [
            getattr(self, f.name)
            for f in dataclasses.fields(self)
            if self.is_subclass(f.type)
        ]

    def dynamic_dependencies(self) -> "Sequence[Resource]":
        """
        Compute the resource's dynamic direct data dependencies. Each such
        dependency depends on the value of a variable. The runtime fills in the
        values of all variables before invoking this method.
        """
        return []

    def __call__(self, root: Path) -> Any:
        """Compute the data for this resource. Upon invocation by the runtime, all
        direct data dependencies are available in directories off the given
        root. Also, this resource's directory is guaranteed to exist. This
        method may optionally accept a `Progress` argument; it should default to
        `NO_PROGRESS`. Subclasses must implement this method."""
        raise NotImplementedError


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

    def name(self) -> str:
        return to_snake_case(type(self).__name__)

    def exists(self, root: Path) -> bool:
        return self.value is not None

    def static_dependencies(self) -> Sequence[Resource]: # type: ignore
        return []

    def __call__(self, __ignored: None | Path = None) -> V:
        """Compute this dynamic variable's value."""
        raise NotImplementedError


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

    def name(self) -> str:
        return to_snake_case(type(self).__name__)

    def exists(self, root: Path) -> bool:
        return self.value is not None

    def static_dependencies(self) -> Sequence[Resource]: # type: ignore
        return [self.resource]

    def __call__(self, root: Path) -> V:
        """Compute this dynamic variable's value."""
        raise NotImplementedError

# --------------------------------------------------------------------------------------

@dataclasses.dataclass(frozen=True, slots=True)
class ReleaseArchive(Resource):

    CHUNK_SIZE: ClassVar[int] = 64 * 1_024

    date: dt.date

    def name(self) -> str:
        return self.date.isoformat()

    def description(self) -> str:
        return f'downloading entity="release archive", url="{self.url(self.file())}"'

    def url(self, file: str) -> str:
        return f"https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/{file}"

    def directory(self) -> Path:
        return Path(f"{self.date.year}") / f"{self.date.month:02}"

    def file(self) -> str:
        return f"sor-global-{self.name()}-full.zip"

    def __call__(self, root: Path, progress: Progress = NO_PROGRESS) -> int:
        progress.activity(
            f"downloading data for release {self.name()}",
            f"downloading {self.name()}", "byte", with_rate=True,
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

    def name(self) -> str:
        return self.archive.name()

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

    def name(self) -> str:
        return self.archive.name()

    def alias(self) -> str:
        return f"sor-global-{self.name()}-full-{self.index:05}.csv.zip"

    def is_transient(self) -> bool:
        return True

    def description(self) -> str:
        return f'unarchiving entity="batch archive", name="{self.alias()}"'

    def directory(self) -> Path:
        return self.archive.directory() / f"{self.date().day:02}" / "batch"

    def file_pattern(self) -> str:
        return f"{self.archive.file()[:-4]}-{self.index:05}-?????.csv"

    def __call__(self, root: Path) -> None:
        with zipfile.ZipFile(root / self.archive.path()) as archive:
            with archive.open(self.alias()) as source_file:
                with zipfile.ZipFile(source_file) as nested_archive:
                    nested_archive.extractall(self.directory())


@dataclasses.dataclass(frozen=True, slots=True)
class BatchSummary(Resource):

    batch_data: ReleaseBatch

    def date(self) -> dt.date:
        return self.batch_data.date()

    def name(self) -> str:
        return self.batch_data.name()

    def index(self) -> int:
        return self.batch_data.index

    def is_transient(self) -> bool:
        return True

    def description(self) -> str:
        return (
            f'computing entity="summary statistics", batch={self.index()}, '
            f'release="{self.name()}"'
        )

    def directory(self) -> Path:
        return self.batch_data.archive.directory() / f"{self.date().day:02}" / "stats"

    def file(self) -> str:
        return f'{self.name()}-{self.index():05}.stats.parquet'

    def __call__(self, root: Path) -> Counter:
        release = Daily.from_date(self.date())

        counters, frame = StatementsOfReasons().ingest_release(
            root=root,
            release=release,
            index=self.index(),
            name=self.batch_data.alias(),
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
    digest: ReleaseDigest
    batch_count: BatchCount

    def __init__(self, archive: ReleaseArchive) -> None:
        super().__setattr__("archive", archive)
        super().__setattr__("digest", ReleaseDigest(archive))
        super().__setattr__("batch_count", BatchCount(archive))

    def date(self) -> dt.date:
        return self.archive.date

    def name(self) -> str:
        return self.archive.name()

    def directory(self) -> Path:
        return Path("db.stats")

    def file(self) -> str:
        return f'{self.name()}.stats.parquet'

    def dynamic_dependencies(self) -> Sequence[Resource]:
        return [
            BatchSummary(ReleaseBatch(
                self.archive,
                index
            )) for index in range(self.batch_count.value)
        ]

    def __call__(self, root: Path) -> None:
        stats = Statistics.read_all(
            root / BatchSummary(ReleaseBatch(self.archive, 0)).directory(),
            glob=f'{self.name()}-?????.stats.parquet',
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

    def dynamic_dependencies(self) -> Sequence[Resource]:
        return [
            ReleaseSummary(ReleaseArchive(date))
            for date in self.start_date.to(self.end_date)
        ]

    def __call__(self, root: Path) -> None:
        stats = Statistics.read_all(
            root,
            glob=f"{root}/db.stats/????-??-??.stats.parquet",
            file=self.file(),
        )
        stats.write(root, should_finalize=True)


@dataclasses.dataclass(frozen=True, slots=True)
class BatchExtract(Resource):

    batch_data: ReleaseBatch
    filter: Filter

    def date(self) -> dt.date:
        return self.batch_data.date()

    def name(self) -> str:
        return self.batch_data.name()

    def index(self) -> int:
        return self.batch_data.index

    def description(self) -> str:
        return (
            f'distilling batch={self.index()}, '
            f'release="{self.name()}", filter="{self.filter}"'
        )

    def directory(self) -> Path:
        return self.batch_data.directory().parent / "extract"

    def file(self) -> str:
        return f"{self.name()}-{self.index():05}.parquet"

    def __call__(self, root: Path) -> None:
        digest, counters = StatementsOfReasons().distill_release(
            root=root,
            release=Daily.from_date(self.batch_data.date()),
            index=self.index(),
            name=self.batch_data.alias(),
            filter=self.filter,
            # PROGRESS???
        )


@dataclasses.dataclass(frozen=True, slots=True)
class ReleaseExtract(Resource):

    archive: ReleaseArchive
    digest: ReleaseDigest
    batch_count: BatchCount
    filter: Filter

    def __init__(self, archive: ReleaseArchive) -> None:
        super().__setattr__("archive", archive)
        super().__setattr__("digest", ReleaseDigest(archive))
        super().__setattr__("batch_count", BatchCount(archive))

    def dynamic_dependencies(self):
        return [
            BatchExtract(ReleaseBatch(self.archive, index), self.filter)
            for index in range(self.batch_count.value)
        ]


@dataclasses.dataclass(frozen=True, slots=True)
class BatchExtractSummary:

    batch_extract: BatchExtract

    def date(self) -> dt.date:
        return self.batch_extract.date()

    def name(self) -> str:
        return self.batch_extract.name()

    def index(self) -> int:
        return self.batch_extract.index()

    def description(self) -> str:
        return (
            f'computing entity="summary statistics", batch={self.index()}, '
            f'release="{self.name()}", filter="{self.batch_extract.filter}"'
        )

    def directory(self) -> Path:
        return self.batch_extract.directory().with_suffix(".stats")

    def file(self) -> str:
        return f'{self.name()}-{self.index():05}.stats.parquet'

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




class Runtime:

    def __init__(self, date_range: DateRange, staging: Path, archive: Path) -> None:
        self._pending: list[Resource] = []
        self._scheduling = []
        self._staging = staging
        self._archive = archive
        self._date_range = date_range
        self._dependency_table = dict()
        self._metadata = {} # Persist!!!

    def step(self) -> None:
        while True:
            resource = self._pending[-1]

            if resource.exists(self._staging):
                self._pending.pop()
                return
            if not resource.is_transient() and resource.exists(self._archive):
                self.copy(resource, self._archive, self._staging)
                return

            dependencies = self.get_dependencies(resource)
            if dependencies is None:
                continue


    def is_available(self, resource: Resource) -> bool:
        return resource.exists(self._staging) or (
            not resource.is_transient() and resource.exists(self._archive)
        )

    def schedule(self, resource: Resource) -> None:
        self._pending.append(resource)

    def instantiate_top(self) -> bool:
        resource = self._pending[-1]

        if resource.exists(self._staging):
            self._pending.pop()
            return True
        if not resource.is_transient() and resource.exists(self._archive):
            self.copy(resource, self._archive, self._staging)
            self._pending.pop()
            return True

        dependencies = self.get_dependencies(resource)
        if dependencies is None:
            return False

        pending_count = len(self._pending)
        for dependency in dependencies:
            if not self.is_available(dependency):
                self.schedule(dependency)
        if pending_count < len(self._pending):
            return False

        if resource.is_dyn_var():
            result = resource(self._staging)
            object.__setattr__(resource, "value", result)
        else:
            resource.directory().mkdir(parents=True, exist_ok=True)
            resource(self._staging)
            if not resource.is_transient():
                self.copy(resource, self._staging, self._archive)

        self._pending.pop()
        return True

    def get_metadata(self, release: str) -> dict[str, Any]:
        return self._metadata.setdefault(release, {})

    def get_dependencies(self, resource: Resource) -> None | Sequence[Resource]:
        # Look for cached dependencies
        dependencies = self._dependency_table.get(resource)
        if dependencies is not None:
            return dependencies

        # Determine static dependencies
        static_dependencies = resource.static_dependencies()
        variables = only_variables(static_dependencies)
        if len(variables) == 0:
            # No variables -> these are the only dependencies
            all_dependencies = without_duplicates(static_dependencies)
            self._dependency_table[resource] = all_dependencies
            return all_dependencies

        # Check variables
        pending_count = len(self._pending)
        for variable in variables:
            if variable.is_config_var():
                self.resolve_config_var(cast(ConfigVariable, variable))
            elif variable.is_dyn_var():
                self.resolve_dyn_var(cast(DynamicVariable, variable))

        if pending_count < len(self._pending):
            # resolve_dyn_var scheduled work
            return None

        # All variables have been resolved; determine dynamic dependencies
        dynamic_dependencies = resource.dynamic_dependencies()
        if has_variable(dynamic_dependencies):
            raise MisconfigurationError(
                f"{type(resource)} has variables as dynamic dependencies"
            )

        all_dependencies = without_duplicates(
            (*without_variables(static_dependencies), *dynamic_dependencies)
        )
        self._dependency_table[resource] = all_dependencies
        return all_dependencies

    def resolve_config_var(self, variable: ConfigVariable) -> None:
        if variable.value is not None:
            return

        if isinstance(variable, StartDate):
            object.__setattr__(variable, "value", self._date_range.first)
        elif isinstance(variable, EndDate):
            object.__setattr__(variable, "value", self._date_range.last)
        else:
            raise AssertionError(f"unsupported configuration variable {type(variable)}")

    def resolve_dyn_var(self, variable: DynamicVariable) -> None:
        if variable.value is not None:
            return

        key = variable.name()
        entry = self.get_metadata(variable.resource.name())
        if key in entry:
            object.__setattr__(variable, "value", entry[key])
            return

        self.schedule(variable)

    def copy(self, resource: Resource, source: Path, target: Path) -> None:
        if resource.file() is NotImplemented:
            for path in source.glob(resource.file_pattern()):
                safe_copy(path, target / resource.directory() / path.name)
        else:
            safe_copy(
                source / resource.directory() / resource.file(),
                target / resource.directory() / resource.file(),
            )


def to_snake_case(name: str) -> str:
    return re.sub(r"(?<!^)([A-Z])", "_\1", name).lower()


def has_variable(resources: Sequence[Resource]) -> bool:
    return any(r.is_config_var() or r.is_dyn_var() for r in resources)


def only_variables(resources: Iterable[Resource]) -> Sequence[Resource]:
    return [r for r in resources if r.is_config_var() or r.is_dyn_var()]


def without_variables(resources: Iterable[Resource]) -> Sequence[Resource]:
    return [r for r in resources if not r.is_config_var() and not r.is_dyn_var()]


def without_duplicates(resources: Iterable[Resource]) -> Sequence[Resource]:
    deduplicated = []
    seen_before = set()
    for resource in resources:
        if resource in seen_before:
            continue
        seen_before.add(resource)
        deduplicated.append(resource)
    return deduplicated


def safe_copy(source: Path, target: Path) -> None:
    if target.exists():
        raise ValueError(f"copy target {target} already exists")
    tmp = target.with_suffix(f".tmp{target.suffix}")
    shutil.copy2(source, tmp)
    tmp.replace(target)


class MisconfigurationError(Exception):
    pass
