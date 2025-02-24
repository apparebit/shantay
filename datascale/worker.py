from collections import Counter
from contextlib import AbstractContextManager, nullcontext
import datetime as dt
import enum
import json
from pathlib import Path
import shutil
from typing import Any, Self

from .progress import Progress
from .release import Release
from .util import annotate_error


class Schedule:
    def __init__(
        self,
        start: Release,
        stop: Release,
        lock: None | AbstractContextManager = None,
    ) -> None:
        self._start = start
        self._cursor = start
        self._stop = stop
        self._lock = lock if lock else nullcontext()

    def clone(self) -> Self:
        return Schedule(self._start, self._stop, self._lock)

    def next(self) -> None | Release:
        with self._lock:
            if self._cursor is None:
                return None

            result = self._cursor
            if result == self._stop:
                self._cursor = None
            else:
                self._cursor = next(result)

            return result

    def restart(self) -> None:
        with self._lock:
            self._cursor = self._start

    def close(self) -> None:
        with self._lock:
            self._cursor = None


class Task(enum.StrEnum):
    PREPARE_BATCHES = "prepare-batches"
    PROCESS_BATCHES = "process-batches"


class Worker:
    def __init__(
        self,
        archive: Path,
        batches: Path,
        progress: Progress,
        id: None | int = None,
        schedule: None | Schedule = None,
        task: None | Task = None,
    ) -> None:
        self._staging = Path.cwd() / self.staging_for_id(id)
        self._archive = archive
        self._batches = batches

        self._metadata = {}
        self._progress = progress

        self._schedule = schedule
        self._task = task if task is not None else Task.PREPARE_BATCHES

    @classmethod
    def staging_for_id(self, id: None | int = None) -> str:
        return "dsa-db-staging" if id is None else f"dsa-db-staging-{id:03}"

    @property
    def staging(self) -> Path:
        return self._staging

    @property
    def archive(self) -> Path:
        return self._archive

    @property
    def batches(self) -> Path:
        return self._batches

    def register_release(self, release: Release) -> None:
        if self._progress.id != release.id:
            self._progress.with_id(release.id)

    @property
    def progress(self) -> Progress:
        if self._progress is None:
            raise ValueError("no release has been registered")
        return self._progress

    @classmethod
    @annotate_error(filename_arg="target")
    def copy_metadata(cls, source: Path, target: Path) -> None:
        target.mkdir(parents=True, exist_ok=True)
        path = target / "meta.json"
        tmp = target / "meta.tmp.json"
        shutil.copy(source/ "meta.json", tmp)
        tmp.replace(path)

    @classmethod
    def load_metadata(cls, root: Path) -> dict[str, Any]:
        with open(root / "meta.json", mode="r", encoding="utf8") as file:
            return json.load(file)

    def metadata_range(self) -> tuple[dt.date, dt.date]:
        summary = self._metadata.get("summary")
        if summary is None:
            return None
        start = dt.date.fromisoformat(summary["start"])
        stop = dt.date.fromisoformat(summary["stop"])
        return start, stop

    def save_metadata(self, root: Path, metadata: None | dict[str, Any] = None) -> None:
        self._save_metadata(root, metadata if metadata is not None else self._metadata)

    @classmethod
    @annotate_error(filename_arg="root")
    def _save_metadata(self, root: Path, metadata: dict[str, Any]) -> None:
        path = root / "meta.json"
        tmp = root / "meta.tmp.json"

        with open(tmp, mode="w", encoding="utf8") as file:
            json.dump(metadata, file, indent=2)

        tmp.replace(path)

    def lookup_release_metadata(self, release: Release) -> None | dict[str, Any]:
        return self._metadata.get(release.id)

    def update_release_metadata(self, release: Release, data: dict[str, Any]) -> None:
        self._metadata[release.id] = data

    @classmethod
    def merge_metadata(cls, meta1: dict[str, Any], meta2: dict[str, Any]) -> dict[str, Any]:
        meta = dict(meta1)
        for key, value2 in meta2.items():
            if key not in meta1:
                meta[key] = value2
            elif meta1[key] != value2:
                raise ValueError(f"metadata entries for {key} are inconsistent")

    def prepare(self) -> None:
        self._staging.mkdir(parents=True, exist_ok=True)
        if (self._batches / "meta.json").exists():
            self.copy_metadata(self._batches, self._staging)
            self._metadata = self.load_metadata(self._staging)
        else:
            self._metadata = {}
            self.save_metadata(self._staging)

    def is_archive_downloaded(self, release: Release) -> bool:
        return (self._archive / release.directory / release.archive).exists()

    def download_archive(self, release: Release) -> None:
        self.register_release(release)

        if self.is_archive_downloaded(release):
            return

        self.progress.prep(
            f"downloading release {release.id}",
            "downloading", "byte", with_rate=True,
        )
        release.download_archive(self._staging, self.progress)
        self.progress.update(f"validating release {release.id}")
        release.validate_archive(self._staging)
        self.progress.update(f"copying release {release.id} to archive")
        release.copy_archive(self._staging, self._archive)

    def is_archive_staged(self, release: Release) -> bool:
        return (self._staging / release.directory / release.archive).exists()

    def stage_archive(self, release: Release) -> None:
        assert self.is_archive_downloaded(release)
        self.register_release(release)

        if self.is_archive_staged(release):
            return

        self.progress.update(f"copying release {release.id} from archive to staging")
        release.copy_archive(self._archive, self._staging)
        self.progress.update(f"validating release {release.id}")
        release.validate_archive(self._staging)

    def extract_batches(self, release: Release) -> None:
        assert self.is_archive_staged(release)
        self.register_release(release)

        filenames = release.archived_files(self._staging)
        batch_count = len(filenames)
        self.progress.prep(
            f"extracting batches from release {release.id}",
            "extracting", "batch", with_rate=False,
        )
        steps = release.extract_batch_steps() + 1
        self.progress.start(steps * batch_count)

        # Archived files are archives, too. Unarchive one at a time.
        counters = Counter(batch_count=batch_count)
        for index, name in enumerate(filenames):
            self.progress.step(steps * index, "unarchiving data")
            release.unarchive_file(self._staging, index, name)
            counters += release.extract_batch(self._staging, index, name, self.progress)

            shutil.rmtree(self._staging / release.working_directory)

        self.progress.update(f"updating batch metadata for release {release.id}")
        self.update_release_metadata(release, counters)
        self.save_metadata(self._staging)

        self.progress.prep(
            f"copying batches for {release.id} out of staging",
            "copying", "batch", with_rate=False,
        )
        self.progress.start(batch_count)
        release.copy_batches(self._staging, self._batches, batch_count, self.progress)

    def batch_count(self, release: Release) -> None | int:
        meta = self.lookup_release_metadata(release)
        return None if meta is None else meta["batch_count"]

    def prepare_batches(self, release: Release) -> None:
        self.register_release(release)

        if not self.is_archive_downloaded(release):
            self.download_archive(release)

        if (
            (batch_count := self.batch_count(release)) is None
            or release.batches_exist(self._batches, batch_count)
        ):
            self.stage_archive(release)
            self.extract_batches(release)

            shutil.rmtree(self._staging / release.directory)

        self.progress.update(f"done with {release.id}")
        self.progress.finish()

    def run(self) -> None:
        if self._schedule is None:
            raise ValueError("no schedule available")
        if self._task is None:
            self
        if self._task is Task.PREPARE_BATCHES:
            while True:
                cursor = self._schedule.next()
                if cursor is None:
                    return
                self.prepare_batches(cursor)
        if self._task is Task.PROCESS_BATCHES:
            total_batch_count = 0
            while True:
                cursor = self._schedule.next()
                if cursor is None:
                    break
                batch_count = self.batch_count(cursor)
                if batch_count is None:
                    raise ValueError(f"release {cursor.id} without batch_count")
                total_batch_count += batch_count

            self._schedule.restart()
            self._progress.prep(
                description="uplifting column type", activity="uplifting",
                unit="batch", with_rate=False
            )
            self._progress.start(total_batch_count)
            offset = 0

            while True:
                cursor = self._schedule.next()
                if cursor is None:
                    break

                batch_count = self.batch_count(cursor)
                if batch_count is None:
                    raise ValueError(f"release {cursor.id} without batch_count")

                for index in range(batch_count):
                    cursor.process_batch(self._batches, index)
                    self._progress.step(offset + index)

                offset += batch_count

    @classmethod
    def shutdown_all(cls, sources: list[Path], target: Path) -> None:
        # Merge metadata from parallel workers
        metadata = None
        for source in sources:
            if metadata is None:
                metadata = cls.load_metadata(source)
            else:
                cls.merge_metadata(metadata, cls.load_metadata(source))

        # Sort metadata by daily key
        sorted_metadata = { key: metadata[key] for key in sorted(metadata.keys()) }

        # Determine start and stop (inclusive)
        start = stop = None
        for key in sorted_metadata.keys():
            if start is None:
                start = key
            stop = key

        sorted_metadata["summary"] = {
            "start": start,
            "stop": stop,
        }

        # Save metadata
        cls._save_metadata(target, sorted_metadata)
