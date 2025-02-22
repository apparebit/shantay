from collections import Counter
import json
from pathlib import Path
import shutil
from typing import Any

from .progress import Progress
from .release import Release
from .sor import DailySoR


_COUNTER = 0
def _counter() -> int:
    global _COUNTER
    c = _COUNTER
    _COUNTER += 1
    return c


class Worker:
    def __init__(self, archive: Path, batches: Path) -> None:
        self._no = _counter()
        self._staging = Path.cwd() / f"dsa-db-staging-{self._no}"
        self._archive = archive
        self._batches = batches

        self._metadata = {}
        self._progress = None

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
        if self._progress is None or self._progress.release != release.id:
            self._progress = Progress(release.id)

    @property
    def progress(self) -> Progress:
        if self._progress is None:
            raise ValueError("no release has been registered")
        return self._progress

    @classmethod
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

    def save_metadata(self, root: Path, metadata: None | dict[str, Any] = None) -> None:
        self._save_metadata(root, metadata if metadata is not None else self._metadata)

    @classmethod
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

    def stage_archive(self, release: DailySoR) -> None:
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

    def process(self, release: Release) -> None:
        self.register_release(release)

        if not self.is_archive_downloaded(release):
            self.download_archive(release)

        meta = self.lookup_release_metadata(release)
        if meta is None or not release.batches_exist(self._batches, meta["batch_count"]):
            self.stage_archive(release)
            self.extract_batches(release)

            shutil.rmtree(self._staging / release.directory)

        self.progress.update(f"done with {release.id}")
        self.progress.finish()

    @classmethod
    def shutdown_all(cls, sources: list[Path], target: Path) -> None:
        metadata = None
        for source in sources:
            if metadata is None:
                metadata = cls.load_metadata(source)
            else:
                cls.merge_metadata(metadata, cls.load_metadata(source))

        sorted_metadata = { key: metadata[key] for key in sorted(metadata.keys()) }
        cls._save_metadata(target, sorted_metadata)
