from concurrent.futures import Future
import logging
import os
from typing import Any

from .metadata import Metadata
from .model import Coverage, Dataset, Release, Storage
from .pool import Pool, WorkerProgress
from .processor import extracted_data_exists, Processor
from .progress import NO_PROGRESS, Progress


_logger = logging.getLogger(__spec__.parent)


class Multiprocessor[R: Release]:

    def __init__(
        self,
        dataset: Dataset[R],
        storage: Storage,
        coverage: Coverage[R],
        metadata: Metadata,
        progress: Progress = NO_PROGRESS,
    ) -> None:
        self._dataset = dataset
        self._storage = storage
        self._coverage = coverage
        self._metadata = metadata
        self._cursor = coverage.first
        self._pool = Pool()

    def run(self, task: str) -> None:
        assert task == "prepare"

        _logger.info('running multiprocessor with pid=%d, task="%s"', os.getpid(), task)
        _logger.info('    key="dataset.name",         value="%s"', self._dataset.name)
        _logger.info('    key="storage.archive_root", value="%s"', self._storage.archive_root)
        _logger.info('    key="storage.working_root", value="%s"', self._storage.working_root)
        _logger.info('    key="storage.staging_root", value="%s"', self._storage.staging_root)
        _logger.info('    key="coverage.filter",      value="%s"', self._coverage.filter)
        _logger.info('    key="coverage.first",       value="%s"', self._coverage.first.id)
        _logger.info('    key="coverage.last",        value="%s"', self._coverage.last.id)

        for _ in range(self._pool.size):
            if not self.prepare_release():
                break

    def prepare_release(self) -> bool:
        release = self._next_release()
        if release is None:
            return False

        future = self._pool.submit(
            prepare_on_worker,
            self._dataset,
            self._storage,
            self._metadata.filter,
            release,
        )

        future.add_done_callback(self._done_with_task)
        return True

    def _next_release(self) -> None | Release:
        while (
            self._cursor <= self._coverage.last
            and self._cursor in self._metadata
            and extracted_data_exists(
                self._storage.working_root,
                self._cursor,
                self._metadata
            )
        ):
            self._cursor = self._cursor.next()

        if self._coverage.last < self._cursor:
            return None

        release = self._cursor
        self._cursor = release.next()
        return release

    def _done_with_task(self, future: Future) -> bool:
        try:
            record = future.result()
        except:
            pass
        else:
            release = record["release"]
            del record["release"]
            self._metadata[release] = record
            self._metadata.write_json(self._storage.staging_root, sort_keys=True)

        return self.prepare_release()

    def stop(self) -> None:
        self._pool.stop()


def prepare_on_worker[R: Release](
    dataset: Dataset[R],
    storage: Storage,
    filter: str,
    release: R,
    tracker_index: int,
) -> Any:
    pid = os.getpid()
    coverage = Coverage(release, release, filter)
    metadata = Metadata(filter)

    _logger.debug('preparing release %s in worker %d', release, pid)

    processor = Processor(
        dataset=dataset,
        storage=storage.isolate(pid),
        coverage=coverage,
        metadata=metadata,
        progress=WorkerProgress(),
    )
    processor.prepare_batches(release)

    record = metadata[release]
    _logger.debug(
        'prepared release %s with %d batches in worker %d',
        release, record["batch_count"], pid,
    )
    return dict(release=release, **record)
