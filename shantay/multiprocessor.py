from concurrent.futures import Future
import logging
import multiprocessing as mp
import os
import signal
import sys
import time
from types import FrameType
from typing import Any

from .framing import collect_release_metadata, Collector
from .metadata import Metadata
from .model import (
    Coverage, DataFrameType, Dataset, Release, Storage
)
from .pool import Cancelled, Pool, WorkerProgress
from .processor import extracted_data_exists, Processor


_logger = logging.getLogger(__spec__.parent)


class Multiprocessor[R: Release]:

    def __init__(
        self,
        dataset: Dataset[R],
        storage: Storage,
        coverage: Coverage[R],
        metadata: Metadata,
        size: int,
    ) -> None:
        self._dataset = dataset
        self._storage = storage
        self._coverage = coverage
        self._metadata = metadata
        self._metadata_frame = None

        # Prepare processes daily releases, whereas analyze processes monthly ones
        self._task = None
        self._cursor = None
        self._last = None

        self._pool = None
        self._register_handlers()
        # Use the same level as the root logger
        self._pool = Pool(size=size, log_level=logging.getLogger().level)

        self._runtime = 0

    @property
    def runtime(self) -> float:
        return self._runtime

    def run(self, task: str, wait: bool = True) -> None:
        assert self._pool is not None
        self._task = task

        _logger.info('running multiprocessor with pid=%d, task="%s"', os.getpid(), task)
        _logger.info('    key="dataset.name",         value="%s"', self._dataset.name)
        _logger.info('    key="storage.archive_root", value="%s"', self._storage.archive_root)
        _logger.info('    key="storage.working_root", value="%s"', self._storage.working_root)
        _logger.info('    key="storage.staging_root", value="%s"', self._storage.staging_root)
        _logger.info('    key="coverage.filter",      value="%s"', self._coverage.filter)
        _logger.info('    key="coverage.first",       value="%s"', self._coverage.first.id)
        _logger.info('    key="coverage.last",        value="%s"', self._coverage.last.id)
        _logger.info('    key="pool.size",            value=%d', self._pool.size)

        # See Processor.run() for an explanation for time.time().
        start_time = time.time()
        if task == "prepare":
            cover = self._coverage
        elif task == "analyze":
            date_cover, metadata = collect_release_metadata(self._metadata.records)
            cover = date_cover.to_release_range().to_monthly()
            self._metadata_frame = metadata
        else:
            raise ValueError(f"invalid task {task}")

        self._cursor = cover.first
        self._last = cover.last

        for _ in range(self._pool.size):
            if not self._schedule_task():
                break

        if wait:
            self._pool.wait()
        self._runtime = time.time() - start_time

    def _schedule_task(self) -> bool:
        release = self._next_release()
        if release is None:
            # Make sure the pool finishes
            assert self._pool is not None
            self._pool.finish()
            return False

        assert self._pool is not None
        try:
            future = self._pool.submit(
                run_on_worker,
                task=self._task,
                dataset=self._dataset,
                storage=self._storage,
                filter=self._metadata.filter,
                metadata=self._metadata_frame,
                release=release,
            )
        except Exception as x:
            _logger.error('task rejected by pool="%s"', self._pool.id)
            return False

        future.add_done_callback(self._done_with_task)
        return True

    def _next_release(self) -> None | Release:
        assert self._cursor is not None
        assert self._last is not None

        # For prepare, skip release if we already extracted the working data
        if self._task == "prepare":
            while (
                self._cursor <= self._last
                and self._cursor in self._metadata
                and extracted_data_exists(
                    self._storage.working_root,
                    self._cursor,
                    self._metadata
                )
            ):
                self._cursor = self._cursor.next()

        if self._last < self._cursor:
            return None

        release = self._cursor
        self._cursor = release.next()
        return release

    def _done_with_task(self, future: Future) -> bool:
        assert self._pool is not None

        try:
            result = future.result()
        except Cancelled as x:
            _logger.debug('worker=%d, status="cancelled"', x.args[2])
            self._pool.stop()
            return False
        except Exception as x:
            return self._schedule_task()

        if self._task == "prepare":
            release = result["release"]
            del result["release"]
            self._metadata[release] = result
            self._metadata.write_json(self._storage.staging_root, sort_keys=True)
            # If the working root contains a meta.json, then the tool module
            # instantiates _metadata with that file's data. Since copy_json()
            # first writes to a temporary file and then atomically replaces the
            # original, it's ok to update that file here. In fact, it's more
            # than ok because we just updated the metadata with a new release.
            Metadata.copy_json(self._storage.staging_root, self._storage.working_root)

            return self._schedule_task()
        else:
            raise AssertionError(f"invalid task {self._task}")

    def stop(self) -> bool:
        assert self._pool is not None
        return self._pool.stop()

    def _register_handlers(self) -> None:
        assert self._pool is None
        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def _handle_signal(self, signum: int, frame: None | FrameType) -> None:
        signame = signal.strsignal(signum)
        if signum not in (signal.SIGINT, signal.SIGTERM):
            _logger.warning('received unexpected signal="%s"', signame)
            return
        elif self._pool is None:
            _logger.warning(
                'exiting process after receiving signal="%s", status="not running"',
                signame
            )
            sys.exit(1)

        if self._pool.stop():
            _logger.info('cancelling workers after receiving signal="%s"', signame)
            return

        _logger.info(
            'terminating workers after receiving repeated signal="%s"', signame
        )
        for process in mp.active_children():
            process.terminate()
            process.join()

        sys.exit(1)


def run_on_worker[R: Release](
    task: str,
    dataset: Dataset[R],
    storage: Storage,
    filter: str,
    metadata_frame: DataFrameType,
    release: R,
) -> Any:
    """
    Run a task in a worker process.

    This function runs a prepare or analyze task in a worker process. All of the
    function's arguments are used for both tasks, with exception of filter,
    which is only used by prepare, and metadata_frame, which is only used by
    analyze. The result for a prepare task is the metadata entry for the
    release. The result for an analyze task is the statistics data frame for the
    release.
    """
    pid = os.getpid()

    coverage = Coverage(release, release, filter)
    if task == "prepare":
        metadata = Metadata(filter)
    else:
        metadata = Metadata.read_json(storage.working_root)
    processor = Processor(
        dataset=dataset,
        storage=storage.isolate(pid),
        coverage=coverage,
        metadata=metadata,
        progress=WorkerProgress(),
    )

    _logger.debug('running task=%s, release="%s", worker=%d', task, release, pid)
    if task == "prepare":
        processor.prepare_batches(release)
        record = metadata[release]
        result = dict(release=release, **record)
    elif task == "analyze":
        with dataset.analysis_context():
            collector = Collector()
            processor.analyze_release(release, metadata_frame, collector)
            result = collector.to_frame()
    else:
        raise AssertionError(f"invalid task {task}")

    _logger.debug('finished task=%s, release=%s, worker=%d', task, release, pid)
    return result
