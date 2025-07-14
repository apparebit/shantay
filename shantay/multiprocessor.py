from collections import deque
from collections.abc import Iterator
from concurrent.futures import Future
import logging
import multiprocessing as mp
import os
import signal
import sys
import time
import traceback
from types import FrameType
from typing import Any, cast

from .metadata import Metadata
from .model import (
    Coverage, Daily, DataFrameType, Dataset, FullMetadataEntry, MetadataEntry, Storage
)
from .pool import Cancelled, check_not_cancelled, Pool, Task, WorkerProgress
from .processor import distilled_category_exists, Processor
from .schema import MissingPlatformError, update_platforms
from .stats import Collector, Statistics


_PID = os.getpid()


_logger = logging.getLogger(__spec__.parent)


class Multiprocessor:

    def __init__(
        self,
        dataset: Dataset,
        storage: Storage,
        coverage: Coverage[Daily],
        metadata: Metadata,
        size: int,
        offline: bool = False,
    ) -> None:
        self._dataset = dataset
        self._storage = storage
        self._coverage = coverage
        self._metadata = metadata
        self._stats = None
        self._offline = offline

        self._task = None
        self._iter = None
        self._continuations = deque()

        self._pool = None
        self._register_handlers()
        # Use the same level as the root logger
        self._pool = Pool(size=size, log_level=logging.getLogger().level)

        self._running_time = 0

    @property
    def stats_file(self) -> str:
        return f"{self._coverage.stem()}.parquet"

    @property
    def latency(self) -> float:
        return self._running_time

    def run(self, task: str) -> None | DataFrameType:
        assert self._pool is not None
        self._task = task

        _logger.info('running multiprocessor with pid=%d, task="%s"', _PID, task)
        _logger.info('    key="dataset.name",         value="%s"', self._dataset.name)
        _logger.info('    key="storage.archive_root", value="%s"',
            "" if self._storage.archive_root is None else self._storage.archive_root)
        _logger.info('    key="storage.extract_root", value="%s"',
            "" if self._storage.extract_root is None else self._storage.extract_root)
        _logger.info('    key="storage.staging_root", value="%s"', self._storage.staging_root)
        _logger.info('    key="coverage.category",    value="%s"',
            "" if self._coverage.category is None else self._coverage.category)
        _logger.info('    key="coverage.first",       value="%s"', self._coverage.first.id)
        _logger.info('    key="coverage.last",        value="%s"', self._coverage.last.id)
        _logger.info('    key="coverage.frequency",   value="%s"', self._coverage.frequency())
        _logger.info('    key="statistics.file",      value="%s"', self.stats_file)
        _logger.info('    key="network.offline",      value="%s"', self._offline)
        _logger.info('    key="pool.size",            value=%d', self._pool.size)

        # See Processor.run() for an explanation for time.time()
        start_time = time.time()

        # Determine cursor's first and final values as well as increment
        if task in ("download", "distill"):
            pass
        elif task == "summarize-category":
            self._stats = Statistics.from_storage(
                self.stats_file,
                self._storage.staging_root,
                self._storage.the_extract_root,
            )
        elif task == "summarize-all":
            self._stats = Statistics.from_storage(
                self.stats_file,
                self._storage.staging_root,
                self._storage.the_archive_root,
            )
        else:
            raise ValueError(f"invalid task {task}")

        if task.startswith("summarize"):
            assert self._stats is not None
            if not self._stats.is_empty():
                date_range = self._stats.date_range()
                _logger.info(
                    'existing statistics cover start_date="%s", end_date="%s"',
                    date_range.first, date_range.last
                )

        self._iter = iter(self._coverage)
        _logger.info('    key="iter.first",           value="%s"', self._coverage.first)
        _logger.info('    key="iter.last",            value="%s"', self._coverage.last)

        self._pool.run(self._task_iter(), self._done_with_task)

        if task in ("distill", "summarize-category"):
            meta_json = f"{self._coverage.stem()}.json"
            Metadata.copy_json(
                self._storage.staging_root / meta_json,
                self._storage.the_extract_root / meta_json)
        elif task == "summarize-all":
            Metadata.copy_json(
                self._storage.staging_root / "db.json",
                self._storage.the_archive_root / "db.json",
            )

        if task.startswith("summarize"):
            assert self._stats is not None

            _logger.info(
                'writing rechunked summary statistics to file="%s"',
                self._storage.staging_root / self.stats_file
            )
            self._stats.write(self._storage.staging_root, should_finalize=True)

            if self._storage.extract_root is not None:
                persistent = self._storage.the_extract_root
            else:
                persistent = self._storage.the_archive_root
            _logger.info(
                'copying summary statistics to persistent file="%s"',
                persistent / self.stats_file
            )
            Statistics.copy(self.stats_file, self._storage.staging_root, persistent)

        self._running_time = time.time() - start_time
        return None if self._stats is None else self._stats.frame()

    def _task_iter(self) -> Iterator[Task]:
        assert self._pool is not None

        while True:
            if 0 < len(self._continuations):
                release = self._continuations.popleft()
                effective_task = "summarize-category"
            else:
                release = self._next_release()
                if release is None:
                    break

                if self._task != "summarize-category":
                    effective_task = self._task
                else:
                    if (
                        release not in self._metadata
                        or not distilled_category_exists(
                            self._storage.the_extract_root, release, self._metadata
                        )
                    ):
                        effective_task = "distill"
                    else:
                        effective_task = "summarize-category"

            if effective_task == "summarize-category":
                metadata_entry = self._metadata[release]
            else:
                metadata_entry = None

            _logger.info(
                'submitting task="%s", release="%s", pool="%s"',
                effective_task, release, self._pool.id
            )

            yield Task(
                run_on_worker,
                (),
                dict(
                    task=effective_task,
                    dataset=self._dataset,
                    storage=self._storage,
                    category=self._coverage.category,
                    metadata_entry=metadata_entry,
                    release=release,
                    offline=self._offline,
                )
            )

    def _next_release(self) -> None | Daily:
        # Keep iterating over the next release if the work has already been done.
        assert self._iter is not None
        release = next(self._iter, None)

        if self._task == "download":
            while (
                release is not None
                and (
                    self._storage.the_archive_root
                    / release.parent_directory
                    / self._dataset.archive_name(release)
                ).exists()
            ):
                _logger.debug('archive already downloaded for release="%s"', release.id)
                release = next(self._iter, None)
        elif self._task == "distill":
            while (
                release is not None
                and release in self._metadata
                and distilled_category_exists(
                    self._storage.the_extract_root,
                    release,
                    self._metadata
                )
            ):
                release = next(self._iter, None)
        elif self._task is not None and self._task.startswith("summarize"):
            assert self._stats is not None
            while release is not None and release.date in self._stats:
                _logger.debug('summary statistics already cover release="%s"', release.id)
                release = next(self._iter, None)

        # Ensure graceful termination in offline mode.
        if self._offline and release is not None and not (
            self._storage.the_archive_root
            / release.parent_directory
            / self._dataset.archive_name(release)
        ).exists():
            _logger.debug(
                'stopping due to missing archive in offline mode '
                'for task="%s", release="%s"',
                self._task, release.id
            )
            self._iter = iter([])
            return None

        return release

    def _done_with_task(self, task: Task, future: Future) -> None:
        assert self._pool is not None

        try:
            tag, result = future.result()
        except Exception as x:
            # For arbitrary exceptions, fail fast. Trying the same or the next
            # release may just encounter the same error again.
            _logger.error(
                'task running in worker pool raised unexpected exception', exc_info=x
            )
            return

        if tag == "cancel":
            raise Cancelled(*result)
        elif tag == "platforms":
            update_platforms(result[0])
            raise MissingPlatformError(*result)

        if task.kwargs["task"] == "download":
            pass
        elif task.kwargs["task"] == "distill":
            release = self._update_metadata(result)

            # If distill was scheduled as part of summarize, schedule summarization
            if self._task == "summarize-category":
                self._continuations.append(release)
        elif task.kwargs["task"].startswith("summarize"):
            if result[0] is not None:
                self._update_metadata(result[0])

            assert self._stats is not None
            self._stats.append(result[1])

            # This method executes in coordinator and writes to coordinator's
            # staging, making it safe to update the file.
            self._stats.write(self._storage.staging_root)
        else:
            raise AssertionError(f"invalid task {self._task}")

    def _update_metadata(self, entry: FullMetadataEntry) -> Daily:
        release = entry["release"]
        del entry["release"] # pyright: ignore[reportGeneralTypeIssues]
        self._metadata[release] = entry

        # This method runs in the coordinator and uses the coordinator's
        # staging, making this write safe.
        meta_json = f"{self._coverage.stem()}.json"
        meta_staging = self._storage.staging_root / meta_json
        self._metadata.write_json(meta_staging, sort_keys=True)

        return cast(Daily, release)

    def stop(self) -> None:
        assert self._pool is not None
        self._pool.stop()

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


def run_on_worker(
    task: str,
    dataset: Dataset,
    storage: Storage,
    category: None | str,
    metadata_entry: None | MetadataEntry,
    release: Daily,
    offline: bool,
) -> Any:
    """Run a task in a worker process."""
    # For reasons unbeknownst to man, the process pool executor unpickles all
    # worker exceptions as instances of the same type. To work around this
    # madness, we turn exceptions that require special handling in the
    # coordinator into tagged values. We still raise unexpected, arbitrary
    # exceptions, which trigger the coordinator to fail fast.
    try:
        result = _run_on_worker(
            task,
            dataset,
            storage,
            category,
            metadata_entry,
            release,
            offline,
        )
        _logger.info(
            'returning result for task="%s", release="%s", category="%s", worker=%d',
            task, release, category or "", _PID
        )
        return "value", result
    except Cancelled as x:
        _logger.warning(
            'cancelled task="%s", release="%s", category="%s", worker=%d',
            task, release, category or "", _PID
        )
        return "cancel", x.args
    except MissingPlatformError as x:
        _logger.warning(
            'missing platform names in task="%s", release="%s", category="%s", worker=%d',
            task, release, category or "", _PID
        )
        return "platforms", x.args
    except Exception as x:
        _logger.error(
            'unexpected error in task="%s", release="%s", category="%s", worker=%d',
            task, release, category or "", _PID, exc_info=x
        )
        print(f"unexpected exception thrown by worker with pid={_PID}:")
        traceback.format_exception(x)
        raise

def _run_on_worker(
    task: str,
    dataset: Dataset,
    storage: Storage,
    category: None | str,
    metadata_entry: None | MetadataEntry,
    release: Daily,
    offline: bool,
) -> Any:
    # Check for cancellation
    check_not_cancelled()

    # Create a minimal coverage object necessary for the task
    coverage = Coverage(release, release, category)

    # Create a minimal metadata object necessary for the task
    if task == "download":
        metadata = Metadata()
    elif task == "distill":
        metadata = Metadata(category)
    elif task == "summarize-all":
        metadata = Metadata()
    elif task == "summarize-category":
        assert metadata_entry is not None
        metadata = Metadata(category, {str(release): metadata_entry})
    else:
        raise AssertionError(f"invalid task {task}")

    # Instantiate a processor
    processor = Processor(
        dataset=dataset,
        storage=storage.isolate(_PID),
        coverage=coverage,
        metadata=metadata,
        offline=offline,
        progress=WorkerProgress(),
    )

    # Actually run the task
    _logger.debug(
        'running task="%s", release="%s", category="%s", worker=%d',
        task, release, category or "", _PID
    )
    if task == "download":
        processor.download_archive(release)
        result = None
    if task == "distill":
        processor.distill_category_release(release)
        result = metadata[release] | dict(release=release)
    elif task == "summarize-all":
        collector = Collector()
        processor.summarize_database_release(release, collector)
        result = metadata[release] | dict(release=release), collector.frame()
    elif task == "summarize-category":
        assert metadata_entry is not None
        collector = Collector()
        processor.summarize_category_release(release, metadata_entry, collector)
        result = None, collector.frame()
    else:
        raise AssertionError(f"invalid task {task}")

    return result
