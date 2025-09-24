from collections import deque
from collections.abc import Iterator
from concurrent.futures import Future
import logging
import multiprocessing as mp
import os
import signal
import sys
import time
from types import FrameType
from typing import Any, cast

from .logutil import log_max_rss
from .metadata import Metadata
from .model import (
    Config, Daily, DataFrameType, Dataset, FullMetadataEntry, ReleaseRange, Storage
)
from .pool import (
    Cancelled, check_not_cancelled, ErrorTrace, ErrorTraceFactory, Pool, Task,
    WorkerProgress
)
from .processor import is_distilled, prepare_statistics, Processor
from .progress import NO_PROGRESS, Progress
from .schema import MissingPlatformError, update_platforms
from .stats import Statistics


_PID = os.getpid()


_logger = logging.getLogger(__spec__.parent)


class Multiprocessor:

    def __init__(
        self,
        dataset: Dataset,
        storage: Storage,
        coverage: ReleaseRange[Daily],
        config: Config,
        metadata: Metadata,
        # This argument only exists for compatibility with Processor
        progress: Progress = NO_PROGRESS,
    ) -> None:
        self._dataset = dataset
        self._storage = storage
        self._coverage = coverage
        self._config = config
        self._metadata = metadata
        self._pre_existing_stats = None

        self._task = None
        self._iter = None
        self._continuations = deque()

        self._pool = None
        self._register_handlers()
        # Use the same level as the root logger
        self._pool = Pool(size=config.workers, log_level=logging.getLogger().level)

        self._running_time = 0

    @property
    def stem(self) -> str:
        return self._metadata.stem

    @property
    def latency(self) -> float:
        return self._running_time

    def run(self, task: str) -> None | DataFrameType:
        assert self._pool is not None
        self._task = task

        _logger.info('running multiprocessor with pid=%d, task="%s"', _PID, task)
        _logger.info('    key="runtime.offline",      value="%s"', self._config.offline)
        _logger.info('    key="runtime.workers",      value=%d', self._config.workers)
        _logger.info('    key="runtime.progress",     value="%s"', self._config.progress)
        _logger.info('    key="dataset.name",         value="%s"', self._dataset.name)
        _logger.info('    key="storage.archive_root", value="%s"', self._storage.archive_root or "")
        _logger.info('    key="storage.extract_root", value="%s"', self._storage.extract_root or "")
        _logger.info('    key="storage.staging_root", value="%s"', self._storage.staging_root)
        _logger.info('    key="coverage.first",       value="%s"', self._coverage.first.id)
        _logger.info('    key="coverage.last",        value="%s"', self._coverage.last.id)
        _logger.info('    key="coverage.frequency",   value="%s"', self._coverage.frequency)
        _logger.info('    key="coverage.filter",      value="%s"', self._metadata.filter or "")
        _logger.info('    key="stratify.category",    value="%s"', self._config.stratify_by_category)
        _logger.info('    key="stratify.all_text",    value="%s"', self._config.stratify_all_text)
        _logger.info('    key="statistics.stem",      value="%s"', self.stem)

        # See Processor.run() for an explanation for time.time()
        start_time = time.time()

        if task not in ("download", "distill", "summarize-all", "summarize-extract"):
            raise ValueError(f"invalid task {task} for multiprocessing")
        if task.startswith("summarize"):
            self._pre_existing_stats = prepare_statistics(
                self.stem, self._storage, self._config
            )
        self._iter = iter(self._coverage)

        try:
            frame = self._run(task)
        finally:
            # Mark workers' staging roots as used
            for worker in self._pool.workers:
                staging = self._storage.isolate_staging_root(worker)
                if not staging.exists():
                    continue
                staging.rename(staging.with_name(f"{staging.name}.done"))

        self._running_time = time.time() - start_time
        return frame

    def _run(self, task: str) -> None | DataFrameType:
        # Do the work
        assert self._pool is not None
        self._pool.run(self._task_iter(), self._done_with_task)

        if not task.startswith("summarize"):
            return None

        # Put data and metadata into long-term storage
        meta_json = f"{self.stem}.json"
        Metadata.copy_json(
            self._storage.staging_root / meta_json,
            self._storage.best_root / meta_json
        )

        _logger.info(
            'combining per-release statistics file-count=%d, glob="%s", file="%s"',
            self._coverage.duration,
            f"{self.stem}.stats/*.parquet",
            f"{self.stem}.parquet"
        )

        stats = Statistics.read_all(
            self._storage.staging_root / f"{self.stem}.stats"
        )
        stats.write(self._storage.staging_root, should_finalize=True)

        _logger.info(
            'copying summary statistics to persistent root="%s"',
            self._storage.best_root
        )
        Statistics.copy_all(
            self.stem, self._storage.staging_root, self._storage.best_root
        )

        return stats.frame()

    def _task_iter(self) -> Iterator[Task]:
        assert self._pool is not None

        while True:
            if 0 < len(self._continuations):
                release = self._continuations.popleft()
                effective_task = "summarize-extract"
            else:
                release = self._next_release()
                if release is None:
                    break

                if self._task != "summarize-extract":
                    effective_task = self._task
                else:
                    if (
                        release not in self._metadata
                        or not is_distilled(self._storage.the_extract_root, release)
                    ):
                        effective_task = "distill"
                    else:
                        effective_task = "summarize-extract"

            # Create a minimal metadata instance for the worker
            if effective_task == "summarize-extract":
                metadata = self._metadata.with_releases_only(release)
            else:
                metadata = self._metadata.with_releases_only()

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
                    config=self._config,
                    release=release,
                    metadata=metadata,
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
                and is_distilled(self._storage.the_extract_root, release)
            ):
                release = next(self._iter, None)
        elif self._task is not None and self._task.startswith("summarize"):
            assert self._pre_existing_stats is not None
            while release is not None:
                stats_path = (
                    self._storage.staging_root
                    / f"{self.stem}.stats"
                    / f"{release}.parquet"
                )
                if release not in self._pre_existing_stats and not stats_path.exists():
                    break

                _logger.debug('summary statistics already cover release="%s"', release)
                release = next(self._iter, None)

        # Ensure graceful termination in offline mode.
        if self._config.offline and release is not None and not (
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
        tag, result = future.result()

        # Retrying the same or the next release on errors probably will fail
        # again. Hence we fail fast on all error conditions.
        if tag == "cancel":
            raise Cancelled(*result)
        elif tag == "platforms":
            update_platforms(result[0])
            raise MissingPlatformError(*result)
        elif tag == "error":
            if not isinstance(result.__cause__, ErrorTrace):
                print(
                    f"*** Unexpected cause {type(result.__cause__)}: "
                    f"{result.__cause__} ***"
                )
            raise result

        if task.kwargs["task"] == "download":
            pass
        elif task.kwargs["task"] == "distill":
            release = self._update_metadata(result)
            assert task.kwargs["release"] == release

            # If distill was scheduled as part of summarize, schedule summarization
            if self._task == "summarize-extract":
                self._continuations.append(release)
        elif task.kwargs["task"].startswith("summarize"):
            if result[0] is not None:
                release = self._update_metadata(result[0])
                assert task.kwargs["release"] == release

            # The coordinator can safely update its own staging area
            result[1].write(
                self._storage.staging_root / f"{self.stem}.stats",
                should_finalize=True,
            )
        else:
            raise AssertionError(f"invalid task {self._task}")

        log_max_rss(task.kwargs["release"].id)

    def _update_metadata(self, entry: FullMetadataEntry) -> Daily:
        release = entry["release"]
        del entry["release"] # pyright: ignore[reportGeneralTypeIssues]
        self._metadata[release] = entry

        # This method runs in the coordinator and uses the coordinator's
        # staging, making this write safe.
        meta_json = f"{self.stem}.json"
        meta_staging = self._storage.staging_root / meta_json
        self._metadata.write_json(meta_staging)

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
    config: Config,
    release: Daily,
    metadata: Metadata,
) -> Any:
    """
    Run a task in a worker process. The metadata instance should be minimal,
    i.e., comprise only stem, filter, and the entry for the current release (if
    any). This function catches any exceptions raised while processing the given
    task and instead returns a tuple with an `ErrorTraceFactory`.
    """
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
            config,
            release,
            metadata,
        )
        _logger.info(
            'returning result for task="%s", release="%s", filter="%s", worker=%d',
            task, release, metadata.filter or "", _PID
        )
        return "value", result
    except Cancelled as x:
        _logger.warning(
            'cancelled task="%s", release="%s", filter="%s", worker=%d',
            task, release, metadata.filter or "", _PID
        )
        return "cancel", x.args
    except MissingPlatformError as x:
        _logger.warning(
            'missing platform names in task="%s", release="%s", filter="%s", worker=%d',
            task, release, metadata.filter or "", _PID
        )
        return "platforms", x.args
    except Exception as x:
        _logger.error(
            'unexpected error in task="%s", release="%s", filter="%s", worker=%d',
            task, release, metadata.filter or "", _PID, exc_info=x
        )
        # Sleep for a spell so that the coordinator can catch up with logging.
        time.sleep(1)
        return "error", ErrorTraceFactory(x)
    finally:
        log_max_rss(release.id)


def _run_on_worker(
    task: str,
    dataset: Dataset,
    storage: Storage,
    config: Config,
    release: Daily,
    metadata: Metadata,
) -> Any:
    # Check for cancellation
    check_not_cancelled()

    # Create a minimal coverage object necessary for the task
    coverage = ReleaseRange(release, release)

    # Instantiate a processor
    processor = Processor(
        dataset=dataset,
        storage=storage.isolate(_PID),
        coverage=coverage,
        config=config,
        metadata=metadata,
        progress=WorkerProgress(),
    )

    # Actually run the task
    _logger.debug(
        'running task="%s", release="%s", filter="%s", worker=%d',
        task, release, metadata.filter or "", _PID
    )
    if task == "download":
        processor.download_archive(release)
        result = None
    elif task == "distill":
        processor.distill_release(release)
        result = metadata[release] | dict(release=release)
    elif task == "summarize-all":
        stats = processor.summarize_full_release(release)
        result = metadata[release] | dict(release=release), stats
    elif task == "summarize-extract":
        stats = Statistics(
            f"{release}.parquet",
            stratify_by_category=config.stratify_by_category,
            stratify_all_text=config.stratify_all_text,
        )
        processor.summarize_release_extract(release, stats)
        result = None, stats
    else:
        raise AssertionError(f"invalid task {task}")

    return result
