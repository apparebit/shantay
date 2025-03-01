from collections.abc import Iterator
import logging
import multiprocessing as mp
from pathlib import Path
import threading
from typing import Any

from .metadata import MetadataConflict
from .multiprogress import ProgressServer, ShimManager
from .release import DownloadFailed, Release
from .runner import Runner, Task
from .schedule import Schedule


_logger = logging.getLogger("shantay")


class Process:
    def __init__(
        self,
        *,
        schedule: Iterator[Release],
        task: Task,
        runner: Runner,
        shim_manager: ShimManager,
        pool: mp.pool.Pool,
    ) -> None:
        self._event = threading.Event()
        self._release = None
        self._task = task
        self._tries = 0
        self._schedule = schedule
        self._shim_manager = shim_manager
        self._runner = runner
        self._pool = pool

    def staging(self) -> Path:
        return self._runner.staging

    def is_closing(self) -> bool:
        return self._pool is None

    def is_done(self) -> bool:
        return self._runner is None

    def run(self) -> None:
        self._release = release = next(self._schedule)
        if release is None:
            self.close()
        self._tries = 1
        self._pool.apply_async(
            getattr(self._runner, f"{self._task.value}_batches"),
            (release,),
            {},
            self.succeed,
            self.fail,
        )

    def succeed(self, _result: object) -> None:
        if self._pool is None:
            self._runner = None
            self._shim_manager.__exit__()
            self._event.set()
            return

        self.run()

    def fail(self, x: Exception) -> None:
        id = self._release.id
        if isinstance(x, DownloadFailed):
            _logger.error(f'failed to download release="%s", reason="%s"', id, x)
        elif isinstance(x, MetadataConflict):
            _logger.error(f'failed to merge metadata release="%s", reason="%s"', id, x)
        else:
            _logger.error(
                f'failed task="%s", release="%s", reason="%s"',
                self._task, id, x, exc_info=x
            )

        if self._pool is None:
            self._runner = None
            self._shim_manager.__exit__()
            self._event.set()
            return

        if 2 < self._tries:
            _logger.info('giving up retrying task="%s", release="%s"', self._task, id)
            return self.run()

        self._tries += 1
        self._pool.apply_async(
            getattr(self._runner, f"{self._task.value}_batches"),
            (self._release,),
            {},
            self.succeed,
            self.fail,
        )

    def close(self) -> None:
        self._pool = None

    def join(self) -> None:
        self._event.join()


class RunPool:
    def __init__(
        self,
        *,
        size: int,
        archive: Path,
        batches: Path,
        schedule: Schedule,
        task: Task,
        context: Any,
    ) -> None:
        self._pool = pool = mp.pool.Pool(size)
        self._progress_server = progress_server = ProgressServer(size, context)
        self._schedule = schedule
        schedule_it = iter(schedule)
        self._processes = []

        for index in range(size):
            shim_manager = progress_server.shim()
            runner = Runner(
                archive=archive,
                batches=batches,
                progress=shim_manager.__enter__(),
                id=index,
            )
            runner.prepare()
            process = Process(
                schedule=schedule_it,
                task=task,
                runner=runner,
                shim_manager=shim_manager,
                pool=pool,
            )
            self._processes.append(process)
            process.run()

    def processes(self) -> Iterator[Process]:
        for process in self._processes:
            yield process

    def close(self) -> None:
        for process in self._processes:
            process.close()

    def join(self) -> None:
        for process in self._processes:
            process.join()
