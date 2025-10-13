"""
A pool of worker processes.

Python's standard library includes two different pools of worker processes,
`multiprocessing.pool.Pool` and `concurrent.futures.ProcessPoolExecutor`.

The former exposes a map-like interface that is missing support for the shuffle
and reduce phases from Google's seminal map-shuffle-reduce framework. The latter
exposes a simpler interface based on asynchronous task execution. However, it
too suffers from overdesign, incorporating a queue of pending tasks instead of
just using an iterator to pull tasks on demand.

Since process-based parallelism is rather heavyweight, incurring overhead for
interprocess communication at a minimum, it is best suited to long-running
tasks. However, neither pool has any support for progress updates or task
cancellation.

This module's `Pool` addresses these short-comings. Out of pragmatic
considerations, its implementation is based on the
`concurrent.futures.ProcessPoolExecutor`. To provide the extra functionality,
`Pool` injects its own initialization function into new worker processes and
takes full control over the run loop, with code using `Pool` providing an
iterator over tasks and a callback for task completion.

As long as the root logger has no handlers, `Pool` automatically installs a log
handler to forward log records from workers to the coordinator, which hands them
over to its own root logger's handlers.

If a task uses an instance of `WorkerProgress`, which has the exact same
interface as `Progress`, invocations are automatically forwarded to the
coordinator, which displays one progress line per worker.

After a call to `finish()`, the pool will not schedule any new tasks, even if
they are available. After a call to `stop()`, it tries to cooperatively cancel
workers' tasks by communicating the signal to workers. Thereafter, a worker's
`is_cancelled()` returns `True` and a task should wind down by raising a
`Cancelled` exception.
"""
from collections.abc import Iterable, Iterator, Mapping, Sequence
from concurrent.futures import FIRST_COMPLETED, ProcessPoolExecutor, wait
import copy
from dataclasses import dataclass
import io
import itertools
import logging
import multiprocessing as mp
import os
import shutil
import sys
import threading
import traceback
from types import TracebackType
from typing import Any, Callable, Self
import uuid

from .progress import Progress
from .util import IndexTable


# In experiments with a pool of two workers summarizing the full database
# starting with 2023-09-25, latency starts at 8s, jumps to 10s, declines to
# almost 8s again, dips sharply to 2s around 2024-02-27, stays at 10s, and then
# starts growing slowly after 2024-06-01 to about 12s in 2024-09-01. The period
# from 2023-09-25 to 2024-06-01 corresponds to 250 days/releases/tasks or 125
# tasks per worker.

_logger = logging.getLogger(__name__)
_PID = os.getpid()
_PROGRESS = ["activity", "start", "step", "perform"]


# ======================================================================================
# Data Structures to Transfer Tasks and Results Between Coordinator and Workers


@dataclass(frozen=True, slots=True)
class Result:
    """
    A container for the result of a task execution. Since
    """

    value: Any
    exception: None | BaseException

    @classmethod
    def from_value(cls, value: Any) -> Self:
        """Create a result with the given value while also marking task
        completion."""
        return cls(value, None)

    @classmethod
    def from_exception(cls, exception: BaseException) -> Self:
        """Create a result with the given exception while also marking task
        completion."""
        return cls(None, WorkerError(exception))

    def is_value(self) -> bool:
        """Determine whether this result is a value."""
        return self.exception is None

    def to_inner(self) -> Any:
        """Get this result's value or raise its exception."""
        if self.exception is not None:
            raise self.exception
        return self.value


@dataclass(frozen=True, slots=True)
class Task:
    """A container combining a function with its arguments."""

    fn: Callable[..., Any]
    args: Sequence[Any]
    kwargs: Mapping[str, Any]

    @property
    def name(self) -> str:
        return f"{self.fn.__module__}.{self.fn.__qualname__}"

    def __repr__(self) -> str:
        buffer = io.StringIO()
        buffer.write(self.fn.__module__)
        buffer.write(".")
        buffer.write(self.fn.__qualname__)
        buffer.write("(")
        for index, arg in enumerate(self.args):
            if 0 < index:
                buffer.write(", ")
            buffer.write(repr(arg))
        for index, (name, arg) in enumerate(self.kwargs.items()):
            if (index == 0 and 0 < len(self.args)) or 0 < index:
                buffer.write(", ")
            buffer.write(name)
            buffer.write("=")
            buffer.write(repr(arg))
        buffer.write(")")
        return buffer.getvalue()


# ======================================================================================
# Code to Run in Coordinator


class Pool:
    """
    A pool of worker processes.

    The implementation wraps a `concurrent.futures.ProcessPoolExecutor`, while
    also adding support for cooperative cancellation of executing tasks,
    per-task progress tracking, and automatic forwarding of log records.
    Unlike the underlying worker pool, this pool can either finish, i.e., run
    accepted tasks to completion, or shut down, i.e., cancel running tasks.
    """

    def __init__(
        self,
        *,
        size: None | int = None,
        context: None | Any = None,
        log_level: int = logging.WARNING,
        max_tasks: None | int = None,
    ) -> None:
        self._id = uuid.uuid1().hex

        if size is None:
            # First available in Python 3.13
            size = getattr(os, "process_cpu_count", lambda:None)()
            if size is None:
                # Previously sanctioned method, which is not available on macOS
                affinity = getattr(os, "sched_getaffinity", None)
                if affinity is not None:
                    size = len(affinity)
            if size is None:
                # Inexact fallback
                size = os.cpu_count() or 1
            if 1 < size:
                # Leave one CPU for scheduler and desktop
                size -= 1
        self._size = size

        if context is None:
            context = mp.get_context("spawn")
        self._context = context

        self._state = _PoolState()

        self._status_queue = context.SimpleQueue()
        self._cancel_queue = context.SimpleQueue()

        _, height = shutil.get_terminal_size()
        self._trackers = [Progress(row=height - i) for i in range(size)]
        self._index_table = _PoolIndexTable(size)

        self._status_manager = threading.Thread(
            target=_manage_status,
            args=(self._status_queue, self._trackers, self._index_table, self.id),
            daemon=True,
        )
        self._status_manager.start()

        self._executor = ProcessPoolExecutor(
            max_workers=size,
            mp_context = context,
            initializer=_initialize_worker,
            initargs=(
                self._status_queue,
                self._cancel_queue,
                log_level,
                self.id,
                max_tasks,
            ),
            max_tasks_per_child=max_tasks,
        )

        self._done = threading.Event()

    @property
    def id(self) -> str:
        return self._id

    @property
    def size(self) -> int:
        return self._size

    @property
    def all_workers(self) -> Iterable[int]:
        return self._index_table.all_workers

    def is_running(self) -> bool:
        """Determine whether this pool is running, hence accepting tasks."""
        return self._state.is_running()

    def is_stopping(self) -> bool:
        """Determine whether this pool is stopping."""
        return self._state.is_stopping()

    def __enter__(self) -> Self:
        self._executor.__enter__()
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> None | bool:
        """
        Beware: This method internally invokes
        `ProcessPoolExecutor.shutdown(True)`, which cleans up the executor's
        resources by, amongst other things, joining the future callback thread.
        Hence, this method must not be called from the future callback thread.
        It is, however, safe to call from the main thread.
        """
        # See doc comment above.
        self._executor.shutdown(True)

        # With all workers gone, there won't be any status updates anymore.
        try:
            self._status_queue.put(None)
        except BaseException as x:
            _logger.error(
                'failed to write to queue="status", pool="%s"', self._id, exc_info=x
            )

        if self._status_manager != threading.current_thread():
            self._status_manager.join()

        self._done.set()

    def run(
        self,
        tasks: Iterator[Task],
        on_completion: Callable[[Task, Result], None],
    ) -> None:
        """Run this pool."""
        # Loosely based on https://github.com/alexwlchan/concurrently
        _logger.debug('start processing tasks in pool="%s"', self._id)
        with self:
            futures = {}
            for task in itertools.islice(tasks, self._size):
                _logger.debug('submit task="%s", pool="%s"', task.name, self._id)
                fut = self._executor.submit(_run_task, task)
                futures[fut] = task

            while futures:
                done, _ = wait(futures, return_when=FIRST_COMPLETED)

                for fut in done:
                    task = futures.pop(fut)

                    # Fail fast on unhandled exceptions indicating a pool bug
                    if (exception := fut.exception()) is not None:
                        _logger.error(
                            'unhandled exception in task="%s", pool="%s"',
                            task.name, self.id, exc_info=exception
                        )
                        self.stop()
                        raise exception

                    # Fail fast on unexpected exceptions indicating a task bug
                    result: Result = fut.result()

                    if result.exception is not None:
                        _logger.error(
                            'unexpected exception in task="%s", pool="%s"',
                            task.name, self.id, exc_info=result.exception
                        )
                        self.stop()
                        raise result.exception

                    try:
                        on_completion(task, result)
                    except:
                        self.stop()
                        raise

                self._index_table.sync()

                if self._state.is_running():
                    for task in itertools.islice(tasks, len(done)):
                        _logger.debug(
                            'submit task="%s", pool="%s"',
                            task.name, self._id
                        )
                        fut = self._executor.submit(_run_task, task)
                        futures[fut] = task

            _logger.debug('done processing tasks in pool="%s"', self._id)

    def finish(self) -> None:
        """
        Run already accepted tasks but reject new ones, shutting down upon
        completion.
        """
        if self._state.set_finishing():
            _logger.debug('finishing pool="%s"', self._id)

    def stop(self) -> None:
        """
        Cancel running tasks, shutting down pool upon completion. This method
        returns `True` if it initiated shut down and `False` if it was already
        shutting down.
        """
        if not self._state.set_stopping():
            return

        _logger.debug('stopping pool="%s"', self._id)
        for _ in range(self._size):
            try:
                self._cancel_queue.put(None)
            except BaseException as x:
                _logger.error(
                    'failed to write to queue="cancel", pool="%s"', self._id, exc_info=x
                )
                break

    def wait(self, timeout: None | float = None) -> None:
        """Wait for all of this pool's workers to be done."""
        self._done.wait(timeout)


class _PoolState:
    """
    A process pool is either running or in one of three states of shutting down.
    In the finishing state, the pool does not accept new tasks but allows
    current tasks to run to completion. In the stopping state, the pool does not
    accept new tasks and uses the cooperative cancel protocol to stop current
    tasks early. In the terminating state, the pool does not accept new tasks
    and just terminates worker processes. Since termination is synchronous and,
    ahem, terminal, the terminating state is not reified by this class.
    """
    RUNNING = 1
    FINISHING = 2
    STOPPING = 3

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._state = self.RUNNING

    def is_running(self) -> bool:
        return self._is(self.RUNNING)

    def is_finishing(self) -> bool:
        return self._is(self.FINISHING)

    def is_stopping(self) -> bool:
        return self._is(self.STOPPING)

    def _is(self, state) -> bool:
        with self._lock:
            return self._state == state

    def set_finishing(self) -> bool:
        return self._set(self.FINISHING, self.RUNNING)

    def set_stopping(self) -> bool:
        return self._set(self.STOPPING, self.RUNNING, self.FINISHING)

    def _set(self, new_state: int, *old_states: int) -> bool:
        with self._lock:
            if self._state not in old_states:
                return False
            self._state = new_state
            return True


class _PoolIndexTable:
    """
    A table mapping process IDs to indexes between 0 and some maximum size.
    """
    # The implementation actually maintains a table of exactly double the
    # maximum size, i.e., allows for two process IDs to share the same index.
    # That ensures index availability even during times of transition from one
    # worker to another, which is not an instantaneous event but subject to
    # delays etc. This does assume, however, that two processes overlap only for
    # a duration that is significantly smaller than the usual lifetime of a
    # worker.
    def __init__(self, capacity: int) -> None:
        self._lock = threading.Lock()
        self._table = IndexTable(capacity * 2)
        self._all_workers = []
        self._capacity = capacity

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def all_workers(self) -> Iterable[int]:
        return self._all_workers

    def sync(self) -> int:
        """
        Synchronize this table with the list of known subprocesses. This method
        removes any entry with a process ID that is not a child process. It does
        *not* add any mappings.
        """
        count = 0
        with self._lock:
            active = frozenset((p.pid for p in mp.active_children()))
            # Copy the keys into a list since we may update the table.
            for pid in list(self._table.keys()):
                if pid not in active:
                    del self._table[pid]
                    count += 1
        return count

    def __contains__(self, pid: int) -> bool:
        """Determine whether the PID is included in the table."""
        with self._lock:
            return pid in self._table

    def __getitem__(self, pid: int) -> int:
        """Look up the ID's index."""
        with self._lock:
            is_new_mapping = pid not in self._table
            index = self._table[pid] % self._capacity
            if is_new_mapping:
                self._all_workers.append(pid)
            return index

    def __delitem__(self, pid: int) -> None:
        """Delete an ID from this table, making the index available again."""
        with self._lock:
            if pid in self._table:
                del self._table[pid]


def _manage_status(
    status_queue: mp.SimpleQueue,
    trackers: list[Progress],
    index_table: _PoolIndexTable,
    pool_id: str,
) -> None:
    while True:
        try:
            message = status_queue.get()
        except BaseException as x:
            _logger.error('failed to read from queue="status"', exc_info=x)
            break
        if message is None:
            _logger.debug('received command="finish" thread="status_manager"')
            break

        pid, cmd, *args = message
        if cmd == "retire":
            _logger.info('retiring worker pid=%d, pool="%s"', pid, pool_id)
            del index_table[pid]
            continue
        if cmd == "log":
            for handler in logging.getLogger().handlers:
                handler.handle(args[0])
            continue
        if cmd in _PROGRESS:
            getattr(trackers[index_table[pid]], cmd)(*args)
            continue

        _logger.error('received invalid command="%s", worker=%d', cmd, pid)


# ======================================================================================
# Code to Run in Worker


_is_cancelled = threading.Event()


def check_not_cancelled() -> None:
    """
    Check that the worker has not been cancelled. Raise `Cancelled`
    otherwise.
    """
    if _is_cancelled.is_set():
        raise Cancelled()


def is_cancelled() -> bool:
    """Determine whether this worker process has been cancelled."""
    return _is_cancelled.is_set()


class Cancelled(Exception):
    """
    Signal for a cancelled task execution. The exception's *three* `args`
    usually are automatically filled in, comprising a helpful error message, the
    native thread ID, and the process ID of the cancelled thread/process.
    However, they may also be explicitly passed to the constructor to recreate
    an exception. They also are conveniently accessible through dedicated
    properties.
    """
    def __init__(self, *args) -> None:
        count = len(args)
        match count:
            case 0:
                tid = threading.get_native_id()
                pid = os.getpid()
                super().__init__(f"thread {tid}, process {pid} was cancelled", tid, pid)
            case 3:
                super().__init__(*args)
            case _:
                raise ValueError(
                    f"Cancelled() takes either 0 or 3 arguments not {len(args)}"
                )

    @property
    def msg(self) -> str:
        """Get a descriptive message for the cancelled task."""
        return self.args[0]

    @property
    def tid(self) -> int:
        """Get the native thread ID of the cancelled task."""
        return self.args[1]

    @property
    def pid(self) -> int:
        """Get the process ID of the cancelled task."""
        return self.args[2]


# --------------------------------------------------------------------------------------


class ErrorTrace(Exception):
    """
    An exception wrapping a textual stack trace akin to `concurrent.futures`'
    private `_RemoteTraceback`.
    """
    def __init__(self, trace: str) -> None:
        self.trace = trace

    def __str__(self):
        return self.trace


def with_error_trace[E: BaseException](exc: E, trace: str) -> E:
    """Decorate the given exception with the given error trace."""
    exc.__cause__ = ErrorTrace(trace)
    return exc


class WorkerError(Exception):
    """
    A worker error.

    Each instance of this class wraps another exception. Furthermore, while the
    stack trace of regular exceptions does not survive pickling and hence does
    not survive transmission across process boundaries, this class eagerly
    captures the stack trace during instantiation and preserves that trace when
    being pickled. Upon unpickling, a worker error becomes an instance of the
    wrapped exception, but with an error trace as cause.

    This class is the equivalent of `concurrent.futures`' private
    `_ExceptionWithTraceback`, except `WorkerError` is an exception whereas
    `_ExceptionWithTraceback` is not.
    """
    def __init__(self, exc: BaseException) -> None:
        trace = "".join(traceback.format_exception(exc))
        self.exc = exc
        self.exc.__traceback__ = None
        self.trace = f'\n"""\n{trace}"""'

    def __reduce__(self) -> Any:
        return with_error_trace, (self.exc, self.trace)


# --------------------------------------------------------------------------------------


class WorkerProgress(Progress):
    """Tracking the progress of a worker process."""

    def __init__(self) -> None:
        # Don't call super. We don't need any of the original machinery.
        pass

    def activity(self, description: str, label: str, unit: str, with_rate: bool) -> Self:
        """Describe a new activity."""
        _send_status_update("activity", description, label, unit, with_rate)
        return self

    def start(self, total: None | int = None) -> Self:
        """Set the total for the new activity."""
        _send_status_update("start", total)
        return self

    def step(self, processed: int, extra: None | str = None) -> Self:
        """Set the steps for the current activity."""
        _send_status_update("step", processed, extra)
        return self

    def perform(self, description: str) -> Self:
        """Update the progress marker with a one-shot activity."""
        _send_status_update("perform", description)
        return self

    def done(self) -> None:
        """Do nothing."""
        pass


class WorkerLogHandler(logging.Handler):
    """A handler to forward worker process log records to the coordinator."""

    def emit(self, record: logging.LogRecord) -> None:
        """Emit the log record"""
        _send_status_update("log", self.prepare(record))

    def prepare(self, record: logging.LogRecord) -> logging.LogRecord:
        """Prepare the log record."""
        msg = self.format(record)
        # bpo-35726: make copy of record to avoid affecting other handlers in the chain.
        record = copy.copy(record)
        record.message = msg
        record.msg = msg
        record.args = None
        record.exc_info = None
        record.exc_text = None
        record.stack_info = None
        return record


# --------------------------------------------------------------------------------------


def _has_console_handler(logger: logging.Logger) -> bool:
    """Determine whether the logger has a handler printing to the console."""
    for handler in logger.handlers:
        if not isinstance(handler, logging.StreamHandler):
            continue
        if handler.stream in (sys.stdout, sys.stderr):
            return True
    return False


# The five globals are only used within worker processes, which are tied to a
# pool instance. Hence, a process may instantiate more than one Pool.
_status_queue = None
_terminator = None
_max_task_run_count = None
_task_run_count = 0
class _worker:
    logger = _logger


def _initialize_worker(
    status_queue: mp.SimpleQueue,
    cancel_queue: mp.SimpleQueue,
    log_level: int,
    pool_id: str,
    max_tasks: None | int,
) -> None:
    global _max_task_run_count, _status_queue, _terminator
    _max_task_run_count = max_tasks
    status_queue._reader.close() # pyright: ignore[reportAttributeAccessIssue]
    _status_queue = status_queue

    # If root logger has no handler, add one that forwards to coordinator
    logger = logging.getLogger()
    if len(logger.handlers) == 0:
        logger.addHandler(WorkerLogHandler())
        logger.setLevel(log_level)

    # While debugging pool activity, be sure to print log entries to console
    if "pool" in os.getenv("DEBUG_SHANTAY", ""):
        if not _has_console_handler(logger):
            logger.addHandler(logging.StreamHandler())
        logger.setLevel(logging.DEBUG)

    # Set up thread waiting for cancel signal
    _terminator = threading.Thread(
        target=_wait_for_cancellation,
        args=(cancel_queue,),
        daemon=True,
    )
    _terminator.start()

    # Log under module name
    _worker.logger = logging.getLogger(__name__)
    _worker.logger.info(
        'initialized worker process pid=%d, max_tasks=%s, pool="%s"',
        _PID, '""' if max_tasks is None else f"{max_tasks}", pool_id
    )


def _wait_for_cancellation(signal: mp.SimpleQueue) -> None:
    try:
        signal.get()
    except BaseException as x:
        _worker.logger.error(
            'failed reading from queue="cancel", worker=%d', _PID, exc_info=x
        )
    else:
        _worker.logger.info("cancellation signal received by worker=%d", _PID)
        _is_cancelled.set()


def _send_status_update(cmd: str, *args: Any) -> bool:
    try:
        assert _status_queue is not None
        _status_queue.put((_PID, cmd, *args))
        return True
    except BaseException as x:
        _worker.logger.error(
            'failed writing to queue="status", worker=%d', _PID, exc_info=x
        )
        return False


def _run_task(task: Task) -> Result:
    global _task_run_count

    try:
        return Result.from_value(task.fn(*task.args, **task.kwargs))
    except BaseException as x:
        return Result.from_exception(x)
    finally:
        if _max_task_run_count is not None:
            _task_run_count += 1
            if _max_task_run_count <= _task_run_count:
                _send_status_update("retire")
