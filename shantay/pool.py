"""
A pool of worker processes.

Python's standard library includes two different pools of worker processes,
`multiprocessing.pool.Pool` and `concurrent.futures.ProcessPoolExecutor`. The
former exposes a map-like interface, which is missing the shuffle and reduce
parts of Google's seminal map-shuffle-reduce framework. While the latter offers
a simpler interface based on asynchronous task execution, it too suffers from
the unnecessary overhead of a queue for pending tasks. Since process-based
parallelism is rather heavyweight, incurring overhead for interprocess
communication at a minimum, it is best suited to long-running tasks. However,
neither pool has support for progress updates or task cancellation.

This module's `Pool` addresses these short-comings. Out of pragmatic
considerations, its implementation is based on the
`concurrent.futures.ProcessPoolExecutor`. To provide the extra functionality,
`Pool` injects its own initialization function into new worker processes,
intercepts new tasks as they are scheduled, and executes additional logic when a
future completes.

Notably, if the root logger has no handlers, `Pool` automatically installs a log
handler that forwards log records from workers to the coordinator, which hands
them over to its root logger's handlers.

If the worker uses an instance of `WorkerProgress`, which has the exact same
interface as `Progress`, invocations are automatically forwarded to the
coordinator, which display one progress line per worker.

To cooperatively cancel a worker's task, the coordinator uses a multiprocessing
`SimpleQueue` to signal workers. After receiving the signal, a worker's
`is_cancelled()` returns `True`. In that case, the worker should wind down its
task processing by raising a `Cancelled` exception.
"""
from concurrent.futures import Future, ProcessPoolExecutor
import copy
import logging
import multiprocessing as mp
import os
import shutil
import threading
from typing import Any, Self

from .progress import Progress


_PID = os.getpid()
_PROGRESS = ["activity", "start", "step", "perform"]
_logger = logging.getLogger(__spec__.parent)
_status_queue = None
_terminator = None


# --------------------------------------------------------------------------------------
# The coordinator


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
    ) -> None:
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
        self._index_table = _IndexTable(size)
        self._pending_tasks = 0

        self._status_manager = threading.Thread(
            target=_manage_status,
            args=(self._status_queue, self._trackers, self._index_table),
            daemon=True,
        )
        self._status_manager.start()

        self._executor = ProcessPoolExecutor(
            max_workers=size,
            mp_context = context,
            initializer=_initialize_worker,
            initargs=(self._status_queue, self._cancel_queue, log_level),
        )

        self._done = threading.Event()

    @property
    def size(self) -> int:
        return self._size

    def is_running(self) -> bool:
        """Determine whether this pool is running, hence accepting tasks."""
        return self._state.is_running()

    def is_stopping(self) -> bool:
        """Determine whether this pool is stopping."""
        return self._state.is_stopping()

    def submit(self, fn, /, *args, **kwargs) -> Future:
        """Submit a new task."""
        assert self._state.is_running(), "pool is not accepting new tasks"

        _logger.debug(
            'submit fn=%s.%s, pool=0x%x, pending_tasks=%d',
            fn.__module__,
            fn.__qualname__,
            id(self),
            self._pending_tasks,
        )

        future = self._executor.submit(fn, *args, **kwargs)
        future.add_done_callback(self._on_task_completion)
        self._pending_tasks += 1
        self._index_table.sync()
        return future

    def _on_task_completion(self, future: Future) -> None:
        self._pending_tasks -= 1
        if self._pending_tasks == 0 and not self._state.is_running():
            _logger.debug('shut down pool=0x%x, cause="task completion"', id(self))
            self._shutdown()

    def finish(self) -> None:
        """
        Run already accepted tasks but reject new ones, shutting down upon
        completion.
        """
        if self._state.set_finishing() and self._pending_tasks == 0:
            _logger.debug('shut down pool=0x%x, cause="finish"', id(self))
            self._shutdown()

    def stop(self) -> bool:
        """
        Cancel running tasks, shutting down pool upon completion. This method
        returns `True` if it initiated shut down and `False` if it was already
        shutting down.
        """
        if not self._state.set_stopping():
            return False

        if self._pending_tasks == 0:
            _logger.debug('shut down pool=0x%x, cause="stop"', id(self))
            self._shutdown()
            return True

        _logger.debug("cancel workers of pool=0x%x", id(self))
        for _ in range(self._size):
            try:
                self._cancel_queue.put(None)
            except BaseException as x:
                _logger.error('failed writing to pool=0x%x queue="cancel"', id(self), exc_info=x)
                break

        return True

    def _shutdown(self) -> None:
        """
        Complete shutdown of this pool. This method releases the resources
        consumed by this pool and wakes any threads waiting for completion.
        """
        # Shut down executor. Do not wait to avoid exception being thrown.
        self._executor.shutdown(False)

        # With all workers gone, there won't be any status updates anymore.
        try:
            self._status_queue.put(None)
        except BaseException as x:
            _logger.error('failed to write to queue="status"', exc_info=x)

        if self._status_manager != threading.current_thread():
            self._status_manager.join()

        self._done.set()

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


class _IndexTable:
    """
    A table mapping process IDs to indexes between 0 and some maximum size.
    """

    def __init__(self, size: int) -> None:
        self._lock = threading.Lock()
        self._table = {}
        self._slots = (1 << size) - 1
        self._size = size  # do not change

    @property
    def size(self) -> int:
        return self._size

    def sync(self) -> None:
        """
        Synchronize this table with the list of known subprocesses. This method
        removes any entry with a process ID that is not a child process. It does
        *not* add any mappings.
        """
        with self._lock:
            active = frozenset((p.pid for p in mp.active_children()))
            # Copy the keys into a list since we may update the table.
            for pid in list(self._table.keys()):
                if pid not in active:
                    self._deallocate(pid)

    def setdefault(self, pid: int) -> int:
        """
        Look up the process ID. If the table does not already contain a mapping,
        this method adds one using the next available index.
        """
        with self._lock:
            return self._table[pid] if pid in self._table else self._allocate(pid)

    def __contains__(self, pid: int) -> bool:
        """Determine whether the PID is included in the table."""
        with self._lock:
            return pid in self._table

    def __getitem__(self, pid: int) -> int:
        """Look up the ID's index."""
        with self._lock:
            return self._table[pid]

    def __delitem__(self, pid: int) -> None:
        """Delete an ID from this table, making the index available again."""
        with self._lock:
            self._deallocate(pid)

    def _allocate(self, pid: int) -> int:
        slots = self._slots
        assert slots != 0, "no index slot available"

        index = (slots & -slots).bit_length() - 1
        self._slots &= ~(1 << index)
        self._table[pid] = index
        return index

    def _deallocate(self, pid: int) -> int:
        index = self._table[pid]
        del self._table[pid]
        self._slots |= (1 << index)
        return index


def _manage_status(
    status_queue: mp.SimpleQueue,
    trackers: list[Progress],
    index_table: _IndexTable,
) -> None:
    while True:
        try:
            message = status_queue.get()
        except BaseException as x:
            _logger.error('failed to read from queue="status"', exc_info=x)
            break
        if message is None:
            _logger.debug('cancelled thread="status_manager"')
            break

        pid, cmd, *args = message
        if cmd == "log":
            for handler in logging.getLogger().handlers:
                handler.handle(args[0])
            continue
        if cmd in _PROGRESS:
            getattr(trackers[index_table.setdefault(pid)], cmd)(*args)
            continue

        _logger.error('invalid command="%s", worker=%d', cmd, pid)


# ======================================================================================


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
    """Signal for a cancelled task execution."""


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

    def perform(self, activity: str) -> Self:
        """Update the progress marker with a one-shot activity."""
        _send_status_update("perform", activity)
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


def _initialize_worker(
    status_queue: mp.SimpleQueue,
    cancel_queue: mp.SimpleQueue,
    log_level: int,
) -> None:
    global _status_queue, _terminator
    status_queue._reader.close() # pyright: ignore[reportAttributeAccessIssue]
    _status_queue = status_queue

    logger = logging.getLogger()
    if len(logger.handlers) == 0:
        logger.addHandler(WorkerLogHandler())
        logger.setLevel(log_level)

    _terminator = threading.Thread(
        target=_wait_for_cancellation,
        args=(cancel_queue,),
        daemon=True,
    )
    _terminator.start()


def _wait_for_cancellation(signal: mp.SimpleQueue) -> None:
    try:
        signal.get()
    except BaseException as x:
        _logger.error('failed reading from queue="cancel", worker=%d', _PID, exc_info=x)
    else:
        _logger.debug("cancelled worker=%d", _PID)
        _is_cancelled.set()


def _send_status_update(cmd: str, *args: Any) -> bool:
    if _status_queue is None:
        return False
    try:
        _status_queue.put((_PID, cmd, *args))
        return True
    except BaseException as x:
        _logger.error('failed writing to queue="status", worker=%d', _PID, exc_info=x)
        return False
