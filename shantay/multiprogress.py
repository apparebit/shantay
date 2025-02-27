from contextlib import AbstractContextManager
import multiprocessing as mp
import shutil
import threading
from typing import Any, Self

from .progress import Progress


class ProgressShim:
    """A shim for the progress class that forwards to a multiprocessing queue."""
    def __init__(self, id: int, queue: mp.SimpleQueue) -> None:
        self._id = id
        self._queue = queue

    def activity(self, description: str, label: str, unit: str, with_rate: bool) -> Self:
        """Describe a new activity."""
        self._queue.put((self._id, "activity", description, label, unit, with_rate))
        return self

    def start(self, total: None | int = None) -> Self:
        """Set the total for the new activity."""
        self._queue.put((self._id, "start", total))
        return self

    def step(self, processed: int, extra: None | str = None) -> Self:
        """Set the steps for the current activity."""
        self._queue.put((self._id, "step", processed, extra))
        return self

    def perform(self, activity: str) -> Self:
        """Update the progress marker with a one-shot activity."""
        self._queue.put((self._id, "perform", activity))
        return self

    def done(self) -> None:
        """Done."""
        self._queue.put((self._id, "perform", ""))
        pass


_VALID_OPS = frozenset([
    "activity",
    "start",
    "step",
    "perform",
])

class ShimManager(AbstractContextManager):

    __slots__ = ("_server", "_slot", "_shim")

    def __init__(self, server: "ProgressServer") -> None:
        self._server = server
        self._slot = None

    def __enter__(self) -> ProgressShim:
        """Allocate a new progress shim."""
        if self._slot is not None:
            raise ValueError("already managing a progress shim")
        if self._server._slots == 0:
            raise ValueError("all progress shims have been allocated")

        all_slots = self._server._slots
        self._slot = (all_slots & -all_slots).bit_length() - 1
        self._server._slots &= ~(1 << self._slot)
        self._shim = ProgressShim(self._slot, self._server._queue)
        return self._shim

    def __exit__(self, *exc_info: object) -> None:
        """
        Deallocate the shim again.

        After this method has been invoked, any progress shim previously
        allocated by this context manager won't function anymore.
        """
        self._shim._id = None
        self._shim._queue = None
        self._shim = None
        self._server._slots |= (1 << self._slot)
        self._slot = None


class ProgressServer:
    """
    A progress server.

    This class maintains `size` progress trackers. They are driven by a worker
    thread that acts on `(index, method, *args)` messages sent through a
    multiprocessing queue. Use the `shim()` method to access a context manager
    that allocates and deallocates a `ProgressShim` for writing such messages
    with the exact same interface as `Progress`.

    To use the shim with a pool:
    ```
    server = ProgressServer(5)
    shim_manager = server.shim()
    pool.apply_async(
        fn,
        (shim_manager.__enter__(),),
        {},
        shim_manager.__exit__,
        shim_manager.__exit__,
    )
    ```
    """
    def __init__(self, size: int, context: Any = None) -> None:
        height = shutil.get_terminal_size()[0]
        if height <= size:
            raise ValueError(f"more progress bars ({size}) than terminal rows ({height})")

        self._keep_running = True
        self._queue = (context or mp.get_context()).SimpleQueue()
        trackers = [Progress(height - size + i) for i in range(size)]

        self._runner = threading.Thread(
            target=ProgressServer.run,
            args=(self._queue, trackers)
        )
        self._runner.daemon = True
        self._runner._keep_running = True
        self._runner.start()

        self._slots = (1 << size) - 1

    def has_shims(self) -> bool:
        """Determine whether any shims are available."""
        return self._slots != 0

    def shim(self) -> ShimManager:
        """Get a shim manager for allocating a shim."""
        return ShimManager(self)

    @staticmethod
    def run(queue: mp.SimpleQueue, trackers: list[Progress]) -> None:
        """Run the worker thread."""
        tracker_count = len(trackers)
        thread = threading.current_thread()
        get = queue.get

        while True:
            try:
                msg = get()
            except (OSError, EOFError):
                break

            if not thread._keep_running:
                break
            id, op, *args = msg
            if not 0 <= id < tracker_count or op not in _VALID_OPS:
                continue

            getattr(trackers[id], op)(*args)

        # Drain queue
        while not queue.empty():
            try:
                get()
            except:
                pass

    def close(self) -> None:
        """Close this progress server."""
        if not self._keep_running:
            return

        self._keep_running = False
        self._runner._keep_running = False
        try:
            self._queue.put(None)
        except:
            pass

    def join(self) -> None:
        """Join this progress server's internal worker thread."""
        self._runner.join()
