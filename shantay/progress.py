from contextlib import AbstractContextManager, nullcontext
import shutil
import time
from typing import Callable

_BLOCKS = " ▎▌▊█"

def _bar(percent: float, color: str = "38;5;69") -> str:
    """
    Format a progress bar for the given percentage. The color is the CSI
    parameter and defaults to a subdued blue.
    """
    percent = max(0, min(100, percent))  # Clamp to 0..=100.0
    full, partial = divmod(round(percent), 4)
    bar = _BLOCKS[-1] * full
    if partial > 0:
        bar += _BLOCKS[partial]
    bar = bar.ljust(25, _BLOCKS[0])
    return f"┫\x1b[{color}m{bar}\x1b[39m┣ {percent:5.1f}%"


def _scale(value: float) -> tuple[float, str]:
    """Scale the value to three digits before the decimal and a unit prefix."""
    if value < 0.001:
        return value * 1_000_000, "micro"
    elif value < 1:
        return value * 1_000, "milli"
    elif value < 1_000:
        return value, ""
    elif value < 1_000_000:
        return value / 1_000, "kilo"
    else:
        return value / 1_000_000, "mega"


_SECOND_NS = 1_000_000_000


class Progress:
    """
    A visual progress tracker.

    This class emits status updates for a single workflow. A status update may
    be a simple textual message or incorporate a progress bar tracking i/n
    steps.

    For the latter, the implementation automatically delays the display of the
    bar for some fraction of a second and adds the percentage of steps completed
    after the bar. It optionally displays the rate of progress as well.

    By default, this class emits all updates on the current line. If it is
    instantiated with the row argument, it uses that row instead.

    The lock argument to the constructor, if provided, controls access to
    standard output.
    """

    def __init__(
        self,
        row: None | int = None,
        timer: None | Callable[[], int] = None,
        lock: None | AbstractContextManager = None,
    ) -> None:
        self._id = None
        self._description = None
        self._activity = None
        self._unit = None
        self._with_rate = None

        self._size = shutil.get_terminal_size()
        self._row = row if row is None else min(row, self._size[1])

        self._timer = timer if timer is not None else time.monotonic_ns
        self._lock = lock if lock else nullcontext()

        self._reset()

    def with_id(self, id: str) -> None:
        self._id = id
        self._reset()

    def _reset(self) -> None:
        self._showing_bar = False
        self._timestamp = None
        self._processed = 0
        self._total = None
        self._samples = 0
        self._rate = 0

    @property
    def id(self) -> str:
        return self._id

    @property
    def prefix(self) -> str:
        if self._row is None:
            return "\x1b[G"
        else:
            return f"\x1b[{self._row};H"

    @property
    def suffix(self) -> str:
        return "\x1b[0K"

    def prep(self, description: str, activity: str, unit: str, with_rate: bool) -> None:
        """Update the configuration of this progress tracker."""
        self._description = description
        self._activity = activity
        self._unit = unit
        self._with_rate = with_rate
        self._reset()

        self.update(description)

    def start(self, total: None | int = None) -> None:
        """Start an activity with total steps."""
        self._timestamp = self._timer()
        self._total = total

    def step(self, processed: int, extra: None | str = None) -> None:
        """Update a previously started activity with processed steps."""
        # Determine whether progress bar should be shown
        timestamp = None
        duration = None
        if not self._showing_bar or self._with_rate:
            timestamp = self._timer()
            duration = (timestamp - self._timestamp) / _SECOND_NS

        if not self._showing_bar:
            if duration < 0.2:
                return
            self._showing_bar = True

        # Update the processing rate
        if self._with_rate and 0.5 < duration:
            rate = (processed - self._processed) / duration
            self._processed = processed
            self._timestamp = timestamp

            self._samples +=1
            self._rate += (rate - self._rate) / self._samples

        # Format progress bar or fallback
        msg = f"{self.prefix}{self._activity} {self._id} "
        columns = len(msg) - 3

        if self._total:
            msg += _bar(processed / self._total * 100)
            columns += 34
        else:
            value, prefix = _scale(processed)
            if value == processed:
                s = f"{processed:,} {self._unit}"
            else:
                s = f"{value:,.1f} {prefix}{self._unit}"
            msg += s
            columns += len(s)

        # Add rate
        if self._with_rate and self._rate != 0:
            value, prefix = _scale(self._rate)
            s = f" at {value:,.1f} {prefix}{self._unit}/s"
            if columns + len(s) < self._size[0]:
                msg += s
                columns += len(s)

        # Add extra
        if extra and columns + 3 + len(extra) < self._size[0]:
            msg += f" • {extra}"

        msg += self.suffix

        # Render progress
        self._render(msg)

    def update(self, activity: str) -> None:
        """Update a one-shot activity."""
        self._render(f"{self.prefix}{activity}{self.suffix}")

    def finish(self) -> None:
        """Finish."""
        if self._row is None:
            self._render("\n")

    def _render(self, text: str) -> None:
        with self._lock:
            print(text, end="", flush=True)
