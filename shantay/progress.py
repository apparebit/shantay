from collections.abc import Iterator
from contextlib import contextmanager
import shutil
import time
from typing import Callable, Self

from .util import scale


_BLOCKS = " ▎▌▊█"
_SECOND_NS = 1_000_000_000


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
    """

    def __init__(
        self,
        row: None | int = None,
        timer: None | Callable[[], int] = None,
    ) -> None:
        self._size = shutil.get_terminal_size()
        self._row = row if row is None else min(row, self._size[1])

        self._timer = timer if timer is not None else time.monotonic_ns

        self._reset_activity()
        self._reset_stats()

    def _reset_activity(self) -> None:
        self._label = self._unit = self._with_rate = None

    def _reset_stats(self) -> None:
        self._showing_bar = False
        self._timestamp = self._timer()
        self._processed = 0
        self._total = None
        self._rate = None

    @property
    def _prefix(self) -> str:
        if self._row is None:
            return "\x1b[G"
        else:
            return f"\x1b[{self._row};H"

    @property
    def _suffix(self) -> str:
        return "\x1b[0K"

    @contextmanager
    def nested(self) -> Iterator[Self]:
        """
        Pause the current activity, perform another activity afresh, and then
        resume the current activity.
        """
        saved_label = self._label
        saved_unit = self._unit
        saved_with_rate = self._with_rate
        saved_showing_bar = self._showing_bar
        saved_timestamp = self._timestamp
        saved_processed = self._processed
        saved_total = self._total
        saved_rate = self._rate

        self._reset_activity()
        self._reset_stats()

        try:
            yield self
        finally:
            self._label = saved_label
            self._unit = saved_unit
            self._with_rate = saved_with_rate
            self._showing_bar = saved_showing_bar
            self._timestamp = saved_timestamp
            self._processed = saved_processed
            self._total = saved_total
            self._rate = saved_rate

    def activity(self, description: str, label: str, unit: str, with_rate: bool) -> Self:
        """Update the configuration of this progress tracker."""
        self._label = label
        self._unit = unit
        self._with_rate = with_rate
        self._reset_stats()

        self._render(f"{self._prefix}{description}{self._suffix}")
        return self

    def start(self, total: None | int = None) -> Self:
        """Start an activity with total steps."""
        assert self._label is not None, "Progress.activity() must precede Progress.start()"

        self._timestamp = self._timer()
        self._total = total
        return self

    def step(self, processed: int, extra: None | str = None) -> Self:
        """Update a previously started activity with processed steps."""
        assert self._label is not None, "Progress.start() must precede Progress.step()"

        # Handle timings: Should we update screen? What's the processing rate?
        if not self._showing_bar or self._with_rate:
            timestamp = self._timer()
            duration = (timestamp - self._timestamp) / _SECOND_NS

            # Only show progress bar after some delay
            if not self._showing_bar:
                if duration < 0.2:
                    return self
                self._showing_bar = True

            # Update the processing rate
            elif 0.5 < duration:
                rate = (processed - self._processed) / duration
                self._processed = processed
                self._timestamp = timestamp
                self._rate = (
                    rate if self._rate is None else 0.7 * rate + 0.3 * self._rate
                )

        # Format progress bar or fallback
        msg = f"{self._prefix}{self._label} "
        columns = len(msg) - 3

        if self._total:
            msg += _bar(processed / self._total * 100)
            columns += 34
        else:
            value, prefix = scale(processed)
            if value == processed:
                s = f"{processed:,} {self._unit}"
            else:
                s = f"{value:,.1f} {prefix}{self._unit}"
            msg += s
            columns += len(s)

        # Add rate
        if self._with_rate and self._rate is not None:
            value, prefix = scale(self._rate)
            s = f" at {value:,.1f} {prefix}{self._unit}/s"
            if columns + len(s) < self._size[0]:
                msg += s
                columns += len(s)

        # Add extra
        if extra and columns + 3 + len(extra) < self._size[0]:
            msg += f" • {extra}"

        msg += self._suffix

        # Render progress
        self._render(msg)
        return self

    def perform(self, description: str) -> Self:
        """Update a one-shot activity."""
        if self._label is not None:
            self._reset_activity()
        self._render(f"{self._prefix}{description}{self._suffix}")
        return self

    def done(self) -> None:
        """Finish."""
        self._reset_activity()
        if self._row is None:
            self._render("\n")

    def _render(self, text: str) -> None:
        print(text, end="", flush=True)


class _NoProgress(Progress):

    @contextmanager
    def nested(self) -> Iterator[Self]:
        yield self

    def activity(self, *args, **kwargs) -> Self:
        return self

    def start(self, total: None | int = None) -> Self:
        return self

    def step(self, processed: int, extra: None | str = None) -> Self:
        return self

    def perform(self, description: str) -> Self:
        return self

    def error(self, msg: str) -> Self:
        print(f"ERROR: {msg}")
        return self

    def done(self) -> None:
        pass


NO_PROGRESS = _NoProgress()
"""An object compatible with `Progress` that does nothing."""
del _NoProgress
