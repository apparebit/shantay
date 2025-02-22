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
    def __init__(self, id: str, timer: None | Callable[[], int] = None) -> None:
        self._id = id
        self._description = None
        self._activity = None
        self._unit = None
        self._with_rate = None

        self._timer = timer if timer is not None else time.monotonic_ns
        self._columns = shutil.get_terminal_size()[0]

        self._reset()

    def _reset(self) -> None:
        self._showing_bar = False
        self._timestamp = None
        self._processed = 0
        self._total = None
        self._samples = 0
        self._rate = 0

    @property
    def release(self) -> str:
        return self._id

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
        msg = f"\x1b[G{self._activity} {self._id} "
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
            if columns + len(s) < self._columns:
                msg += s
                columns += len(s)

        # Add extra
        if extra and columns + 3 + len(extra) < self._columns:
            msg += f" • {extra}"

        msg += "\x1b[0K"

        # Render progress
        print(msg, end="", flush=True)

    def update(self, activity: str) -> None:
        """Update a one-shot activity."""
        print(f"\x1b[G{activity}\x1b[0K", end="", flush=True)

    def finish(self) -> None:
        """Finish."""
        print()
