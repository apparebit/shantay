from collections.abc import Iterator
import datetime as dt
import polars as pl


_DEBUG = False


class Collector:
    """
    A class to simplify the piecemeal construction of data frames.
    """

    def __init__(self) -> None:
        self._all = {}
        self._current = None

    def release(self, release: object) -> None:
        """Register the release for subsequent value and frame registrations."""
        # Using mid-month as the date is less bad than the extremes
        self._current = self._all.setdefault(release, {})

    def frames(self, **kwargs: pl.DataFrame) -> None:
        """Register partial, named data frames."""
        assert isinstance(self._current, dict)
        self._current |= kwargs

    def consume_frames(self) -> Iterator[tuple[str, pl.DataFrame]]:
        """Iterate over data frames after concatenation of partial frames."""
        frames = {}
        for release in self._all.values():
            for k, v in release.items():
                frames.setdefault(k, []).append(v)

        for k, v in frames.items():
            yield k, pl.concat(v, how="vertical")
