from collections.abc import Iterator
import datetime as dt
import polars as pl
from .schedule import YearMonth


_DEBUG = False


class Collector:
    """
    A class to simplify the piecemeal construction of data series and frames.
    """

    def __init__(self) -> None:
        self._timeline = []
        self._series = {}
        self._frames = {}

    def month(self, year_month: YearMonth) -> None:
        """Register year-month for subsequent values() and frames()."""
        # Using mid-month as the date is less bad than the extremes
        self._timeline.append(dt.date(year_month.year, year_month.month, 15))

    def values(self, **kwargs: float) -> None:
        """Register values for named series."""
        for k, v in kwargs.items():
            self._series.setdefault(k, []).append(v)

    def frames(self, **kwargs: pl.DataFrame) -> None:
        """Register partial, named data frames."""
        for k, v in kwargs.items():
            self._frames.setdefault(k, []).append(v)

    def frame_for_values(self) -> pl.DataFrame:
        """Return a data frame comprising individually registered values."""
        if _DEBUG:
            for k, v in self._series.items():
                print(f"{k:>50} :: {len(v)}")
        return pl.DataFrame({
            "date": self._timeline,
            **self._series
        })

    def consume_frames(self) -> Iterator[tuple[str, pl.DataFrame]]:
        """Iterate over data frames after concatenation of partial frames."""
        for k, v in self._frames.items():
            yield k, pl.concat(v, how="vertical")
