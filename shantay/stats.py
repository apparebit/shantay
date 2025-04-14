import datetime as dt
from pathlib import Path
import shutil
from typing import Any, ClassVar, Self

import polars as pl

from .framing import (
    aggregates, Collector, formatted_summary, groupies, validate_row_counts
)
from .model import DateRange, Release
from .schema import STATISTICS_SCHEMA


def _range_of(frame: pl.DataFrame) -> DateRange:
    return DateRange(*frame.select(
        pl.col("start_date").min(),
        pl.col("end_date").max(),
    ).row(0))


class Statistics:
    """
    Wrapper around statistics describing the DSA transparency database.
    Conceptually, the descriptive statistics form a single data frame. However,
    to support incremental collection of those statistics, this class may
    temporarily wrap more than one data frame, lazily materializing a single
    frame only on demand.
    """
    FILE: ClassVar[str] = "statistics.parquet"

    def __init__(self, *frames: pl.DataFrame) -> None:
        self._frames = list(frames)
        self._collector = None

    @classmethod
    def from_storage(cls, staging: Path, persistent: Path) -> Self:
        """
        Pick the more complete statistics from staging and the persistent root
        directory, i.e., archive or working.
        """
        s1 = cls.read(staging) if (staging / cls.FILE).exists() else None
        s2 = cls.read(persistent) if (persistent / cls.FILE).exists() else None
        if s1 is None:
            return cls() if s2 is None else s2
        elif s2 is None:
            return s1

        r1 = s1.range()
        r2 = s2.range()
        if r1.first != r2.first:
            raise ValueError(
                f"inconsistent start dates {r1.first.isoformat()} "
                f"and {r2.first.isoformat()} for statistics coverage"
            )

        return s1 if r2.last < r1.last else s2

    @classmethod
    def read(cls, directory: Path) -> Self:
        """Instantiate a new statistics frame from the given directory"""
        return cls(pl.read_parquet(directory / cls.FILE))

    def __dataframe__(self) -> Any:
        return self.frame().__dataframe__()

    def frame(self, validate: bool = False, group_by_day: bool = False) -> pl.DataFrame:
        """Materialize a single data frame with the statistics."""
        # Fast paths for repeated read-access to not yet built or finished frame.
        if (
            self._collector is None
            and len(self._frames) == 1
            and not validate
            and not group_by_day
        ):
            return self._frames[0]
        elif len(self._frames) == 0:
            return pl.DataFrame([], schema=STATISTICS_SCHEMA)

        # Combine frame fragments into one frame
        all_frames = list(self._frames)
        if self._collector is not None:
            all_frames.append(self._collector.frame())
        frame = pl.concat(all_frames, how="vertical")

        # Take care of validation and grouping
        if validate:
            validate_row_counts(frame)
        if group_by_day:
            frame = frame.group_by(*groupies(), maintain_order=True).agg(*aggregates())

        # Update internal state
        self._frames = [frame]
        self._collector = None
        return frame

    def is_empty(self) -> bool:
        frame = self.frame()
        return frame.height == 0

    def range(self) -> DateRange:
        frame = self.frame()
        if frame.height == 0:
            raise ValueError("no statistics available")
        return _range_of(frame)

    def missing_range(self) -> None | DateRange:
        frame = self.frame()
        if frame.height == 0:
            return DateRange(
                dt.date(2023, 9, 25),
                dt.date.today() - dt.timedelta(days=2)
            )

        range = _range_of(frame)
        return range.uncovered_near_past()

    def collect(
        self,
        release: Release,
        frame: pl.DataFrame | pl.LazyFrame,
        tag: None | str = None,
        metadata: None | pl.DataFrame = None,
    ) -> None:
        """
        Add summary statistics for the frame with transparency database data.
        Use `append()` for frames with already computed statistics.
        """
        if self._collector is None:
            self._collector = Collector()
        self._collector.collect(release, frame, tag=tag, metadata=metadata)

    def append(self, frame: pl.DataFrame) -> None:
        """
        Append the data frame with summary statistics. Use `collect()` for
        frames with transparency database data.
        """
        self._frames.append(frame)

    def summary(self, markdown: bool = False) -> str:
        """Create a summary table formatted as Unicode or Markdown."""
        return formatted_summary(self.frame(), markdown=markdown)

    def write(self, directory: Path, rechunk: bool = False) -> Self:
        """Write this statistics frame to the given directory."""
        frame = self.frame()
        if rechunk:
            self._frames = [frame.rechunk()]
            frame = self._frames[0]

        tmp = (directory / self.FILE).with_suffix(".tmp.parquet")
        frame.write_parquet(tmp)
        tmp.replace(directory / self.FILE)

        return self

    @classmethod
    def copy(cls, source: Path, target: Path) -> None:
        """
        Copy the statistics file in the source directory to the target directory
        via an intermediate temporary file on the same file system as the target
        directory.
        """
        tmp = (target / cls.FILE).with_suffix(".tmp.parquet")
        shutil.copy(source / cls.FILE, tmp)
        tmp.replace(target / cls.FILE)
