"""
A structured representation of Shantay's log. Each line is a `LogEntry`, whose
last component is a `LogMessage`. Both dataclasses can `parse(str)` their
textual representation in Shantay's log and regenerate the same text again
(modulo extra whitespace).
"""

from collections.abc import Iterator, Mapping
import dataclasses
import datetime as dt
from io import StringIO
from pathlib import Path
import re
import sys
from typing import cast, Literal, Self, TextIO

from .model import Release


COMMA_SPACE = re.compile(r",\s+")
DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
BATCH = re.compile(r"(?<=-full-)[0-9]{5}")
RULE = re.compile(r"^▁+$")


class NoArgument:
    pass


@dataclasses.dataclass(slots=True)
class LogMessage:
    """
    The actual log message, comprising a prefix and key, value pairs, both of
    which are optional.
    """

    prefix: None | str
    props: Mapping[str, None | bool | int | float | str ]

    @classmethod
    def parse(cls, s: str) -> Self:
        """Parse the log message."""
        prefix = None
        props = {}

        parts = COMMA_SPACE.split(s.strip())
        for index, part in enumerate(parts):
            key, sep, value = part.partition("=")
            if sep == '':
                if index != 0 or len(parts) != 1:
                    raise ValueError(f'malformed log message "{s}"')
                key = key.strip()
                if key:
                    prefix = key
                break

            if index == 0:
                fix, _, key = key.rpartition(" ")
                fix = fix.strip()
                if fix:
                    prefix = fix

            if value.startswith('"'):
                if not value.endswith('"'):
                    raise ValueError(f"malformed key, value '{part}'")
                value = value[1:-1]

                if value == "":
                    value = None
                elif value.lower() == "false":
                    value = False
                elif value.lower() == "true":
                    value = True
            else:
                value = value.replace("_", "")
                try:
                    value = int(value)
                except ValueError:
                    value = float(value)

            props[key] = value

        return cls(prefix, props)

    def __contains__(self, key: str) -> bool:
        return key in self.props

    def has(self, *keys: str, prefix: None | str | type = NoArgument) -> bool:
        """Determine whether the message has all given properties."""
        if prefix is not NoArgument and prefix != self.prefix:
            return False
        for key in keys:
            if not key in self:
                return False
        return True

    def release(self) -> None | Release:
        """Get the release if any."""
        if "release" in self:
            return Release.of(cast(str, self.props["release"]))
        if "file" in self:
            date = DATE.search(cast(str, self.props["file"]))
            if date is not None:
                return Release.of(date.group(0))

        return None

    def batches(self) -> None | int:
        """Get the batch number if any."""
        if "count" in self:
            return cast(int, self.props["count"])
        if "file-count" in self:
            return cast(int, self.props["file-count"])
        if "file" in self:
            batch = BATCH.search((cast(str, self.props["file"])))
            if batch is not None:
                return int(batch.group(0))

        return None

    def latency(self) -> None | float:
        """Get the latency if any in seconds."""
        latency = self.props.get("latency", None)
        if latency is None:
            return None
        if not isinstance(latency, float):
            raise ValueError(f'latency "{latency}" is not a number')

        unit = self.props["unit"]
        match unit:
            case "sec":
                return latency
            case "min":
                return 60 * latency
            case "hour":
                return 60 * 60 * latency
            case _:
                return 24 * 60 * 60 * latency

    def resident_set_size(self) -> None | float:
        """Get the resident-set size if any in GB."""
        size = self.props.get("resident-set-size", None)
        if size is None:
            return None
        if not isinstance(size, (int, float)):
            raise ValueError(f'resident-set-size "{size}" is not a number')
        unit = self.props.get("unit", "B")
        match unit:
            case "B":
                return size / 1024**3
            case "KB":
                return size / 1024**2
            case "MB":
                return size / 1024
            case _:
                return size

    def __str__(self) -> str:
        """Get the log message as a string."""
        s = StringIO()
        self.write(s)
        return s.getvalue()

    def write(self, stream: TextIO) -> None:
        """Write the log message to the stream."""
        if self.prefix is not None:
            stream.write(self.prefix)
            stream.write(" ")

        for index, (key, value) in enumerate(self.props.items()):
            if index != 0:
                stream.write(", ")

            if value is None:
                value ='""'
            elif isinstance(value, bool):
                value = f'"{value}"'.lower()
            elif isinstance(value, int):
                value = f'{value}'
            elif isinstance(value, float):
                value = f'{value}'
            else:
                value = f'"{value}"'

            stream.write(key)
            stream.write("=")
            stream.write(value)


@dataclasses.dataclass(slots=True)
class LogEntry:
    """
    A structured log entry, comprising the timestamp, the process ID, the
    module, the level, the actual message, and the optional exception
    information on subsequent lines.
    """

    line_number: int
    timestamp: dt.datetime
    pid: int
    module: str
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    message: LogMessage
    exc_info: None | str = None

    @classmethod
    def parse_file(cls, path: Path) -> Iterator[Self]:
        """Parse the contents of the log file. Since log files may get rather
        large, this method is a generator."""
        file_name = str(path)

        with open(path, mode="r", encoding="utf8") as file:
            line = file.readline()
            line_number = 1

            while line != "":
                # Parse an entry
                entry = cls.parse(file_name, line_number, line)

                # Collect subsequent lines that are not log entries
                trace = []
                while (line := file.readline()):
                    line_number += 1

                    if "︙" in line:
                        break
                    trace.append(line)

                # Add as exception info to entry
                if 0 < len(trace):
                    entry.exc_info = "\n".join(trace)

                yield entry

    @classmethod
    def parse(cls, file: str, line_number: int, line: str) -> Self:
        """Parse a log line."""
        parts = line.strip().split("︙")
        if len(parts) != 5:
            raise ValueError(f'{file}:{line_number}: malformed log entry')
        level = parts[3]
        if level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ValueError(f'{file}:{line_number}: malformed log level')

        return cls(
            line_number,
            dt.datetime.fromisoformat(parts[0]),
            int(parts[1]),
            parts[2],
            level,
            LogMessage.parse(parts[4]),
        )

    def is_rule(self) -> bool:
        """Determine whether the log entry contains a horizontal rule as message."""
        return (
            self.message.prefix is not None
            and self.message.prefix[:3] in ("───", "━━━", "═══", "▁▁▁", "___")
        )

    def is_task_start(self) -> bool:
        """Determine whether the log entry marks the beginning of a task."""
        if not self.message.has("pid", "task"):
            return False
        prefix = self.message.prefix
        return (
            prefix == "running processor with"
            or prefix == "running multiprocessor with"
        )

    def is_concurrent_task_start(self) -> bool:
        """Determine whether the log entry marks the beginning of a concurrent
        task."""
        return self.message.has("pid", "task", prefix="running multiprocessor with")

    def is_key_value(self) -> bool:
        """Determine whether the log entry contains a key, value pair describing
        a task."""
        return self.message.has("key", "value", prefix=None)

    def is_job_start1(self) -> bool:
        """Determine whether the log entry is the first entry for concurrently
        processing a release."""
        return self.message.has("task", "release", "pool", prefix="submitting")

    def is_job_start2(self) -> bool:
        """Determine whether the log entry is the second entry for concurrently
        processing a release."""
        return self.message.has("fn", "pool", prefix="submit")

    def is_job_start3(self) -> bool:
        """Determine whether the log entry is the third entry for concurrently
        processing a release."""
        return self.message.has("task", "release", "filter", "worker", prefix="running")

    def is_worker_init(self) -> bool:
        """Determine whether the log entry marks the initialization of a worker
        process. Note that this entry may occur between, for example, the second
        and third entries of a new job."""
        return self.message.has("pid", "pool", prefix="initialized worker process")

    def is_job_done(self) -> bool:
        """Determine whether the long entry marks the end of concurrently
        processing a release."""
        return self.message.has(
            "task", "release", "filter", "worker",
            prefix="returning result for"
        )

    def is_summarized_batch(self) -> bool:
        """Determine whether the log entry marks a summarized batch file."""
        return self.message.has("file", "latency", "unit", prefix="summarized batch")

    def is_combined_batches(self) -> bool:
        """Determine whether the log entry marks the combining of per-batch stats."""
        if self.message.has("file-count", "glob", "release", prefix="combining"):
            return True
        return self.message.has("entity", "count", prefix="combining")

    def is_max_rss(self) -> bool:
        """Determine whether the log entry reports the maximum resident-set size
        for a process."""
        return self.message.has("resident-set-size", prefix="maximum")

    def __str__(self) -> str:
        """Get the log message as a string."""
        s = StringIO()
        self.write(s)
        return s.getvalue()

    def write(self, stream: TextIO) -> None:
        """Write the log entry to the stream."""
        stream.write(self.timestamp.isoformat())
        stream.write("︙")
        stream.write(f"{self.pid}")
        stream.write("︙")
        stream.write(self.module)
        stream.write("︙")
        stream.write(self.level)
        stream.write("︙")
        self.message.write(stream)
        if self.exc_info is not None:
            stream.write("\n")
            stream.write(self.exc_info)


@dataclasses.dataclass(frozen=True, slots=True)
class TimeSeriesEntry:

    timestamp: dt.datetime
    process_id: int
    worker_id: None | int
    release: None | Release
    batch: None | int
    metric: Literal["latency", "max-rss", "batches"]
    value: int | float

    @property
    def label(self) -> str:
        if self.worker_id is None:
            return "coordinator"
        else:
            return f"worker-{self.worker_id}"

    def __str__(self) -> str:
        s = StringIO()
        self.write(s)
        return s.getvalue()

    @classmethod
    def write_header(cls, stream: TextIO, *, with_label: bool = False) -> None:
        stream.write("timestamp,process_id,worker_id,")
        if with_label:
            stream.write("label,")
        stream.write("release,batch,metric,value")

    def write(self, stream: TextIO, *, with_label: bool = False) -> None:
        stream.write(self.timestamp.isoformat())
        stream.write(",")
        stream.write(str(self.process_id))
        stream.write(",")
        stream.write("" if self.worker_id is None else str(self.worker_id))
        stream.write(",")
        if with_label:
            stream.write(self.label)
            stream.write(",")
        stream.write("" if self.release is None else self.release.id)
        stream.write(",")
        stream.write("" if self.batch is None else str(self.batch))
        stream.write(",")
        stream.write(self.metric)
        stream.write(",")
        stream.write(f"{self.value:.3f}")

    @classmethod
    def extract(cls, log_file: str, log: Iterator[LogEntry]) -> Iterator[None | Self]:
        coordinator = None
        worker_count = 0
        workers = {}
        latest_release = {}

        for index, entry in enumerate(log):
            if entry.is_rule() and entry.module == "shantay":
                print(
                    f"WARNING {log_file}:{entry.line_number}: restarting extraction, "
                    "as log record marks new run",
                    file=sys.stderr
                )

                coordinator = entry.pid
                worker_count = 0
                workers.clear()
                latest_release.clear()
                yield None
                continue

            if entry.is_summarized_batch():
                batch = entry.message.batches()
                metric = "latency"
                value = entry.message.latency()
            elif entry.is_max_rss():
                batch = None
                metric = "max-rss"
                value = entry.message.resident_set_size()
            elif entry.is_combined_batches():
                batch = entry.message.batches()
                metric = "batches"
                value = batch
            else:
                continue

            if value is None:
                print(
                    f'WARNING {log_file}:{entry.line_number}: skipping log record, '
                    f'as it lacks {metric} entry',
                    file=sys.stderr,
                )
                continue

            if entry.pid == coordinator:
                worker_id = None
            else:
                if entry.pid not in workers:
                    worker_count += 1
                    workers[entry.pid] = worker_count
                worker_id = workers[entry.pid]

            # Patch in latest release if missing from log entry
            release = entry.message.release()
            if release is None:
                release = latest_release.get(entry.pid)
            else:
                latest_release[entry.pid] = release

            yield cls(
                entry.timestamp,
                entry.pid,
                worker_id,
                release,
                batch,
                metric,
                value,
            )

    @classmethod
    def extract_into_file(
        cls,
        log_file: str,
        log: Iterator[LogEntry],
        output: Path
    ) -> None:
        with open(output, mode="w", encoding="utf8") as file:
            for datum in cls.extract(log_file, log):
                if datum is None:
                    file.seek(0)
                    file.truncate()
                    cls.write_header(file, with_label=True)
                    file.write("\n")
                else:
                    datum.write(file, with_label=True)
                    file.write("\n")

    @classmethod
    def visualize(cls, csv: Path, svg: Path) -> None:
        import polars as pl
        import altair as alt
        from .color import Palette

        frame = pl.read_csv(csv)
        data = frame.filter(pl.col("label").ne("coordinator"))
        if data.height == 0:
            data = frame
            labels = ["coordinator"]
            colors = [Palette.BLUE]
        else:
            count = data.select(pl.col("label").n_unique()).item()
            labels = [f"worker-{n}" for n in range(1, count + 1)]
            colors = [Palette[n] for n in ["BLUE", "RED", "GREEN", "PINK"][:count]]

        min, max = data.select(
            pl.col("release").min().alias("min"),
            pl.col("release").max().alias("max"),
        ).row(0)
        count = Release.of(max) - Release.of(min) + 1
        if count < 100:
            dot_size = 10
        elif count < 500:
            dot_size = 6
        else:
            dot_size = 2

        base = alt.Chart(
            data
        ).encode(
            alt.X("release:T").title("Release")
        )

        latency = base.transform_filter(
            alt.datum.metric == "latency"
        ).mark_circle(
            size=dot_size,
        ).encode(
            alt.Y("value:Q").title("Dots: Latency (seconds)"),
            alt.Color("label:N").scale(domain=labels, range=colors),
        )

        rss = base.transform_filter(
            alt.datum.metric == "max-rss"
        ).mark_line(
        ).encode(
            alt.Y("value:Q").title("Lines: Maximum Resident-Size Size (GB)"),
            alt.Color("label:N").scale(domain=labels, range=colors),
        )

        chart = latency + rss

        chart.properties(
            width = 1_000,
        ).resolve_scale(
            y = "independent",
        ).save(svg)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--log",
        type=Path,
        default=Path("shantay.log"),
        help="the log file to parse (default: 'shantay.log')",
    )
    parser.add_argument(
        "--csv",
        type=Path,
        default=Path("shantay-perf.csv"),
        help="the CSV file to generate (default: 'shantay-perf.csv')",
    )
    parser.add_argument(
        "--svg",
        type=Path,
        help="the SVG file to generate (default: CSV file with '.svg' suffix)"
    )
    parser.add_argument(
        "task",
        choices=["csv", "svg", "csv+svg"],
        default="csv+svg",
        nargs="?",
        help="the task to perform (default: csv+svg)",
    )
    options = parser.parse_args(sys.argv[1:])

    if "csv" in options.task:
        print(
            f'INFO: extracting metrics from log "{options.log}" into "{options.csv}"',
            file=sys.stderr,
        )
        file_parser = LogEntry.parse_file(options.log)
        TimeSeriesEntry.extract_into_file(str(options.log), file_parser, options.csv)

    if "svg" in options.task:
        svg = options.csv.with_suffix(".svg") if options.svg is None else options.svg
        print(
            f'INFO: visualizing metrics from "{options.csv}" into "{svg}"',
            file=sys.stderr,
        )
        TimeSeriesEntry.visualize(options.csv, svg)
