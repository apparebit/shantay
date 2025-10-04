"""
A structured representation of Shantay's log. Each line is a `LogEntry`, whose
last component is a `LogMessage`. Both dataclasses can `parse(str)` their
textual representation in Shantay's log and regenerate the same text again
(modulo extra whitespace).
"""

from collections.abc import Iterator, Mapping
import dataclasses
import datetime as dt
import enum
from io import StringIO
from pathlib import Path
import re
import sys
import traceback
from typing import Any, Literal, Self, TextIO

from .model import Release


_KEY_VALUE = re.compile(
    r"""
    (?P<key> [-_a-zA-Z0-9]+)
    [=]
    (?P<value>
        [_0-9]+ ([.][0-9]+)?
        | ["] [^"]* ["]
    )
    ([,] \s*)?
    """,
    re.VERBOSE
)

_DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_BATCH = re.compile(r"(?<=-full-)[0-9]{5}")


@dataclasses.dataclass(slots=True)
class LogMessage:
    """
    The actual log message, comprising a prefix and key, value pairs, both of
    which are optional.
    """

    prefix: str
    props: Mapping[str, Any]

    @classmethod
    def parse(cls, s: str) -> Self:
        """Parse the log message."""
        if "=" not in s:
            return cls(s, {})

        prefix = ""

        t = s[:s.index("=")]
        if " " in t:
            cut = t.rindex(" ")
            prefix = t[:cut].strip()
            s = s[cut + 1:]

        props = {}
        pos = 0
        while (match := _KEY_VALUE.match(s, pos)) is not None:
            pos = match.end(0)
            key = match.group("key")
            value = match.group("value")

            if value.startswith('"'):
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

    def has(self, *keys: str, prefix: None | str = None) -> bool:
        """Determine whether the message has all given properties."""
        if prefix is not None and prefix != self.prefix:
            return False
        for key in keys:
            if not key in self:
                return False
        return True

    def release(self) -> None | Release:
        """Get the release if any."""
        if "release" in self:
            return Release.of(self.props["release"])
        if "file" in self:
            date = _DATE.search(self.props["file"])
            if date is not None:
                return Release.of(date.group(0))

        return None

    def batches(self) -> None | int:
        """Get the batch number if any."""
        if "count" in self:
            return self.props["count"]
        if "file-count" in self:
            return self.props["file-count"]
        if "file" in self:
            batch = _BATCH.search(self.props["file"])
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
        if self.prefix != "":
            stream.write(self.prefix)
            stream.write(" ")

        for index, (key, value) in enumerate(self.props.items()):
            if index != 0:
                stream.write(", ")
            stream.write(key)
            stream.write("=")
            stream.write(self.format_value(value))

    @classmethod
    def format_value(cls, value: Any) -> str:
        if value is None:
            return '""'
        elif isinstance(value, bool):
            return f'"{value}"'.lower()
        elif isinstance(value, int):
            return f'{value}'
        elif isinstance(value, float):
            return f'{value}'
        else:
            return f'"{value}"'


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
        return self.message.prefix[:3] in ("───", "━━━", "═══", "▁▁▁", "___")

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
        return self.message.has("key", "value", prefix="")

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

    def is_retired_worker(self) -> bool:
        """Determine whether the log entry marks the retirement of a worker."""
        return self.message.has("worker", "pool", prefix="retiring")

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

# --------------------------------------------------------------------------------------

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
        if isinstance(self.value, int):
            stream.write(f"{self.value:_}")
        else:
            stream.write(f"{self.value:_.3f}")

# ======================================================================================

class Printer:
    """Message the user."""

    def __init__(self, stream: None | TextIO = None) -> None:
        self._stream = stream or sys.stderr
        self._file = ""
        self._line = 0

    @property
    def location(self) -> str:
        if self._file == "":
            return ""
        return f"{self._file}:{self._line} "

    def processing(self, /, file: None | str = None, line: None | int = None) -> Self:
        if file is not None:
            self._file = file
        if line is not None:
            self._line = line
        return self

    def _pln(self, s: str) -> Self:
        stream = self._stream
        stream.write(s)
        stream.write("\n")
        stream.flush()
        return self

    def info(self, s: str) -> Self:
        return self._pln(f"INFO {self.location}{s}")

    def warn(self, s: str) -> Self:
        return self._pln(f"WARN {self.location}{s}")

    def error(self, x: BaseException) -> Self:
        return self._pln(f"ERROR {self.location}{x}")

# --------------------------------------------------------------------------------------

class Continuation(enum.StrEnum):
    """The continuation of control flow."""
    RESTART = "restart"
    CONTINUE = "continue"
    FINISH = "finish"


class Analyzer:

    def __init__(self, log: Iterator[LogEntry], printer: Printer) -> None:
        # Log content
        self._log = log
        self._entry = None

        # Message printer
        self._cli = printer

        # Analysis state
        self._header = None
        self.reset()

    def reset(self) -> None:
        self._coordinator = None
        self._worker_count = 0
        self._workers = {}
        self._releases = {}

    def next(self) -> None | LogEntry:
        if self._entry is None:
            entry = next(self._log, None)
        else:
            entry, self._entry = self._entry, None

        if entry is not None:
            self._cli.processing(line=entry.line_number)

        return entry

    def push_back(self, entry: LogEntry) -> None:
        if self._entry is not None:
            raise ValueError("unable to push back second log entry")

        self._entry = entry

    def get_worker_id(self, entry: LogEntry) -> None | int:
        if self._coordinator is None:
            self._coordinator = entry.pid
            return None
        elif entry.pid == self._coordinator:
            return None
        elif entry.pid not in self._workers:
            self._worker_count += 1
            self._workers[entry.pid] = self._worker_count
        return self._workers[entry.pid]

    def get_release(self, entry: LogEntry) -> None | Release:
        release = entry.message.release()
        if release is not None:
            self._releases[entry.pid] = release
        return self._releases[entry.pid]

    def get_metric(self, entry: LogEntry) -> None | TimeSeriesEntry:
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
            return None

        if value is None:
            self._cli.warn(f"skipping log record lacking {metric} entry")
            return None

        worker_id = self.get_worker_id(entry)
        release = self.get_release(entry)

        return TimeSeriesEntry(
            timestamp=entry.timestamp,
            process_id=entry.pid,
            worker_id=worker_id,
            release=release,
            batch=batch,
            metric=metric,
            value=value,
        )

    def read_task_start(self) -> None | LogEntry:
        # Skip log entries about updating platform names
        while (entry := self.next()) and not entry.is_task_start():
            pass
        return entry

    def read_header(self, start: LogEntry) -> None | dict[str, Any]:
        engine = "Multiprocessor" if "Multi" in start.message.prefix else "Processor"
        props = {
            "task": start.message.props["task"],
            "engine": engine,
        }

        while (entry := self.next()) and entry.message.has("key", "value", prefix=""):
            props[entry.message.props["key"]] = entry.message.props["value"]
        if entry is None:
            return None

        self.push_back(entry)
        return props

    def process_header(self) -> Continuation:
        # Skip entries about platform names until entry marking task start
        entry = self.read_task_start()
        if entry is None:
            return Continuation.FINISH

        # Read header with key, value pairs
        header = self.read_header(entry)
        if header is None:
            return Continuation.FINISH
        if self._header is None:
            self._header = header
            return Continuation.RESTART

        simple_tasks = ("info", "summarize-builtin", "visualize")
        if self._header["task"] in simple_tasks or header in simple_tasks:
            return Continuation.RESTART

        # Compare header with previous header
        other = self._header
        if not all(header.get(k) == other.get(k) for k in (
            "task", "filter", "coverage.first"
        )):
            self._header = header
            return Continuation.RESTART

        # Read until next entry with release
        release = None
        while (entry := self.next()) and not (release := entry.message.release()):
            pass
        if entry is None:
            return Continuation.FINISH

        if any(abs(release - r) <= 1 for r in self._releases.values()):
            return Continuation.CONTINUE
        else:
            return Continuation.RESTART

    def extract(self) -> Iterator[None | TimeSeriesEntry]:
        while (entry := self.next()):
            if entry.is_rule() and entry.module == "shantay":
                continuation = self.process_header()
                if continuation is Continuation.FINISH:
                    self._cli.info("finishing because logged task ended")
                    return
                elif continuation is Continuation.CONTINUE:
                    self._cli.info('continuing because logged task continued')
                    continue
                elif continuation is Continuation.RESTART:
                    self._cli.info('restarting because logged task changed')
                    assert self._header is not None
                    for k, v in self._header.items():
                        self._cli.info(f'    {k}={LogMessage.format_value(v)}')
                    self.reset()
                    yield None
                    continue

            ts_entry = self.get_metric(entry)
            if ts_entry is not None:
                yield ts_entry

    def extract_into_file(self, output: Path) -> None:
        with open(output, mode="w", encoding="utf8") as file:
            TimeSeriesEntry.write_header(file, with_label=True)
            file.write("\n")

            for datum in self.extract():
                if datum is None:
                    file.seek(0)
                    file.truncate()
                    TimeSeriesEntry.write_header(file, with_label=True)
                    file.write("\n")
                else:
                    datum.write(file, with_label=True)
                    file.write("\n")

# --------------------------------------------------------------------------------------

def visualize(csv: Path, svg: Path, by_timestamp: bool = False) -> None:
    import polars as pl
    import altair as alt
    from .color import Palette

    frame = pl.read_csv(
        csv, schema_overrides={"value": pl.String}
    ).with_columns(
        pl.col("value").str.replace_all("_", "").cast(pl.Float64)
    )

    data = frame.filter(pl.col("label").ne("coordinator"))
    if data.height == 0:
        data = frame
        labels = ["coordinator"]
        colors = [Palette.BLUE]
    else:
        count = data.select(pl.col("label").n_unique()).item()
        labels = [f"worker-{n}" for n in range(1, count + 1)]
        colors = [
            Palette[n] for n in (
                ["BLUE", "RED", "CYAN", "PINK", "PURPLE", "ORANGE"] * (count // 6 + 1)
            )[:count]
        ]

    column = "timestamp" if by_timestamp else "release"
    min, max = data.select(
        pl.col(column).min().alias("min"),
        pl.col(column).max().alias("max"),
    ).row(0)
    if by_timestamp:
        dot_size = 3
    else:
        count = Release.of(max) - Release.of(min) + 1

        if count < 100:
            dot_size = 10
        elif count < 500:
            dot_size = 5
        else:
            dot_size = 2

    if by_timestamp:
        x_axis = alt.X("timestamp:T").title("Timestamp")
    else:
        x_axis = alt.X("release:T").title("Release")

    base = alt.Chart(
        data
    ).encode(
        x_axis
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
        width = 800,
    ).resolve_scale(
        y = "independent",
    ).save(svg)

# --------------------------------------------------------------------------------------

def get_options(argv: None | list[str] = None) -> Any:
    import argparse

    parser = argparse.ArgumentParser()
    group = parser.add_argument_group("file names")
    group.add_argument(
        "--log",
        type=Path,
        default=Path("shantay.log"),
        help="the log file to parse (default: 'shantay.log')",
    )
    group.add_argument(
        "--csv",
        type=Path,
        help="the CSV file to generate (default: '<logfile-name>.csv')",
    )
    group.add_argument(
        "--svg",
        type=Path,
        help="the SVG file to generate (default: '<logfile-name>.svg')"
    )

    group = parser.add_argument_group("visualization")
    group.add_argument(
        "--by-timestamp",
        action="store_true",
        help="use timestamp as x-axis instead of release date",
    )

    parser.add_argument(
        "task",
        choices=["csv", "svg", "csv+svg"],
        default="csv+svg",
        nargs="?",
        help="the task to perform (default: csv+svg)",
    )

    return parser.parse_args(argv)


def main(printer: Printer, argv: None | list[str]) -> int:
    options = get_options(argv)

    log_file = options.log
    csv_file = log_file.with_suffix(".csv") if options.csv is None else options.csv
    svg_file = log_file.with_suffix(".svg") if options.svg is None else options.svg

    if "csv" in options.task:
        printer.info(f'extracting metrics from "{log_file}" into "{csv_file}"')
        printer.processing(file=str(log_file))
        file_parser = LogEntry.parse_file(log_file)
        Analyzer(file_parser, printer).extract_into_file(csv_file)
        printer.processing(file="")

    if "svg" in options.task:
        printer.info(f'visualizing metrics from "{csv_file}" into "{svg_file}"')
        visualize(csv_file, svg_file, by_timestamp=options.by_timestamp)

    return 0


if __name__ == "__main__":
    printer = Printer()
    try:
        sys.exit(main(printer, sys.argv[1:]))
    except Exception as x:
        printer.error(x)
        traceback.print_exception(x)
        sys.exit(1)
