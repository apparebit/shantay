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
    props: Mapping[str, None | bool | int | float | str]

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

    def batch(self) -> None | int:
        """Get the batch number if any."""
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
        if not isinstance(size, float):
            raise ValueError(f'resident-set-size "{size}" is not a number')
        unit = self.props["unit"]
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
        with open(path, mode="r", encoding="utf8") as file:
            line = file.readline()
            no = 1

            while line != "":
                # Parse an entry
                entry = cls.parse(no, line)

                # Collect subsequent lines that are not log entries
                trace = []
                while (line := file.readline()):
                    no += 1

                    if "︙" in line:
                        break
                    trace.append(line)

                # Add as exception info to entry
                if 0 < len(trace):
                    entry.exc_info = "\n".join(trace)

                yield entry

    @classmethod
    def parse(cls, number: int, line: str) -> Self:
        """Parse a log line."""
        parts = line.strip().split("︙")
        if len(parts) != 5:
            raise ValueError(f'malformed log entry in line {number:,}:{line}')
        level = parts[3]
        if level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ValueError(f'malformed log level in line {number:,}:{line}')

        return cls(
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
            and self.message.prefix[:3] in ("───", "━━━", "═══")
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

    def is_summarized_file(self) -> bool:
        """Determine whether the log entry marks a successfully summarized file."""
        return self.message.has("file", "latency", "unit", prefix="summarized")

    def is_max_rss(self) -> bool:
        """Determine whether the log entry reports the maximum resident-set size
        for a process."""
        return self.message.has("resident-set-size", "unit", prefix="maximum")

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
    ts: dt.datetime
    process_kind: Literal["coordinator", "worker"]
    process_id: int
    release: None | Release
    batch: None | int
    metric: Literal["latency", "max-rss"]
    value: int | float

    @property
    def label(self) -> str:
        return f"{self.process_kind}-{self.process_id}"

    def __str__(self) -> str:
        s = StringIO()
        self.write(s)
        return s.getvalue()

    def write(self, stream: TextIO) -> None:
        stream.write(self.ts.isoformat())
        stream.write(",")
        stream.write(self.process_kind)
        stream.write(",")
        stream.write(str(self.process_id))
        stream.write("" if self.release is None else self.release.id)
        stream.write(",")
        stream.write("" if self.batch is None else str(self.batch))
        stream.write(",")
        stream.write(self.metric)
        stream.write(",")
        stream.write(f"{self.value:.3f}")

    @classmethod
    def extract(cls, log: Iterator[LogEntry]) -> Iterator[None | Self]:
        coordinator = None
        for index, entry in enumerate(log):
            if entry.is_rule() and entry.module == "shantay":
                print(
                    f"log record {index:,} marks new run; starting afresh",
                    file=sys.stderr
                )

                coordinator = entry.pid
                yield None
                continue

            if entry.is_summarized_file():
                batch = entry.message.batch()
                metric = "latency"
                value = entry.message.latency()
            elif entry.is_max_rss():
                batch = None
                metric = "max-rss"
                value = entry.message.resident_set_size()
            else:
                continue

            process_kind = "coordinator" if entry.pid == coordinator else "worker"
            release = entry.message.release()
            assert value is not None

            yield cls(
                entry.timestamp,
                process_kind,
                entry.pid,
                release,
                batch,
                metric,
                value,
            )

    @classmethod
    def extract_into_file(cls, log: Iterator[LogEntry], path: Path) -> None:
        with open(path, mode="w", encoding="utf8") as file:
            for entry in cls.extract(log):
                if entry is None:
                    file.seek(0)
                    file.truncate()
                else:
                    entry.write(file)
                    file.write("\n")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "in",
        type=Path,
        default=Path("shantay.log"),
        dest="input",
        help="The log file to parse",
    )
    parser.add_argument(
        "out",
        type=Path,
        default=Path("shantay-perf.csv"),
        help="the csv file to generator",
    )
    options = parser.parse_args(sys.argv[1:])

    file_parser = LogEntry.parse_file(options.input)
    TimeSeriesEntry.extract_into_file(file_parser, options.out)
