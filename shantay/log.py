"""
A structured representation of Shantay's log. Each line is a `LogEntry`, whose
last component is a `LogMessage`. Both dataclasses can `parse(str)` their
textual representation in Shantay's log and regenerate the same text again
(modulo extra whitespace).
"""

from collections.abc import Iterator, Mapping
from dataclasses import dataclass
import datetime as dt
from io import StringIO
from pathlib import Path
import re
import sys
from typing import cast, Literal, Self, TextIO

from .model import Release


COMMA_SPACE = re.compile(r",\s+")
DATE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
RULE = re.compile(r"^▁+$")


class NoArgument:
    pass


@dataclass(slots=True)
class LogMessage:
    """
    The actual log message, comprising a prefix and key, value pairs, both of
    which are optional.
    """

    prefix: None | str
    props: Mapping[str, None | bool | int | str]

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
            file = DATE.search(cast(str, self.props["file"]))
            if file is not None:
                return Release.of(file.group(0))

        return None

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


@dataclass(slots=True)
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
        return self.message.prefix is not None and "▁▁▁▁▁" in self.message.prefix

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
        return self.message.has("task", "release", "category", "worker", prefix="running")

    def is_worker_init(self) -> bool:
        """Determine whether the log entry marks the initialization of a worker
        process. Note that this entry may occur between, for example, the second
        and third entries of a new job."""
        return self.message.has("pid", "pool", prefix="initialized worker process")

    def is_job_done(self) -> bool:
        """Determine whether the long entry marks the end of concurrently
        processing a release."""
        return self.message.has(
            "task", "release", "category", "worker",
            prefix="returning result for"
        )

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

    def print(self, stream: TextIO = sys.stdout, end: None | str = "\n") -> None:
        """Print the log entry."""
        self.write(stream)
        if end is not None:
            stream.write(end)


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "path",
        type=Path,
        help="The log file to parse",
    )
    options = parser.parse_args(sys.argv[1:])

    for entry in LogEntry.parse_file(options.path):
        if (
            entry.is_rule()
            or entry.is_task_start()
            or entry.is_key_value()
            or entry.is_job_start1()
        ):
            entry.write(sys.stdout)
            sys.stdout.write("\n")
