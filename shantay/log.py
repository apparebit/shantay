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
from typing import Literal, Self, TextIO


COMMA_SPACE = re.compile(r",\s+")
DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
RULE = re.compile(r"^▁+$")


@dataclass(frozen=True, slots=True)
class LogMessage:

    prefix: None | str
    props: Mapping[str, None | bool | int | str | dt.date]

    @classmethod
    def parse(cls, s: str) -> Self:
        prefix = None
        props = {}

        parts = COMMA_SPACE.split(s.strip())
        for index, part in enumerate(parts):
            key, sep, value = part.partition("=")
            if sep == '':
                if index != 0 or len(parts) != 1:
                    raise ValueError(f'malformed log message "{s}"')
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
                elif DATE.match(value):
                    value = dt.date.fromisoformat(value)
            else:
                value = int(value)

            props[key] = value

        return cls(prefix, props)

    def __str__(self) -> str:
        s = StringIO()
        self.write(s)
        return s.getvalue()

    def write(self, stream: TextIO) -> None:
        if self.prefix is not None:
            stream.write(self.prefix)
            stream.write(" ")

        for index, (key, value) in enumerate(self.props.items()):
            if index != 0:
                stream.write(", ")

            if value is None:
                value ='""'
            elif isinstance(value, bool):
                value = f'"{value}"'
            elif isinstance(value, int):
                value = f'{value}'
            else:
                value = f'"{value}"'

            stream.write(key)
            stream.write("=")
            stream.write(value)


@dataclass(frozen=True, slots=True)
class LogEntry:

    timestamp: dt.datetime
    pid: int
    module: str
    level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
    message: LogMessage

    @classmethod
    def ingest(cls, path: Path) -> Iterator[Self]:
        with open(path, mode="r", encoding="utf8") as file:
            while (line := file.readline()):
                yield cls.parse(line)

    @classmethod
    def parse(cls, line: str) -> Self:
        parts = line.strip().split("︙")
        if len(parts) != 5:
            raise ValueError(f'malformed log entry "{line}"')
        level = parts[3]
        if level not in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
            raise ValueError(f'malformed log level "{level}')

        return cls(
            dt.datetime.fromisoformat(parts[0]),
            int(parts[1]),
            parts[2],
            level,
            LogMessage.parse(parts[4]),
        )

    def __str__(self) -> str:
        s = StringIO()
        self.write(s)
        return s.getvalue()

    def write(self, stream: TextIO) -> None:
        stream.write(self.timestamp.isoformat())
        stream.write("︙")
        stream.write(f"{self.pid}")
        stream.write("︙")
        stream.write(self.module)
        stream.write("︙")
        stream.write(self.level)
        stream.write("︙")
        self.message.write(stream)

    def print(self, stream: TextIO = sys.stdout, end: None | str = "\n") -> None:
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

    processes = {}
    for entry in LogEntry.ingest(options.path):
        processes.setdefault(entry.pid, []).append(entry)

    for process, entries in processes.items():
        label = f"PID {process}"
        print(label)
        print("-" * len(label))
        print()
        for entry in entries[:20]:
            entry.print()
        print("...\n\n")
