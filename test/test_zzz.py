"""Tests are loaded and executed in alphabetical order of module names. This
module needs to run last because it further tests the contents of the log."""
from pathlib import Path
import os
import unittest
import sys

from .runtime import StyledStream

from shantay.log import LogEntry
from shantay.model import Release


LOG = Path(__file__).parent / "tmp" / "log.log"
PID = os.getpid()


class TestZzz(unittest.TestCase):

    def test_log(self) -> None:
        # The next line tests the parser...
        log = [*LogEntry.parse_file(LOG)]

        if os.getenv("DEBUG", None) == "true":
            styled = StyledStream(sys.stdout)
            print()
            print(styled.h1("Shantay's Log"))
            for entry in log:
                entry.print(sys.stdout)
            print("", flush=True)

        worker_entries = 0
        test_entries = 0
        warning_entries = 0
        traces = []
        release = Release.of(2024, 3, 14)
        state = None

        for entry in log:
            # Count entries with unusual PID, level, or module; collect traces
            if entry.pid != PID:
                worker_entries += 1
            if entry.module == "test.test_pool":
                test_entries += 1
            if entry.level == "WARNING":
                warning_entries += 1
                if entry.exc_info is not None:
                    traces.append(entry.exc_info)

            # Switch release testing on and off as needed
            if (
                state != "release"
                and entry.level == "INFO"
                and entry.message.prefix in ("staged", "distill")
            ):
                state = "release"
            elif state == "release" and entry.level == "WARNING":
                state = None

            # Test release
            if state == "release":
                self.assertEqual(entry.message.release(), release)

        self.assertEqual(worker_entries, 5)
        self.assertEqual(test_entries, 3)
        self.assertEqual(warning_entries, 6)
        self.assertEqual(len(traces), 3)
        for index in range(1, 3):
            self.assertEqual(traces[0], traces[index])
        for trace in traces:
            trace.startswith("Traceback (most recent call last):")
            trace.endswith("Akékoľvek metadáta` is not properly escaped.```")
