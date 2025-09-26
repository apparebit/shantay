"""Tests are loaded and executed in alphabetical order of module names. This
module needs to run last because it further tests the contents of the log."""
from pathlib import Path
import os
import unittest
import sys

from .runtime import StyledStream

from shantay.log import LogEntry
from shantay.model import Release, ReleaseRange


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
                print(entry)
            print("", flush=True)

        warnings = errors = starts = completions = 0
        traces = []
        is_multi = None

        for entry in log:
            if entry.level == "WARNING":
                warnings += 1
            if entry.level == "ERROR":
                errors += 1
            if entry.exc_info is not None:
                traces.append(entry.exc_info)
            if entry.message.has("runner", "task") or entry.message.has("component"):
                if entry.message.prefix == "testing":
                    starts += 1
                    is_multi = (
                        entry.message.has("component")
                        or entry.message.props.get("runner", "").startswith("Multi")
                    )
                elif entry.message.prefix == "completed test for":
                    completions += 1
                    is_multi = None
            if is_multi is not None:
                if entry.pid != PID:
                    self.assertTrue(is_multi)
                if entry.module == "shantay.pool":
                    self.assertTrue(is_multi)

        self.assertEqual(warnings, 10)
        self.assertEqual(errors, 0)
        self.assertEqual(len(traces), 5)
        for index in range(1, 5):
            self.assertEqual(traces[0], traces[index])
        for trace in traces:
            trace.startswith("Traceback (most recent call last):")
            trace.endswith("Akékoľvek metadáta` is not properly escaped.```")
