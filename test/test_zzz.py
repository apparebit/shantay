"""Tests are loaded and executed in alphabetical order of module names. This
module needs to run last because it further tests the contents of the log."""
from pathlib import Path
import os
import unittest

from shantay.log import LogEntry
from shantay.model import Release


LOG = Path(__file__).parent / "tmp" / "log.log"
PID = os.getpid()

class TestZzz(unittest.TestCase):

    def test_log(self) -> None:
        # The next line tests the parser...
        log = [*LogEntry.parse_file(LOG)]

        worker_entries = 0
        test_entries = 0
        warning = None
        release = Release.of(2024, 3, 14)

        for entry in log:
            if entry.pid != PID:
                worker_entries += 1
            if entry.module == "test.test_pool":
                test_entries += 1
            if entry.level == "WARNING" and warning is None:
                warning = entry
            if release is not None:
                self.assertEqual(entry.message.release(), release)
            if entry.message.has(prefix="summarizing"):
                release = None

        self.assertEqual(worker_entries, 5)
        self.assertEqual(test_entries, 3)

        self.assertIsNotNone(warning)
        self.assertIsNotNone(
            warning.exc_info # pyright: ignore[reportOptionalMemberAccess]
        )
        self.assertIn(
            "Traceback (most recent call last):",
            warning.exc_info # pyright: ignore[reportArgumentType,reportOptionalMemberAccess]
        )
