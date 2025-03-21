import datetime as dt
import logging
from pathlib import Path
import shutil
import unittest

from shantay.pool import Pool
from shantay.tool import configure_logging


ROOT = Path(__file__).parent
FIXTURE = ROOT / "fixture"
ARCHIVE = FIXTURE / "archive"

# We never copy the parquet files out of staging.
# So we only need ARCHIVE and STAGING.
STAGING = ROOT / "tmp"
LOGFILE = STAGING / "log.log"
SENTINEL = STAGING / "pool.run"


logger = logging.getLogger(__name__)


def setUpModule():
    # Since the staging directory and log file are shared across test modules,
    # we use per-test-module sentinel files to detect new runs.
    if SENTINEL.exists():
        shutil.rmtree(STAGING)
    STAGING.mkdir(exist_ok=True)
    SENTINEL.write_text(f"{dt.datetime.now()}\n")

    configure_logging(str(LOGFILE), verbose=True)

def tearDownModule():
    pass


def task1(value: str) -> str:
    logger.info('task1 processes "%s"', value)
    return value


def task2(value: str) -> str:
    logger.info('task2 processes "%s"', value)
    return value


def task3(value: str) -> str:
    logger.info('task3 processes "%s"', value)
    return value


class TestPool(unittest.TestCase):

    def test_pool(self) -> None:
        pool = Pool(size=2, log_level=logging.DEBUG)
        future1 = pool.submit(task1, "1")
        future2 = pool.submit(task2, "2")

        future3 = None
        def schedule3(_: object) -> None:
            nonlocal future3
            if future3 is None:
                future3 = pool.submit(task3, "3")

        future1.add_done_callback(schedule3)
        future2.add_done_callback(schedule3)

        with self.subTest("check results"):
            self.assertEqual(future1.result(), "1")
            self.assertEqual(future2.result(), "2")
            assert future3 is not None
            self.assertEqual(future3.result(), "3")

        pool.finish()

        with self.subTest("check log"):
            lines = LOGFILE.read_text("utf8").splitlines(keepends=True)

            offset = -1
            for offset, line in enumerate(reversed(lines)):
                if 'running process pool with 2 processes' in line:
                    break
            self.assertNotEqual(offset, -1)
            offset = len(lines) - offset - 1
            self.assertTrue(offset + 8 <= len(lines))
            lines = lines[offset:offset + 8]
            self.assertIn(
                "adding test.test_pool.task1() to process pool with 0 pending tasks",
                lines[1]
            )
            self.assertIn(
                "adding test.test_pool.task2() to process pool with 1 pending tasks",
                lines[2]
            )

            # The order of the next four lines is largely non-deterministic,
            # except that task1 or task2 must run before task3 can be added and
            # task3 must be added before it can run. We test for these
            # invariants.
            run1 = run2 = add3 = run3 = -1
            for index in range(3, 7):
                line = lines[index]
                if 'task1 processes "1"' in line and run1 == -1:
                    run1 = index
                elif 'task2 processes "2"' in line and run2 == -1:
                    run2 = index
                elif 'task3 processes "3"' in line and run3 == -1:
                    run3 = index
                elif 'adding test.test_pool.task3() to process pool' in line and add3 == -1:
                    add3 = index

            self.assertTrue(run1 < add3 or run2 < add3)
            self.assertTrue(add3 < run3)
            self.assertTrue(run1 == 3 or run2 == 3)
            self.assertTrue(add3 == 4 or add3 == 5)
            self.assertTrue(run3 == 5 or run3 == 6)

            self.assertIn("shutting down process pool on finish", lines[7])
