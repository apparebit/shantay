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

ONE = "1"
TWO = "2"
THREE = "3"


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
        future1 = pool.submit(task1, ONE)
        future2 = pool.submit(task2, TWO)

        future3 = None
        def schedule3(_: object) -> None:
            nonlocal future3
            if future3 is None:
                future3 = pool.submit(task3, THREE)

        future1.add_done_callback(schedule3)
        future2.add_done_callback(schedule3)

        # Check task results, which also waits for task completion
        with self.subTest("check results"):
            self.assertEqual(future1.result(), ONE)
            self.assertEqual(future2.result(), TWO)
            assert future3 is not None
            self.assertEqual(future3.result(), THREE)

        pool.finish()

        with self.subTest("check log"):
            lines = LOGFILE.read_text("utf8").splitlines(keepends=True)

            offset = -1
            for offset, line in enumerate(lines):
                if 'submit fn=' in line:
                    break
            self.assertNotEqual(offset, -1)

            # The order of log lines is mostly non-deterministic, since the log
            # combines entries written by three different processes, two of
            # which forward entries to the third. For that reason, we only check
            # that all the expected lines appear in the log. Annoyingly,
            # Python's worker pool doesn't always start all its workers. So we
            # have to account for that, too.
            self.assertTrue(offset + 9 <= len(lines))
            if offset + 10 <= len(lines):
                lines = lines[offset:offset + 10]
                expected_init = 2
            else:
                lines = lines[offset:offset + 9]
                expected_init = 1

            init = 0
            submit = 0
            run1 = run2 = run3 = shutdown = cancel = -1
            for index, line in enumerate(lines[offset:offset + 10]):
                if 'initialized worker pool process pid=' in line:
                    init += 1
                elif (
                    'submit fn="test.test_pool.task' in line
                    and 'pool="pool-1", pending-tasks' in line
                ):
                    submit +=1
                elif 'task1 processes "1"' in line and run1 == -1:
                    run1 = index
                elif 'task2 processes "2"' in line and run2 == -1:
                    run2 = index
                elif 'task3 processes "3"' in line and run3 == -1:
                    run3 = index
                elif (
                    'shut down pool="pool-1", cause="finish()"' in line
                    and shutdown == -1
                ):
                    shutdown = index
                elif 'cancelled thread="status_manager"' in line and cancel == -1:
                    cancel = index
                else:
                    raise AssertionError(f"unexpected log line '{line}'")

            self.assertEqual(init, expected_init)
            self.assertEqual(submit, 3)
            self.assertNotEqual(run1, -1)
            self.assertNotEqual(run2, -1)
            self.assertNotEqual(run3, -1)
            self.assertNotEqual(shutdown, -1)
            self.assertNotEqual(cancel, -1)
