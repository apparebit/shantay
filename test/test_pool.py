from collections.abc import Iterator
import datetime as dt
import logging
from pathlib import Path
import shutil
import unittest

from shantay.pool import Future, Pool, Task
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


def tasks() -> Iterator[Task]:
    yield Task(task1, (ONE,), dict())
    yield Task(task2, (TWO,), dict())
    yield Task(task3, (THREE,), dict())


class TestPool(unittest.TestCase):

    def test_pool(self) -> None:
        pool = Pool(size=2, log_level=logging.DEBUG)

        def upon_completion(task: Task, fut: Future) -> None:
            result = fut.result()
            if task.fn is task1:
                self.assertEqual(result, ONE)
            elif task.fn is task2:
                self.assertEqual(result, TWO)
            else:
                self.assertEqual(result, THREE)

        pool.run(tasks(), upon_completion)

        with self.subTest("check log"):
            lines = LOGFILE.read_text("utf8").splitlines(keepends=True)

            offset = -1
            for offset, line in enumerate(lines):
                if 'submit fn=' in line:
                    break
            self.assertNotEqual(offset, -1)

            # Since the log combines entries written by three processes, their
            # order is mostly non-deterministic. For that reason, we only check
            # that all expected lines are present—after lexically sorting the
            # lines. We also account for Python's worker pool not starting all
            # of its workers.
            self.assertTrue(offset + 9 <= len(lines))
            if offset + 10 <= len(lines):
                lines = lines[offset:offset + 10]
                expected_init = 2
            else:
                lines = lines[offset:offset + 9]
                expected_init = 1

            lines = sorted(l[l.index("︙", 24) + 1:] for l in lines)
            for index in range(expected_init):
                self.assertIn("root︙INFO︙initialized worker pool process pid", lines[index])
            self.assertIn('shantay︙DEBUG︙cancelled thread="status_manager"', lines[expected_init])
            self.assertIn('shantay︙DEBUG︙done processing tasks in pool="pool-1"', lines[expected_init + 1])
            self.assertIn('shantay︙DEBUG︙submit fn="test.test_pool.task1", pool="pool-1"', lines[expected_init + 2])
            self.assertIn('shantay︙DEBUG︙submit fn="test.test_pool.task2", pool="pool-1"', lines[expected_init + 3])
            self.assertIn('shantay︙DEBUG︙submit fn="test.test_pool.task3", pool="pool-1"', lines[expected_init + 4])
            self.assertIn('test.test_pool︙INFO︙task1 processes "1"', lines[expected_init + 5])
            self.assertIn('test.test_pool︙INFO︙task2 processes "2"', lines[expected_init + 6])
            self.assertIn('test.test_pool︙INFO︙task3 processes "3"', lines[expected_init + 7])
