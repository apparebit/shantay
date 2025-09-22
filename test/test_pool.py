from collections.abc import Iterator
import logging
from pathlib import Path
import unittest

from shantay.pool import Future, Pool, Task


ROOT = Path(__file__).parent
FIXTURE = ROOT / "fixture"
ARCHIVE = FIXTURE / "archive"

# We never copy the parquet files out of staging.
# So we only need ARCHIVE and STAGING.
LOGFILE = ROOT / "tmp" / "log.log"

ONE = "1"
TWO = "2"
THREE = "3"


logger = logging.getLogger(__name__)


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
                if "shantay.pool" in line:
                    break
            self.assertNotEqual(offset, -1)

            # Since the log combines entries written by three processes, their
            # order is mostly non-deterministic. To nonetheless make meaningful
            # assertions about the log entries, we sort them in lexical
            # order.
            lines = sorted(l[l.index("︙", 24) + 1:] for l in lines[offset:])

            length = len(lines)
            self.assertIn(length, (10, 11))

            for index, snippet in enumerate([
                'shantay.pool︙DEBUG︙done processing tasks in pool=',
                'shantay.pool︙DEBUG︙received command="finish" thread="status_manager"',
                'shantay.pool︙DEBUG︙start processing tasks in pool=',
                'shantay.pool︙DEBUG︙submit fn="test.test_pool.task1", pool=',
                'shantay.pool︙DEBUG︙submit fn="test.test_pool.task2", pool=',
                'shantay.pool︙DEBUG︙submit fn="test.test_pool.task3", pool=',
                *(['shantay.pool︙INFO︙initialized worker process pid='] * (length-9)),
                'test.test_pool︙INFO︙task1 processes "1"',
                'test.test_pool︙INFO︙task2 processes "2"',
                'test.test_pool︙INFO︙task3 processes "3"',
            ]):
                self.assertIn(snippet, lines[index ])
