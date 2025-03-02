from argparse import ArgumentParser
from dataclasses import dataclass
import datetime as dt
import logging
from pathlib import Path
import traceback

from .metadata import Metadata, MetadataConflict
from .progress import Progress
from .release import DownloadFailed
from .schedule import Schedule
from .schema import normalize_category
from .sor import DailySoR
from .runner import Task, Runner


@dataclass(frozen=True, slots=True)
class Options:
    task: Task
    category: str
    metadata: Metadata

    archive: Path
    batches: Path
    staging: Path

    start: dt.date
    stop: dt.date

    logfile: Path
    verbose: bool


def _get_options(args: list[str]) -> Options:
    parser = ArgumentParser(prog="shantay")
    parser.add_argument(
        "--archive",
        type=Path,
        help="set the directory for storing downloaded archives"
    )
    parser.add_argument(
        "--batches",
        type=Path,
        help="set the directory for storing extracted category data"
    )
    parser.add_argument(
        "--start",
        help="set the start date"
    )
    parser.add_argument(
        "--stop",
        help="set the stop date (inclusive)",
    )
    parser.add_argument(
        "--logfile",
        default="shantay.log",
        type=Path,
        help="set the file receiving log output"
    )
    parser.add_argument(
        "-v", "--verbose",
        action="store_true",
        help="enable verbose logging, which usually is a good idea"
    )
    parser.add_argument(
        "--category",
        help="set category for extracting data (which may omit STATEMENT_CATEGORY_ prefix "
        "and be written in lower case)",
    )
    parser.add_argument(
        "task",
        choices=["prepare", "analyze"],
        default="prepare",
        help="select the task to execute",
    )

    raw_options = parser.parse_args(args)
    archive = raw_options.archive if raw_options.archive else Path.cwd() / "dsa_db-distributions"
    batches = raw_options.batches if raw_options.batches else Path.cwd() / "dsa_db-data"
    staging = Path.cwd() / "dsa-db-staging"
    staging.mkdir(parents=True, exist_ok=True)

    # Make sure we have a category
    category = None
    if raw_options.category is not None:
        category = normalize_category(raw_options.category)

    metadata = Metadata.merge(staging, batches, not_exist_ok=True)
    if category:
        metadata.set_category(category)
    else:
        category = metadata.category
    if metadata.category is None:
        raise ValueError("cannot determine category, please provide --category option")
    metadata.write_json(staging)

    # Make sure we have start and stop dates.
    if raw_options.task == "prepare":
        start = dt.date(2023, 9, 25)
        stop = dt.date.today() - dt.timedelta(days=2)
    elif 0 < len(metadata):
        start, stop = metadata.coverage
    else:
        start = stop = None

    if raw_options.start is not None:
        start = dt.date.fromisoformat(raw_options.start)
    if raw_options.stop is not None:
        stop = dt.date.fromisoformat(raw_options.stop)

    if start is None:
        raise ValueError("cannot determine start date, please provide --start option")
    if stop is None:
        raise ValueError("cannot determine stop date, please provide --stop option")

    # We have options
    return Options(
        task=Task(raw_options.task),
        category=category,
        metadata=metadata,

        archive=archive,
        batches=batches,
        staging=staging,

        start=start,
        stop=stop,

        verbose=raw_options.verbose,
        logfile=raw_options.logfile,
    )

def _run(args: list[str]) -> None:
    options = _get_options(args)

    logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename=options.logfile,
        encoding="utf8",
        level=logging.DEBUG if options.verbose else logging.INFO,
    )

    schedule = Schedule(DailySoR(options.start), DailySoR(options.stop))

    progress = Progress(row=None)
    runner = Runner(
        archive=options.archive,
        batches=options.batches,
        staging=options.staging,
        metadata=options.metadata,
        progress=progress,
    )
    runner.start(options.task)

    if options.task is Task.PREPARE:
        runner.prepare(schedule, category=options.category)
    elif options.task is Task.ANALYZE:
        runner.analyze(schedule)

    if options.task is Task.PREPARE:
        Metadata.copy_json(options.staging, options.batches)

def run(args: list[str]) -> int:
    # Hide cursor
    print("\x1b[?25l", end="", flush=True)
    try:
        _run(args)
        return 0
    except (DownloadFailed, MetadataConflict) as x:
        print(str(x))
        return 1
    except Exception as x:
        print("".join(traceback.format_exception(x)))
        return 1
    finally:
        # Show cursor again
        print("\x1b[?25h", end="", flush=True)
