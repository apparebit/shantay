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
    archive: Path
    batches: Path
    start: dt.date
    stop: dt.date
    pipelines: int
    logfile: Path
    verbose: bool


def _get_options(args: list[str]) -> Options:
    parser = ArgumentParser(prog="datascale")
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
        "--pipelines",
        default=1,
        type=int,
        help="set the number of parallel pipelines"
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
        default="protection_of_minors",
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

    return Options(
        task=Task(raw_options.task),
        category=normalize_category(raw_options.category),
        archive=raw_options.archive if raw_options.archive else Path.cwd() / "dsa-db-archive",
        batches=raw_options.batches if raw_options.batches else Path.cwd() / "dsa-db-batches",
        start=dt.date.fromisoformat(raw_options.start) if raw_options.start
            else dt.date(2023, 9, 25),
        stop=dt.date.fromisoformat(raw_options.stop) if raw_options.stop
            else dt.date.today() - dt.timedelta(days=2),
        logfile=raw_options.logfile,
        verbose=raw_options.verbose,
        pipelines=raw_options.pipelines if 1 <= raw_options.pipelines else 1
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

    if 1 < options.pipelines:
        import multiprocessing as mp
        from .runpool import RunPool
        context = mp.get_context("spawn")

        pool = RunPool(
            size=options.pipelines,
            archive=options.archive,
            batches=options.batches,
            schedule=schedule,
            task=options.task,
            context=context,
        )

        staging_directories = [p.staging for p in pool.processes()]
        if options.task is Task.PREPARE:
            Metadata.merge(*staging_directories).write_json(options.batches)
        return

    progress = Progress(row=None)
    runner = Runner(
        archive=options.archive,
        batches=options.batches,
        progress=progress,
    )
    runner.start()
    staging_directories = [runner.staging]

    if options.task is Task.PREPARE:
        runner.prepare(options.category, schedule)
    elif options.task is Task.ANALYZE:
        runner.analyze(schedule)

    if options.task is Task.PREPARE:
        Metadata.merge(*staging_directories).write_json(options.batches)

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
