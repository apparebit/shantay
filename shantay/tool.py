from argparse import ArgumentParser
from dataclasses import dataclass
import datetime as dt
from pathlib import Path
import traceback

from .metadata import Metadata
from .progress import Progress
from .release import Schedule
from .sor import DailySoR
from .runner import Task, Runner


@dataclass(frozen=True, slots=True)
class Options:
    task: Task
    archive: Path
    batches: Path
    start: dt.date
    stop: dt.date
    pipelines: int


def _get_options(args: list[str]) -> Options:
    parser = ArgumentParser(prog="datascale")
    parser.add_argument(
        "task",
        choices=["prepare", "analyze"],
        default="prepare",
        help="select task to execute",
    )
    parser.add_argument(
        "--archive",
        type=Path,
        help="set archive directory"
    )
    parser.add_argument(
        "--batches",
        type=Path,
        help="set batches directory"
    )
    parser.add_argument(
        "--start",
        help="set start date"
    )
    parser.add_argument(
        "--stop",
        help="set stop date (inclusive)",
    )
    parser.add_argument(
        "--pipelines",
        default=1,
        type=int,
        help="set the number of parallel pipelines"
    )
    raw_options = parser.parse_args(args)

    return Options(
        task=Task(raw_options.task),
        archive=raw_options.archive if raw_options.archive else Path.cwd() / "dsa-db-archive",
        batches=raw_options.batches if raw_options.batches else Path.cwd() / "dsa-db-batches",
        start=dt.date.fromisoformat(raw_options.start) if raw_options.start
            else dt.date(2023, 9, 25),
        stop=dt.date.fromisoformat(raw_options.stop) if raw_options.stop
            else dt.date.today() - dt.timedelta(days=2),
        pipelines=raw_options.pipelines if 1 <= raw_options.pipelines else 1
    )

def _run(args: list[str]) -> None:
    options = _get_options(args)
    schedule = Schedule(DailySoR(options.start), DailySoR(options.stop))

    progress = Progress(row=None)
    runner = Runner(
        archive=options.archive,
        batches=options.batches,
        progress=progress,
        id=None,
    )
    staging_directories = [runner.staging]

    runner.prepare()
    try:
        for release in schedule.releases():
            if options.task is Task.PREPARE:
                runner.prepare_batches(release)
            elif options.task is Task.ANALYZE:
                runner.analyze_batches(release)
    finally:
        if options.task is Task.PREPARE:
            Metadata.merge(staging_directories, options.batches)

def run(args: list[str]) -> int:
    try:
        _run(args)
        return 0
    except Exception as x:
        print("".join(traceback.format_exception(x)))
        return 1
