from argparse import ArgumentParser
from dataclasses import dataclass
import datetime as dt
from pathlib import Path

from .progress import Progress
from .sor import DailySoR
from .worker import Schedule, Task, Worker


@dataclass(frozen=True, slots=True)
class Options:
    task: Task
    archive: Path
    batches: Path
    start: dt.date
    stop: dt.date


def get_options(args: list[str]) -> Options:
    parser = ArgumentParser(prog="datascale")
    parser.add_argument(
        "task",
        choices=["prepare", "process"],
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
    raw_options = parser.parse_args(args)

    return Options(
        task=Task.of(raw_options.task),
        archive=raw_options.archive if raw_options.archive else Path.cwd() / "dsa-db-archive",
        batches=raw_options.batches if raw_options.batches else Path.cwd() / "dsa-db-batches",




    )


    if options.task == "prepare":
        task = Task.PREPARE_BATCHES
    elif options.task == "process":
        task = Task.PROCESS_BATCHES
    else:
        raise ValueError(f"unknown task {options.task}")


    start = (
        dt.date.fromisoformat(options.start) if options.start
        else dt.date(2023, 9, 25)
    )
    stop = (
        dt.date.fromisoformat(options.stop) if options.stop
        else dt.date.today() - dt.timedelta(days=1)
    )
    schedule = Schedule(DailySoR(start), DailySoR(stop))

    progress = Progress(row=None)
    worker = Worker(
        archive,
        batches,
        progress=progress,
        id=None,
        schedule=schedule,
        task=task,
    )
    staging_directories = [worker.staging]

    worker.prepare()
    worker.run()

    Worker.shutdown_all(staging_directories, batches)
