from argparse import ArgumentParser
import datetime as dt
from pathlib import Path

from .sor import DailySoR
from .worker import Worker

if __name__ == "__main__":
    parser = ArgumentParser(prog="datascale")
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
        help="set stop date",
    )
    options = parser.parse_args()

    archive = options.archive if options.archive else Path.cwd() / "dsa-db-archive"
    batches = options.batches if options.batches else Path.cwd() / "dsa-db-batches"

    start = (
        dt.date.fromisoformat(options.start) if options.start
        else dt.date(2023, 9, 25)
    )
    stop = (
        dt.date.fromisoformat(options.stop) if options.stop
        else dt.date.today() - dt.timedelta(days=1)
    )

    worker = Worker(archive, batches)
    worker.prepare()

    release = DailySoR(start)
    while release.date < stop:
        worker.process(release)
        release = next(release)

    Worker.shutdown_all([worker.staging])
