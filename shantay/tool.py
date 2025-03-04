from argparse import ArgumentParser
import datetime as dt
import logging
from pathlib import Path
import traceback
from typing import Any

from .dsa_sor import StatementsOfReasons
from .metadata import Metadata
from .model import Coverage, Daily, DownloadFailed, MetadataConflict, Storage
from .processor import Processor
from .progress import Progress
from .schema import normalize_category


def _parse_options(args: list[str]) -> Any:
    parser = ArgumentParser(prog="shantay")

    group = parser.add_argument_group("data storage")
    group.add_argument(
        "--archive",
        type=Path,
        help="set directory for storing downloaded archives (defaults to "
        "`dsa-db-archive` in current working directory)"
    )
    group.add_argument(
        "--working",
        type=Path,
        help="set directory for storing extracted working set (defaults to "
        "`dsa-db-working` in current working directory)"
    )
    group.add_argument(
        "--staging",
        type=Path,
        help="set directory for storing temporary files (`dsa_db-staging` in current "
        "working directory)"
    )

    group = parser.add_argument_group("coverage of working set")
    group.add_argument(
        "--first",
        help="set the start date (defaults to earliest possible date)"
    )
    group.add_argument(
        "--last",
        help="set the stop date (inclusive, defaults to day before yesterday)",
    )
    group.add_argument(
        "--category",
        help="set category to filter for(which may omit STATEMENT_CATEGORY_ prefix "
        "and be written in lower case)",
    )

    group = parser.add_argument_group("logging")
    group.add_argument(
        "--logfile",
        default="shantay.log",
        type=Path,
        help="set file receiving log output (defaults to `shantay.log` in current "
        "working directory)"
    )
    group.add_argument(
        "--quiet",
        dest="verbose",
        action="store_false",
        help="disable verbose logging, which is the default"
    )

    parser.add_argument(
        "task",
        choices=["prepare", "analyze"],
        default="prepare",
        help="select the task to execute",
    )

    return parser.parse_args(args)

def _configure(options: Any) -> tuple[Storage, Coverage, Metadata, Progress]:
    # Set up storage
    storage = Storage(
        archive=options.archive if options.archive else Path.cwd() / "dsa_db-archive",
        working=options.working if options.working else Path.cwd() / "dsa_db-working",
        staging=options.staging if options.staging else Path.cwd() / "dsa_db-staging",
    )

    # Set up filter and metadata
    filter = None
    if options.category is not None:
        filter = normalize_category(options.category)

    metadata = Metadata.merge(storage.staging, storage.working, not_exist_ok=True)
    if filter:
        metadata.set_filter(filter)
    else:
        filter = metadata.filter
    if metadata.filter is None:
        raise ValueError("cannot determine category, please provide --category option")
    storage.staging.mkdir(parents=True, exist_ok=True)
    metadata.write_json(storage.staging)

    # Make sure we have start and stop dates.
    if options.task == "prepare":
        first = dt.date(2023, 9, 25)
        last = dt.date.today() - dt.timedelta(days=2)
    elif 0 < len(metadata):
        first, last = metadata.range
    else:
        first = last = None

    if options.first is not None:
        first = dt.date.fromisoformat(options.first)
    if options.last is not None:
        last = dt.date.fromisoformat(options.last)

    if first is None:
        raise ValueError("cannot determine start date, please provide --first option")
    if last is None:
        raise ValueError("cannot determine stop date, please provide --last option")

    # Finish it all up
    coverage = Coverage(Daily(first), Daily(last), filter)
    progress = Progress()
    return storage, coverage, metadata, progress

def configure_logging(logfile: str, *, verbose: bool) -> None:
    logging.basicConfig(
        format='%(asctime)s [%(levelname)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        filename=logfile,
        encoding="utf8",
        level=logging.DEBUG if verbose else logging.INFO,
    )

def _run(args: list[str]) -> None:
    options = _parse_options(args)
    configure_logging(options.logfile, verbose=options.verbose)
    storage, coverage, metadata, progress = _configure(options)
    processor = Processor(
        dataset=StatementsOfReasons(),
        storage=storage,
        coverage=coverage,
        metadata=metadata,
        progress=progress,
    )
    processor.start(options.task)
    if options.task == "prepare":
        Metadata.copy_json(storage.staging, storage.batches)

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
