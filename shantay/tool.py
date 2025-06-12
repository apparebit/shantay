import datetime as dt
from pathlib import Path
import traceback
from typing import Any

import polars as pl

from .dsa_sor import StatementsOfReasons
from .metadata import fsck, Metadata
from .model import (
    ConfigError, Coverage, DateRange, DownloadFailed, file_stem_for, META_FILE,
    MetadataConflict, Storage
)
from .multiprocessor import Multiprocessor
from .processor import Processor
from .progress import Progress
from .schema import MissingPlatformError, normalize_category, StatementCategory
from .stats import Statistics
from .util import scale_time


def get_configuration(
    options: Any
) -> tuple[Storage, Coverage, Metadata]:
    # Handle --archive, --extract, and --staging options
    storage = Storage(
        archive_root=options.archive,
        extract_root=options.extract,
        staging_root=options.staging if options.staging else Path.cwd() / "dsa-db-staging",
    )

    # Check task-specific conditions
    if options.task == "download":
        if storage.extract_root is not None:
            raise ConfigError(
                "please do not specify --extract directory for `download` task"
            )
        if options.offline:
            raise ConfigError(
                "cannot `download` daily distributions when --offline"
            )
    elif options.task in ("distill", "recover"):
        if storage.extract_root is None:
            raise ConfigError(
                f"please specify --extract directory for `{options.task}` task"
            )

    # Handle --category option
    category = normalize_category(options.category)

    # Handle metadata
    if storage.extract_root is None:
        if category is not None:
            raise ConfigError(
                "please do not specify --category without --extract directory"
            )

        metadata = Metadata.merge(
            storage.staging_root / META_FILE,
            storage.archive_root / META_FILE,
            not_exist_ok=True,
        )

        filestem = "db"
    else:
        try:
            metadata = Metadata.read_json(storage.extract_root / META_FILE)
        except FileNotFoundError:
            metadata = Metadata()

        if metadata.category is None:
            if category is None:
                raise ConfigError(
                    "please specify --category for --extract directory"
                )
            metadata.set_category(category)
        elif category is None:
            category = metadata.category
        elif category != metadata.category:
            raise ConfigError(
                f"--category {category} differs from {metadata.category} "
                "in --extract directory's meta.json"
            )

        filestem = file_stem_for(category)

        try:
            metadata = metadata.merge_with(
                Metadata.read_json(storage.staging_root / f"{filestem}.json")
            )
        except FileNotFoundError:
            pass

    # Make sure staging directory exists and store latest metadata in it
    storage.staging_root.mkdir(parents=True, exist_ok=True)
    metadata.write_json(storage.staging_root / f"{filestem}.json")

    # Handle --first and --last, with the latter including one day for the
    # Americas being a day behind Europe for several hours every day and another
    # two days for posting delays
    earliest = dt.date(2023, 9, 25)
    latest = dt.date.today() - dt.timedelta(days=3)

    if options.first is not None:
        first = dt.date.fromisoformat(options.first)
        if first < earliest:
            raise ConfigError(
                f"{first.isoformat()} is earlier than first possible date 2023-09-25"
            )
    else:
        first = earliest

    if options.last is not None:
        last = dt.date.fromisoformat(options.last)
        if latest < last:
            raise ConfigError(
                f"{last.isoformat()} is later than last possible date {latest.isoformat()}"
            )
    else:
        last = latest

    range = DateRange(first, last)
    if options.task == "visualize":
        range = range.monthlies()
    else:
        range = range.dailies()
    coverage = Coverage.of(range, category)

    # Handle --workers
    if options.workers < 1:
        raise ConfigError(f"worker number must be positive but is {options.workers}")
    if options.task in ("info", "recover", "visualize"):
        options.workers = 1

    # Finish it all up
    return storage, coverage, metadata


def configure_printing() -> None:
    # As of April 2025, the transparency database contains data for 102 platforms
    pl.Config.set_tbl_rows(200)
    pl.Config.set_float_precision(3)
    pl.Config.set_thousands_separator(",")
    pl.Config.set_tbl_cell_numeric_alignment("RIGHT")
    pl.Config.set_fmt_str_lengths(
        max((max(len(s) for s in StatementCategory) // 10 + 2) * 10, 500)
    )
    pl.Config.set_tbl_cols(20)


def _run(options: Any) -> None:
    storage, coverage, metadata = get_configuration(options)
    configure_printing()

    if options.task == "recover":
        fsck(storage.the_extract_root, progress=Progress())
        return

    # Internally, we distinguish between two versions of summarize
    task = options.task
    if task == "summarize":
        if storage.extract_root is None:
            task = "summarize-all"
        else:
            task = "summarize-category"

    if 1 < options.workers:
        dataset = StatementsOfReasons()
        # Since the multiprocessor doesn't do `visualize`, there is no need for
        # stat_source either
        processor = Multiprocessor(
            dataset=dataset,
            storage=storage,
            coverage=coverage,
            metadata=metadata,
            offline=options.offline,
            size=options.workers,
        )
        frame = processor.run(task)
    else:
        # Processor uses an analysis context as necessary internally.
        processor = Processor(
            dataset=StatementsOfReasons(),
            storage=storage,
            coverage=coverage,
            metadata=metadata,
            offline=options.offline,
            progress=Progress(),
        )
        frame = processor.run(task)

    if options.task == "summarize":
        assert frame is not None
        stats = Statistics(f"{coverage.stem()}.parquet", frame)
        print("\n")
        print(stats.summary())

    v, u = scale_time(processor.runtime)
    print(f"\nCompleted task {task} in {v:,.1f} {u}")


def run(options: Any) -> int:
    # Hide cursor
    print("\x1b[?25l", end="", flush=True)
    try:
        _run(options)
        return 0
    except KeyboardInterrupt as x:
        print("".join(traceback.format_exception(x)))
        # Put cursor into bottom right corner of terminal before printing
        print('\x1b[999;999H\n\ninterrupted by user; terminating...')
        return 1
    except MissingPlatformError as x:
        platforms = "platform" if len(x.args[0]) == 1 else "platforms"
        names = ", ".join(f'"{n}"' for n in x.args[0])
        print(f"\x1b[999;999H\n\nSource data contains new {platforms} {names}")
        print("Please rerun shantay with the same command line arguments!")
        return 1
    except (ConfigError, DownloadFailed, MetadataConflict, MissingPlatformError) as x:
        # They are package-specific exceptions and indicate preanticipated
        # errors. Hence, we do not need to print an exception trace.
        print("\x1b[999;999H\n")
        print(str(x))
        return 1
    except Exception as x:
        # For all other exceptions, that most certainly doesn't hold. They are
        # surprising and we need as much information about them as we can get.
        print("\x1b[999;999H\n")
        print("".join(traceback.format_exception(x)))
        return 1
    finally:
        # Show cursor again
        print("\x1b[?25h", end="", flush=True)
