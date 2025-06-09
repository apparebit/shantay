import datetime as dt
from pathlib import Path
import traceback
from typing import Any

import polars as pl

from .dsa_sor import StatementsOfReasons
from .metadata import fsck, Metadata
from .model import (
    ConfigError, Coverage, DateRange, DownloadFailed, META_FILE, MetadataConflict,
    Storage
)
from .multiprocessor import Multiprocessor
from .processor import Processor
from .progress import Progress
from .schema import MissingPlatformError, normalize_category, StatementCategory
from .stats import Statistics
from .util import scale_time


def get_storage(options: Any) -> Storage:
    return Storage(
        archive_root=options.archive,
        working_root=options.working,
        staging_root=options.staging if options.staging else Path.cwd() / "dsa-db-staging",
    )


def get_configuration(
    options: Any
) -> tuple[Storage, Coverage, Metadata, str]:
    # Handle --archive, --working, and --staging options
    storage = get_storage(options)

    # Handle --category option
    category = normalize_category(options.category)

    # Prepare metadata
    metadata = Metadata.merge(
        storage.staging_root, storage.working_root, storage.archive_root,
        not_exist_ok=True
    )

    if metadata.category is None:
        if category is None and storage.working_root is not None:
            raise ConfigError("metadata lacks --category; please specify option")
        metadata.set_category(category)
    elif category is None:
        category = metadata.category
    elif metadata.category != category:
        raise ConfigError(
            f'metadata has --category {metadata.category} but option is {category}'
        )

    storage.staging_root.mkdir(parents=True, exist_ok=True)
    metadata.write_json(storage.staging_root / META_FILE)

    # Determine name of file with summary statistics
    if storage.working_root is None:
        stats_file = Statistics.DB_STATS_FILE
    elif category is None:
        raise ConfigError("metadata lacks --category; please specify option")
    else:
        stats_file = Statistics.file_name_for(category)

    # Handle --first and --last. The last date allows for GMT to be a day ahead
    # and two days delay to post data.
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

    # Handle daily/monthly
    if options.frequency is not None and options.task != "visualize":
        raise ConfigError(
            f"--{options.frequency} can only be used with the `visualize` task"
        )

    if options.frequency == "daily" or options.task != "visualize":
        range = DateRange(first, last).dailies()
    else:
        range = DateRange(first, last).monthlies()
    coverage = Coverage.of(range, category)

    # Handle --multiproc
    if options.multiproc < 1:
        raise ConfigError(f"process number must be positive but is {options.multiproc}")
    if options.multiproc != 1 and options.task not in ("extract", "summarize"):
        raise ConfigError("--multiproc works only with the extract and summarize tasks")

    # Finish it all up
    return storage, coverage, metadata, stats_file


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
    configure_printing()

    # Handle recovery task before getting configuration
    if options.task == "recover":
        storage = get_storage(options)
        if storage.working_root is None:
            raise ConfigError("cannot recover --working root without its path")
        fsck(storage.working_root, progress=Progress())
        return

    storage, coverage, metadata, stats_file = get_configuration(options)

    # Internally, we distinguish between two versions of summarize
    task = options.task
    if task == "summarize":
        if storage.working_root is None:
            task = "summarize-all"
        else:
            task = "summarize-category"

    if 1 < options.multiproc:
        dataset = StatementsOfReasons()
        # Since the multiprocessor doesn't do `visualize`, there is no need for
        # stat_source either
        processor = Multiprocessor(
            dataset=dataset,
            storage=storage,
            coverage=coverage,
            metadata=metadata,
            stats_file=stats_file,
            size=options.multiproc,
        )
        frame = processor.run(task)
    else:
        # Processor uses an analysis context as necessary internally.
        processor = Processor(
            dataset=StatementsOfReasons(),
            storage=storage,
            coverage=coverage,
            metadata=metadata,
            stats_file=stats_file,
            progress=Progress(),
        )
        frame = processor.run(task)

    if options.task == "summarize":
        assert frame is not None
        stats = Statistics(stats_file, frame)
        print("\n")
        print(stats.summary())

    v, u = scale_time(processor.runtime)
    print(f"\nCompleted task {task} in {v:,.1f} {u}")


def run(args: list[str]) -> int:
    # Hide cursor
    print("\x1b[?25l", end="", flush=True)
    try:
        _run(args)
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
