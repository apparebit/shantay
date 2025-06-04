from argparse import ArgumentParser, RawDescriptionHelpFormatter
import datetime as dt
import logging
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
from .schema import normalize_category, StatementCategory
from .stats import MissingPlatformError, Statistics
from .util import scale_time


def _parse_options(args: list[str]) -> Any:
    parser = ArgumentParser(
        prog="shantay",
        formatter_class=RawDescriptionHelpFormatter,
        description="""
        `recover` scans the working directory to validate contents and
        restore metadata.

        `prepare` downloads daily distributions that haven't been
        downloaded yet and extracts the working subset.

        `analyze` computes summary statistics about the working
        subset.

        `summarize` downloads daily distributions that haven't been
        downloaded yet and computes summary statistics about the
        entire dataset.

        `visualize` visualizes summary statistics derived from
        working subset or full dataset.

        Since prepare and summarize may download distributions and process
        the complete dataset, they are slow, taking at least half a day.
        By contrast, analyze-working is much faster, taking a few minutes
        only.
        """
    )

    group = parser.add_argument_group("data storage")
    group.add_argument(
        "--archive",
        type=Path,
        help="set directory for downloaded archives (`./dsa-db-archive` by default)",
    )
    group.add_argument(
        "--working",
        type=Path,
        help="set directory for parquet files with working data (`./dsa-db-working` by default)"
    )
    group.add_argument(
        "--staging",
        type=Path,
        help="set directory for temporary files (`./dsa_db-staging` by default)"
    )

    group = parser.add_argument_group("coverage of working set")
    group.add_argument(
        "--first",
        help="set the start date (2023-09-25 by default)"
    )
    group.add_argument(
        "--last",
        help="set the stop date (the day before yesterday by default)",
    )
    group.add_argument(
        "--category",
        help="set category to filter (may omit the STATEMENT_CATEGORY_ prefix and/or"
        "use lower case)",
    )
    group.add_argument(
        "--daily",
        dest="frequency",
        action="store_const",
        const="daily",
        help="use daily aggregates for visualizing statistics; precludes --monthly (default)",
    )
    group.add_argument(
        "--monthly",
        dest="frequency",
        action="store_const",
        const="monthly",
        help="use monthly aggregates for visualizing statistics; precludes --daily",
    )

    group = parser.add_argument_group("logging")
    group.add_argument(
        "--logfile",
        default="shantay.log",
        type=Path,
        help="set file receiving log output (`./shantay.log` by default)",
    )
    group.add_argument(
        "--quiet",
        dest="verbose",
        action="store_false",
        help="disable verbose logging, which is the default"
    )

    group = parser.add_argument_group("source statistics for visualization")
    group.add_argument(
        "--with-archive",
        action="store_true",
        help="visualize the summary statistics stored in the archive root (not working "
        "root)"
    )
    group.add_argument(
        "--with-working",
        action="store_true",
        help="visualize the summary statistics stored in the working root (not archive "
        "root)"
    )

    parser.add_argument(
        "--multiproc",
        default=1,
        type=int,
        help="use several processes for downloading archives and extracting working data",
    )

    parser.add_argument(
        "task",
        choices=["recover", "prepare", "analyze", "summarize", "visualize"],
        default="prepare",
        help="select the task to execute",
    )

    return parser.parse_args(args)


def get_storage(
    options: Any, with_archive: bool = False, with_working: bool = False
) -> Storage:
    if with_archive and options.archive is None:
        raise ConfigError("please provide an --archive path")
    if with_working and options.working is None:
        raise ConfigError("please provide a --working path")

    return Storage(
        archive_root=options.archive,
        working_root=options.working,
        staging_root=options.staging if options.staging else Path.cwd() / "dsa-db-staging",
    )


def get_configuration(
    options: Any
) -> tuple[Storage, Coverage, Metadata, str]:
    # Handle --with-archive and --with-working (part 1)
    if options.with_archive and options.with_working:
        raise ConfigError("--with-archive and --with-working are mutually exclusive")
    if (options.with_archive or options.with_working) and options.task != "visualize":
        raise ConfigError(
            "--with-archive and --with-working control `visualize` task only"
        )
    if options.task == "visualize" and (
        not options.with_archive and not options.with_working
    ):
        if options.working is not None:
            options.with_working = True
        else:
            # If no archive root is specified, we fall back on built-in statistics.
            options.with_archive = True

    # Handle --archive, --working, and --staging options
    storage = get_storage(
        options,
        # If the archive directory doesn't exist, we use the built-in frame.
        with_archive=(options.task in ("prepare", "summarize")),
        with_working=(
            options.task in ("prepare", "analyze")
            or options.task == "visualize" and options.with_working
        ),
    )

    # Handle --category option
    category = normalize_category(options.category)
    if category is not None and options.task in ("summarize", "visualize"):
        raise ConfigError(f'task `{options.task}` does not require a --category ')

    # Prepare metadata
    metadata = Metadata.merge(
        storage.staging_root, storage.working_root, storage.archive_root,
        not_exist_ok=True
    )

    if (
        options.task in ("prepare", "analyze") or
        options.task == "visualize" and options.with_working
    ):
        if metadata.filter is None:
            if category is None:
                raise ConfigError("metadata lacks --category; please specify option")
            metadata.set_filter(category)
        elif category is None:
            category = metadata.filter
        elif metadata.filter != category:
            raise ConfigError(
                f'metadata has --category {metadata.filter} but option is {category}'
            )

    storage.staging_root.mkdir(parents=True, exist_ok=True)
    metadata.write_json(storage.staging_root / META_FILE)

    # Handle --with-archive and --with-working (part 2)
    if options.with_working and category is None:
        raise ConfigError(
            "--with-working requires lacks --category; please specify option"
        )

    # Determine name of file with summary statistics
    if (
        options.task == "summarize" or
        options.task == "visualize" and options.with_archive
    ):
        stats_file = Statistics.DB_STATS_FILE
    else:
        stats_file = Statistics.file_name_for(category)

    # Handle --first and --last
    earliest = dt.date(2023, 9, 25)
    latest = dt.date.today() - dt.timedelta(days=2)

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

    if options.frequency == "daily":
        range = DateRange(first, last).dailies()
    else:
        range = DateRange(first, last).monthlies()
    coverage = Coverage.of(range, category)

    # Handle --multiproc
    if options.multiproc < 1:
        raise ConfigError(f"process number must be positive but is {options.multiproc}")
    if (
        options.multiproc != 1
        and options.task not in ("prepare", "analyze", "summarize")
    ):
        raise ConfigError(
            "only prepare, analyze, and summarize support more than one process"
        )

    # Finish it all up
    return storage, coverage, metadata, stats_file


def configure_printing() -> None:
    # As of April 2025, the transparency database contains data for 102 platforms
    pl.Config.set_tbl_rows(200)
    pl.Config.set_float_precision(3)
    pl.Config.set_thousands_separator(",")
    pl.Config.set_tbl_cell_numeric_alignment("RIGHT")
    pl.Config.set_fmt_str_lengths(
        (max(len(s) for s in StatementCategory) // 10 + 2) * 10
    )
    pl.Config.set_tbl_cols(20)


def configure_logging(logfile: str, *, verbose: bool) -> None:
    logging.Formatter.default_msec_format = "%s.%03d"
    logging.basicConfig(
        format='%(asctime)s︙%(process)d︙%(name)s︙%(levelname)s︙%(message)s',
        filename=logfile,
        encoding="utf8",
        level=logging.DEBUG if verbose else logging.INFO,
    )


def _run(args: list[str]) -> None:
    options = _parse_options(args)
    configure_printing()
    configure_logging(options.logfile, verbose=options.verbose)
    # Instantiate logger only *after* logging has been configured
    logger = logging.getLogger("shantay")
    # A very visible horizontal bar to mark a new tool run
    logger.info(
        '▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁'
        '▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁'
    )
    logger.info('')

    # Handle recovery task before getting configuration
    if options.task == "recover":
        storage = get_storage(options, with_working=True)
        if storage.working_root is None:
            raise ConfigError("cannot recover --working root without its path")
        fsck(storage.working_root, progress=Progress())
        return

    storage, coverage, metadata, stats_file = get_configuration(options)

    if (
        options.task in ("prepare", "analyze", "summarize")
        and 1 < options.multiproc
    ):
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
        frame = processor.run(options.task)
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
        frame = processor.run(options.task)

    if options.task in ("analyze", "summarize"):
        assert frame is not None
        stats = Statistics(stats_file, frame)
        print("\n")
        print(stats.summary())

    v, u = scale_time(processor.runtime)
    print(f"\nCompleted task {options.task} in {v:,.1f} {u}")


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
        platforms = "platform" if len(x.args[2]) == 1 else "platforms"
        names = ", ".join(f'"{n}"' for n in x.args[2])
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
