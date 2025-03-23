from argparse import ArgumentParser
import datetime as dt
import logging
from pathlib import Path
import traceback
from typing import Any

import polars as pl

from .dsa_sor import StatementsOfReasons
from .framing import format_summary, one_column_summary, resolve_query_binding
from .metadata import fsck, Metadata
from .model import (
    ConfigError, Coverage, DownloadFailed, MetadataConflict, Release, Storage
)
from .multiprocessor import Multiprocessor
from .processor import Processor
from .progress import Progress
from .schema import normalize_category
from .util import scale_time


_logger = logging.getLogger()


def _parse_options(args: list[str]) -> Any:
    parser = ArgumentParser(prog="shantay")

    group = parser.add_argument_group("data storage")
    group.add_argument(
        "--root",
        type=Path,
        help="set directories for `archive` and working `data` to the eponymous subdirectories"
    )
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
        "--filter",
        help="set the module name, colon, and global variable name for the Pola.rs"
        "expression filtering out all but the data of interest",
    )
    group.add_argument(
        "--category",
        help="set category to filter (may omit the STATEMENT_CATEGORY_ prefix and/or"
        "use lower case)",
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

    parser.add_argument(
        "--multiproc",
        default=1,
        type=int,
        help="use several processes for downloading archives and extracting working data",
    )

    parser.add_argument(
        "task",
        choices=["recover", "prepare", "analyze", "visualize"],
        default="prepare",
        help="select the task to execute: recover validates parquet files and restores "
        "metadata; prepare downloads distributions and extracts working data; analyze "
        "processes the working data; visualize graphs the analysis results",
    )

    return parser.parse_args(args)


def get_storage(options: Any) -> Storage:
    archive = options.archive
    working = options.working
    if options.root:
        if not archive:
            archive = options.root / "archive"
        if not working:
            working = options.root / "data"

    return Storage(
        archive_root=archive if archive else Path.cwd() / "dsa-db-archive",
        working_root=working if working else Path.cwd() / "dsa-db-working",
        staging_root=options.staging if options.staging else Path.cwd() / "dsa-db-staging",
    )


def get_configuration(options: Any) -> tuple[Storage, Coverage, Metadata]:
    # Handle --archive, --working, and --staging options
    storage = get_storage(options)

    # Handle --category and --filter options
    if options.category is not None and options.filter is not None:
        raise ConfigError("--category and --filter are mutually exclusive")

    filter_name = filter_value = None
    if options.category is not None:
        filter_name = filter_value = normalize_category(options.category)
    if options.filter is not None:
        filter_name = options.filter
        filter_value = resolve_query_binding(options.filter)

    # Prepare metadata
    metadata = Metadata.merge(storage.staging_root, storage.working_root, not_exist_ok=True)
    if metadata.filter is None:
        if filter_name is None:
            raise ConfigError(
                "no metadata from previous run is available; please specify --category or --filter"
            )
        metadata.set_filter(filter_name)
    elif filter_name is None:
        filter_name = metadata.filter
        if filter_name.startswith("STATEMENT_CATEGORY"):
            filter_value = filter_name
        else:
            filter_value = resolve_query_binding(filter_name)
    elif metadata.filter != filter_name:
        raise ConfigError(
            f'metadata from previous run is incompatible with --category/--filter option'
        )

    storage.staging_root.mkdir(parents=True, exist_ok=True)
    metadata.write_json(storage.staging_root)

    # Handle --first and --last
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
        raise ConfigError("cannot determine first date, please provide --first option")
    if last is None:
        raise ConfigError("cannot determine last date, please provide --last option")

    # Handle --multiproc
    if options.multiproc < 1:
        raise ConfigError(f"process number must be positive but is {options.multiproc}")
    if options.multiproc != 1 and options.task not in ("prepare", "analyze"):
        raise ConfigError("only prepare and analyze support more than one process")

    # Finish it all up
    assert filter_value is not None
    coverage = Coverage(Release.of(first), Release.of(last), filter_value)
    return storage, coverage, metadata


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
    configure_logging(options.logfile, verbose=options.verbose)
    # A very visible horizontal bar to mark a new tool run
    _logger.info('▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂▂')
    _logger.info('')

    # Handle recovery task before getting configuration
    if options.task == "recover":
        storage = get_storage(options)
        fsck(storage.working_root, progress=Progress())
        return

    storage, coverage, metadata = get_configuration(options)

    if options.task in ("prepare", "analyze") and 1 < options.multiproc:
        processor = Multiprocessor(
            dataset=StatementsOfReasons(),
            storage=storage,
            coverage=coverage,
            metadata=metadata,
            size=options.multiproc,
        )
        processor.run(options.task)
    else:
        processor = Processor(
            dataset=StatementsOfReasons(),
            storage=storage,
            coverage=coverage,
            metadata=metadata,
            progress=Progress()
        )

        result = processor.run(options.task)

        if options.task == "prepare":
            Metadata.copy_json(storage.staging_root, storage.working_root)
        elif options.task == "analyze":
            assert isinstance(result, pl.DataFrame)
            print("\n")
            print(format_summary(one_column_summary(result), as_markdown=False))

    v, u = scale_time(processor.runtime)
    print(f"\nCompleted task {options.task} in {v:,.1f} {u}")


def run(args: list[str]) -> int:
    # Hide cursor
    print("\x1b[?25l", end="", flush=True)
    try:
        _run(args)
        return 0
    except KeyboardInterrupt:
        print('\ninterrupted by user; terminating...')
        return 1
    except (ConfigError, DownloadFailed, MetadataConflict) as x:
        # They are package-specific exceptions and indicate preanticipated
        # errors. Hence, we do not need to print an exception trace.
        print(str(x))
        return 1
    except Exception as x:
        # For all other exceptions, that most certainly doesn't hold. They are
        # surprising and we need as much information about them as we can get.
        print("".join(traceback.format_exception(x)))
        return 1
    finally:
        # Show cursor again
        print("\x1b[?25h", end="", flush=True)
