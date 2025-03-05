from argparse import ArgumentParser
import datetime as dt
from importlib import import_module
import logging
from pathlib import Path
import traceback
from typing import Any

import polars as pl

from .dsa_sor import StatementsOfReasons
from .metadata import Metadata
from .model import (
    ConfigError, Coverage, Daily, DownloadFailed, MetadataConflict, Storage
)
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


def get_configuration(options: Any) -> tuple[Storage, Coverage, Metadata, Progress]:
    # Handle --archive, --working, and --staging options
    storage = Storage(
        archive_root=options.archive if options.archive else Path.cwd() / "dsa_db-archive",
        working_root=options.working if options.working else Path.cwd() / "dsa_db-working",
        staging_root=options.staging if options.staging else Path.cwd() / "dsa_db-staging",
    )

    # Handle --category and --filter options
    if options.category is not None and options.filter is not None:
        raise ConfigError("--category and --filter are mutually exclusive")
    if options.category is not None:
        filter_name = filter_value = normalize_category(options.category)
    if options.filter is not None:
        filter_name = options.filter
        filter_value = _resolve_module_binding(options.filter)

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
        if filter_name.startwith("STATEMENT_CATEGORY"):
            filter_value = filter_name
        else:
            filter_value = _resolve_module_binding(filter_name)
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

    # Finish it all up
    coverage = Coverage(Daily.of(first), Daily.of(last), filter_value)
    progress = Progress()
    return storage, coverage, metadata, progress


def _resolve_module_binding(s: str) -> object:
    module, _, binding = s.partition(":")
    if not module:
        raise ConfigError(f'module binding "{s}" without module (before colon)')
    if not binding:
        raise ConfigError(f'module binding "{s}" without binding (after colon)')

    try:
        m = import_module(module)
    except ImportError:
        raise ConfigError(f'unable to import module for module binding "{s}"')
    try:
        v = getattr(m, binding)
    except AttributeError:
        raise ConfigError(f'attribute not found for module binding "{s}"')
    if not isinstance(v, pl.Expr):
        raise ConfigError(f'value of module binding "{s}" is not a Pola.rs expression')
    return v


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
    storage, coverage, metadata, progress = get_configuration(options)

    processor = Processor(
        dataset=StatementsOfReasons(),
        storage=storage,
        coverage=coverage,
        metadata=metadata,
        progress=progress,
    )
    processor.start(options.task)
    if options.task == "prepare":
        Metadata.copy_json(storage.staging_root, storage.working_root)


def run(args: list[str]) -> int:
    # Hide cursor
    print("\x1b[?25l", end="", flush=True)
    try:
        _run(args)
        return 0
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
