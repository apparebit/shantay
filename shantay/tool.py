import atexit
import datetime as dt
import errno
import os
from pathlib import Path
import re
import shutil
import textwrap
import traceback
from typing import Any, cast

import polars as pl

from .color import Style
from .dsa_sor import StatementsOfReasons
from .metadata import fsck, Metadata
from .model import (
    Config, ConfigError, CONFIG_OPTIONS, DateRange, DownloadFailed, Filter,
    MetadataConflict, ReleaseRange, StagingIsBusy, Storage
)
from .multiprocessor import Multiprocessor
from .processor import Processor
from .progress import NO_PROGRESS, LinePrinter, Progress
from .schema import MissingPlatformError, PlatformLookupTable, StatementCategory
from .stats import Statistics
from .util import scale_time


_LOCK_FILE = None

def acquire_staging_lock(staging: Path) -> None:
    """
    Acquire the file system lock in the staging directory. Or die trying.
    Arguably, we should do the same for the archive and extract roots, since
    Shantay may very well write to them. But that seems less urgent than the
    staging directory, which has a well-known default.
    """
    global _LOCK_FILE

    # Acquire lock file for staging
    path = staging / f"staging.lock"
    try:
        fd = os.open(str(path), os.O_CREAT | os.O_EXCL | os.O_RDWR)
        with os.fdopen(fd, "a") as file:
            file.write(f"{os.getpid()}@{dt.datetime.now().isoformat()}")
    except OSError as x:
        if x.errno != errno.EEXIST:
            raise

        # Continue after the try/except/else.
    else:
        _LOCK_FILE = path
        atexit.register(lambda: cast(Path, _LOCK_FILE).unlink(missing_ok=True))
        return

    try:
        with open(str(path), mode="r", encoding="utf8") as file:
            provenance = file.read()
        pid, _, ts = provenance.strip().partition("@")
        info = f"process {pid} at {ts}"
    except Exception as x:
        info = x

    if isinstance(info, str):
        print(textwrap.fill(f"""\
The staging root "{staging}" contains a "staging.lock" file created by {info}.
{Style.BOLD}You can safely delete the lock file and run Shantay again---as long
as that process stopped running.{Style.RESET}
"""
        ))
    else:
        print(textwrap.fill(f"""\
The staging root "{staging}" contains a "staging.lock" file. However, trying to
read that file results in an {info} error. {Style.BOLD}You can safely delete the
lock file and run Shantay again---as long as no other instance of the tool is
running.{Style.RESET}
"""
        ))

    raise StagingIsBusy(str(staging))


def get_configuration(
    options: Any
) -> tuple[Storage, ReleaseRange, Metadata, Config]:
    """
    Turn the command line options into internal configuration objects.
    """
    # Handle --archive, --extract, and --staging options
    storage = Storage(
        archive_root=options.archive,
        extract_root=options.extract,
        staging_root=(
            options.staging if options.staging else Path.cwd() / "dsa-db-staging"
        )
    )

    # Acquire lock file
    storage.staging_root.mkdir(parents=True, exist_ok=True)
    acquire_staging_lock(storage.staging_root)

    # Clean up done staging
    pattern = re.compile(fr"^{storage.staging_root.name}[.]\d+[.]done$")
    for path in storage.staging_root.parent.glob("*.done"):
        if pattern.match(path.name):
            shutil.rmtree(path, ignore_errors=True)

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

    # Handle --category, --platform, and --filter options
    platforms = None
    if options.platform is not None:
        if options.category is not None or options.filter is not None:
            raise ConfigError(
                f"--category, --platform, and --filter are mututually exclusive"
            )
        # Validate platforms and resolve to canonical form
        platforms = []
        for platform in options.platform:
            resolved_platform = PlatformLookupTable.get(platform.casefold())
            if resolved_platform is None:
                raise ConfigError(f'--platform "{platform}" is unknown')
            platforms.append(resolved_platform)

        filter = Filter.with_platforms(*platforms)
    elif options.category is not None:
        if options.filter is not None:
            raise ConfigError(
                f"--category, --platform, and --filter are mututually exclusive"
            )
        filter = Filter.with_category(options.category)
    elif options.filter is not None:
        filter = Filter.with_expression(options.filter)
    else:
        filter = None

    if filter is not None and storage.extract_root is None:
        raise ConfigError(
            "please do not specify --category, --platform, or --filter "
            "without --extract directory or `visualize` task"
        )

    # Handle metadata
    if storage.archive_root is None and storage.extract_root is None:
        if options.task not in ("info", "visualize"):
            raise ConfigError(
                f"please specify --archive for `{options.task}` task"
            )
        metadata = Metadata("builtin")
    else:
        try:
            metapath =  Metadata.find_file(storage.best_root)
            metadata = Metadata.read_json(metapath)
        except FileNotFoundError:
            if storage.extract_root is not None and filter is None:
                raise ConfigError(
                    "please specify --category, --platform, or --filter "
                    "for --extract directory"
                )
            stem = "db" if storage.extract_root is None else storage.extract_root.stem
            metadata = Metadata(stem, filter)

        try:
            metadata_too = Metadata.read_json(
                storage.staging_root / f"{metadata.stem}.json"
            )
        except FileNotFoundError:
            pass
        else:
            metadata = metadata.merge_with(metadata_too)

        if (
            storage.extract_root is not None
            and filter is not None
            and filter != metadata.filter
            and (options.platform is None or options.task == "visualize")
        ):
            raise ConfigError(
                f"--category, --platform, or --filter {filter} differs "
                f"from metadata {metadata.filter}"
            )

        metadata.write_json(storage.staging_root / f"{metadata.stem}.json")

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
                f"{last.isoformat()} is later than last "
                f"possible date {latest.isoformat()}"
            )
    else:
        last = latest

    date_range = DateRange(first, last)
    if options.task == "visualize":
        range = date_range.monthlies()
    else:
        range = date_range.dailies()

    # Handle --workers
    if options.workers < 0:
        raise ConfigError(
            f"worker number must be non-negative but is {options.workers}"
        )
    if options.task in ("info", "recover", "visualize") or storage.archive_root is None:
        options.workers = 0

    if options.interactive_report and options.task != "visualize":
        raise ConfigError("please only use --interactive-report with `visualize` task")
    if options.clamp_outliers and options.task != "visualize":
        raise ConfigError("please only use --clamp-outliers with `visualize` task")

    # Instantiate the config object
    config = Config(
        progress=True,
        platforms=platforms if platforms is None else tuple(platforms),
        max_tasks=90 if 0 < options.workers else None,
        **{n: getattr(options, n) for n in CONFIG_OPTIONS}
    )

    # Finish it all up
    return storage, range, metadata, config


def configure_printing() -> None:
    """
    Configure Pola.rs to print more columns, more rows, and longer strings, also
    include a thousands separator, and align numeric cells to the right.
    """
    # As of April 2025, the transparency database contains data for 102
    # platforms, which define around 600 other reasons for moderating
    # visibility.
    pl.Config.set_tbl_rows(1_000)
    pl.Config.set_float_precision(3)
    pl.Config.set_thousands_separator(",")
    pl.Config.set_tbl_cell_numeric_alignment("RIGHT")
    pl.Config.set_fmt_str_lengths(
        max((max(len(s) for s in StatementCategory) // 10 + 2) * 10, 500)
    )
    pl.Config.set_tbl_cols(20)


def _run(options: Any) -> None:
    storage, range, metadata, config = get_configuration(options)
    configure_printing()

    if options.task == "recover":
        fsck(storage.the_extract_root)
        return

    # Determine the progress tracker
    if not config.progress:
        progress = NO_PROGRESS
    elif 2 <= options.verbose or "CI" in os.environ:
        # Level 2 prints a lot, which makes a progress bar difficult to render
        progress = LinePrinter()
    else:
        progress = Progress()

    # Internally, we distinguish between two plus versions of summarize
    task = options.task
    if task == "summarize":
        if storage.archive_root is None:
            task = "summarize-builtin"
        elif storage.extract_root is None:
            task = "summarize-all"
        else:
            task = "summarize-extract"

    processor = (Processor if config.workers == 0 else Multiprocessor)(
        dataset=StatementsOfReasons(),
        storage=storage,
        coverage=range,
        config=config,
        metadata=metadata,
        progress=progress,
    )
    frame = processor.run(task)

    if options.task == "summarize":
        assert frame is not None
        stats = Statistics(f"{metadata.stem}.parquet", frame)
        print("\n")
        print(stats.summary())

    v, u = scale_time(processor.latency)
    print(f"\nCompleted task {task} in {v:,.1f} {u}")


def run(options: Any) -> int:
    """Run Shantay with the given options and return the appropriate
    exit code."""
    print(Style.HIDE_CURSOR, end="", flush=True)

    try:
        _run(options)
        print(f'{Style.EOS}\n{Style.HAPPY}Happy, happy, joy, joy!{Style.RESET}')
        return 0
    except StagingIsBusy:
        return 1
    except KeyboardInterrupt as x:
        print("".join(traceback.format_exception(x)))
        # Put cursor into bottom right corner of terminal before printing
        print(f'{Style.EOS}\n\n{Style.WARN} Terminated by user {Style.RESET}')
        return 1
    except MissingPlatformError as x:
        # Actually force an update of the platform names
        from ._platform import predate_platforms
        predate_platforms()

        platforms = "platform" if len(x.args[0]) == 1 else "platforms"
        names = ", ".join(f'"{n}"' for n in x.args[0])
        print(
            f"{Style.EOS}\n\n{Style.ERROR} Source data contains "
            f"new {platforms} {names} {Style.RESET}"
        )
        print(
            f"{Style.BOLD}Please rerun shantay with the same command line "
            f"arguments!{Style.RESET}"
        )
        return 1
    except (ConfigError, DownloadFailed, MetadataConflict, FileNotFoundError) as x:
        # They are package-specific exceptions and indicate preanticipated
        # errors. Hence, we do not need to print an exception trace.
        print(f"{Style.EOS}\n{Style.ERROR} {x} {Style.RESET}")
        return 1
    except Exception as x:
        # For all other exceptions, that most certainly doesn't hold. They are
        # surprising and we need as much information about them as we can get.
        print(f"{Style.EOS}\n{Style.ERROR} {x} {Style.RESET}")
        print("".join(traceback.format_tb(x.__traceback__)))
        print(
            f"{Style.BOLD}Shantay's log in \"{options.logfile}\" may contain "
            f"further information{Style.RESET}"
        )
        return 1
    finally:
        # Always delete lock file
        if _LOCK_FILE is not None:
            _LOCK_FILE.unlink(missing_ok=True)

        # Show cursor again
        print(Style.SHOW_CURSOR, end="", flush=True)
