import atexit
import datetime as dt
import errno
import os
from pathlib import Path
import textwrap
import traceback
from typing import Any, cast

import polars as pl

from .dsa_sor import StatementsOfReasons
from .metadata import fsck, Metadata
from .model import (
    ConfigError, Coverage, DateRange, DownloadFailed, file_stem_for,
    MetadataConflict, StagingIsBusy, Storage
)
from .multiprocessor import Multiprocessor
from .processor import Processor
from .progress import Progress
from .schema import MissingPlatformError, normalize_category, StatementCategory
from .stats import Statistics
from .util import scale_time


_LOCK_FILE = None


def acquire_staging_lock(staging: Path) -> None:
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
        info = f"It was created by process {pid} at {ts}."
    except Exception as x:
        info = x

    if isinstance(info, str):
        print(textwrap.fill(f"""\
The staging root "{staging}" already contains a "staging.lock" file.
{info}
If that process is still running, please use another staging directory.
Otherwise, feel free to delete the lock file and run Shantay again.
"""
        ))
    else:
        print(textwrap.fill(f"""\
The staging root {staging} already contains a "staging.lock" file.
However, trying to read that file failed with an error:
{info}
If that file still exists and some process is still running Shantay,
please use another staging directory. If no process is running Shantay,
you can safely delete the lock file and run Shantay again.
"""
        ))

    raise StagingIsBusy(str(staging))


def get_configuration(
    options: Any
) -> tuple[Storage, Coverage, Metadata]:
    # Handle --archive, --extract, and --staging options
    storage = Storage(
        archive_root=options.archive,
        extract_root=options.extract,
        staging_root=options.staging if options.staging else Path.cwd() / "dsa-db-staging",
    )

    # Acquire lock file
    storage.staging_root.mkdir(parents=True, exist_ok=True)
    acquire_staging_lock(storage.staging_root)

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
    if storage.archive_root is None and storage.extract_root is None:
        if options.task not in ("info", "summarize", "visualize"):
            raise ConfigError(
                f"please specify --archive for `{options.task}` task"
            )
        metadata = Metadata()
        filestem = None
    elif storage.extract_root is None:
        if category is not None:
            raise ConfigError(
                "please do not specify --category without --extract directory"
            )

        metadata = Metadata.merge(
            storage.staging_root / "db.json",
            storage.the_archive_root / "db.json",
            not_exist_ok=True,
        )
        if metadata.category is not None:
            raise ConfigError(
                f'archive metadata really is for category {metadata.category}'
            )

        filestem = "db"
    else:
        try:
            metapath = Metadata.find_file(storage.extract_root)
            metadata = Metadata.read_json(metapath)
        except FileNotFoundError:
            metapath = None
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

        if metapath is not None and filestem != metapath.stem:
            raise ConfigError(
                f'metadata category {category} does not match '
                f'file name "{metapath.name}"'
            )

        # Merge the metadata ...
        try:
            metadata = metadata.merge_with(
                Metadata.read_json(storage.staging_root / f"{filestem}.json")
            )
        except FileNotFoundError:
            pass

        # ... and write out merged metadata
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
    if options.task in ("info", "recover", "visualize") or storage.archive_root is None:
        options.workers = 1

    # Finish it all up
    return storage, coverage, metadata


def configure_printing() -> None:
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
    storage, coverage, metadata = get_configuration(options)
    configure_printing()

    if options.task == "recover":
        fsck(storage.the_extract_root, progress=Progress())
        return

    # Internally, we distinguish between two plus versions of summarize
    task = options.task
    if task == "summarize":
        if storage.archive_root is None:
            task = "summarize-builtin"
        elif storage.extract_root is None:
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

    v, u = scale_time(processor.latency)
    print(f"\nCompleted task {task} in {v:,.1f} {u}")


_HAPPY_STYLE = "\x1b[1;32m"
_ERROR_STYLE = "\x1b[1;41;38;5;255m"
_WARN_STYLE = "\x1b[1;48;5;220;30m"
_RESET_STYLE = "\x1b[m"


def run(options: Any) -> int:
    no_color = os.getenv("NO_COLOR")
    happy = "" if no_color else _HAPPY_STYLE
    error = "" if no_color else _ERROR_STYLE
    warning = "" if no_color else _WARN_STYLE
    reset = "" if no_color else _RESET_STYLE

    # Hide cursor
    print("\x1b[?25l", end="", flush=True)

    try:
        _run(options)
        print(f'\x1b[999;999H\n{happy}Happy, happy, joy, joy!{reset}')
        return 0
    except StagingIsBusy:
        return 1
    except KeyboardInterrupt as x:
        print("".join(traceback.format_exception(x)))
        # Put cursor into bottom right corner of terminal before printing
        print(f'\x1b[999;999H\n\n{warning} Terminated by user {reset}')
        return 1
    except MissingPlatformError as x:
        platforms = "platform" if len(x.args[0]) == 1 else "platforms"
        names = ", ".join(f'"{n}"' for n in x.args[0])
        print(
            f"\x1b[999;999H\n\n{error} Source data contains "
            f"new {platforms} {names} {reset}"
        )
        print("Please rerun shantay with the same command line arguments!")
        return 1
    except (ConfigError, DownloadFailed, MetadataConflict) as x:
        # They are package-specific exceptions and indicate preanticipated
        # errors. Hence, we do not need to print an exception trace.
        print(f"\x1b[999;999H\n{error} {x} {reset}")
        return 1
    except Exception as x:
        # For all other exceptions, that most certainly doesn't hold. They are
        # surprising and we need as much information about them as we can get.
        print(f"\x1b[999;999H\n{error} {x} {reset}")
        print("".join(traceback.format_tb(x.__traceback__)))
        return 1
    finally:
        # Always delete lock file
        if _LOCK_FILE is not None:
            _LOCK_FILE.unlink(missing_ok=True)

        # Show cursor again
        print("\x1b[?25h", end="", flush=True)
