from argparse import ArgumentParser, RawDescriptionHelpFormatter
import logging
from pathlib import Path
import sys
from typing import Any


def parse_options(args: list[str]) -> Any:
    parser = ArgumentParser(
        prog="shantay",
        formatter_class=RawDescriptionHelpFormatter,
        description="""
        `extract` downloads daily distributions and extracts a category-specific
        subset. It requires `--archive` and `--working` directories. For a newly
        created subset, it also requires the `--category` to extract. That
        category and other metadata are stored in `meta.json`.

        `recover` scans the `--working` directory to validate contents and
        restore (some of the) metadata in `meta.json`.

        `summarize` collects summary statistics either for the full database or
        a category-specific subset, depending on whether `--archive` only (for
        the full database) or both `--archive` and `--working` (for a subset)
        are specified.

        `visualize` generates an HTML document that visualizes summary
        statistics. `--archive` and `--working` again determine the scope of the
        visualization.

        Summary statistics are stored in `all-data.parquet` for the full
        database and in a file named after the category, such as
        `protection-of-minors.parquet`, for category-specific data. The HTML
        document follows the same naming convention; only the extension is
        `.html`.
        """
    )

    group = parser.add_argument_group("data storage")
    group.add_argument(
        "--archive",
        type=Path,
        help="set directory for downloaded archives",
    )
    group.add_argument(
        "--working",
        type=Path,
        help="set directory for parquet files with category-specific data"
    )
    group.add_argument(
        "--staging",
        type=Path,
        help="set directory for temporary files (`./dsa_db-staging` by default)"
    )

    group = parser.add_argument_group("data coverage")
    group.add_argument(
        "--first",
        help="set the start date (2023-09-25 by default)"
    )
    group.add_argument(
        "--last",
        help="set the stop date (three days before Greenwhich's day by default)",
    )
    group.add_argument(
        "--category",
        help="select subset category (may omit the STATEMENT_CATEGORY_ prefix and/or"
        "use lower case); precludes --all-data",
    )
    group.add_argument(
        "--monthly",
        dest="frequency",
        action="store_const",
        const="monthly",
        help="use --monthly, not --daily granularity for visualizing statistics "
        "(the default)"
    )
    group.add_argument(
        "--daily",
        dest="frequency",
        action="store_const",
        const="daily",
        help="use --daily, not --monthly granularity for visualizing statistics"
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
        help="disable verbose logging, which is the default",
    )

    parser.add_argument(
        "--multiproc",
        default=1,
        type=int,
        help="use several processes for downloading archives and extracting working data",
    )
    parser.add_argument(
        "task",
        choices=["extract", "recover", "summarize", "visualize"],
        default="prepare",
        help="select the task to execute",
    )

    return parser.parse_args(args)


def configure_logging(logfile: str, *, verbose: bool) -> None:
    logging.Formatter.default_msec_format = "%s.%03d"
    logging.basicConfig(
        format='%(asctime)s︙%(process)d︙%(name)s︙%(levelname)s︙%(message)s',
        filename=logfile,
        encoding="utf8",
        level=logging.DEBUG if verbose else logging.INFO,
    )


if __name__ == "__main__":
    # Make sure logging is configured before dealing with platform names.
    options = parse_options(sys.argv[1:])
    configure_logging(options.logfile, verbose=options.verbose)
    logger = logging.getLogger(__package__)
    logger.info(
        '▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁'
        '▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁▁'
    )

    # To be fully effective, this function must be invoked before the model,
    # schema, or stats modules have been loaded. That is the case right here.
    from ._platform import sync_web_platforms
    action = sync_web_platforms()
    if action == "disk":
        raise AssertionError(
            "Syncing the platform names only updated the on-disk shantay._platform\n"
            "module, but somehow couldn't update the in-memory representation.\n"
            "Please file a bug report at\n"
            "    https://github.com/apparebit/shantay/issues/new/choose\n\n"
        )

    from .tool import run
    sys.exit(run(options))
