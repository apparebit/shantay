from argparse import ArgumentParser, RawDescriptionHelpFormatter
from collections.abc import Sequence
import logging
from pathlib import Path
import shutil
import sys
from typing import Any


_WIDTH = None

def _formatter(**kwargs) -> Any:
    # ArgumentParser repeatedly instantiates the class with the program name
    # only, even though the base HelpFormatter supports more options, including
    # one to limit the width. We inject that option in this wrapper.
    global _WIDTH
    if _WIDTH is None:
        _WIDTH = shutil.get_terminal_size().columns
    return RawDescriptionHelpFormatter(**kwargs, width=min(_WIDTH, 80))


def get_parser(style: Any) -> ArgumentParser:
    parser = ArgumentParser(
        prog="shantay",
        formatter_class=_formatter,
        description="""

supported tasks:
  `download` makes sure that daily distributions are locally available,
  retrieving them as necessary. This task lets your prepare for future
  `--offline` operation by downloading archives as expediently as possible
  and not performing any other processing.

  `distill` extracts a subset from the full database. It requires
  `--archive` and `--extract` directories. For a newly created extract
  directory, it also requires a `--category`, `--platform`, or `--filter`.

  `recover` scans the `--extract` directory to validate contents and
  restore (some of the) metadata in `meta.json`.

  `summarize` collects summary statistics for the full database or some
  subset, depending on whether only `--archive` (for the full database) or
  both `--archive` and `--extract` (for a subset) are specified.

  `info` displays helpful information about Shantay, critical
  dependencies, the Python interpreter, the operating system, as well as
  the contents of the `--archive` and `--extract` directories.

  `visualize` generates an HTML document that visualizes summary
  statistics. `--archive` and `--extract` again determine the scope of the
  visualization.

  Summary statistics are stored in `db.parquet` for the full database and
  in a file named after the distillation filter otherwise. For example,
  `protection-of-minors.parquet` stores statistics for the
  `STATEMENT_CATEGORY_PROTECTION_OF_MINORS` category. The corresponding
  metadata is stored in a JSON file in the same directory. The same naming
  convention applies to the JSON metadata and HTML visualizations.\
        """
    )

    group = parser.add_argument_group("data storage")
    group.add_argument(
        "--archive",
        type=Path,
        help="set directory for downloaded archives (required)",
    )
    group.add_argument(
        "--extract",
        type=Path,
        help="set directory for parquet files with distilled data (optional)"
    )
    group.add_argument(
        "--staging",
        type=Path,
        help="set directory for temporary files (default: `./dsa_db-staging`)"
    )

    group = parser.add_argument_group("data coverage")
    group.add_argument(
        "--first",
        help="set the start date (default: 2023-09-25)"
    )
    group.add_argument(
        "--last",
        help="set the stop date (default: three days before today)",
    )
    group.add_argument(
        "--category",
        help="select statement category for extract (optional; may omit "
        "`STATEMENT_CATEGORY_` prefix and/or use lower-case)",
    )
    group.add_argument(
        "--platform",
        action="append",
        help="select platforms for extract or visualization (optional; may "
        "be repeated)",
    )
    group.add_argument(
        "--filter",
        help="provide Pola.rs filter expression for extract (optional)",
    )
    group.add_argument(
        "--stratify-by-category",
        action="store_true",
        help="break down summary statistics by category in addition to platform"
    )
    group.add_argument(
        "--stratify-all-text",
        action="store_true",
        help="include value counts for all text-valued columns in the summary "
        "statistics"
    )

    group = parser.add_argument_group("resource requirements")
    group.add_argument(
        "--offline",
        action="store_true",
        help="make do with already downloaded database releases (optional)",
    )
    group.add_argument(
        "--workers",
        default=1,
        type=int,
        help="use the given number of worker processes (default: 1)",
    )

    group = parser.add_argument_group("output")
    group.add_argument(
        "--logfile",
        default="shantay.log",
        type=Path,
        help="set file receiving log output (default: `./shantay.log`)",
    )
    group.add_argument(
        "--quiet",
        dest="verbose",
        action="store_false",
        help="disable verbose logging (optional)",
    )
    group.add_argument(
        "--interactive-report",
        action="store_true",
        help="dynamically generate interactive charts with JavaScript "
        "instead of embedding SVG",
    )
    group.add_argument(
        "--clamp_outliers",
        action="store_true",
        help="if one or two months have more SoRs than the rest, clamp "
        "those outliers"
    )

    parser.add_argument(
        "task",
        choices=["info", "download", "distill", "recover", "summarize", "visualize"],
        help="select the task to execute",
    )

    return parser


def configure_logging(logfile: str, *, verbose: bool) -> None:
    logging.Formatter.default_msec_format = "%s.%03d"
    logging.basicConfig(
        format='%(asctime)s︙%(process)d︙%(name)s︙%(levelname)s︙%(message)s',
        filename=logfile,
        encoding="utf8",
        level=logging.DEBUG if verbose else logging.INFO,
    )


def main(argv: Sequence[str]) -> None:
    # Handle command line options
    from .color import Style
    parser = get_parser(Style)
    options = parser.parse_args(argv)
    if options.task is None:
        parser.print_help()
        sys.exit(1)

    # Configure logging, since sync_web_platforms writes to the log
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
            "Updated the platform names in `~/.shantay/platforms.json`,\n"
            "but could not update their in-memory representation.\n"
            "Please file a bug report at\n"
            "    https://github.com/apparebit/shantay/issues/new/choose\n\n"
        )

    from .tool import run
    sys.exit(run(options))


if __name__ == "__main__":
    main(sys.argv[1:])
