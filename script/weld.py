import argparse
from pathlib import Path
import sys

import polars as pl

from shantay.framing import finalize
from shantay.stats import Statistics


def finish(level: str, msg: str) -> int:
    level = level.upper()
    sys.stderr.write(f"{level}: {msg}\n")
    sys.stderr.flush()
    return 1 if level in ("WARN", "ERROR") else 0


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--fix-schema",
        action="store_true",
        help="ensure that every per-release data frame has the latest schema",
    )
    parser.add_argument(
        "--combine",
        action="store_true",
        help="combine the per-release data frames into one",
    )
    parser.add_argument(
        "--stats-dir",
        type=Path,
        help="the directory with per-release data frames",
    )
    options = parser.parse_args(argv)

    stats_dir = Path(options.stats_dir)
    tmp_dir = stats_dir.with_suffix(".tmp")
    glob = f"*.parquet"

    if stats_dir.suffix != ".stats":
        return finish(
            "ERROR",
            f'directory name "{stats_dir.name}" does not include ".stats" suffix'
        )

    if options.fix_schema:
        tmp_dir.mkdir(exist_ok=True)

        for index, file in enumerate(sorted(stats_dir.glob(glob))):
            stats = Statistics.read(file)
            stats.write(tmp_dir, should_finalize=True)

            sys.stderr.write("∙")
            if index % 80 == 0:
                sys.stderr.write("\n")
            sys.stderr.flush()

        sys.stderr.write("\n")
        sys.stderr.flush()

        return finish(
            "INFO",
            f'saved "{stats_dir.name}/*.parquet" with latest '
            f'schema into "{tmp_dir.name}"'
        )

    if options.combine:
        stats = Statistics.read_all(tmp_dir, file=f"{stats_dir.stem}.parquet")
        stats.write(stats_dir.parent, should_finalize=True)
        return finish(
            "INFO",
            f'combined "{stats_dir.name}/*.parquet" into "{stats.file}"'
        )

    return finish("ERROR", 'provide --fix-schema or --combine')

if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
