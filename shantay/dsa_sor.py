from collections import Counter
import csv
import hashlib
import logging
from pathlib import Path

import polars as pl

from .collector import Collector
from .model import (
    Coverage, Daily, Dataset, KEYWORDS_FILE, MetadataEntry, PLATFORMS_FILE, Release,
    STATS_FILE
)
from .progress import NO_PROGRESS, Progress
from .schema import (
    BASE_SCHEMA, ContentLanguageType, ContentType, CountryGroups, DecisionVisibility,
    Keyword, SCHEMA, SCHEMA_OVERRIDES, StatementCategory, TerritorialScopeType
)
from .util import annotate_error


_logger = logging.getLogger(__package__)


class StatementsOfReasons(Dataset[Daily]):

    @property
    def name(self) -> str:
        return "EU-DSA-SoR-DB"

    def url(self, filename: str) -> str:
        return f"https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/{filename}"

    def archive_name(self, release: Daily) -> str:
        return f"sor-global-{release.id}-full.zip"

    def digest_name(self, release: Daily) -> str:
        return f"{self.archive_name(release)}.sha1"

    @property
    def extract_data_step_count(self) -> int:
        return 12

    def extract_data_step_number(self, index: int, step: int) -> int:
        return (self.extract_data_step_count + 1 ) * index + step

    @annotate_error(filename_arg="root")
    def extract_file_data(
        self,
        *,
        root: Path,
        release: Daily,
        index: int,
        name: str,
        filter: str | pl.Expr,
        progress: Progress = NO_PROGRESS
    ) -> tuple[str, Counter]:
        path = root / release.temp_directory
        csv_files = f"{path}/sor-global-{release.id}-full-{index:05}-*.csv"

        total_rows, total_rows_with_keywords = self._extract_row_counts(
            csv_files, index, name, progress
        )
        frame = self._extract_filtered_rows(csv_files, index, name, filter, progress)

        self._validate_schema(frame)
        path = root / release.directory
        path.mkdir(parents=True, exist_ok=True)
        path = path / release.batch_file(index)

        # Write the parquet file and immediately read it again to compute
        # digest. Experiments with a large file suggest that this performs at
        # least as well as intercepting writes for computing the digest.
        frame.write_parquet(path)
        with open(path, mode="rb") as file:
            digest = hashlib.file_digest(file, "sha256").hexdigest()

        return digest, self._assemble_frame_counters(frame, total_rows, total_rows_with_keywords)

    def _extract_row_counts(
        self, csv_files: str, index: int, name: str, progress: Progress = NO_PROGRESS
    ) -> tuple[int, int]:
        """
        Determine number of rows and rows with keywords across all CSV files in
        the batch.
        """
        progress.step(self.extract_data_step_number(index, 1), extra="count rows")
        rows, rows_with_keywords = (
            pl.scan_csv(csv_files, infer_schema=False)
            .select(
                pl.len(),
                # Minimum length of 3 bytes accounts for "[]"
                (2 < pl.col("category_specification").str.len_bytes()).sum(),
            )
            .collect()
            .row(0)
        )
        _logger.debug('counted filter="none", rows=%d, file="%s"', rows, name)
        _logger.debug(
            'counted filter="with_keywords", rows=%d, file="%s"',
            rows_with_keywords, name
        )
        return rows, rows_with_keywords

    def _extract_filtered_rows(
        self,
        csv_files: str,
        index: int,
        name: str,
        filter: str | pl.Expr,
        progress: Progress = NO_PROGRESS
    ) -> pl.DataFrame:
        """
        Extract rows with the filter applied across all CSV files in the batch.
        This method first does the expedient thing and tries to process all CSV
        files in one Polars operation. If that fails, it tries again, processing
        one CSV file at a time, first with Polars and then with Python's
        standard library.
        """
        # Fast path: Process several CSV files in one lazy Polars operation
        progress.step(self.extract_data_step_number(index, 2), extra="extracting working data")
        try:
            frame = self.finish_frame(
                self._scan_csv_with_polars(csv_files, filter)
            ).collect()
            _logger.debug(
                'extracted rows=%d, using="Pola.rs with glob", file="%s"',
                frame.height, name
            )
            return frame
        except Exception as x:
            _logger.warning(
                'failed to read CSV using="Pola.rs with glob", file="%s"', name, exc_info=x
            )

        # Slow path: Process each CSV file by itself, trying first with the same
        # lazy Polars operation and falling back onto Python's CSV module.
        split = csv_files.rindex("/")
        path = Path(csv_files[:split])
        glob = csv_files[split + 1:]

        files = sorted(path.glob(glob))
        assert 0 < len(files), f'glob "{csv_files}" matches no files'

        frames = []
        for file_no, file_path in enumerate(files):
            progress.step(
                self.extract_data_step_number(index, 2 + file_no), extra=f"extracting {file_path.name}"
            )

            try:
                frame = self.finish_frame(
                    self._scan_csv_with_polars(file_path, filter)
                ).collect()
                frames.append(frame)

                _logger.debug(
                    'extracted rows=%d, using="Pola.rs", file="%s"',
                    frame.height, file_path.name
                )
                continue
            except:
                _logger.warning('failed to read CSV using="Pola.rs", file="%s"', file_path.name)

            try:
                frame = self.finish_frame(
                    self._read_csv_row_by_row(file_path, filter).lazy()
                ).collect()
                frames.append(frame)

                _logger.debug(
                    'extracted rows=%d, using="Python\'s CSV module", file="%s"',
                    frame.height, file_path.name
                )
            except Exception as x:
                _logger.error(
                    'failed to parse using="Python\'s CSV module", file="%s"',
                    file_path.name, exc_info=x
                )
                raise

        return pl.concat(frames, how="vertical")

    def _scan_csv_with_polars(self, path: str | Path, filter: str | pl.Expr) -> pl.LazyFrame:
        """
        Read one or more CSV files with Polars' CSV reader, while also applying
        the filter.

        The path string may include a wildcard to read more than one CSV file at
        the same time. The returned LazyFrame has not been collect()ed.
        """
        if isinstance(filter, str):
            filter = (
                (pl.col("category") == filter)
                | pl.col("category_addition").str.contains(filter, literal=True)
            )

        return pl.scan_csv(
            str(path),
            null_values=["", "[]"],
            schema_overrides=SCHEMA_OVERRIDES,
            infer_schema=False,
        ).filter(filter)

    def _read_csv_row_by_row(self, path: str | Path, filter: str | pl.Expr) -> pl.DataFrame:
        """
        Read a CSV file using Python's CSV reader row by row, while also
        applying the filter.
        """
        has_category = isinstance(filter, str)
        header = None
        rows = []

        with open(path, mode="r", encoding="utf8") as file:
            # Per Python documentation, quoting=csv.QUOTE_NOTNULL should turn
            # empty fields into None. The source code suggests the same.
            # https://github.com/python/cpython/blob/630dc2bd6422715f848b76d7950919daa8c44b99/Modules/_csv.c#L655
            # Alas, it doesn't seem to work.
            reader = csv.reader(file)
            header = next(reader)

            if has_category:
                category_index = header.index("category")
                addition_index = header.index("category_addition")
                if category_index < 0:
                    raise ValueError(f'"{path}" does not include "category" column')
                if addition_index < 0:
                    raise ValueError(f'"{path}" does not include "category_addition" column')

                predicate = (
                    lambda row: row[category_index] == filter or filter in row[addition_index]
                )
            else:
                predicate = lambda _row: True

            for row in reader:
                if predicate(row):
                    row = [None if field in ("", "[]") else field for field in row]
                    rows.append(row)

        frame = pl.DataFrame(list(zip(*rows)), schema=BASE_SCHEMA)
        return frame if has_category else frame.filter(filter)

    def finish_frame(self, frame: pl.LazyFrame) -> pl.LazyFrame:
        """
        Finish the frame by patching in the names of country groups, parsing
        list-valued columns, as well as casting list elements and date columns
        to their types. This method does not collect lazy frames.
        """
        return (
            frame
            # Patch in the names of country groups
            .with_columns(
                pl.when(pl.col("territorial_scope") == CountryGroups.EEA)
                    .then(pl.lit("[\"EEA\"]"))
                    .when(pl.col("territorial_scope") == CountryGroups.EEA_no_IS)
                    .then(pl.lit("[\"EEA_no_IS\"]"))
                    .when(pl.col("territorial_scope") == CountryGroups.EU)
                    .then(pl.lit("[\"EU\"]"))
                    .otherwise(pl.col("territorial_scope"))
                    .alias("territorial_scope"),
            )
            # Parse list-valued columns (assumes no [] values)
            .with_columns(
                pl.col(
                    "decision_visibility",
                    "category_addition",
                    "category_specification",
                    "content_type",
                    "territorial_scope",
                )
                    .str.strip_prefix("[")
                    .str.strip_suffix("]")
                    .str.replace_all('"', "", literal=True)
                    .str.split(","),
            )
            # Cast list elements and date columns to their types
            .with_columns(
                pl.col("decision_visibility").cast(pl.List(DecisionVisibility)),
                pl.col("category_addition").cast(pl.List(StatementCategory)),
                pl.col("category_specification").cast(pl.List(Keyword)),
                pl.col("content_type").cast(pl.List(ContentType)),
                pl.col("content_language").cast(ContentLanguageType),
                pl.col("territorial_scope").cast(pl.List(TerritorialScopeType)),
                pl.col(
                    "end_date_visibility_restriction",
                    "end_date_monetary_restriction",
                    "end_date_service_restriction",
                    "end_date_account_restriction",
                    "content_date",
                    "application_date",
                    "created_at",
                ).str.to_datetime("%Y-%m-%d %H:%M:%S", time_unit="ms")
            )
        )

    def _validate_schema(self, frame: pl.DataFrame) -> None:
        """Validate the schema of the given data frame."""
        for name in frame.columns:
            actual = frame.schema[name]
            expected = SCHEMA[name]
            if actual != expected:
                raise TypeError(f"column {name} has type {actual} not {expected}")

    def _assemble_frame_counters(
        self, frame: pl.DataFrame, total_rows: int, total_rows_with_keywords: int
    ) -> Counter:
        batch_rows = frame.height
        batch_rows_with_keywords = frame.select(
            (0 < pl.col("category_specification").list.len()).sum()
        ).item()
        batch_memory = frame.estimated_size()

        return Counter(
            total_rows=total_rows,
            total_rows_with_keywords=total_rows_with_keywords,
            batch_rows=batch_rows,
            batch_rows_with_keywords=batch_rows_with_keywords,
            batch_memory=int(batch_memory),
        )

    @annotate_error(filename_arg="root")
    def analyze_release(
        self, root: Path, release: Release, metadata: MetadataEntry, collector: Collector
    ) -> None:
        # Read all Parquet files for entire month, filter rows with keywords
        frame = pl.read_parquet(f"{root}/{release.batch_glob}")
        last_day = release.last_day.to_date()

        stats = frame.select(
            # Timing the data
            pl.lit(release.first_day.to_date()).alias("first_day"),
            pl.lit(last_day).alias("last_day"),
            pl.lit(release.monthly.year).alias("year"),
            pl.lit(release.monthly.month).alias("month"),

            # Stats about archival data
            pl.lit(metadata.get("total_rows")).alias("total_rows"),
            pl.lit(metadata.get("total_rows_with_keywords")).alias("total_rows_with_keywords"),

            # Stats about batching
            pl.lit(metadata["batch_count"]).alias("batch_count"),

            # Stats about working set
            pl.len().alias("rows"),
            pl.col("category_specification").list.len().sum().alias("keywords"),
            pl.col("category_specification").list.len().gt(0).count().alias("rows_with_keywords"),
            pl.col("category_specification").list.len().max().alias("max_keywords_per_row"),
        ).with_columns(
            pl.exclude(
                "first_day", "last_day", "year", "month", "batch_count",
                "max_keywords_per_row"
            ).cast(pl.UInt64),
            pl.col("batch_count").cast(pl.UInt64),
            pl.col("max_keywords_per_row").cast(pl.UInt32),
        )

        keywords = frame.select(
            pl.col("category_specification")
            .list.explode().drop_nulls()
            .value_counts()
            .struct.unnest()
        ).with_columns(
            # date vs year/month are redundant but simplify aggregations
            pl.lit(last_day).alias("date"),
            pl.lit(release.monthly.year).alias("year"),
            pl.lit(release.monthly.month).alias("month"),
        )

        platforms = frame.select(
            pl.col("platform_name"),
            pl.col("category_specification").is_null().not_().alias("has_keyword"),
        ).group_by("platform_name", "has_keyword").agg(
            pl.len().alias("count")
        ).with_columns(
            # date vs year/month are redundant but simplify aggregations
            pl.lit(last_day).alias("date"),
            pl.lit(release.monthly.year).alias("year"),
            pl.lit(release.monthly.month).alias("month"),
        )

        collector.release(release)
        collector.frames(
            stats=stats,
            keywords=keywords,
            platforms=platforms,
        )

        # with_csam = with_keywords.filter(
        #     pl.col("category_specification").list.contains("KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL")
        # ).select(
        #     pl.count().alias("total"),
        #     pl.col("decision_ground").eq("DECISION_GROUND_ILLEGAL_CONTENT").count(),
        #     pl.col("decision_account").eq("DECISION_ACCOUNT_TERMINATED").count(),
        # )

    @annotate_error(filename_arg="root")
    def combine_releases(
        self, root: Path, coverage: Coverage, collector: Collector
    ) -> dict[str, pl.DataFrame]:
        summary = {}

        for key, frame in collector.consume_frames():
            if key == "stats":
                summary[key] = frame
                self.write_parquet(frame, root / STATS_FILE)
            elif key == "keywords":
                summary[key] = (
                    frame.select(pl.exclude("year", "month"))
                    .group_by("category_specification")
                    .agg(pl.col("count").sum())
                    .sort("count", descending=True)
                )
                self.write_parquet(frame, root / KEYWORDS_FILE)
            elif key == "platforms":
                summary[key] = (
                    frame.select(pl.exclude("year", "month"))
                    .group_by("platform_name", "has_keyword")
                    .agg(pl.col("count").sum()) # Sum up partial aggregates
                    .sort(["platform_name", "has_keyword"])
                )
                self.write_parquet(frame, root / PLATFORMS_FILE)
            else:
                raise ValueError(f"unexpected frame {key}")

        return summary

    def write_parquet(self, frame: pl.DataFrame, path: Path) -> None:
        tmp = path.with_suffix(".tmp.parquet")
        frame.write_parquet(tmp)
        tmp.replace(path)
