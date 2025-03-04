from collections import Counter
import csv
import logging
from pathlib import Path

import polars as pl

from .collector import Collector
from .model import Coverage, Daily, Dataset, Release
from .progress import NO_PROGRESS, Progress
from .schema import (
    BASE_SCHEMA, ContentLanguageType, ContentType, CountryGroups, DecisionVisibility,
    EXTRA_KEYWORDS_MINOR_PROTECTION, Keyword, KEYWORDS_MINOR_PROTECTION, SCHEMA,
    SCHEMA_OVERRIDES, StatementCategory, TerritorialScopeType
)
from .util import annotate_error


_logger = logging.getLogger(__package__)


class StatementsOfReasons(Dataset[Daily]):
    def name(self) -> str:
        return "EU-DSA-SoR-DB"

    def url(self, filename: str) -> str:
        return f"https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/{filename}"

    def archive(self, release: Daily) -> str:
        return f"sor-global-{release.id}-full.zip"

    def digest(self, release: Daily) -> str:
        return f"{self.archive(release)}.sha1"

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
        filter: str,
        progress: Progress = NO_PROGRESS
    ) -> Counter:
        path = root / release.temp_directory
        csv_files = f"{path}/sor-global-{release.id}-full-{index:05}-*.csv"

        total_rows, total_rows_with_keywords = self._extract_row_counts(
            csv_files, index, name, progress
        )
        frame = self._extract_filtered_rows(csv_files, index, name, filter, progress)

        self._validate_schema(frame)
        path = root / release.directory
        path.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path / release.batch_file(index))

        return self._assemble_frame_counters(frame, total_rows, total_rows_with_keywords)

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
        category: str,
        progress: Progress = NO_PROGRESS
    ) -> pl.DataFrame:
        """
        Extract rows with the category of interest across all CSV files in the
        batch. This method first does the expedient thing and tries to process
        all CSV files in one Polars operation. If that fails, it tries again,
        processing one CSV file at a time, first with Polars and then with
        Python's standard library.
        """
        # Fast path: Read all CSV files in one lazy Polars operation.
        progress.step(self.extract_data_step_number(index, 2), extra="extracting category data")
        try:
            frame = self._finish_frame(self._scan_csv_with_polars(csv_files, category))
            _logger.debug(
                'extracted rows=%d, using="Pola.rs with glob", file="%s"',
                frame.height, name
            )
            return frame
        except Exception as x:
            _logger.warning(
                'failed to read CSV using="Pola.rs with glob", file="%s"', name, exc_info=x
            )

        # Slow path: Read each CSV file by itself, first using Polars again but
        # falling back to Python's standard library when that fails.
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
                frame = self._finish_frame(self._scan_csv_with_polars(file_path, category))
                frames.append(frame)

                _logger.debug(
                    'extracted rows=%d, using="Pola.rs", file="%s"',
                    frame.height, file_path.name
                )
                continue
            except:
                _logger.warning('failed to read CSV using="Pola.rs", file="%s"', file_path.name)

            try:
                frame = self._finish_frame(self._read_csv_row_by_row(file_path, category))
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

    def _scan_csv_with_polars(self, path: str | Path, category: str) -> pl.LazyFrame:
        """
        Read one or more CSV files with Polars' CSV reader, filtering for the
        given category.

        The path string may include a wildcard to read more than one CSV file at
        the same time. The returned LazyFrame has not been collect()ed.
        """
        return (
            pl.scan_csv(
                str(path),
                null_values=["", "[]"],
                schema_overrides=SCHEMA_OVERRIDES,
                infer_schema=False,
            )
            .filter(
                (pl.col("category") == category)
                | pl.col("category_addition").str.contains(category, literal=True)
            )
        )

    def _read_csv_row_by_row(self, path: str | Path, category: str) -> pl.DataFrame:
        """
        Read a CSV file using Python's CSV reader row by row.

        This method filters out all rows but those that have the given category.
        """
        header = None
        rows = []

        with open(path, mode="r", encoding="utf8") as file:
            # Per Python documentation, quoting=csv.QUOTE_NOTNULL should turn
            # empty fields into None. The source code suggests the same.
            # https://github.com/python/cpython/blob/630dc2bd6422715f848b76d7950919daa8c44b99/Modules/_csv.c#L655
            # Alas, it doesn't seem to work.
            reader = csv.reader(file)
            header = next(reader)

            category_index = header.index("category")
            addition_index = header.index("category_addition")
            if category_index < 0:
                raise ValueError(f'"{path}" does not include "category" column')
            if addition_index < 0:
                raise ValueError(f'"{path}" does not include "category_addition" column')

            for row in reader:
                if row[category_index] == category or category in row[addition_index]:
                    row = [None if field in ("", "[]") else field for field in row]
                    rows.append(row)

        return pl.DataFrame(list(zip(*rows)), schema=BASE_SCHEMA)

    def _finish_frame(self, frame: pl.LazyFrame | pl.DataFrame) -> pl.DataFrame:
        """
        Finish the frame by patching in the names of country groups, parsing
        list-valued columns, as well as casting list elements and date columns
        to their types,
        """
        frame = (
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

        if isinstance(frame, pl.LazyFrame):
            frame = frame.collect()
        return frame

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
            batch_memory=batch_memory,
        )

    @annotate_error(filename_arg="root")
    def analyze_release[R: Release](
        self, root: Path, release: R, collector: Collector
    ) -> None:
        # Read all Parquet files for entire month, filter rows with keywords
        frame = pl.read_parquet(f"{root}/{release.batch_glob}")
        with_keywords = frame.filter(pl.col("category_specification").list.len() != 0)
        # with_csam = with_keywords.filter(
        #     pl.col("category_specification").list.contains("KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL")
        # ).select(
        #     pl.count().alias("total"),
        #     pl.col("decision_ground").eq("DECISION_GROUND_ILLEGAL_CONTENT").count(),
        #     pl.col("decision_account").eq("DECISION_ACCOUNT_TERMINATED").count(),
        # )

        # Collect value counts for keywords
        keyword_counts = {}
        keyword_count_total = 0
        for keyword, count in (
            with_keywords.select(
                pl.col("category_specification")
                .list.explode()
                .value_counts()
            )
            .unnest("category_specification")
            .rows()
        ):
            if keyword is None:
                keyword = "NO_KEYWORD"
            keyword_count_total += count
            keyword_counts[keyword.lower()] = count

        # Make sure that all columns are represented so that they have same length
        for keyword in KEYWORDS_MINOR_PROTECTION + EXTRA_KEYWORDS_MINOR_PROTECTION:
            keyword_counts.setdefault(keyword.lower(), 0)

        # Actually collect statistics
        collector.release(release)
        collector.values(
            # Platforms
            platforms=frame.select(pl.col("platform_name").n_unique()).item(),
            platforms_with_keywords=with_keywords.select(pl.col("platform_name").n_unique()).item(),

            # Rows
            rows=frame.height,
            rows_with_keywords=with_keywords.height,
            rows_with_keywords_old=with_keywords.select(pl.col("category_specification")).count().item(),

            # Keywords
            max_keywords_per_row=frame.select(pl.col("category_specification").list.len().max()).item(),
            keyword_count=keyword_count_total,
            **keyword_counts,

            # CSAM
            #csam_count=with_csam.height,
            #csam_count=with_csam.ite

        )
        collector.frames(
            platforms=frame.select(pl.col("platform_name").unique()),
            platforms_with_keywords=with_keywords.select(pl.col("platform_name").unique()),
        )

    @annotate_error(filename_arg="root")
    def combine_releases(
        self, root: Path, coverage: Coverage, collector: Collector
    ) -> pl.DataFrame:
        from IPython.display import display

        print("\n")
        for key, value in collector.consume_frames():
            if key in ("platforms", "platforms_with_keywords"):
                series = value.select(pl.col("platform_name").unique())
                print(f"{key} reporting category SoRs:")
                for index in range(series.height):
                    print(f"    {series.item(index, 0)}")
                print()
            else:
                raise ValueError(f"unknown collection {key}")

        df = collector.frame_for_values()
        tmp = root / "stats.tmp.parquet"
        df.write_parquet(tmp)
        tmp.replace(root / "stats.parquet")

        display(df)
        return df
