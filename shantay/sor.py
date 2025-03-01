from collections import Counter
import csv
import logging
from pathlib import Path

import polars as pl

from .collector import Collector
from .progress import NO_PROGRESS, Progress
from .release import DailyRelease
from .schedule import YearMonth, MonthlySchedule
from .schema import (
    BASE_SCHEMA, ContentType, ContentLanguageType, CountryGroups, DecisionVisibility,
    EXTRA_KEYWORDS_MINOR_PROTECTION, Keyword, KEYWORDS_MINOR_PROTECTION,
    StatementCategory, TerritorialScopeType, SCHEMA, SCHEMA_OVERRIDES
)
from .util import annotate_error


_logger = logging.getLogger("shantay")


class DailySoR(DailyRelease):

    @property
    def archive(self) -> str:
        return f"sor-global-{self.id}-full.zip"

    @property
    def digest(self) -> str:
        return self.archive + ".sha1"

    def batch(self, number: int) -> str:
        if not 0 <= number <= 99_999:
            raise ValueError(f"batch {number} is out of permissible range")
        return f"{self.id}-{number:05}.parquet"

    @property
    def url(self) -> str:
        return "https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com"

    def extract_data_step_count(self) -> int:
        return 13

    @annotate_error(filename_arg="root")
    def extract_data(
        self,
        root: Path,
        index: int,
        name: str,
        category: str,
        progress: Progress = NO_PROGRESS
    ) -> None:
        path = root / self.working_directory
        csv_files = f"{path}/sor-global-{self.id}-full-{index:05}-*.csv"

        total_rows = self._extract_total_rows(csv_files, index, name, progress)
        total_rows_with_keyword = self._extract_rows_with_keyword(
            csv_files, index, name, progress
        )
        frame = self._extract_frame(csv_files, index, name, category, progress)

        batch_rows = frame.height
        batch_rows_with_keyword = frame.filter(
            pl.col("category_specification").is_not_null()
                & (0 < pl.col("category_specification").list.len())
        ).select(pl.len()).item()
        batch_memory = frame.estimated_size()

        self._validate_schema(frame)
        path = root / self.batch_directory
        path.mkdir(parents=True, exist_ok=True)
        frame.write_parquet(path / self.batch(index))

        return Counter(
            total_rows=total_rows,
            total_rows_with_keywords=total_rows_with_keyword,
            batch_rows=batch_rows,
            batch_rows_with_keywords=batch_rows_with_keyword,
            batch_memory=batch_memory,
        )

    def _extract_total_rows(
        self, csv_files: str, index: int, name: str, progress: Progress = NO_PROGRESS
    ) -> int:
        """Determine the total number of rows across all CSV files in the batch."""
        progress.step(self.extract_data_step(index, 1), extra="count rows")
        row_count = (
            pl.scan_csv(csv_files, infer_schema=False)
            .select(pl.len())
            .collect()
            .item()
        )
        _logger.debug('counted filter="all", rows=%d, file="%s"', row_count, name)
        return row_count

    def _extract_rows_with_keyword(
        self, csv_files: str, index: int, name: str, progress: Progress = NO_PROGRESS
    ) -> int:
        """
        Determine the number of rows with one or more keywords across all CSV
        files in the batch.
        """
        progress.step(self.extract_data_step(index, 2), extra="count rows with keywords")
        row_count = (
            pl.scan_csv(csv_files, infer_schema=False)
            .filter(
                pl.col("category_specification").is_not_null()
                    & (2 < pl.col("category_specification").str.len_bytes())
            )
            .select(pl.len())
            .collect()
            .item()
        )
        _logger.debug('counted filter="with keyword" rows=%d, file="%s"', row_count, name)
        return row_count

    def _extract_frame(
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
        progress.step(self.extract_data_step(index, 3), extra="extracting category data")
        try:
            frame = self._finish_frame(self._scan_csv_with_polars(csv_files, category))
            _logger.debug(
                'extracted rows=%d, using="Pola.rs fast path", file="%s"',
                frame.height, name
            )
            return frame
        except Exception as x:
            _logger.warning(
                'failed to read CSV using="Pola.rs with glob", file="%s"',
                name, exc_info=x
            )

        # Slow path: Read each CSV file by itself, first using Polars again but
        # falling back to Python's standard library when that fails.
        split = csv_files.rindex("/")
        path = Path(csv_files[:split])
        glob = csv_files[split + 1:]

        frames = []
        for file_no, file_path in enumerate(sorted(path.glob(glob))):
            progress.step(
                self.extract_data_step(index, 3 + file_no), extra=f"extracting {file_path.name}"
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
            pl.scan_csv(str(path), schema_overrides=SCHEMA_OVERRIDES, infer_schema=False)
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
                    row = [None if field == "" else field for field in row]
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
            # Parse list-valued columns
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

    @classmethod
    @annotate_error(filename_arg="root")
    def analyze_month(cls, root: Path, month: YearMonth, collector: Collector) -> None:
        # Read all Parquet files for entire month, filter rows with keywords
        frame = pl.read_parquet(month.daily_glob(root))
        with_keywords = frame.filter(pl.col("category_specification").list.len() != 0)

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
        collector.month(month)
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
        )
        collector.frames(
            platforms=frame.select(pl.col("platform_name").unique()),
            platforms_with_keywords=with_keywords.select(pl.col("platform_name").unique()),
        )

    @classmethod
    @annotate_error(filename_arg="root")
    def combine_months(cls, root: Path, schedule: MonthlySchedule, collector: Collector) -> pl.DataFrame:
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
