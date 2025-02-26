from collections import Counter
from pathlib import Path

import polars as pl

from .progress import NO_PROGRESS, Progress
from .release import DailyRelease
from .schema import (
    ContentType, ContentLanguageType, CountryGroups, DecisionVisibility, Keyword,
    StatementCategory, TerritorialScopeType, SCHEMA, SCHEMA_OVERRIDES
)
from .util import annotate_error

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

    def extract_batch_step_count(self) -> int:
        return 3

    @annotate_error(filename_arg="root")
    def extract_batch(
        self,
        root: Path,
        index: int,
        name: str,
        progress: Progress = NO_PROGRESS
    ) -> None:
        path = root / self.working_directory
        csv_files = f"{path}/sor-global-{self.id}-full-{index:05}-*.csv"

        progress.step(self.extract_batch_step(index, 1), extra="count rows")
        total_rows = (
            pl.scan_csv(csv_files, infer_schema=False)
            .select(pl.len())
            .collect()
            .item()
        )

        progress.step(self.extract_batch_step(index, 2), extra="count rows with keywords")
        total_rows_with_keyword = (
            pl.scan_csv(csv_files, infer_schema=False)
            .filter(
                pl.col("category_specification").is_not_null()
                    & (2 < pl.col("category_specification").str.len_bytes())
            )
            .select(pl.len())
            .collect()
            .item()
        )

        progress.step(self.extract_batch_step(index, 3), extra="assembling category data")
        frame = (
            # Lazily scan CSV files, while also...
            pl.scan_csv(
                csv_files,
                schema_overrides=SCHEMA_OVERRIDES,
                infer_schema=False,
            )
            # ...retaining only protection-of-minors statements
            .filter(
                (pl.col("category") == "STATEMENT_CATEGORY_PROTECTION_OF_MINORS")
                | pl.col("category_addition")
                    .str.contains(
                        "STATEMENT_CATEGORY_PROTECTION_OF_MINORS", literal=True)
            )
            # ...patching in the names of country groups
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
            # ...parsing list-valued columns
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
            # ...casting list elements to their types
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
            .collect()
        )

        batch_rows = frame.height
        batch_rows_with_keyword = frame.filter(
            pl.col("category_specification").is_not_null()
                & (0 < pl.col("category_specification").list.len())
        ).select(pl.len()).item()
        batch_memory = frame.estimated_size()

        self.validate_schema(frame)
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

    def validate_schema(self, frame: pl.DataFrame) -> None:
        for name in frame.columns:
            actual = frame.schema[name]
            expected = SCHEMA[name]
            if actual != expected:
                raise TypeError(f"column {name} has type {actual} not {expected}")

    @annotate_error(filename_arg="root")
    def analyze_batch(self, root: Path, index: int) -> None:
        path = root / self.batch_directory / self.batch(index)
        frame = pl.read_parquet(path)
        frame = frame.with_columns(
            pl.col("content_language").cast(ContentLanguageType)
        )
        tmp = path.with_suffix(".tmp.parquet")
        frame.write_parquet(tmp)
        tmp.replace(path)
