from collections import Counter
import csv
import hashlib
import logging
from pathlib import Path

import polars as pl

from .model import CollectorProtocol, Daily, Dataset, MetadataEntry
from .progress import NO_PROGRESS, Progress
from .schema import (
    BASE_SCHEMA, CanonicalPlatformNames, KeywordChildSexualAbuseMaterial,
    PARTIAL_SCHEMA, SCHEMA, StatementCategoryProtectionOfMinors, TerritorialAlias
)
from .util import annotate_error


_logger = logging.getLogger(__spec__.parent)


def parse_list(*columns: str) -> pl.Expr:
    return (
        pl.col(
            *columns
        )
        # Turn list string into list of strings
        .str.strip_prefix("[")
        .str.strip_suffix("]")
        .str.replace_all('"', "", literal=True)
        .str.split(",")
        # Keep list elements that are not null
        .list.eval(
            pl.element().filter(pl.element().is_not_null())
        )
        # Keep list elements that are not the empty string
        .list.eval(
            pl.element().filter(
                pl.element().ne(
                    pl.lit("")
                )
            )
        )
    )


def empty_list_to_null(column: str) -> pl.Expr:
    return pl.when(
        pl.col(column).list.len() == 0
    ).then(
        pl.lit(None).alias(column)
    ).otherwise(
        pl.col(column)
    )


class StatementsOfReasons(Dataset):

    @property
    def name(self) -> str:
        return "EU-DSA-SoR-DB"

    def url(self, filename: str) -> str:
        return f"https://dsa-sor-data-dumps.s3.eu-central-1.amazonaws.com/{filename}"

    def archive_name(self, release: Daily) -> str:
        return f"sor-global-{release.id}-full.zip"

    def digest_name(self, release: Daily) -> str:
        return f"{self.archive_name(release)}.sha1"

    @annotate_error(filename_arg="root")
    def ingest_database_data(
        self,
        *,
        root: Path,
        release: Daily,
        index: int,
        name: str,
        progress: Progress = NO_PROGRESS,
    ) -> tuple[Counter, pl.DataFrame]:
        path = root / release.temp_directory
        csv_files = f"{path}/sor-global-{release.id}-full-{index:05}-*.csv"

        frame = self._read_rows(
            csv_files=csv_files,
            release=release,
            index=index,
            name=name,
            category=None,
            progress=progress
        )
        self._validate_schema(frame)

        counter = Counter(
            total_rows=frame.height,
            total_rows_with_keywords=frame.select(
                pl.col("category_specification").is_not_null().sum()
            ).item(),
        )

        return counter, frame

    @annotate_error(filename_arg="root")
    def distill_category_data(
        self,
        *,
        root: Path,
        release: Daily,
        index: int,
        name: str,
        category: str,
        progress: Progress = NO_PROGRESS
    ) -> tuple[str, Counter]:
        path = root / release.temp_directory
        csv_files = f"{path}/sor-global-{release.id}-full-{index:05}-*.csv"

        progress.step(index, extra="count rows")
        total_rows, total_rows_with_keywords = self.get_total_row_counts(
            csv_files, index, name
        )

        frame = self._read_rows(
            csv_files=csv_files,
            release=release,
            index=index,
            name=name,
            category=category,
            progress=progress
        )

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

        return digest, self._assemble_frame_counters(
            frame, total_rows, total_rows_with_keywords
        )

    def get_total_row_counts(
        self, csv_files: str, index: int, name: str
    ) -> tuple[int, int]:
        """
        Determine number of rows (`total_rows`) and rows with keywords
        (`total_rows_with_keywords`) across all CSV files in the batch.
        """
        # It's too bad that we parse this column twice, once for the full frame
        # and once for the filtered version. But since ingesting the full frame
        # has two levels of fall back, adding the logic for also parsing the
        # full frame doesn't seem to fit. Previously, this function avoided
        # parsing and used a simpler syntactic test. Alas, that failed for maybe
        # twenty out of 600 something releases.
        rows, rows_with_keywords = (
            pl.scan_csv(csv_files, infer_schema=False)
            .select(
                parse_list("category_specification"),
            ).with_columns(
                empty_list_to_null("category_specification"),
            ).select(
                pl.len(),
                pl.col("category_specification").is_not_null().sum(),
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

    def _read_rows(
        self,
        *,
        csv_files: str,
        release: Daily,
        index: int,
        name: str,
        category: None | str,
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
        _, _, pattern = csv_files.rpartition("/")
        progress.step(index, extra=f"loading {pattern}")
        try:
            frame = self.finish_frame(
                release,
                self._scan_csv_with_polars(csv_files, category)
            ).collect()
            _logger.debug(
                'ingested rows=%d, strategy=1, using="globbing Pola.rs", file="%s"',
                frame.height, name
            )
            return frame
        except Exception as x:
            _logger.warning(
                'failed to read CSV with strategy=1, using="globbing Pola.rs", file="%s"',
                name, exc_info=x
            )

        # Slow path: Process each CSV file by itself, trying first with the same
        # lazy Polars operation and falling back onto Python's CSV module.
        split = csv_files.rindex("/")
        path = Path(csv_files[:split])
        glob = csv_files[split + 1:]

        files = sorted(path.glob(glob))
        assert 0 < len(files), f'glob "{csv_files}" matches no files'

        frames = []
        for file_path in files:
            progress.step(index, extra=f"loading {file_path.name}")

            try:
                frame = self.finish_frame(
                    release,
                    self._scan_csv_with_polars(file_path, category)
                ).collect()
                frames.append(frame)

                _logger.debug(
                    'ingested rows=%d, strategy=2, using="Pola.rs", file="%s"',
                    frame.height, file_path.name
                )
                continue
            except:
                _logger.warning(
                    'failed to read CSV with strategy=2, using="Pola.rs", file="%s"',
                    file_path.name
                )

            try:
                frame = self.finish_frame(
                    release,
                    self._read_csv_row_by_row(file_path, category).lazy()
                ).collect()
                frames.append(frame)

                _logger.debug(
                    'ingested rows=%d, strategy=3, using="Python\'s CSV module", file="%s"',
                    frame.height, file_path.name
                )
            except Exception as x:
                _logger.error(
                    'failed to parse with strategy=3, using="Python\'s CSV module", file="%s"',
                    file_path.name, exc_info=x
                )
                raise

        return pl.concat(frames, how="vertical", rechunk=True)

    def _scan_csv_with_polars(
        self, path: str | Path, category: None | str = None
    ) -> pl.LazyFrame:
        """
        Read one or more CSV files with Polars' CSV reader, while also applying
        the filter.

        The path string may include a wildcard to read more than one CSV file at
        the same time. The returned LazyFrame has not been collect()ed.
        """
        frame = pl.scan_csv(
            str(path),
            null_values=["", "[]"],
            schema_overrides=PARTIAL_SCHEMA,
            infer_schema=False,
        )

        if isinstance(category, str):
            frame = frame.filter(
                (pl.col("category") == category)
                | pl.col("category_addition").str.contains(category, literal=True)
            )

        return frame

    def _read_csv_row_by_row(
        self, path: str | Path, category: None | str = None
    ) -> pl.DataFrame:
        """
        Read a CSV file using Python's CSV reader row by row, while also
        applying the filter.
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

            if isinstance(category, str):
                category_index = header.index("category")
                addition_index = header.index("category_addition")
                if category_index < 0:
                    raise ValueError(f'"{path}" does not include "category" column')
                if addition_index < 0:
                    raise ValueError(f'"{path}" does not include "category_addition" column')

                predicate = (
                    lambda row: row[category_index] == category or category in row[addition_index]
                )
            else:
                predicate = lambda _row: True

            for row in reader:
                if predicate(row):
                    row = [None if field in ("", "[]") else field for field in row]
                    rows.append(row)

        frame = pl.DataFrame(list(zip(*rows)), schema=BASE_SCHEMA)
        return frame

    def finish_frame(self, release: Daily, frame: pl.LazyFrame) -> pl.LazyFrame:
        """
        Finish the frame by patching in the names of country groups, parsing
        list-valued columns, as well as casting list elements and date columns
        to their types. This method does not collect lazy frames.
        """
        return (
            frame
            # Patch in the names of country groups as well as canonical platform names
            .with_columns(
                pl.when(pl.col("territorial_scope") == TerritorialAlias.EEA.value)
                    .then(pl.lit("[\"EEA\"]"))
                    .when(pl.col("territorial_scope") == TerritorialAlias.EEA_no_IS.value)
                    .then(pl.lit("[\"EEA_no_IS\"]"))
                    .when(pl.col("territorial_scope") == TerritorialAlias.EU.value)
                    .then(pl.lit("[\"EU\"]"))
                    .otherwise(pl.col("territorial_scope"))
                    .alias("territorial_scope"),
                pl.col("platform_name").replace(CanonicalPlatformNames),
            )
            # Parse list-valued columns
            .with_columns(
                parse_list(
                    "decision_visibility",
                    "category_addition",
                    "category_specification",
                    "content_type",
                    "territorial_scope",
                )
            )
            .with_columns(
                # Replace empty lists with None. This method used to assume that
                # the value never is the empty list. That assumption becomes
                # superfluous with introduction of this clause.
                empty_list_to_null("decision_visibility"),
                empty_list_to_null("category_specification"),
                empty_list_to_null("content_type"),
                empty_list_to_null("territorial_scope"),
            )
            # Cast list elements and date columns to their types. Add released_on.
            .with_columns(
                pl.col("decision_visibility").cast(SCHEMA["decision_visibility"]),
                pl.col("category_addition").cast(SCHEMA["category_addition"]),
                pl.col("category_specification").cast(SCHEMA["category_specification"]),
                pl.col("content_type").cast(SCHEMA["content_type"]),
                pl.col("content_language").cast(SCHEMA["content_language"]),
                pl.col("territorial_scope").cast(SCHEMA["territorial_scope"]),
                pl.col(
                    "end_date_visibility_restriction",
                    "end_date_monetary_restriction",
                    "end_date_service_restriction",
                    "end_date_account_restriction",
                    "content_date",
                    "application_date",
                    "created_at",
                ).str.to_datetime("%Y-%m-%d %H:%M:%S", time_unit="ms"),
                pl.lit(release.date, dtype=pl.Date).alias("released_on"),
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
            pl.col("category_specification").is_not_null().sum(),
        ).item()

        return Counter(
            total_rows=total_rows,
            total_rows_with_keywords=total_rows_with_keywords,
            batch_rows=batch_rows,
            batch_rows_with_keywords=batch_rows_with_keywords,
        )

    @annotate_error(filename_arg="root")
    def summarize_release(
        self,
        root: Path,
        release: Daily,
        category: str,
        metadata_entry: MetadataEntry,
        collector: CollectorProtocol,
    ) -> None:
        count = sum(1 for _ in (root / release.directory).glob(release.batch_glob))
        glob = f"{root}/{release.directory}/{release.batch_glob}"
        _logger.debug(
            'summarizing release="%s", file-count=%d, glob="%s"', release, count, glob
        )

        # With scan_parquet(), Shantay makes rapid progress only to get stuck at
        # the 100% mark executing collect(). Hence, even if eager processing is
        # a bit slower, it provides a consistent appearance of progress.
        extract = pl.read_parquet(glob).with_columns(
            pl.col("platform_name").replace(CanonicalPlatformNames)
        )

        collector.collect(
            release,
            extract,
            metadata_entry=metadata_entry,
            tag=category,
        )

        if category == StatementCategoryProtectionOfMinors:
            csam = extract.filter(
                pl.col("category_specification").list.contains(
                    KeywordChildSexualAbuseMaterial
                )
            )
            collector.collect(release, csam, tag=KeywordChildSexualAbuseMaterial)
