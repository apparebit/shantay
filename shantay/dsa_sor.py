from collections import Counter
import csv
import datetime as dt
import hashlib
import logging
from pathlib import Path

import polars as pl

from .framing import validate_csam_statistics, validate_general_statistics
from .model import (
    CollectorProtocol, Coverage, Daily, Dataset, KEYWORDS_FILE,
    PLATFORMS_FILE, Release, STATISTICS_FILE
)
from .progress import NO_PROGRESS, Progress
from .schema import (
    BASE_SCHEMA, ContentLanguageType, ContentType, CountryGroups, DecisionVisibility,
    Keyword, SCHEMA, SCHEMA_OVERRIDES, StatementCategory, TerritorialScopeType
)
from .util import annotate_error


_DEBUG_OUTLIERS = False
_logger = logging.getLogger(__spec__.parent)


def fix_prefix(prefix: str) -> str:
    if prefix != "" and not prefix.endswith("_"):
        prefix = f"{prefix}_"
    return prefix


def mean_timing(prefix: str) -> list[pl.Expr]:
    prefix = fix_prefix(prefix)

    return [
        (pl.col("application_date").dt.date() - pl.col("content_date").dt.date())
        .mean()
        .alias(f"{prefix}mean_moderation_delay"),
        (pl.col("created_at").dt.date() - pl.col("application_date").dt.date())
        .mean()
        .alias(f"{prefix}mean_reporting_delay"),
    ]


def decision_type_breakdown(prefix: str) -> list[pl.Expr]:
    prefix = fix_prefix(prefix)
    return [
        # Kinds of Decision
        pl.col("decision_visibility").is_null()
        .and_(pl.col("decision_monetary").is_null())
        .and_(pl.col("decision_provision").is_null())
        .and_(pl.col("decision_account").is_null())
        .sum().alias(f"{prefix}null_decision"),
        pl.col("decision_visibility").is_null().not_()
        .and_(pl.col("decision_monetary").is_null())
        .and_(pl.col("decision_provision").is_null())
        .and_(pl.col("decision_account").is_null())
        .sum().alias(f"{prefix}visibility_decision_only"),
        pl.col("decision_visibility").is_null()
        .and_(pl.col("decision_monetary").is_null().not_())
        .and_(pl.col("decision_provision").is_null())
        .and_(pl.col("decision_account").is_null())
        .sum().alias(f"{prefix}monetary_decision_only"),
        pl.col("decision_visibility").is_null()
        .and_(pl.col("decision_monetary").is_null())
        .and_(pl.col("decision_provision").is_null().not_())
        .and_(pl.col("decision_account").is_null())
        .sum().alias(f"{prefix}provision_decision_only"),
        pl.col("decision_visibility").is_null()
        .and_(pl.col("decision_monetary").is_null())
        .and_(pl.col("decision_provision").is_null())
        .and_(pl.col("decision_account").is_null().not_())
        .sum().alias(f"{prefix}account_decision_only"),
        pl.col("decision_visibility").is_null().not_()
        .and_(pl.col("decision_monetary").is_null().not_())
        .and_(pl.col("decision_provision").is_null())
        .and_(pl.col("decision_account").is_null())
        .sum().alias(f"{prefix}visibility_monetary_decision"),
        pl.col("decision_visibility").is_null().not_()
        .and_(pl.col("decision_monetary").is_null())
        .and_(pl.col("decision_provision").is_null().not_())
        .and_(pl.col("decision_account").is_null())
        .sum().alias(f"{prefix}visibility_provision_decision"),
        pl.col("decision_visibility").is_null().not_()
        .and_(pl.col("decision_monetary").is_null())
        .and_(pl.col("decision_provision").is_null())
        .and_(pl.col("decision_account").is_null().not_())
        .sum().alias(f"{prefix}visibility_account_decision"),
        pl.col("decision_visibility").is_null()
        .and_(pl.col("decision_monetary").is_null().not_())
        .and_(pl.col("decision_provision").is_null().not_())
        .and_(pl.col("decision_account").is_null())
        .sum().alias(f"{prefix}monetary_provision_decision"),
        pl.col("decision_visibility").is_null()
        .and_(pl.col("decision_monetary").is_null().not_())
        .and_(pl.col("decision_provision").is_null())
        .and_(pl.col("decision_account").is_null().not_())
        .sum().alias(f"{prefix}monetary_account_decision"),
        pl.col("decision_visibility").is_null()
        .and_(pl.col("decision_monetary").is_null())
        .and_(pl.col("decision_provision").is_null().not_())
        .and_(pl.col("decision_account").is_null().not_())
        .sum().alias(f"{prefix}provision_account_decision"),
        pl.col("decision_visibility").is_null()
        .and_(pl.col("decision_monetary").is_null().not_())
        .and_(pl.col("decision_provision").is_null().not_())
        .and_(pl.col("decision_account").is_null().not_())
        .sum().alias(f"{prefix}monetary_provision_account_decision"),
        pl.col("decision_visibility").is_null().not_()
        .and_(pl.col("decision_monetary").is_null())
        .and_(pl.col("decision_provision").is_null().not_())
        .and_(pl.col("decision_account").is_null().not_())
        .sum().alias(f"{prefix}visibility_provision_account_decision"),
        pl.col("decision_visibility").is_null().not_()
        .and_(pl.col("decision_monetary").is_null().not_())
        .and_(pl.col("decision_provision").is_null())
        .and_(pl.col("decision_account").is_null().not_())
        .sum().alias(f"{prefix}visibility_monetary_account_decision"),
        pl.col("decision_visibility").is_null().not_()
        .and_(pl.col("decision_monetary").is_null().not_())
        .and_(pl.col("decision_provision").is_null().not_())
        .and_(pl.col("decision_account").is_null().not_())
        .sum().alias(f"{prefix}visibility_monetary_provision_decision"),
        pl.col("decision_visibility").is_null().not_()
        .and_(pl.col("decision_monetary").is_null().not_())
        .and_(pl.col("decision_provision").is_null().not_())
        .and_(pl.col("decision_account").is_null().not_())
        .sum().alias(f"{prefix}all_kinds_decision"),
    ]


def decision_visibility_breakdown(prefix: str) -> list[pl.Expr]:
    prefix = fix_prefix(prefix)
    return [
        pl.col("decision_visibility").list.len().max().alias(f"{prefix}max_visibility_per_row"),
        pl.col("decision_visibility").explode().drop_nulls().len().alias(f"{prefix}visibility_values"),
        pl.col("decision_visibility").is_null().not_().sum().alias(f"{prefix}rows_with_visibility"),
        pl.col("decision_visibility").is_null().sum().alias(f"{prefix}null_visibility_decision"),

        pl.col("decision_visibility").list.contains("DECISION_VISIBILITY_CONTENT_REMOVED")
        .sum().alias(f"{prefix}content_removed"),
        pl.col("decision_visibility").list.contains("DECISION_VISIBILITY_CONTENT_DISABLED")
        .sum().alias(f"{prefix}content_disabled"),
        pl.col("decision_visibility").list.contains("DECISION_VISIBILITY_CONTENT_DEMOTED")
        .sum().alias(f"{prefix}content_demoted"),
        pl.col("decision_visibility").list.contains("DECISION_VISIBILITY_CONTENT_AGE_RESTRICTED")
        .sum().alias(f"{prefix}content_age_restricted"),
        pl.col("decision_visibility").list.contains("DECISION_VISIBILITY_CONTENT_INTERACTION_RESTRICTED")
        .sum().alias(f"{prefix}content_interaction_restricted"),
        pl.col("decision_visibility").list.contains("DECISION_VISIBILITY_CONTENT_LABELLED")
        .sum().alias(f"{prefix}content_labeled"),
        pl.col("decision_visibility").list.contains("DECISION_VISIBILITY_OTHER")
        .sum().alias(f"{prefix}other_visibility"),
    ]


def decision_monetary_breakdown(prefix: str) -> list[pl.Expr]:
    prefix = fix_prefix(prefix)

    return [
        pl.col("decision_monetary").eq("DECISION_MONETARY_SUSPENSION")
        .sum().alias(f"{prefix}monetary_suspension"),
        pl.col("decision_monetary").eq("DECISION_MONETARY_TERMINATION")
        .sum().alias(f"{prefix}monetary_termination"),
        pl.col("decision_monetary").eq("DECISION_MONETARY_OTHER")
        .sum().alias(f"{prefix}monetary_other"),
        pl.col("decision_monetary").is_null()
        .sum().alias(f"{prefix}null_monetary_decision"),
    ]


def decision_provision_breakdown(prefix: str) -> list[pl.Expr]:
    prefix = fix_prefix(prefix)

    return [
        pl.col("decision_provision").eq("DECISION_PROVISION_PARTIAL_SUSPENSION")
        .sum().alias(f"{prefix}provision_partial_suspension"),
        pl.col("decision_provision").eq("DECISION_PROVISION_TOTAL_SUSPENSION")
        .sum().alias(f"{prefix}provision_total_suspension"),
        pl.col("decision_provision").eq("DECISION_PROVISION_PARTIAL_TERMINATION")
        .sum().alias(f"{prefix}provision_partial_termination"),
        pl.col("decision_provision").eq("DECISION_PROVISION_TOTAL_TERMINATION")
        .sum().alias(f"{prefix}provision_total_termination"),
        pl.col("decision_provision").is_null()
        .sum().alias(f"{prefix}null_provision_decision"),
    ]


def decision_account_breakdown(prefix: str) -> list[pl.Expr]:
    prefix = fix_prefix(prefix)

    return [
        pl.col("decision_account").eq("DECISION_ACCOUNT_SUSPENDED")
        .sum().alias(f"{prefix}account_suspended"),
        pl.col("decision_account").eq("DECISION_ACCOUNT_TERMINATED")
        .sum().alias(f"{prefix}account_terminated"),
        pl.col("decision_account").is_null()
        .sum().alias(f"{prefix}null_account_decision"),
    ]


def account_type_breakdown(prefix: str) -> list[pl.Expr]:
    prefix = fix_prefix(prefix)

    return [
        pl.col("account_type").eq("ACCOUNT_TYPE_BUSINESS")
        .sum().alias(f"{prefix}account_type_business"),
        pl.col("account_type").eq("ACCOUNT_TYPE_PRIVATE")
        .sum().alias(f"{prefix}account_type_private"),
        pl.col("account_type").is_null()
        .sum().alias(f"{prefix}null_account_type"),
    ]


def decision_ground_breakdown(prefix: str) -> list[pl.Expr]:
    prefix = fix_prefix(prefix)

    return [
        pl.col("decision_ground").eq("DECISION_GROUND_ILLEGAL_CONTENT")
        .sum().alias(f"{prefix}illegal_content"),
        pl.col("decision_ground").eq("DECISION_GROUND_INCOMPATIBLE_CONTENT")
        .sum().alias(f"{prefix}incompatible_content"),
        pl.col("decision_ground").is_null()
        .sum().alias(f"{prefix}null_decision_ground"),
        pl.col("incompatible_content_illegal").eq("Yes")
        .sum().alias(f"{prefix}incompatible_content_illegal_yes"),
        pl.col("incompatible_content_illegal").eq("No")
        .sum().alias(f"{prefix}incompatible_content_illegal_no"),
        pl.col("incompatible_content_illegal").is_null()
        .sum().alias(f"{prefix}null_incompatible_content_illegal"),
    ]


def source_type_breakdown(prefix: str) -> list[pl.Expr]:
    prefix = fix_prefix(prefix)

    return [
        pl.col("source_type").eq("SOURCE_ARTICLE_16")
        .sum().alias(f"{prefix}source_article_16"),
        pl.col("source_type").eq("SOURCE_TRUSTED_FLAGGER")
        .sum().alias(f"{prefix}source_trusted_flagger"),
        pl.col("source_type").eq("SOURCE_TYPE_OTHER_NOTIFICATION")
        .sum().alias(f"{prefix}source_other_notification"),
        pl.col("source_type").eq("SOURCE_VOLUNTARY")
        .sum().alias(f"{prefix}source_voluntary"),
        pl.col("source_type").is_null()
        .sum().alias(f"{prefix}null_source_type"),
    ]


def automated_detection_and_decision_breakdown(prefix: str) -> list[pl.Expr]:
    prefix = fix_prefix(prefix)

    return [
        pl.col("automated_detection").eq("Yes")
        .sum().alias(f"{prefix}automated_detection_yes"),
        pl.col("automated_detection").eq("No")
        .sum().alias(f"{prefix}automated_detection_no"),
        pl.col("automated_detection").is_null()
        .sum().alias(f"{prefix}null_automated_detection"),
        pl.col("automated_decision").eq("AUTOMATED_DECISION_FULLY")
        .sum().alias(f"{prefix}automated_decision_fully"),
        pl.col("automated_decision").eq("AUTOMATED_DECISION_PARTIALLY")
        .sum().alias(f"{prefix}automated_decision_partially"),
        pl.col("automated_decision").eq("AUTOMATED_DECISION_NOT_AUTOMATED")
        .sum().alias(f"{prefix}automated_decision_not_automated"),
        pl.col("automated_decision").is_null()
        .sum().alias(f"{prefix}null_automated_decision"),
    ]


def content_type_breakdown(prefix: str) -> list[pl.Expr]:
    prefix = fix_prefix(prefix)

    return [
        pl.col("content_type").list.len().max().alias(f"{prefix}max_content_types_per_row"),
        pl.col("content_type").explode().drop_nulls().len().alias(f"{prefix}content_type_values"),
        pl.col("content_type").is_null().not_().sum().alias(f"{prefix}rows_with_content_type"),
        pl.col("content_type").is_null().sum().alias(f"{prefix}null_content_types"),

        pl.col("content_type").list.contains("CONTENT_TYPE_APP")
        .sum().alias(f"{prefix}content_type_app"),
        pl.col("content_type").list.contains("CONTENT_TYPE_AUDIO")
        .sum().alias(f"{prefix}content_type_audio"),
        pl.col("content_type").list.contains("CONTENT_TYPE_IMAGE")
        .sum().alias(f"{prefix}content_type_image"),
        pl.col("content_type").list.contains("CONTENT_TYPE_PRODUCT")
        .sum().alias(f"{prefix}content_type_product"),
        pl.col("content_type").list.contains("CONTENT_TYPE_SYNTHETIC_MEDIA")
        .sum().alias(f"{prefix}content_type_synthetic_media"),
        pl.col("content_type").list.contains("CONTENT_TYPE_TEXT")
        .sum().alias(f"{prefix}content_type_text"),
        pl.col("content_type").list.contains("CONTENT_TYPE_VIDEO")
        .sum().alias(f"{prefix}content_type_video"),
        pl.col("content_type").list.contains("CONTENT_TYPE_OTHER")
        .sum().alias(f"{prefix}content_type_other"),
    ]


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
        frame = self._extract_filtered_rows(
            csv_files=csv_files,
            release=release,
            index=index,
            name=name,
            filter=filter,
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
        *,
        csv_files: str,
        release: Daily,
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
                release,
                self._scan_csv_with_polars(csv_files, filter)
            ).collect()
            _logger.debug(
                'extracted rows=%d, strategy=1, using="globbing Pola.rs", file="%s"',
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
        for file_no, file_path in enumerate(files):
            progress.step(
                self.extract_data_step_number(index, 2 + file_no), extra=f"extracting {file_path.name}"
            )

            try:
                frame = self.finish_frame(
                    release,
                    self._scan_csv_with_polars(file_path, filter)
                ).collect()
                frames.append(frame)

                _logger.debug(
                    'extracted rows=%d, strategy=2, using="Pola.rs", file="%s"',
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
                    self._read_csv_row_by_row(file_path, filter).lazy()
                ).collect()
                frames.append(frame)

                _logger.debug(
                    'extracted rows=%d, strategy=3, using="Python\'s CSV module", file="%s"',
                    frame.height, file_path.name
                )
            except Exception as x:
                _logger.error(
                    'failed to parse with strategy=3, using="Python\'s CSV module", file="%s"',
                    file_path.name, exc_info=x
                )
                raise

        return pl.concat(frames, how="vertical", rechunk=True)

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

    def finish_frame(self, release: Daily, frame: pl.LazyFrame) -> pl.LazyFrame:
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
            # Cast list elements and date columns to their types. Add released_on.
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
                ).str.to_datetime("%Y-%m-%d %H:%M:%S", time_unit="ms"),
                pl.lit(release.start_date).alias("released_on"),
            )
        )

    def _validate_schema(self, frame: pl.DataFrame) -> None:
        """Validate the schema of the given data frame."""
        for name in frame.columns:
            # FIXME: Remove exemption when production data has been upgraded
            if name == "released_on":
                continue
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
        self,
        root: Path,
        release: Release,
        metadata: pl.DataFrame,
        collector: CollectorProtocol,
    ) -> None:
        frame = pl.read_parquet(f"{root}/{release.batch_glob}")
        start_date = pl.lit(release.start_date).alias("start_date")
        end_date = pl.lit(release.end_date).alias("end_date")

        batch_count, total_rows, total_rows_with_keywords = metadata.select(
            pl.col("batch_count").sum(),
            pl.col("total_rows").sum(),
            pl.col("total_rows_with_keywords").sum(),
        ).row(0)

        csam = (
            frame.filter(pl.col("category_specification")
            .list.contains("KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL"))
        ).select(
            # Just CSAM
            pl.len().alias("csam"),

            # Timing, content types
            *mean_timing("csam_"),
            *content_type_breakdown("csam_"),

            # Decision combinations
            *decision_type_breakdown("csam_"),
            *decision_visibility_breakdown("csam_"),
            *decision_monetary_breakdown("csam_"),
            *decision_provision_breakdown("csam_"),
            *decision_account_breakdown("csam_"),

            # CSAM, Account Suspended, End Date Account Restriction
            pl.col("decision_account").eq("DECISION_ACCOUNT_SUSPENDED")
            .and_(pl.col("end_date_account_restriction").is_null())
            .sum().alias("csam_account_suspended_null_date"),
            pl.col("decision_account").eq("DECISION_ACCOUNT_SUSPENDED")
            .and_(pl.col("end_date_account_restriction").is_null().not_())
            .sum().alias("csam_account_suspended_until_date"),

            # CSAM, Account Terminated, End Date Account Restriction
            pl.col("decision_account").eq("DECISION_ACCOUNT_TERMINATED")
            .and_(pl.col("end_date_account_restriction").is_null())
            .sum().alias("csam_account_terminated_null_date"),
            pl.col("decision_account").eq("DECISION_ACCOUNT_TERMINATED")
            .and_(pl.col("end_date_account_restriction").is_null().not_())
            .sum().alias("csam_account_terminated_until_date"),

            *account_type_breakdown("csam_"),
            *decision_ground_breakdown("csam_"),
            *source_type_breakdown("csam_"),
            *automated_detection_and_decision_breakdown("csam_"),
        )

        stats = frame.select(
            # Age the data
            start_date,
            end_date,

            # Stats about archival data
            pl.lit(total_rows).alias("total_rows"),
            pl.lit(total_rows_with_keywords).alias("total_rows_with_keywords"),

            # Timing, content types
            *mean_timing(""),
            *content_type_breakdown(""),

            # Stats about batching
            pl.lit(batch_count).alias("batch_count"),

            # Stats about working set
            pl.len().alias("rows"),
            pl.col("category_specification").list.len().sum()
            .alias("keywords"),
            pl.col("category_specification").list.len().gt(0).sum()
            .alias("rows_with_keywords"),
            pl.col("category_specification").list.len().max()
            .alias("max_keywords_per_row"),

            # Decision Kind, Provision Decisions
            *decision_type_breakdown(""),
            *decision_visibility_breakdown(""),
            *decision_monetary_breakdown(""),
            *decision_provision_breakdown(""),
            *decision_account_breakdown(""),
            *account_type_breakdown(""),
            *decision_ground_breakdown(""),

            # Account Suspended, End Date Account Restriction
            pl.col("decision_account").eq("DECISION_ACCOUNT_SUSPENDED").and_(
                pl.col("end_date_account_restriction").is_null()
            ).sum().alias("account_suspended_null_date"),
            pl.col("decision_account").eq("DECISION_ACCOUNT_SUSPENDED").and_(
                pl.col("end_date_account_restriction").is_null().not_()
            ).sum().alias("account_suspended_until_date"),

            # Account Terminated, End Date Account Restriction
            pl.col("decision_account").eq("DECISION_ACCOUNT_TERMINATED").and_(
                pl.col("end_date_account_restriction").is_null()
            ).sum().alias("account_terminated_null_date"),
            pl.col("decision_account").eq("DECISION_ACCOUNT_TERMINATED").and_(
                pl.col("end_date_account_restriction").is_null().not_()
            ).sum().alias("account_terminated_until_date"),

            *source_type_breakdown(""),
            *automated_detection_and_decision_breakdown(""),
        ).with_columns(
            # FIXME Update to UInt128 when that type can be written to parquet files.
            pl.exclude("start_date", "end_date", "batch_count", "max_keywords_per_row")
            .cast(pl.UInt64),
            pl.col("batch_count").cast(pl.UInt64),
            pl.col("max_keywords_per_row").cast(pl.UInt32),
        ).with_columns(
            pl.col("start_date", "end_date").cast(dt.date)
        )

        stats = stats.hstack(csam)

        keywords = frame.select(
            pl.col("category_specification")
            .list.explode()
            .drop_nulls()
            .value_counts()
            .struct.unnest()
        ).select(
            start_date,
            end_date,
            pl.col("category_specification").alias("keyword"),
            pl.col("count"),
        )

        platforms = frame.select(
            pl.col("platform_name").alias("platform"),
            pl.col("category_specification"),
        ).group_by("platform").agg(
            pl.len().alias("total"),
            pl.col("category_specification").is_null().not_().sum().alias("has_keyword"),
            pl.col("category_specification").list.contains("KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL")
            .sum().alias("is_csam"),
        ).select(
            start_date,
            end_date,
            pl.col("platform", "total", "has_keyword", "is_csam")
        )

        frames = dict(
            stats=stats,
            keywords=keywords,
            platforms=platforms,
        )

        if _DEBUG_OUTLIERS:
            frames["outliers"] = frame.filter(
                (2 <= pl.col("category_specification").list.len())
                | (2 <= pl.col("decision_visibility").list.len())
            )

        collector.add_frames(release, **frames)

    @annotate_error(filename_arg="root")
    def combine_releases(
        self, root: Path, coverage: Coverage, collector: CollectorProtocol
    ) -> pl.DataFrame:
        summary = None

        for key, frame in collector.consume_frames():
            if key == "stats":
                summary = frame
                validate_general_statistics(frame)
                validate_csam_statistics(frame)
                self.write_parquet(frame, root / STATISTICS_FILE)
            elif key == "outliers":
                # Requires _DEBUG_OUTLIERS
                self.write_parquet(frame, root / "outliers.parquet")
            elif key == "keywords":
                self.write_parquet(frame, root / KEYWORDS_FILE)
            elif key == "platforms":
                self.write_parquet(frame, root / PLATFORMS_FILE)
            else:
                raise ValueError(f"unexpected frame {key}")

        assert summary is not None
        return summary

    def write_parquet(self, frame: pl.DataFrame, path: Path) -> None:
        tmp = path.with_suffix(".tmp.parquet")
        frame.write_parquet(tmp)
        tmp.replace(path)
