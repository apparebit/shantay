from collections import Counter
import csv
import hashlib
import logging
from pathlib import Path
from typing import cast

import polars as pl

from .metadata import fill_entry
from .model import (
    CollectorProtocol, Daily, Dataset, Filter, FilterKind, FullMetadataEntry,
    MetadataEntry, MetadataProtocol, Release
)
from .progress import NO_PROGRESS, Progress
from .schema import (
    BASE_SCHEMA_V1, BASE_SCHEMA_V2, CanonicalPlatformNames,
    PARTIAL_SCHEMA, SCHEMA, TerritorialAlias, validate
)
from .util import annotate_error


PROBLEMATIC_RELEASES = tuple(Release.of(*d) for d in (
    (2024, 3, 23),
    (2024, 3, 24),
    (2024, 3, 25),
    (2024, 3, 26),
    (2024, 3, 27),
    (2024, 3, 28),
    (2024, 3, 29),
    (2024, 3, 30),
    (2024, 3, 31),
    (2024, 4, 1),
    (2024, 4, 2),
    (2024, 4, 9),
    (2024, 4, 10),
    (2024, 4, 11),
    (2024, 4, 12),
    (2024, 4, 13),
    (2024, 4, 14),
    (2024, 4, 15),
    (2024, 4, 16),
))


_logger = logging.getLogger(__spec__.parent)


def _parse_list(*columns: str) -> pl.Expr:
    """
    This function currently assumes that list values do *not* contain commas,
    which holds for list-valued DSA SoR DB entries.
    """
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


def _empty_list_to_null(column: str) -> pl.Expr:
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
    def ingest_release(
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
            filter=None,
            progress=progress
        )
        validate(frame, SCHEMA)

        counters = self.get_frame_row_counts(frame, is_extract=False)
        return counters, frame

    @annotate_error(filename_arg="root")
    def distill_release(
        self,
        *,
        root: Path,
        release: Daily,
        index: int,
        name: str,
        filter: Filter,
        progress: Progress = NO_PROGRESS
    ) -> tuple[str, Counter]:
        path = root / release.temp_directory
        csv_files = f"{path}/sor-global-{release.id}-full-{index:05}-*.csv"

        progress.step(index, "count rows")
        counters = self.get_batch_row_counts(root, release, index)

        frame = self._read_rows(
            csv_files=csv_files,
            release=release,
            index=index,
            name=name,
            filter=filter,
            progress=progress
        )
        validate(frame, SCHEMA)

        path = root / release.directory
        path.mkdir(parents=True, exist_ok=True)
        path = path / release.batch_file(index)

        # Write the parquet file and immediately read it again to compute
        # digest. Experiments with a large file suggest that this performs at
        # least as well as intercepting writes for computing the digest,
        # presumably because the OS cache absorbs most of that I/O.
        frame.write_parquet(path)
        with open(path, mode="rb") as file:
            digest = hashlib.file_digest(file, "sha256").hexdigest()

        counters += self.get_frame_row_counts(frame, is_extract=True)
        return digest, counters

    def get_batch_row_counts(
        self,
        root: Path,
        release: Daily,
        index: int,
    ) -> Counter:
        """
        Get the total and keyword row counts for the CSV files belonging to an
        archive batch.
        """
        path = root / release.temp_directory

        counters = Counter()
        for csv_file in sorted(
            path.glob(f"sor-global-{release.id}-full-{index:05}-*.csv")
        ):
            counters += self.get_csv_row_counts(csv_file)
        return counters

    def get_csv_row_counts(self, path: Path) -> Counter:
        """
        Get the total and keyword row counts for a single CSV file from an
        archive batch.

        In the common case of CSV files being parsed with Pola.rs, this method
        does another pass over the CSV file. That obviously is less than ideal.
        But since we can't interpose on the parsing of individual rows, that
        also seems unavoidable.

        This function used to read files with Pola.rs' `scan_csv()` and then
        also parse the category_specification column; the latter was a
        workaround to badly formatted entries in about 20 out of 600 releases.
        But when `scan_csv()` became stricter with v1.31.0 (and hence failed on
        some DSA DB releases), I switched the implementation to a much simpler
        textual scan. To avoid running the full data through the processor cache
        twice, the new implementation interleaves scanning for end-of-line with
        scanning for "KEYWORD" followed by an underscore.
        """
        total_rows = keyword_rows = 0
        with open(path, mode="r", encoding="utf8") as file:
            file.readline()
            while (line := file.readline()):
                total_rows += 1
                if "KEYWORD_" in line:
                    keyword_rows += 1

        _logger.debug(
            'counted rows=%d, rows-with-keywords=%s, file="%s"',
            total_rows, keyword_rows, path.name
        )
        return Counter(total_rows=total_rows, total_rows_with_keywords=keyword_rows)

    def get_extract_row_counts(self, path: str | Path) -> Counter:
        """
        Get the total and keyword rows counts for a specific parquet file or all
        parquet files matching a glob.
        """
        return self.get_frame_row_counts(pl.read_parquet(path), is_extract=True)

    def get_frame_row_counts(
        self, frame: pl.DataFrame, is_extract: bool = False
    ) -> Counter:
        """
        Get the total and keyword row counts for the data frame. Their names
        depend on the `is_extract` flag.
        """
        keyword_rows = frame.select(
            pl.col("category_specification").is_not_null().sum(),
        ).item()

        return Counter(
            extract_rows=frame.height,
            extract_rows_with_keywords=keyword_rows
        ) if is_extract else Counter(
            total_rows=frame.height,
            total_rows_with_keywords=keyword_rows
        )

    def _read_rows(
        self,
        *,
        csv_files: str,
        release: Daily,
        index: int,
        name: str,
        filter: None | Filter,
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
                self._scan_csv_with_polars(csv_files, filter)
            )
            # Reading CSV files generally should be logged at DEBUG level.
            # However, if the first strategy fails, success should be logged at
            # INFO level to maximize chances that record will make it into log.
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
                    self._scan_csv_with_polars(file_path, filter)
                )
                frames.append(frame)

                _logger.info(
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
                frame = self._read_csv_row_by_row(file_path, filter)
                frame = self.finish_frame(release, frame.lazy())
                frames.append(frame)

                _logger.info(
                    'ingested rows=%d, strategy=3, using="Python\'s CSV module", file="%s"',
                    frame.height, file_path.name
                )
            except Exception as x:
                _logger.error(
                    'failed to read CSV with strategy=3, using="Python\'s CSV module", file="%s"',
                    file_path.name, exc_info=x
                )
                raise

        return pl.concat(frames, how="vertical", rechunk=True)

    def _scan_csv_with_polars(
        self, path: str | Path, filter: None | Filter = None
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

        if filter is not None:
            frame = frame.filter(filter.to_query())

        return frame

    def _read_csv_row_by_row(
        self, path: str | Path, filter: None | Filter = None
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
            schema = BASE_SCHEMA_V2 if "content_id_ean" in header else BASE_SCHEMA_V1

            if filter is not None and filter.kind is FilterKind.CATEGORY:
                try:
                    category_index = header.index("category")
                except ValueError as x:
                    raise ValueError(
                        f'"{path}" does not include "category" column'
                    ) from x
                try:
                    addition_index = header.index("category_addition")
                except ValueError as x:
                    raise ValueError(
                        f'"{path}" does not include "category_addition" column'
                    ) from x

                predicate = (
                    lambda row: (
                        row[category_index] == filter.criterion
                        or filter.criterion in row[addition_index]
                    )
                )
            elif filter is not None and filter.kind is FilterKind.PLATFORM:
                try:
                    platform_index = header.index("platform_name")
                except ValueError as x:
                    raise ValueError(
                        f'"{path}" does not include "platform_name" column'
                    ) from x

                predicate = lambda row: row[platform_index] in filter.criterion
            else:
                # If the filter is an arbitrary expression, we apply the query
                # after the fact.
                predicate = lambda _row: True

            for row in reader:
                if predicate(row):
                    row = [None if field in ("", "[]") else field for field in row]
                    rows.append(row)

        frame = pl.DataFrame(list(zip(*rows)), schema=schema)
        if filter is not None and filter.kind is FilterKind.EXPRESSION:
            frame = frame.filter(filter.to_query())
        return frame

    def finish_frame(self, release: Daily, frame: pl.LazyFrame) -> pl.DataFrame:
        """
        Finish the frame by patching in the names of country groups, parsing
        list-valued columns, as well as casting list elements and date columns
        to their types.
        """
        result = frame.with_columns(
            # Patch in the names of country groups as well as canonical platform names
            pl.when(pl.col("territorial_scope") == TerritorialAlias.EEA.value)
                .then(pl.lit("[\"EEA\"]"))
                .when(pl.col("territorial_scope") == TerritorialAlias.EEA_no_IS.value)
                .then(pl.lit("[\"EEA_no_IS\"]"))
                .when(pl.col("territorial_scope") == TerritorialAlias.EU.value)
                .then(pl.lit("[\"EU\"]"))
                .otherwise(pl.col("territorial_scope"))
                .alias("territorial_scope"),
            pl.col("platform_name").replace(CanonicalPlatformNames),
        ).with_columns(
            # Parse list-valued columns
            _parse_list(
                "decision_visibility",
                "category_addition",
                "category_specification",
                "content_type",
                "territorial_scope",
            )
        ).with_columns(
            # Replace empty lists with None. This method used to assume that
            # the value never is the empty list. That assumption becomes
            # superfluous with introduction of this clause.
            _empty_list_to_null("decision_visibility"),
            _empty_list_to_null("category_specification"),
            _empty_list_to_null("content_type"),
            _empty_list_to_null("territorial_scope"),
        ).with_columns(
        # Cast list elements and date columns to their types. Add released_on.
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
        ).collect()

        if "content_id_ean" not in result.columns:
            result = result.with_columns(
                pl.lit(None, dtype=pl.String).alias("content_id_ean")
            )

        return result

    @annotate_error(filename_arg="root")
    def summarize_release_extract(
        self,
        root: Path,
        release: Daily,
        metadata: MetadataProtocol,
        collector: CollectorProtocol,
    ) -> FullMetadataEntry:
        count = sum(1 for _ in (root / release.directory).glob(release.batch_glob))
        glob = f"{root}/{release.directory}/{release.batch_glob}"
        _logger.debug(
            'summarizing file-count=%d, glob="%s", release="%s", filter="%s"',
            count, glob, release, metadata.filter or ""
        )

        # With scan_parquet(), Shantay makes rapid progress only to get stuck at
        # the 100% mark executing collect(). Hence, even if eager processing is
        # a bit slower, it provides a consistent appearance of progress.
        extract = pl.read_parquet(glob).with_columns(
            pl.col("platform_name").replace(CanonicalPlatformNames)
        )

        if "content_id_ean" not in extract.columns:
            extract = extract.with_columns(
                pl.lit(None, dtype=pl.String).alias("content_id_ean")
            )

        metadata_entry = metadata[release] if release in metadata else {}
        if "batch_count" not in metadata_entry:
            metadata_entry["batch_count"] = count
        if "extract_rows" not in metadata_entry:
            metadata_entry["extract_rows"] = len(extract)
        if "extract_rows_with_keywords" not in metadata_entry:
            metadata_entry["extract_rows_with_keywords"] = extract.select(
                pl.col("category_specification").is_null().not_().sum()
            ).item()

        collector.collect(
            release,
            extract,
            filter=metadata.filter,
            metadata_entry=cast(MetadataEntry, metadata_entry),
        )

        return fill_entry(release, cast(MetadataEntry, metadata_entry))
