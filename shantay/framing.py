"""
Utility functions for using data frames.

The model, metadata, and processor modules define shantay's internal API surface
to its data processing pipeline. They are designed to be independent of the use
case, the DSA transparency database. As such, they should not need to touch upon
data frames with the actual data. Furthermore, from an interface design
perspective, it is preferrable to keep implementation details contained within
the implementation and leak their types through the API. The choice of data
frames qualifies as such an implementation detail.

Currently, there are a few method signatures that require data frames. There
also are a few places that need to mediate between API surface and data frames.
This module collects the functions necessary for the latter.
"""
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import datetime as dt
import enum
from importlib import import_module
from typing import Any, Self

import polars as pl

from .metadata import FullMetadataEntry
from .model import ConfigError, DateRange, Period, QueryExpression, Release
from .schema import ColumnValueType, EntityValueType, VariantValueType
from .util import scale_time


_DECISION_OFFSET = len("decision_")

_DECISION_TYPES = (
    "decision_visibility",
    "decision_monetary",
    "decision_provision",
    "decision_account",
)


def collect_release_metadata(
    records: Iterator[FullMetadataEntry]
) -> tuple[DateRange, pl.DataFrame]:
    """
    Collect metadata release records into a data frame.

    This function does *not* depend on the particulars of the DSA SoR DB schema.

    The data frame uses `u64` for columns containing counts. The corresponding
    resolution is barely sufficient for the current use case and hence switching
    to `u128` is highly desirable. However, for now, that is impossible because
    Pola.rs does not yet support writing parquet files with the later integers.
    """
    frame = pl.json_normalize([*records]).with_columns(
        pl.col("release").str.to_date("%Y-%m-%d"),
        pl.selectors.integer().as_expr().exclude("batch_count").cast(pl.UInt64),
    ).select(
        pl.col("release").alias("start_date"),
        pl.col("release").alias("end_date"),
        pl.exclude("release"),
    )

    start_date, end_date = frame.select(
        pl.col("start_date").min().alias("start_date"),
        pl.col("end_date").max().alias("end_date"),
    ).row(0)

    return DateRange(start_date, end_date), frame


def extract_category_from_parquet(glob: str) -> None | str:
    """
    Return the category, if the parquet files matching the glob have a
    consistent value for that column. Otherwise, return `None`.

    This function is specific to the DSA SoR DB schema.
    """
    counts = pl.scan_parquet(glob).select(
        pl.col("category")
        .drop_nulls()
        .value_counts(sort=True)
        .struct.field("category")
    ).collect()

    return counts.item() if counts.height == 1 else None


def is_row_within_period(period: Period) -> pl.Expr:
    """
    Create the query predicate testing whether a row's `start_date` and
    `end_date` fall within the given period.
    """
    return (
        (period.start_date <= pl.col("start_date")) & (pl.col("end_date") <= period.end_date)
    )


def filter_period(frame: pl.DataFrame, period: Period) -> pl.DataFrame:
    """Filter the data frame for rows that fall within the given period."""
    return frame.filter(is_row_within_period(period))


def resolve_query_binding(s: str) -> QueryExpression:
    module, _, binding = s.partition(":")
    if not module:
        raise ConfigError(f'module binding "{s}" without module (before colon)')
    if not binding:
        raise ConfigError(f'module binding "{s}" without binding (after colon)')

    try:
        m = import_module(module)
    except ImportError:
        raise ConfigError(f'unable to import module for module binding "{s}"')
    try:
        v = getattr(m, binding)
    except AttributeError:
        raise ConfigError(f'attribute not found for module binding "{s}"')
    if not isinstance(v, pl.Expr):
        raise ConfigError(f'value of module binding "{s}" is not a Pola.rs expression')
    return v


class _FieldType(enum.Enum):
    """The different field types in a snapshot."""
    ROWS = enum.auto()
    VALUE_COUNTS = enum.auto()
    LIST_VALUE_COUNTS = enum.auto()
    DECISION_TYPE = enum.auto()


@dataclass(frozen=True, slots=True)
class _DurationField:
    """A duration is the difference of two datetimes."""
    start: str
    end: str


@dataclass(frozen=True, slots=True)
class _ValueCountsPlusField:
    """Value counts for a field as well as in combination with another one."""
    other_field: str
    other_value: None | str = None
    other_is_list: None | bool = None


# The fields cover all DSA transparency database entries without unconstrained text.
_FIELDS = {
    "rows": _FieldType.ROWS,
    "decision_type": _FieldType.DECISION_TYPE,
    "decision_visibility": _ValueCountsPlusField("end_date_visibility_restriction", None, True),
    "visibility_restriction_duration": _DurationField("application_date", "end_date_visibility_restriction"),
    "decision_monetary": _FieldType.VALUE_COUNTS,
    "monetary_restriction_duration": _DurationField("application_date", "end_date_monetary_restriction"),
    "decision_provision": _ValueCountsPlusField("end_date_service_restriction"),
    "service_restriction_duration": _DurationField("application_date", "end_date_service_restriction"),
    "decision_account": _ValueCountsPlusField("end_date_account_restriction"),
    "account_restriction_duration": _DurationField("application_date", "end_date_account_restriction"),
    "account_type": _FieldType.VALUE_COUNTS,
    "decision_ground": _FieldType.VALUE_COUNTS,
    "incompatible_content_illegal": _FieldType.VALUE_COUNTS,
    "category": _FieldType.VALUE_COUNTS,
    "category_addition": _FieldType.LIST_VALUE_COUNTS,
    "category_specification": _FieldType.LIST_VALUE_COUNTS,
    "content_type": _FieldType.LIST_VALUE_COUNTS,
    "content_language": _FieldType.VALUE_COUNTS,
    "moderation_delay": _DurationField("content_date", "application_date"),
    "disclosure_delay": _DurationField("application_date", "created_at"),
    "source_type": _FieldType.VALUE_COUNTS,
    "automated_detection": _FieldType.VALUE_COUNTS,
    "automated_decision": _FieldType.VALUE_COUNTS,
    "platform_name": _ValueCountsPlusField(
        "category_specification",
        "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL",
        True
    ),
}


class Reducer:

    def __init__(self) -> None:
        self._source = None
        self._tag = None
        self._release = None
        self._frames = []

    @contextmanager
    def release(
        self,
        frame: pl.DataFrame | pl.LazyFrame,
        tag: str,
        release: Release
    ) -> Iterator[Self]:
        """Create a context for the release."""
        old_source, self._source = self._source, frame
        old_tag, self._tag = self._tag, tag
        old_release, self._release = self._release, release
        try:
            yield self
        finally:
            self._source = old_source
            self._tag = old_tag
            self._release = old_release

    @contextmanager
    def tagged_frame(
        self,
        tag: str,
        frame: pl.DataFrame | pl.LazyFrame,
    ) -> Iterator[Self]:
        """Create a tagged context."""
        old_source, self._source = self._source, frame
        old_tag, self._tag = self._tag, tag
        try:
            yield self
        finally:
            self._source = old_source
            self._tag = old_tag


class Collector(Reducer):

    def add_frame(
        self,
        column: str,
        entity: None | str = None,
        # Three value columns
        duration: None | pl.Expr = None,
        variant: None | str = None,
        count: None | int | pl.Expr = None,
        # value_counts replaces variant and/or count
        value_counts: None | pl.Expr = None,
    ) -> None:
        """Add a frame, which has a small number of rows."""
        if duration is not None:
            effective_duration = duration.cast(pl.Duration).alias("duration")
        else:
            effective_duration = pl.lit(None, dtype=pl.Duration).alias("duration")

        if value_counts is None and count is None:
            effective_count_variant = [
                pl.lit(None, dtype=VariantValueType).alias("variant"),
                pl.lit(None, dtype=pl.UInt64).alias("count"),
            ]
        elif value_counts is not None:
            assert count is None, "pass either value_counts or counts/variant but not all"
            assert variant is None, "pass either value_counts or counts/variant but not all"
            effective_count_variant = [
                value_counts.value_counts().list.explode().struct.with_fields(
                    pl.field(column).cast(VariantValueType),
                    pl.field("count").cast(pl.UInt64),
                ).struct.unnest()
            ]
        elif isinstance(count, int): # value_count must be None
            effective_count_variant = [
                pl.lit(None, dtype=VariantValueType).alias("variant"),
                pl.lit(count, dtype=pl.UInt64).alias("count"),
            ]
        elif isinstance(count, pl.Expr):
            effective_count_variant = [
                pl.lit(None, dtype=VariantValueType).alias("variant"),
                count.cast(pl.UInt64).alias("count"),
            ]
        else:
            raise AssertionError("unreachable")

        tag = self._tag if self._tag != "" else None
        entity = entity if entity != "" else None

        assert self._source is not None
        assert self._release is not None
        frame = self._source.select(
            pl.lit(self._release.start_date, dtype=pl.Date).alias("start_date"),
            pl.lit(self._release.end_date, dtype=pl.Date).alias("end_date"),
            pl.lit(tag, dtype=pl.String).alias("tag"),
            pl.lit(column, dtype=ColumnValueType).alias("column"),
            pl.lit(entity, dtype=EntityValueType).alias("entity"),
            effective_duration,
            *effective_count_variant,
        )

        if value_counts is not None:
            frame = frame.rename({
                column: "variant",
            })

        self._frames.append(frame)

    def collect_value_counts(self, column: str, entity: str, values: pl.Expr) -> None:
        """Collect values counts for column."""
        self.add_frame(column, entity=entity, value_counts=values)

    def collect_duration(self, name: str, start: str, end: str) -> None:
        """Collect difference between end and start columns as duration."""
        self.add_frame(
             name, entity=f"{start}__{end}", duration=(pl.col(end)-pl.col(start)).drop_nulls(), count=1
        )

    def collect_decision_type(self) -> None:
        # 4 decision types makes for 16 combinations thereof
        for count in range(16):
            expr = None
            suffix = []

            for shift, column in enumerate(_DECISION_TYPES):
                if count & (1 << shift) != 0:
                    clause = pl.col(column).is_null().not_()
                    suffix.append(column[_DECISION_OFFSET:_DECISION_OFFSET+3])
                else:
                    clause = pl.col(column).is_null()

                if shift == 0:
                    expr = clause
                else:
                    assert expr is not None
                    expr = expr.and_(clause)

            assert expr is not None
            entity = "all_null" if count == 0 else "_".join(suffix)
            self.add_frame("decision_type", entity=entity, count=expr.sum())

    def collect_header(
        self, batch_count: int, total_rows: int, total_rows_with_keywords: int
    ) -> None:
        self.add_frame("batch_count", count=batch_count)
        self.add_frame("total_rows", count=total_rows)
        self.add_frame("total_rows_with_keywords", count=total_rows_with_keywords)

    def collect_snapshot(self) -> None:
        for key, value in _FIELDS.items():
            match value:
                case _FieldType.ROWS:
                    self.add_frame(key, count=pl.len())
                case _FieldType.VALUE_COUNTS:
                    self.collect_value_counts(key, "", pl.col(key))
                case _FieldType.LIST_VALUE_COUNTS:
                    self.add_frame(
                        key, entity="elements",
                        count=pl.col(key).list.len().cast(pl.UInt64).sum()
                    )
                    self.add_frame(
                        key, entity="max_elements_per_row",
                        count=pl.col(key).list.len().max()
                    )
                    self.add_frame(
                        key, entity="rows_with_elements",
                        count=pl.col(key).list.len().gt(0).sum()
                    )
                    self.collect_value_counts(key, "", pl.col(key).list.explode())
                case _FieldType.DECISION_TYPE:
                    self.collect_decision_type()
                case _DurationField(start, end):
                    self.collect_duration(key, start, end)
                case _ValueCountsPlusField(other_field, other_value, other_is_list):
                    self.collect_value_counts(
                        key, "",
                        values=pl.col(key).list.explode() if other_is_list else pl.col(key))
                    if self._tag != "baseline":
                        continue
                    self.collect_value_counts(
                        key, entity=f"with_{other_field}",
                        values=pl.col(key).filter(pl.col(other_field).is_null().not_())
                    )
                    if other_value is None:
                        continue
                    self.collect_value_counts(
                        key, entity=f"with_{other_value}",
                        values=pl.col(key).filter(
                            pl.col(other_field).eq(other_value) if not other_is_list
                            else pl.col(other_field).list.contains(other_value)
                        )
                    )

    def collect(
        self,
        frame: pl.DataFrame | pl.LazyFrame,
        release: Release,
        batch_count: int,
        total_rows: int,
        total_rows_with_keywords: int,
    ) -> None:
        with self.release(frame, "baseline", release) as this:
            this.collect_header(batch_count, total_rows, total_rows_with_keywords)
            this.collect_snapshot()

        csam = frame.filter(
            pl.col("category_specification").list.contains(
                "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL"
            )
        )
        with self.release(csam, "csam", release) as this:
            this.collect_snapshot()

    def to_frame(self) -> pl.DataFrame:
        frame = pl.concat(self._frames, how="vertical")
        if isinstance(frame, pl.LazyFrame):
            frame = frame.collect()
        return frame


type Summary = list[tuple[str, Any]]


class Summarizer(Reducer):

    def __init__(self) -> None:
        super().__init__()
        self._summary = []

    def extract_count(
        self,
        column: str,
        entity: str = "",
    ) -> int | dt.timedelta:
        assert self._source is not None
        filtered = pl.col("count").filter(
            pl.col("tag").eq(self._tag)
            .and_(pl.col("column").eq(column))
            .and_(pl.col("entity").eq(entity))
        )

        if entity == "min":
            op = "min"
        elif entity in ("max", "max_elements_per_row"):
            op = "max"
        elif entity == "mean":
            op = "mean"
        else:
            op = "sum"
        filtered = getattr(filtered, op)()

        frame = self._source.select(filtered)
        if isinstance(frame, pl.LazyFrame):
            return frame.collect().item()
        else:
            return frame.item()

    def extract_value_counts(self, column: str, entity: str) -> pl.DataFrame:
        assert self._source is not None
        frame = self._source.filter(
            pl.col("tag").eq(self._tag)
            .and_(pl.col("column").eq(column))
            .and_(pl.col("entity").eq(entity))
        ).select(
            pl.col("tag", "column", "entity"),
            pl.col("count").sum()
        ).sort(
            "count", descending=True
        )

        if isinstance(frame, pl.LazyFrame):
            return frame.collect()
        else:
            return frame

    def collect1(self, column: str, entity: str) -> None:
        parts = column.rsplit("_", maxsplit=1)
        assert len(parts) < 2 or parts[-1] not in ("delay", "duration")
        value = self.extract_count(column, entity)
        self._summary.append((f"{column} #{self._tag} @{entity}", value))

    def collect_value_counts(self, column: str, entity: str = "") -> None:
        for row in self.extract_value_counts(column, entity).rows():
            tag, column, entity, count = row
            self._summary.append((f"{column} #{tag} @{entity}", count))

    def summarize_snapshot(self) -> None:
        for field_name, field_type in _FIELDS.items():
            match field_type:
                case _FieldType.ROWS:
                    self.extract_count("rows")
                case _FieldType.VALUE_COUNTS:
                    self._summary.append(("", ""))
                    self.collect_value_counts(field_name)
                case _FieldType.LIST_VALUE_COUNTS:
                    self._summary.append(("", ""))
                    self.collect1(field_name, "elements")
                    self.collect1(field_name, "max_elements_per_row")
                    self.collect1(field_name, "rows_with_elements")
                    self.collect_value_counts(field_name)
                case _DurationField(_, _):
                    self._summary.append(("", ""))
                    self.collect1(field_name, "min")
                    self.collect1(field_name, "mean")
                    self.collect1(field_name, "max")
                    self.collect1(field_name, "nulls")
                case _ValueCountsPlusField(other_field, other_value, _):
                    self._summary.append(("", ""))
                    self.collect_value_counts(field_name)
                    self._summary.append(("", ""))
                    self.collect_value_counts(field_name, f"with_{other_field}")
                    if other_value is not None:
                        self._summary.append(("", ""))
                        self.collect_value_counts(field_name, f"with_{other_value}")
                case _FieldType.DECISION_TYPE:
                    for count in range(16):
                        suffix = []

                        for shift, column in enumerate(_DECISION_TYPES):
                            if count & (1 << shift) != 0:
                                suffix.append(column[_DECISION_OFFSET:_DECISION_OFFSET+3])

                        self.collect1(
                            field_name,
                            "_".join(suffix) if count != 0  else "none",
                        )

    def summarize(self, frame: pl.DataFrame) -> Summary:
        self._summary = [
            ("start_date", frame.select(pl.col("start_date").min()).item()),
            ("end_date", frame.select(pl.col("end_date").max()).item()),
            ("batch_count", self.extract_count("batch_count")),
            ("total_rows", self.extract_count("total_rows")),
            ("total_rows_with_keywords", self.extract_count("total_rows_with_keywords")),
        ]

        for tag in frame.select(pl.col("tag").unique()).get_column("tag"):
            with self.tagged_frame(tag, frame) as this:
                this._summary.append(("", ""))
                this._summary.append(("", ""))
                # The synthetic baseline tag is liable to vanish...
                this._summary.append(("TAG", this._tag if this._tag != "baseline" else "—none—"))
                this._summary.append(("", ""))
                this.summarize_snapshot()

        return self._summary

    def formatted_summary(self, markdown: bool = True) -> str:
        """
        Format the one column summary for fixed-width display.

        The non-Markdown version uses box drawing characters whereas the Markdown
        version emits the necessary ASCII characters for cell delimiters, while
        using U+2800, Braille empty pattern, in the variable and value columns for
        empty rows. That ensures that Markdown table formatting logic recognizes
        these cells as non-empty without actually displaying anything.
        """
        text_summary = []
        for var, val in self._summary:
            try:
                if var == "":
                    var = "\u2800" if markdown else " "

                if val == "":
                    val = "\u2800" if markdown else " "
                elif var.endswith("_pct"):
                    val = f"{val:.3f}"
                elif isinstance(val, dt.date):
                    val = val.isoformat()
                elif isinstance(val, dt.timedelta):
                    # Convert to seconds as float, then scale to suitable unit
                    val /= dt.timedelta(seconds=1)
                    v, u = scale_time(val)
                    val = f"{v:.2f} {u}s"
                elif isinstance(val, float):
                    val = f"FLOAT({val})"
                else:
                    val = f"{int(val):,}"

                text_summary.append((var, val))

            except Exception as x:
                print(f"{var}: {val}")
                import traceback
                traceback.print_exception(x)
                raise

        # Limit the variable and value widths to 100 columns total
        var_width = min(max(len(r[0]) for r in text_summary), 45)
        val_width = min(max(len(r[1]) for r in text_summary), 55)

        if markdown:
            lines = [
                f"| {'Variable':<{var_width}} | {  'Value':>{val_width}} |",
                f"| :{ '-' * (var_width - 1)} | {'-' * (val_width - 1)}: |",
            ]
        else:
            lines = [
                f"┌─{        '─' * var_width}─┬─{      '─' * val_width}─┐",
                f"│ {'Variable':<{var_width}} │ { 'Value':>{val_width}} │",
                f"├─{        '─' * var_width}─┼─{      '─' * val_width}─┤",
            ]

        bar = "|" if markdown else "│"
        for var, val in text_summary:
            lines.append(f"{bar} {var:<{var_width}} {bar} {val:>{val_width}} {bar}")
        if not markdown:
            lines.append(f"└─{'─' * var_width}─┴─{'─' * val_width}─┘")

        return "\n".join(lines)


def formatted_summary(frame: pl.DataFrame, markdown: bool = True) -> str:
    summarizer = Summarizer()
    summarizer.summarize(frame)
    return summarizer.formatted_summary(markdown)
