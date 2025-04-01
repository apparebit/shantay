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
from .schema import ColumnValueType, STATISTICS_SCHEMA
from .util import scale_time


BASELINE_TAG = "Baseline"
CSAM_TAG = "CSAM"


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


# --------------------------------------------------------------------------------------


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
    self_is_list: bool = False

# The fields cover all DSA transparency database entries without unconstrained text.
_FIELDS = {
    "rows": _FieldType.ROWS,
    "decision_type": _FieldType.DECISION_TYPE,
    "decision_visibility": _ValueCountsPlusField("end_date_visibility_restriction", self_is_list=True),
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
    """
    A class to extract summary statistics from a data frame in a principled
    fashion. Concrete subclasses are `Collector` for deriving statistics from
    the working data and `Summarizer` for further condensing a collector's data
    into a (long) list of key, value pairs.
    """
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
        old_tag, self._tag = self._tag, (tag if tag != "" else None)
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
        old_tag, self._tag = self._tag, (tag if tag != "" else None)
        try:
            yield self
        finally:
            self._source = old_source
            self._tag = old_tag


# --------------------------------------------------------------------------------------


class Collector(Reducer):
    """Analyze the data while also collecting the results."""

    def add_row(
        self,
        column: str,
        entity: None | str = None,
        # Three value columns, but count and value_counts are mutually exclusive
        duration: None | pl.Expr = None,
        count: None | int | pl.Expr = None,
        value_counts: None | pl.Expr = None,
    ) -> None:
        """Add a row currently is implemented as adding a frame."""
        if duration is not None:
            effective_values = [duration.cast(pl.Duration(time_unit="ms")).alias("duration")]
        else:
            effective_values = [pl.lit(None, dtype=pl.Duration(time_unit="ms")).alias("duration")]

        if value_counts is None:
            effective_values.append(pl.lit(None, dtype=pl.Categorical).alias("variant"))
            if count is None:
                effective_values.append(pl.lit(None, dtype=pl.UInt64).alias("count"))
            elif isinstance(count, int):
                effective_values.append(pl.lit(count, dtype=pl.UInt64).alias("count"))
            elif isinstance(count, pl.Expr):
                effective_values.append(count.cast(pl.UInt64).alias("count"))
            else:
                raise AssertionError("unreachable")
        else:
            assert count is None, "provide count or value_counts but not both"
            effective_values.append(
                value_counts.value_counts().list.explode().struct.unnest()
            )

        tag = self._tag if self._tag != "" else None
        entity = entity if entity != "" else None

        assert self._source is not None
        assert self._release is not None
        frame = self._source.select(
            pl.lit(self._release.start_date, dtype=pl.Date).alias("start_date"),
            pl.lit(self._release.end_date, dtype=pl.Date).alias("end_date"),
            pl.lit(tag, dtype=pl.Categorical).alias("tag"),
            pl.lit(column, dtype=ColumnValueType).alias("column"),
            pl.lit(entity, dtype=pl.Categorical).alias("entity"),
            *effective_values,
        )

        if value_counts is not None:
            frame = frame.rename({
                column: "variant",
            }).with_columns(
                pl.col("variant").cast(pl.String).cast(pl.Categorical),
                pl.col("count").cast(pl.UInt64)
            )

        self._frames.append(frame)

    def collect_decision_type(self) -> None:
        """Collect counts for the combination of four decision types."""
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
            entity = "is_null" if count == 0 else "_".join(suffix)
            self.add_row("decision_type", entity=entity, count=expr.sum())

    def collect_snapshot(self) -> None:
        """Collect the standard statistics for the current data frame."""
        for key, value in _FIELDS.items():
            match value:
                case _FieldType.ROWS:
                    self.add_row(key, count=pl.len())
                case _FieldType.VALUE_COUNTS:
                    self.add_row(key, value_counts=pl.col(key))
                case _FieldType.LIST_VALUE_COUNTS:
                    self.add_row(
                        key, entity="elements",
                        count=pl.col(key).list.len().cast(pl.UInt64).sum()
                    )
                    self.add_row(
                        key, entity="max_elements_per_row",
                        count=pl.col(key).list.len().max()
                    )
                    self.add_row(
                        key, entity="rows_with_elements",
                        count=pl.col(key).list.len().gt(0).sum()
                    )
                    self.add_row(key, value_counts=pl.col(key).list.explode())
                case _FieldType.DECISION_TYPE:
                    self.collect_decision_type()
                case _DurationField(start, end):
                    self.add_row(key, entity="is_null", count=pl.col(end).is_null().sum())
                    self.add_row(key, entity="count", count=(pl.col(end) - pl.col(start)).count())
                    self.add_row(key, entity="min", duration=(pl.col(end) - pl.col(start)).min())
                    self.add_row(key, entity="mean", duration=(pl.col(end) - pl.col(start)).mean())
                    self.add_row(key, entity="max", duration=(pl.col(end) - pl.col(start)).max())
                case _ValueCountsPlusField(other_field, other_value, other_is_list, self_is_list):
                    values = pl.col(key).list.explode() if self_is_list else pl.col(key)
                    self.add_row(key, value_counts=values)
                    if self._tag != BASELINE_TAG:
                        continue

                    values = pl.col(key).filter(pl.col(other_field).is_null().not_())
                    if self_is_list:
                        values = values.list.explode()
                    self.add_row(
                        key,
                        entity=(
                            "with_end_date" if other_field.startswith("end_date")
                            else f"with_{other_field}"
                        ),
                        value_counts=values,
                    )
                    if other_value is None:
                        continue

                    if other_is_list:
                        values = pl.col(key).filter(pl.col(other_field).list.contains(other_value))
                    else:
                        values = pl.col(key).filter(pl.col(other_field).eq(other_value))
                    if self_is_list:
                        values = values.list.explode()
                    self.add_row(key, entity=f"with_{other_value}", value_counts=values)

    def collect_header(
        self,
        batch_count: int,
        total_rows: int,
        total_rows_with_keywords: int,
    ) -> None:
        """Create a header frame with the given statistics."""
        assert self._release is not None
        header = pl.DataFrame({
            "start_date": 3 * [self._release.start_date],
            "end_date": 3 * [self._release.end_date],
            "tag": 3 * [BASELINE_TAG],
            "column": ["batch_count", "total_rows", "total_rows_with_keywords"],
            "entity": [None, None, None],
            "duration": [None, None, None],
            "variant": [None, None, None],
            "count": [batch_count, total_rows, total_rows_with_keywords],
        }, schema=STATISTICS_SCHEMA)
        self._frames.append(header)

    def collect(
        self,
        frame: pl.DataFrame | pl.LazyFrame,
        release: Release,
        batch_count: int,
        total_rows: int,
        total_rows_with_keywords: int,
    ) -> None:
        """Collect all necessary data in partial data frames."""
        with self.release(frame, BASELINE_TAG, release) as this:
            this.collect_header(batch_count, total_rows, total_rows_with_keywords)
            this.collect_snapshot()

        csam = frame.filter(
            pl.col("category_specification").list.contains(
                "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL"
            )
        )
        with self.release(csam, CSAM_TAG, release) as this:
            this.collect_snapshot()

    def to_frame(self) -> pl.DataFrame:
        """Combine the collected partial frames into one."""
        frame = pl.concat(self._frames, how="vertical")
        if isinstance(frame, pl.LazyFrame):
            frame = frame.collect()
        return frame


# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Tag:
    """A tag."""

    tag: str

    def __format__(self, spec) -> str:
        return str.__format__(self.tag, spec)

    def __len__(self) -> int:
        return len(self.tag) + 2

    def __str__(self) -> str:
        return self.tag


class Spacer:
    """A marker object for empty cells."""
    def __str__(self) -> str:
        return ""

"""The canonical spacer object."""
SPACER = Spacer()


"""
The type of summary statistics, which is a list of key, value pairs. To aid with
presentation, some of the pairs may be empty, containing `SPACER` instances (see
below).
"""
type Summary = list[tuple[str | Tag | Spacer, Any]]


class NothingType:
    pass

NOTHING = NothingType()


class Summarizer(Reducer):
    """Summarize analysis results."""

    def __init__(self) -> None:
        super().__init__()
        self._summary = []

    def predicate(
        self,
        column: NothingType | str = NOTHING,
        entity: NothingType | None | str = NOTHING,
        variant: NothingType | None | str = NOTHING,
    ) -> pl.Expr:
        """Build a predicate for some combination of column, entity, and variant."""
        if self._tag is None or self._tag == "":
            predicate = pl.col("tag").is_null()
        else:
            predicate = pl.col("tag").eq(self._tag)

        if column is not NOTHING:
            predicate = predicate.and_(pl.col("column").eq(column))
        if entity is None:
            predicate = predicate.and_(pl.col("entity").is_null())
        elif entity is not NOTHING:
            predicate = predicate.and_(pl.col("entity").eq(entity))
        if variant is None:
            predicate = predicate.and_(pl.col("variant").is_null())
        elif variant is not NOTHING:
            predicate = predicate.and_(pl.col("variant").eq(variant))

        return predicate

    def extract_value(
        self,
        column: str,
        entity: None | str = None,
        duration: bool = False,
    ) -> int | dt.timedelta:
        """Extract the aggregated value with the given column and entity."""
        assert self._source is not None
        if entity == "mean":
            return self._source.lazy().filter(
                self.predicate(column=column, variant=None)
            ).select(
                (
                    (pl.col("duration").filter(pl.col("entity").eq("mean")).cast(pl.UInt64)
                    * pl.col("count").filter(pl.col("entity").eq("count"))).sum()
                    // pl.col("count").filter(pl.col("entity").eq("count")).sum()
                ).cast(pl.Duration(time_unit="ms"))
            ).collect().item()
        elif entity in ("min", "max"):
            op = entity
        elif entity == "max_elements_per_row":
            op = "max"
        else:
            op = "sum"

        filter = pl.col("duration" if duration else "count").filter(
            self.predicate(column, entity, None)
        )
        frame = self._source.select(getattr(filter, op)())
        if isinstance(frame, pl.LazyFrame):
            return frame.collect().item()
        else:
            return frame.item()

    def extract_value_counts(self, column: str, entity: None | str) -> pl.DataFrame:
        """Extract the value counts for the given column and entity."""
        assert self._source is not None
        frame = self._source.filter(
            self.predicate(column, entity)
        ).select(
            pl.col("tag", "column", "entity", "variant"),
            pl.col("count")
        ).sort(
            "count", descending=True
        )

        if isinstance(frame, pl.LazyFrame):
            return frame.collect()
        else:
            return frame

    @contextmanager
    def spacer_on_demand(self) -> Iterator[None]:
        actual_summary = self._summary
        self._summary = []
        try:
            yield None
        finally:
            if 0 < len(self._summary):
                self.spacer(actual_summary)
                actual_summary.extend(self._summary)
            self._summary = actual_summary

    def spacer(self, summary: None | Summary = None) -> None:
        if summary is None:
            summary = self._summary
        summary.append((SPACER, SPACER))

    def collect1(
        self, column: str, entity: None | str = None, duration: bool = False
    ) -> None:
        variable = column if entity is None or entity == "" else f"{column}.{entity}"
        value = self.extract_value(column, entity, duration=duration)
        self._summary.append((variable, value))

    def collect_value_counts(self, column: str, entity: None | str = None) -> None:
        for row in self.extract_value_counts(column, entity).rows():
            _, column, entity, variant, count = row
            var = column
            if entity:
                var = f"{var}.{entity}"
            if variant is None:
                var = f"{var}.is_null"
            else:
                var = f"{var}.{variant}"

            self._summary.append((var, count))

    def summarize_snapshot(self) -> None:
        for field_name, field_type in _FIELDS.items():
            match field_type:
                case _FieldType.ROWS:
                    self.extract_value("rows")
                case _FieldType.VALUE_COUNTS:
                    self.spacer()
                    self.collect_value_counts(field_name)
                case _FieldType.LIST_VALUE_COUNTS:
                    self.spacer()
                    self.collect1(field_name, "elements")
                    self.collect1(field_name, "max_elements_per_row")
                    self.collect1(field_name, "rows_with_elements")
                    self.collect_value_counts(field_name)
                case _DurationField(start, end):
                    self.spacer()
                    self.collect1(field_name, "is_null")
                    self.collect1(field_name, "count")
                    self.collect1(field_name, "min", duration=True)
                    self.collect1(field_name, "mean", duration=True)
                    self.collect1(field_name, "max", duration=True)
                case _ValueCountsPlusField(other_field, other_value, _):
                    self.spacer()
                    self.collect_value_counts(field_name)

                    with self.spacer_on_demand():
                        entity = (
                            "with_end_date" if other_field.startswith("end_date")
                            else f"with_{other_field}"
                        )
                        self.collect_value_counts(field_name, entity)
                    if other_value is None:
                        continue
                    with self.spacer_on_demand():
                        self.collect_value_counts(field_name, f"with_{other_value}")
                case _FieldType.DECISION_TYPE:
                    for count in range(16):
                        suffix = []

                        for shift, column in enumerate(_DECISION_TYPES):
                            if count & (1 << shift) != 0:
                                suffix.append(column[_DECISION_OFFSET:_DECISION_OFFSET+3])

                        self.collect1(
                            field_name,
                            "_".join(suffix) if count != 0  else "is_null",
                        )

    def summarize(self, frame: pl.DataFrame) -> Summary:
        with self.tagged_frame(BASELINE_TAG, frame) as this:
            this._summary = [
                ("start_date", frame.select(pl.col("start_date").min()).item()),
                ("end_date", frame.select(pl.col("end_date").max()).item()),
                ("batch_count", this.extract_value("batch_count")),
                ("total_rows", this.extract_value("total_rows")),
                ("total_rows_with_keywords", this.extract_value("total_rows_with_keywords")),
            ]

        # Make sure that baseline comes first
        tags = [
            BASELINE_TAG,
            *(
                t
                for t in frame.select(pl.col("tag").unique()).get_column("tag").to_list()
                if t != BASELINE_TAG
            )
        ]

        for tag in tags:
            with self.tagged_frame(tag, frame) as this:
                self.spacer()
                self.spacer()
                assert this._tag is not None
                this._summary.append((Tag(this._tag), Tag(this._tag)))
                self.spacer()
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
        formatted_pairs = []
        for var, val in self._summary:
            try:
                if isinstance(var, Tag):
                    # Delay formatting of tag for non-markdown output
                    # so that we can center it
                    assert isinstance(val, Tag)
                    svar = f"## {var} ##" if markdown else var
                elif var is SPACER:
                    svar = "\u2800" if markdown else " "
                else:
                    svar = var

                if isinstance(val, Tag):
                    assert isinstance(var, Tag)
                    sval = f"## {val} ##" if markdown else val
                elif val is SPACER:
                    sval = "\u2800" if markdown else " "
                elif val is None:
                    sval = "␀"
                elif (
                    var is not SPACER
                    and not isinstance(var, Tag)
                    and var.endswith("_pct")
                ):
                    sval = f"{val:.3f}"
                elif isinstance(val, dt.date):
                    sval = val.isoformat()
                elif isinstance(val, dt.timedelta):
                    # Convert to seconds as float, then scale to suitable unit
                    v, u = scale_time(val / dt.timedelta(seconds=1))
                    sval = f"{v:,.1f} {u}s"
                elif isinstance(val, int):
                    sval = f"{val:,}"
                elif isinstance(val, float):
                    sval = f"{val:.2f}"
                else:
                    sval = f"FIXME({val})"

                formatted_pairs.append((svar, sval))

            except Exception as x:
                print(f"{var}: {val}")
                import traceback
                traceback.print_exception(x)
                raise

        # Limit the variable and value widths to 100 columns total
        var_width = max(len(r[0]) for r in formatted_pairs)
        val_width = max(len(r[1]) for r in formatted_pairs)
        if 120 < var_width + val_width:
            var_width = min(60, var_width)
            val_width = min(60, val_width)

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

        bar = "|" if markdown else "\u2502"
        for var, val in formatted_pairs:
            if isinstance(var, Tag) and not markdown:
                assert isinstance(val, Tag)
                var = f" {var} ".center(var_width + 2, "═")
                val = f" {val} ".center(val_width + 2, "═")
                lines.append(
                    f"╞{var}╪{val}╡"
                )
                continue

            lines.append(
                f"{bar} {var:<{var_width}} {bar} {val:>{val_width}} {bar}"
            )
        if not markdown:
            lines.append(f"└─{'─' * var_width}─┴─{'─' * val_width}─┘")

        return "\n".join(lines)


def formatted_summary(frame: pl.DataFrame, markdown: bool = True) -> str:
    """
    Summarize the analysis results and return a nicely formatted, plain-text
    table.
    """
    summarizer = Summarizer()
    summarizer.summarize(frame)
    return summarizer.formatted_summary(markdown)
