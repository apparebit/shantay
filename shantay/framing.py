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
from collections.abc import Iterator, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
import datetime as dt
from importlib import import_module
from typing import Any, Literal, Self

import polars as pl

from .metadata import FullMetadataEntry
from .model import ConfigError, DateRange, Period, QueryExpression, Release
from .schema import (
    ColumnValueType, DurationTransform, STATISTICS_SCHEMA, TRANSFORMS, TransformType,
    ValueCountsPlusTransform
)
from .util import scale_time


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
    Pola.rs does not yet support writing parquet files with the larger integers.
    """
    frame = pl.json_normalize([*records]).with_columns(
        pl.col("release").str.to_date("%Y-%m-%d"),
        pl.selectors.integer().as_expr().exclude("batch_count").cast(pl.Int64),
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


class Collector:
    """Analyze the data while also collecting the results."""

    def __init__(self) -> None:
        self._source = None
        self._tag = None
        self._release = None
        self._frames = []

    @contextmanager
    def source_data(
        self,
        *,
        frame: pl.DataFrame | pl.LazyFrame,
        release: Release,
        tag: None | str = None,
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

    def add_rows(
        self,
        column: str,
        entity: None | str = None,
        variant: None | pl.Expr = None,
        variant_too: None | pl.Expr = None,
        value_counts: None | pl.Expr = None,
        frame: None | pl.DataFrame | pl.LazyFrame = None,
        **kwargs: None | int | pl.Expr,
    ) -> None:
        """Add new rows."""
        if frame is None:
            assert self._source is not None
            frame = self._source

        tag = None if self._tag == "" else self._tag
        entity = None if entity == "" else entity

        effective_values = []
        if value_counts is None and variant is None:
            effective_values.append(pl.lit(None, dtype=pl.Categorical).alias("variant"))
            effective_values.append(pl.lit(None, dtype=pl.Categorical).alias("variant_too"))
        elif value_counts is None:
            assert variant is not None, "impossible value"
            effective_values.append(variant.cast(pl.Categorical).alias("variant"))
            if variant_too is None:
                effective_values.append(pl.lit(None, dtype=pl.Categorical).alias("variant_too"))
            else:
                effective_values.append(variant_too.cast(pl.Categorical).alias("variant_too"))
        else:
            assert kwargs.get("count", None) is None, "count and value_counts both not None"
            effective_values.extend([
                value_counts.value_counts(sort=True).list.explode().struct.unnest(),
                pl.lit(None, dtype=pl.Categorical).alias("variant_too"),
            ])

        for key in ("count", "min", "mean", "max"):
            if value_counts is not None and key == "count":
                continue

            value = kwargs.get(key, None)
            if value is None or isinstance(value, int):
                effective_values.append(pl.lit(value, dtype=pl.Int64).alias(key))
            else:
                effective_values.append(value.cast(pl.Int64).alias(key))

        assert self._release is not None
        frame = frame.select(
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
                pl.col("count").cast(pl.Int64)
            )

        # Enforce a canonical column order. Otherwise, concatenation won't work!
        self._frames.append(frame.select(
            pl.col(
                "start_date", "end_date", "tag", "column", "entity",
                "variant", "variant_too", "count", "min", "mean", "max"
            )
        ))

    def collect_value_counts_plus(
        self,
        field: str,
        field_is_list: bool,
        other_field: str,
        other_is_list: bool,
    ) -> None:
        """
        Collect value counts for a field in isolation and then for the field in
        combination with another field.
        """
        # Value counts for field
        values = pl.col(field).list.explode() if field_is_list else pl.col(field)
        self.add_rows(field, value_counts=values)
        if self._tag is not None:
            return

        if not is_categorical(other_field):
            values = pl.col(field).filter(pl.col(other_field).is_null().not_())
            if field_is_list:
                values = values.list.explode()
            self.add_rows(
                field,
                entity=(
                    "with_end_date" if other_field.startswith("end_date")
                    else f"with_{other_field}"
                ),
                value_counts=values,
            )
            return

        assert self._source is not None
        frame = self._source
        if field_is_list:
            frame = frame.explode(field)
        if other_is_list:
            frame = frame.explode(other_field)

        frame = frame.group_by(
            field, other_field
        ).agg(
            pl.count().cast(pl.Int64).alias("count"),
        ).sort(
            ["count", field, other_field], descending=True
        ).rename({
            field: "variant",
            other_field: "variant_too",
        })

        self.add_rows(
            field,
            entity=f"with_{other_field}",
            variant=pl.col("variant"),
            variant_too=pl.col("variant_too"),
            count=pl.col("count"),
            frame=frame,
        )

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
            self.add_rows("decision_type", entity=entity, count=expr.sum())

    def collect_body(self) -> None:
        """Collect the standard statistics for the current data frame."""
        for key, value in TRANSFORMS.items():
            match value:
                case TransformType.SKIPPED_DATE:
                    pass
                case TransformType.ROWS:
                    self.add_rows(key, count=pl.len())
                case TransformType.VALUE_COUNTS:
                    self.add_rows(key, value_counts=pl.col(key))
                case TransformType.LIST_VALUE_COUNTS:
                    self.add_rows(
                        key, entity="elements",
                        count=pl.col(key).list.len().cast(pl.Int64).sum()
                    )
                    self.add_rows(
                        key, entity="elements_per_row",
                        max=pl.col(key).list.len().max()
                    )
                    self.add_rows(
                        key, entity="rows_with_elements",
                        count=pl.col(key).list.len().gt(0).sum()
                    )
                    self.add_rows(key, value_counts=pl.col(key).list.explode())
                case TransformType.DECISION_TYPE:
                    self.collect_decision_type()
                case DurationTransform(start, end):
                    # Convert to millseconds, i.e., an integer
                    duration = (pl.col(end) - pl.col(start)).dt.total_milliseconds()
                    assert self._source is not None

                    self.add_rows(
                        key,
                        count=duration.count(),
                        min=duration.min(),
                        mean=duration.mean(),
                        max=duration.max(),
                    )
                case ValueCountsPlusTransform(self_is_list, other_field, other_is_list):
                    self.collect_value_counts_plus(
                        key, self_is_list, other_field, other_is_list
                    )

    def collect_header(self, metadata: pl.DataFrame) -> None:
        """Create a header frame with the given statistics."""
        assert self._release is not None

        # Pola.rs uses different code paths for pl.concat depending on whether
        # the first frame is lazy or not. All but the first three fields are
        # derived from the source frame and hence automatically do the right
        # thing. Let's ensure the first three fields, which are contained in the
        # first frame, also do the right thing.
        column = [
            "batch_count",
            "batch_rows",
            "batch_rows_with_keywords",
            "batch_memory",
            "total_rows",
            "total_rows_with_keywords",
        ]

        count = [
            (metadata.select(pl.col(n).sum()).item() if n in metadata.columns else None)
            for n in (
                "batch_count", "batch_rows", "batch_rows_with_keywords",
                "batch_memory", "total_rows", "total_rows_with_keywords",
            )
        ]

        length = len(column)
        assert length == len(count)

        Frame = pl.LazyFrame if isinstance(self._source, pl.LazyFrame) else pl.DataFrame
        header = Frame({
            "start_date": length * [self._release.start_date],
            "end_date": length * [self._release.end_date],
            "tag": length * [None],
            "column": column,
            "entity": length * [None],
            "variant": length * [None],
            "variant_too": length * [None],
            "count": count,
            "min": length * [None],
            "mean": length * [None],
            "max": length * [None],
        }, schema=STATISTICS_SCHEMA)

        self._frames.append(header)

    def collect(
        self,
        frame: pl.DataFrame | pl.LazyFrame,
        release: Release,
        meta_frame: pl.DataFrame,
    ) -> None:
        """Collect all necessary data in partial data frames."""
        with self.source_data(frame=frame, release=release) as this:
            this.collect_header(meta_frame)
            this.collect_body()

        # FIXME: This is not only specific to the DSA SoR DB but also specific
        # to Protection of Minors. It should be separated out.
        csam = frame.filter(
            pl.col("category_specification").list.contains(
                "KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL"
            )
        )
        with self.source_data(frame=csam, release=release, tag=CSAM_TAG) as this:
            this.collect_body()

    def to_frame(self, validate: bool = False) -> pl.DataFrame:
        """Combine the collected partial frames into one."""
        frame = pl.concat(self._frames, how="vertical")
        if isinstance(frame, pl.LazyFrame):
            frame = frame.collect()
        if validate:
            validate_row_counts(frame)
        return frame


# --------------------------------------------------------------------------------------


def validate_row_counts(frame: pl.DataFrame) -> None:
    frame = frame.filter(pl.col("tag").is_null())
    rows = get_count(frame, "rows")

    for column in (
        "decision_type",
        "decision_monetary",
        "decision_provision",
        "decision_account",
        "account_type",
        "decision_ground",
        "incompatible_content_illegal",
        "category",
        "content_language",
        "moderation_delay",
        "disclosure_delay",
        "source_type",
        "automated_detection",
        "automated_decision",
        "platform_name",
    ):
        if column == "decision_type":
            rows_too = get_count(frame, column)
        else:
            rows_too = get_count(frame, column, entity=None)
        assert rows == rows_too, f"rows={rows:,}, {column}={rows_too:,}"


class _NoArgumentProvided:
    pass

_NO_ARGUMENT_PROVIDED = _NoArgumentProvided()


class NotNull:
    pass

NOT_NULL = NotNull()


def predicate(
    column: str | Sequence[str] | NotNull,
    entity: _NoArgumentProvided | NotNull | None | str = _NO_ARGUMENT_PROVIDED,
    variant: _NoArgumentProvided | NotNull | None | str = _NO_ARGUMENT_PROVIDED,
    variant_too: _NoArgumentProvided | NotNull | None | str = _NO_ARGUMENT_PROVIDED,
    tag: _NoArgumentProvided | NotNull | None | str = _NO_ARGUMENT_PROVIDED,
) -> pl.Expr:
    """
    Create the predicate over the "tag", "column", "entity", and "variant"
    columns. If the argument is a string or list of strings, the predicate tests
    that column for the literal string value(s). If it is None, the predicate
    tests for the column being null. If it is `NOT_NULL`, the predicate tests
    for it being not null.
    """
    # We always query the tag and column
    if tag is None:
        predicate = pl.col("tag").is_null()
    elif isinstance(tag, NotNull):
        predicate = pl.col("tag").is_null().not_()
    elif tag is not _NO_ARGUMENT_PROVIDED:
        predicate = pl.col("tag").eq(tag)
    else:
        predicate = None

    # The column is always required
    if predicate is None:
        if isinstance(column, str):
            predicate = pl.col("column").eq(column)
        elif isinstance(column, NotNull):
            predicate = pl.col("column").is_null().not_()
        else:
            predicate = pl.col("column").is_in(column)
    else:
        if isinstance(column, str):
            predicate = predicate.and_(pl.col("column").eq(column))
        elif isinstance(column, NotNull):
            predicate = predicate.and_(pl.col("column").is_null().not_())
        else:
            predicate = predicate.and_(pl.col("column").is_in(column))

    # However, entity and variant are optional
    if entity is None:
        predicate = predicate.and_(pl.col("entity").is_null())
    elif isinstance(entity, NotNull):
        predicate = predicate.and_(pl.col("entity").is_null().not_())
    elif entity is not _NO_ARGUMENT_PROVIDED:
        predicate = predicate.and_(pl.col("entity").eq(entity))

    if variant is None:
        predicate = predicate.and_(pl.col("variant").is_null())
    elif isinstance(variant, NotNull):
        predicate = predicate.and_(pl.col("variant").is_null().not_())
    elif variant is not _NO_ARGUMENT_PROVIDED:
        predicate = predicate.and_(pl.col("variant").eq(variant))

    if variant_too is None:
        predicate = predicate.and_(pl.col("variant_too").is_null())
    elif isinstance(variant_too, NotNull):
        predicate = predicate.and_(pl.col("variant_too").is_null().not_())
    elif variant_too is not _NO_ARGUMENT_PROVIDED:
        predicate = predicate.and_(pl.col("variant_too").eq(variant_too))

    return predicate


def get_count(
    frame: pl.DataFrame,
    column: str,
    entity: _NoArgumentProvided | None | str = _NO_ARGUMENT_PROVIDED,
    variant: _NoArgumentProvided | None | str = _NO_ARGUMENT_PROVIDED,
) -> int:
    return frame.filter(
        predicate(column, entity=entity, variant=variant)
    ).select(
        pl.col("count").sum()
    ).item()


def aggregates() -> list[pl.Expr]:
    return [
        pl.col("count").sum(),
        pl.col("min").min(),
        (pl.col("mean") * pl.col("count")).sum() // pl.col("count").sum(),
        pl.col("max").max(),
    ]


def is_categorical(column: str) -> bool:
    """Determine whether the named column is categorical."""
    field = TRANSFORMS[column]
    return (
        field in (TransformType.VALUE_COUNTS, TransformType.LIST_VALUE_COUNTS)
        or isinstance(field, ValueCountsPlusTransform)
    )


def is_duration(column: str) -> bool:
    """Determine whether the named column is a duration."""
    return isinstance(TRANSFORMS[column], DurationTransform)


# --------------------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Tag:
    """A tag."""

    tag: None | str

    def __format__(self, spec) -> str:
        return str.__format__(self.tag or "no tag", spec)

    def __len__(self) -> int:
        return len(self.tag or "no tag") + 2

    def __str__(self) -> str:
        return self.tag or "no tag"


class Spacer:
    """A marker object for empty cells."""
    def __str__(self) -> str:
        return ""

"""The canonical spacer object."""
SPACER = Spacer()


type Statistic = Literal["count", "min", "mean", "max"]


"""
The type of summary statistics, which is a list of key, value pairs. To aid with
presentation, some of the pairs may be empty, containing `SPACER` instances (see
below).
"""
type Summary = list[tuple[str | Tag | Spacer, Any]]


class Summarizer:
    """Summarize analysis results."""

    def __init__(self) -> None:
        self._source = None
        self._tag = None
        self._summary = []

    @contextmanager
    def tagged_frame(
        self,
        tag: None | str,
        frame: pl.DataFrame,
    ) -> Iterator[Self]:
        """Create a tagged context."""
        old_tag, self._tag = self._tag, (tag if tag != "" else None)
        if tag is None or tag == "":
            frame = frame.filter(pl.col("tag").is_null())
        else:
            frame = frame.filter(pl.col("tag").eq(tag))

        old_source = self._source
        self._source = frame.group_by(
            pl.col("column", "entity", "variant")
        ).agg(
            *aggregates()
        )

        try:
            yield self
        finally:
            self._source = old_source
            self._tag = old_tag

    def get_value(
        self,
        column: str,
        entity: None | str,
        statistic: Statistic = "count",
    ) -> None | int:
        """Extract a single count for the given column and entity."""
        assert self._source is not None
        frame = self._source.filter(
            predicate(column, entity=entity)
        ).select(
            pl.col(statistic)
        )
        return frame.item() if frame.height == 1 else None

    def get_value_counts(self, column: str, entity: None | str = None) -> pl.DataFrame:
        """Extract the value counts for the given column and entity."""
        assert self._source is not None
        return self._source.filter(
            predicate(column, entity=entity)
        ).select(
            pl.col("column", "entity", "variant", "count")
        ).sort(
            "count", descending=True
        )

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
        self,
        column: str,
        entity: None | str = None,
        statistic: Statistic = "count",
    ) -> None:
        duration = is_duration(column)
        variable = column if entity is None or entity == "" else f"{column}.{entity}"
        if duration or statistic != "count":
            variable = f"{variable}.{statistic}"

        value = self.get_value(column, entity, statistic)
        if duration and statistic != "count" and value is not None:
            value = dt.timedelta(seconds=value // 1_000, milliseconds=value % 1_000)

        self._summary.append((variable, value))

    def collect_value_counts(self, column: str, entity: None | str = None) -> None:
        for row in self.get_value_counts(column, entity).rows():
            column, entity, variant, count = row
            var = column
            if entity:
                var = f"{var}.{entity}"
            if variant is None:
                var = f"{var}.is_null"
            else:
                var = f"{var}.{variant}"

            self._summary.append((var, count))

    def summarize_fields(self) -> None:
        for field_name, field_type in TRANSFORMS.items():
            match field_type:
                case TransformType.SKIPPED_DATE:
                    pass
                case TransformType.ROWS:
                    self.collect1("rows")
                    self.spacer()
                case TransformType.VALUE_COUNTS:
                    self.spacer()
                    self.collect_value_counts(field_name)
                case TransformType.LIST_VALUE_COUNTS:
                    self.spacer()
                    self.collect1(field_name, "elements")
                    self.collect1(field_name, "elements_per_row", "max")
                    self.collect1(field_name, "rows_with_elements")
                    self.collect_value_counts(field_name)
                case DurationTransform(start, end):
                    self.spacer()
                    self.collect1(field_name, statistic="count")
                    self.collect1(field_name, statistic="min")
                    self.collect1(field_name, statistic="mean")
                    self.collect1(field_name, statistic="max")
                case ValueCountsPlusTransform(_, other_field, _):
                    self.spacer()
                    self.collect_value_counts(field_name)

                    with self.spacer_on_demand():
                        entity = (
                            "with_end_date" if other_field.startswith("end_date")
                            else f"with_{other_field}"
                        )
                        self.collect_value_counts(field_name, entity=entity)
                case TransformType.DECISION_TYPE:
                    for count in range(16):
                        suffix = []

                        for shift, column in enumerate(_DECISION_TYPES):
                            if count & (1 << shift) != 0:
                                suffix.append(column[_DECISION_OFFSET:_DECISION_OFFSET+3])

                        self.collect1(
                            field_name,
                            entity="_".join(suffix) if count != 0  else "is_null",
                        )

    def summarize(self, frame: pl.DataFrame) -> Summary:
        with self.tagged_frame(tag=None, frame=frame) as this:
            this._summary = [
                ("start_date", frame.select(pl.col("start_date").min()).item()),
                ("end_date", frame.select(pl.col("end_date").max()).item()),
                ("batch_count", this.get_value("batch_count", entity=None)),
                ("batch_rows", this.get_value("batch_rows", entity=None)),
                ("batch_rows_with_keywords", this.get_value(
                    "batch_rows_with_keywords", entity=None
                )),
                ("batch_memory", this.get_value("batch_memory", entity=None)),
                ("total_rows", this.get_value("total_rows", entity=None)),
                ("total_rows_with_keywords", this.get_value(
                    "total_rows_with_keywords", entity=None
                )),
            ]

        # Make sure that baseline comes first
        tags = [
            None,
            *(
                t
                for t in frame.select(pl.col("tag").unique()).get_column("tag").to_list()
                if t is not None
            )
        ]

        for tag in tags:
            with self.tagged_frame(tag, frame) as this:
                self.spacer()
                self.spacer()
                this._summary.append((Tag(this._tag), Tag(this._tag)))
                self.spacer()
                this.summarize_fields()

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
                    svar = f"***——— {val} ———***" if markdown else var
                elif var is SPACER:
                    svar = "\u2800" if markdown else " "
                else:
                    svar = var

                if isinstance(val, Tag):
                    assert isinstance(var, Tag)
                    sval = f"***— {val} —***" if markdown else val
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
