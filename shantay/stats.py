"""
Support for summary statistics.

This module exports two abstractions for incrementally collecting summary
statistics: `Collector` is the lower-level class for incrementally building a
data frame with statistical data, whereas `Statistics` exposes a more
comprehensive interface that supports computing, combining, and storing
statistics. Both classes implement the `shantay.model` module's
`CollectorProtocol`.
"""
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
import datetime as dt
from importlib.resources import files, as_file
import math
from pathlib import Path
import re
import shutil
from typing import Any, cast, ClassVar, Self

import polars as pl

from .framing import (
    aggregates, finalize, get_quantity, NOT_NULL, predicate, Quantity
)
from .model import Daily, DateRange, MetadataEntry, Release
from .schema import (
    CanonicalPlatformNames, check_stats_platforms, DurationTransform, humanize,
    KeywordChildSexualAbuseMaterial, StatisticsSchema, STRATIFY_BY_CATEGORY,
    TRANSFORM_COUNT, TRANSFORMS, TransformType, ValueCountsPlusTransform,
    VariantValueType,
)
from .util import scale_time


_DECISION_OFFSET = len("decision_")

_DECISION_TYPES = (
    "decision_visibility",
    "decision_monetary",
    "decision_provision",
    "decision_account",
)


def date_range_of(frame: pl.DataFrame) -> DateRange:
    return DateRange(*frame.select(
        pl.col("start_date").min(),
        pl.col("end_date").max(),
    ).row(0))


def _is_categorical(column: str) -> bool:
    field = TRANSFORMS[column]
    return (
        field in (TransformType.VALUE_COUNTS, TransformType.LIST_VALUE_COUNTS)
        or isinstance(field, ValueCountsPlusTransform)
    )


def _is_duration(column: str) -> bool:
    """Determine whether the named column is a duration."""
    return isinstance(TRANSFORMS[column], DurationTransform)


def _validate_row_counts(frame: pl.DataFrame) -> None:
    """Perform consistency checks on statistics data frame."""
    frame = frame.filter(pl.col("tag").is_null())
    rows = get_quantity(frame, "rows")

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
        "source_type",
        "automated_detection",
        "automated_decision",
        "platform_name",
    ):
        if column == "decision_type":
            rows_too = get_quantity(frame, column)
        else:
            rows_too = get_quantity(frame, column, entity=None)
        assert rows == rows_too, f"rows={rows:,}, {column}={rows_too:,}"


def get_tags(frame: pl.DataFrame) -> list[None | str]:
    """
    Get all tags used in the statistics frame. This function returns the tags in
    their canonical order, from least to most specific, i.e., `None`, then
    statement categories, and finally keywords.
    """
    # Filter out total_rows(_with_keywords), since both appear without a tag
    # also in otherwise tagged statistics
    raw_tags = frame.filter(
        pl.col("column").is_in(["total_rows", "total_rows_with_keywords"]).not_()
    ).select(
        pl.col("tag").unique()
    ).get_column("tag").to_list()

    tags = []
    if None in raw_tags:
        tags.append(None)
    for tag in raw_tags:
        if tag is not None and tag.startswith("STATEMENT_CATEGORY_"):
            tags.append(tag)
    for tag in raw_tags:
        if tag is not None and tag.startswith("KEYWORD_"):
            tags.append(tag)

    return tags


# =================================================================================================


class Collector:
    """Analyze the data while also collecting the results."""

    def __init__(self) -> None:
        self._source = pl.DataFrame()
        self._tag = None
        self._release = None
        self._platform = None
        self._category = None
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

    @contextmanager
    def platform_data(self, platform: None | str) -> Iterator[Self]:
        """Create a context for the platform."""
        new_source = self._source.filter(
            pl.col("platform_name").is_null() if platform is None
            else pl.col("platform_name").eq(platform)
        )

        old_source, self._source = self._source, new_source
        old_platform, self._platform = self._platform, platform
        try:
            yield self
        finally:
            self._source = old_source
            self._platform = old_platform

    @contextmanager
    def category_data(self, category: str) -> Iterator[Self]:
        """Create a context for the category."""
        new_source = self._source.filter(pl.col("category").eq(category))

        old_source, self._source = self._source, new_source
        old_category, self._category = self._category, category
        try:
            yield self
        finally:
            self._source = old_source
            self._category = old_category

    def add_rows(
        self,
        column: str,
        entity: None | str = None,
        variant: None | pl.Expr = None,
        value_counts: None | pl.Expr = None,
        text_value_counts: None | pl.Expr = None,
        frame: None | pl.DataFrame | pl.LazyFrame = None,
        **kwargs: None | int | pl.Expr,
    ) -> None:
        """Add new rows."""
        if frame is None:
            frame = self._source

        tag = None if self._tag == "" else self._tag
        entity = None if entity == "" else entity

        effective_values = []
        if value_counts is None:
            if variant is None:
                effective_values.append(
                    pl.lit(None, dtype=VariantValueType).alias("variant")
                )
            else:
                effective_values.append(
                    variant
                        .cast(pl.String)
                        .cast(VariantValueType)
                        .alias("variant")
                )
        else:
            # Without the cast before value_counts(), Pola.rs fails analyze
            # archive with a "can not cast to enum with global mapping" error.
            effective_values.append(
                value_counts
                    .cast(pl.String)
                    .value_counts(sort=True)
                    .list.explode()
                    .struct.unnest()
            )

        if text_value_counts is None:
            effective_values.append(
                pl.lit(None, dtype=pl.String).alias("text")
            )
        else:
            effective_values.append(
                text_value_counts
                    .value_counts(sort=True)
                    .list.explode()
                    .struct.unnest()
            )

        for key in ("count", "min", "mean", "max"):
            if (
                (value_counts is not None or text_value_counts is not None)
                and key == "count"
            ):
                continue

            value = kwargs.get(key, None)
            if value is None or isinstance(value, int):
                effective_values.append(pl.lit(value, dtype=pl.Int64).alias(key))
            else:
                effective_values.append(value.cast(pl.Int64).alias(key))

        assert self._release is not None
        frame = frame.select(
            pl.lit(self._release.start_date).alias("start_date"),
            pl.lit(self._release.end_date).alias("end_date"),
            pl.lit(tag).alias("tag"),
            pl.lit(self._platform).alias("platform"),
            *(
                [pl.lit(self._category).alias("category")] if STRATIFY_BY_CATEGORY
                else []
            ),
            pl.lit(column).alias("column"),
            pl.lit(entity).alias("entity"),
            *effective_values,
        )

        if value_counts is not None:
            frame = frame.rename({
                column: "variant",
            })
        elif text_value_counts is not None:
            frame = frame.rename({
                column: "text",
            })

        frame = frame.cast(StatisticsSchema) # pyright: ignore[reportArgumentType]

        # Enforce canonical column order, so that frames can be concatenated!
        self._frames.append(frame.select(
            pl.col(
                "start_date", "end_date", "tag", "platform",
                *(["category"] if STRATIFY_BY_CATEGORY else []),
                "column", "entity",
                "variant", "text", "count", "min", "mean", "max"
            )
        ))

    def collect_value_counts_plus(
        self,
        field: str,
        field_is_list: bool,
        other_field: str,
    ) -> None:
        """
        Collect value counts for a field in isolation and then for the field in
        combination with another field.
        """
        # Value counts for field
        values = pl.col(field).list.explode() if field_is_list else pl.col(field)
        self.add_rows(field, value_counts=values)

        assert not _is_categorical(other_field)
        self.add_rows(
            field,
            entity=(
                "with_end_date" if other_field.startswith("end_date")
                else f"with_{other_field}"
            ),
            value_counts=values.cast(pl.String),
            frame=self._source.filter(
                pl.col(other_field).is_null().not_()
            ),
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

    def collect_body_data(self) -> None:
        """Collect the standard statistics for the current data frame."""
        for key, value in TRANSFORMS.items():
            match value:
                # Platform name and category name are distinct columns that are
                # filled in while handling other fields.
                case TransformType.PLATFORM_NAME:
                    assert key == "platform_name"
                case TransformType.CATEGORY_NAME:
                    assert key == "category"
                case TransformType.SKIPPED_DATE:
                    pass
                case TransformType.ROWS:
                    self.add_rows(key, count=pl.len())
                case TransformType.VALUE_COUNTS:
                    self.add_rows(key, value_counts=pl.col(key))
                case TransformType.TEXT_VALUE_COUNTS:
                    self.add_rows(key, text_value_counts=pl.col(key))
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
                    # Convert positive durations to seconds, i.e., an integer count
                    duration = pl.when(
                        pl.col(start) <= pl.col(end)
                    ).then(
                        (pl.col(end) - pl.col(start)).dt.total_seconds()
                    ).otherwise(
                        pl.lit(None)
                    )

                    self.add_rows(
                        key,
                        count=duration.count(),
                        min=duration.min(),
                        mean=duration.mean(),
                        max=duration.max(),
                    )

                    self.add_rows(
                        key,
                        entity="null_bc_negative",
                        count=(pl.col(start) > pl.col(end)).sum()
                    )
                case ValueCountsPlusTransform(self_is_list, other_field):
                    self.collect_value_counts_plus(
                        key, self_is_list, other_field
                    )

    def collect_categories(self) -> None:
        categories = self._source.select(
            pl.col("category").unique()
        )
        if isinstance(categories, pl.LazyFrame):
            categories = categories.collect()

        for category in categories.get_column("category"):
            with self.category_data(category) as this:
                this.collect_body_data()

    def collect_platforms(self) -> None:
        platform_names = self._source.select(
            pl.col("platform_name").unique()
        )
        if isinstance(platform_names, pl.LazyFrame):
            platform_names = platform_names.collect()

        for name in platform_names.get_column("platform_name"):
            with self.platform_data(name) as this:
                if STRATIFY_BY_CATEGORY:
                    this.collect_categories()
                else:
                    this.collect_body_data()

    def collect_header(
        self, metadata_entry: None | MetadataEntry = None, tag: None | str = None
    ) -> None:
        """Create a header frame with the given statistics."""
        pairs = {}
        md = cast(dict, metadata_entry or {})

        if isinstance(self._source, pl.LazyFrame):
            self._source = self._source.collect()

        batch_rows_with_keywords = (
            self._source.select(
                pl.col("category_specification").is_null().not_().sum()
            ).item()
        )

        pairs["batch_count"] = md.get("batch_count")
        pairs["batch_rows"] = self._source.height
        pairs["batch_rows_with_keywords"] = batch_rows_with_keywords
        pairs["total_rows"] = self._source.height if tag is None else md.get("total_rows")
        pairs["total_rows_with_keywords"] = (
            batch_rows_with_keywords if tag is None
            else md.get("total_rows_with_keywords")
        )
        height = len(pairs)

        assert self._release is not None
        header = pl.DataFrame({
            "start_date": height * [self._release.start_date],
            "end_date": height * [self._release.end_date],
            "tag": [
                (None if k in ("total_rows", "total_rows_with_keywords") else tag)
                for k in pairs.keys()
            ],
            "platform": height * [None],
        } | (
            {"category": height * [None]} if STRATIFY_BY_CATEGORY else {}
        ) | {
            "column": [k for k in pairs.keys()],
            "entity": height * [None],
            "variant": height * [None],
            "text": height * [None],
            "count": [v for v in pairs.values()],
            "min": height * [None],
            "mean": height * [None],
            "max": height * [None],
        }, schema=StatisticsSchema)

        self._frames.append(header)

    def collect(
        self,
        release: Release,
        frame: pl.DataFrame,
        tag: None | str = None,
        metadata_entry: None | MetadataEntry = None,
    ) -> None:
        """Collect all necessary data in partial data frames."""
        if tag is None or tag.startswith("STATEMENT_CATEGORY_"):
            with self.source_data(frame=frame, release=release, tag=tag) as this:
                this.collect_header(metadata_entry, tag)
                this.collect_platforms()
        else:
            with self.source_data(frame=frame, release=release, tag=tag) as this:
                this.collect_platforms()

    def frame(self, validate: bool = False) -> pl.DataFrame:
        """Combine the collected partial frames into one."""
        frame = pl.concat(self._frames, how="vertical")
        if isinstance(frame, pl.LazyFrame):
            frame = frame.collect()
        frame = frame.cast(StatisticsSchema) # pyright: ignore[reportArgumentType]
        if validate:
            _validate_row_counts(frame)
        return frame


# =================================================================================================


@dataclass(frozen=True, slots=True)
class _Tag:
    """A tag."""

    tag: None | str

    def __format__(self, spec) -> str:
        return str.__format__(str(self), spec)

    def __len__(self) -> int:
        return len(str(self)) + 2

    def __str__(self) -> str:
        return self.tag or "no tag"


class _Spacer:
    """A marker object for empty cells."""
    def __str__(self) -> str:
        return ""

_SPACER = _Spacer()


_WHITESPACE = re.compile(r"\s+", re.UNICODE)


type _Summary = list[tuple[str | _Tag | _Spacer, Any]]


class _Summarizer:
    """Summarize analysis results."""

    def __init__(self, platform: None | str = None) -> None:
        self._source = pl.DataFrame()
        self._source_by_platform = pl.DataFrame()
        self._tag = None
        self._platform = platform
        self._summary = []

    @contextmanager
    def _tagged_frame(
        self,
        tag: None | str,
        frame: pl.DataFrame,
        platform: None | str = None,
    ) -> Iterator[Self]:
        """Create a tagged context."""
        old_tag, self._tag = self._tag, (tag if tag != "" else None)
        if tag is None or tag == "":
            frame = frame.filter(pl.col("tag").is_null())
        else:
            frame = frame.filter(pl.col("tag").eq(tag))

        if self._platform is not None:
            if platform is not None and platform != self._platform:
                raise ValueError(f'platforms "{platform}" and "{self._platform}" differ')
            platform = self._platform

        if platform is not None:
            frame = frame.filter(pl.col("platform").eq(platform))

        old_source = self._source
        old_source_by_platform = self._source_by_platform
        self._source_by_platform = frame.group_by(
            pl.col("platform", "column", "entity", "variant", "text")
        ).agg(
            pl.col("start_date").min(),
            *aggregates()
        )
        self._source = self._source_by_platform.group_by(
            pl.col("column", "entity", "variant", "text")
        ).agg(
            *aggregates()
        )

        try:
            yield self
        finally:
            self._source = old_source
            self._source_by_platform = old_source_by_platform
            self._tag = old_tag

    @contextmanager
    def _spacer_on_demand(self) -> Iterator[None]:
        """
        If the scope adds new summary entries, preface those entries with an
        empty row.
        """
        actual_summary = self._summary
        self._summary = []
        try:
            yield None
        finally:
            if 0 < len(self._summary):
                self._spacer(actual_summary)
                actual_summary.extend(self._summary)
            self._summary = actual_summary

    def _spacer(self, summary: None | _Summary = None) -> None:
        """Add an empty row to the summary of summary statistics."""
        if summary is None:
            summary = self._summary
        summary.append((_SPACER, _SPACER))

    def _collect1(
        self,
        column: str,
        entity: None | str = None,
        quantity: Quantity = "count",
    ) -> None:
        """Collect the given column's statistic value."""
        duration = _is_duration(column)
        variable = column if entity is None or entity == "" else f"{column}.{entity}"
        if duration or quantity != "count":
            variable = f"{variable}.{quantity}"

        value = get_quantity(self._source, column, entity=entity, statistic=quantity)
        if (
            duration
            and quantity != "count"
            and value is not None
            and not math.isnan(value)
        ):
            value = dt.timedelta(seconds=value)

        self._summary.append((variable, value))

    def _collect_value_counts(
        self, column: str, entity: None | str = None, is_text: bool = False
    ) -> None:
        """Collect the given column's value counts."""
        for row in self._source.filter(
            predicate(column, entity=entity)
        ).select(
            pl.col("column", "entity", "variant", "text", "count")
        ).sort(
            ["count", "variant", "text"], descending=True
        ).rows():
            column, entity, variant, text, count = row
            var = column

            if entity == "with_end_date":
                var = f"{var}.{entity}"

            if is_text:
                if text is None:
                    var = f"{var}.is_null"
                else:
                    text = _WHITESPACE.sub(" ", text).replace("|", "")
                    var = f"{var}.{text[:70]}"
                    if 70 < len(text):
                        var += "…"
            else:
                if variant is None:
                    var = f"{var}.is_null"
                else:
                    var = f"{var}.{variant}"

            self._summary.append((var, count))

    def _collect_platform_names(self) -> None:
        base = self._source_by_platform.filter(
            predicate("rows", entity=None)
        ).group_by(
            "platform"
        )

        self._spacer()
        for platform, count in base.agg(
            pl.col("count").sum()
        ).sort(
            "count",
            descending=True
        ).rows():
            self._summary.append((f"platform.{platform}.rows", count))

        self._spacer()
        for platform, start_date in base.agg(
            pl.col("start_date").min()
        ).sort(
            "start_date",
            descending=False
        ).rows():
            self._summary.append((f"platform.{platform}.start_date", start_date))

    def _summarize_fields(self) -> None:
        """Summarize all fields of summary statistics."""
        for field_name, field_type in TRANSFORMS.items():
            match field_type:
                case TransformType.PLATFORM_NAME:
                    assert field_name == "platform_name"
                    self._collect_platform_names()
                case TransformType.SKIPPED_DATE:
                    pass
                case TransformType.ROWS:
                    self._collect1("rows")
                    self._spacer()
                case TransformType.VALUE_COUNTS:
                    self._spacer()
                    self._collect_value_counts(field_name)
                case TransformType.TEXT_VALUE_COUNTS:
                    self._spacer()
                    self._collect_value_counts(field_name, is_text=True)
                case TransformType.LIST_VALUE_COUNTS:
                    self._spacer()
                    self._collect1(field_name, "elements")
                    self._collect1(field_name, "elements_per_row", "max")
                    self._collect1(field_name, "rows_with_elements")
                    self._collect_value_counts(field_name)
                case DurationTransform(_, _):
                    self._spacer()
                    self._collect1(field_name, quantity="count")
                    self._collect1(field_name, quantity="min")
                    self._collect1(field_name, quantity="mean")
                    self._collect1(field_name, quantity="max")
                    self._collect1(
                        field_name, entity="null_bc_negative", quantity="count"
                    )
                case ValueCountsPlusTransform(_, other_field):
                    self._spacer()
                    self._collect_value_counts(field_name)

                    with self._spacer_on_demand():
                        entity = (
                            "with_end_date" if other_field.startswith("end_date")
                            else f"with_{other_field}"
                        )
                        self._collect_value_counts(field_name, entity=entity)
                case TransformType.DECISION_TYPE:
                    for count in range(16):
                        suffix = []

                        for shift, column in enumerate(_DECISION_TYPES):
                            if count & (1 << shift) != 0:
                                suffix.append(column[_DECISION_OFFSET:_DECISION_OFFSET+3])

                        self._collect1(
                            field_name,
                            entity="_".join(suffix) if count != 0  else "is_null",
                        )

    def _summary_intro(self, frame: pl.DataFrame, tag: None | str) -> None:
        platforms = frame.select(
            pl.col("platform").filter(pl.col("platform").is_not_null()).n_unique()
        ).item()

        platforms_with_keywords = frame.filter(
            predicate("category_specification", variant=NOT_NULL, tag=tag)
        ).select(
            pl.col("platform").n_unique()
        ).item()

        platforms_with_csam = frame.filter(
            predicate(
                "category_specification",
                variant=KeywordChildSexualAbuseMaterial,
                tag=tag
            )
        ).select(
            pl.col("platform").n_unique()
        ).item()

        batch_rows = get_quantity(frame, "batch_rows", entity=None, tag=tag)
        total_rows = get_quantity(frame, "total_rows", entity=None, tag=None)
        batch_kw_rows = get_quantity(frame, "batch_rows_with_keywords", entity=None, tag=tag)
        total_kw_rows = get_quantity(frame, "total_rows_with_keywords", entity=None, tag=None)
        assert tag is None or batch_rows is not None
        assert tag is None or batch_kw_rows is not None
        assert total_rows is not None
        assert total_kw_rows is not None

        batch_rows_pct = (
            (batch_rows or 0) / total_rows * 100 if total_rows != 0 else None
        )
        batch_rows_with_keywords_pct = (
            (batch_kw_rows or 0) / batch_rows * 100
            if batch_rows is not None and batch_rows != 0
            else None
        )
        total_rows_with_keywords_pct = (
            total_kw_rows / total_rows * 100 if total_rows != 0 else None
        )

        self._summary = [
            ("start_date", frame.select(pl.col("start_date").min()).item()),
            ("end_date", frame.select(pl.col("end_date").max()).item()),
            ("batch_count", get_quantity(frame, "batch_count", entity=None)),
            ("batch_rows", batch_rows),
            ("batch_rows_pct", batch_rows_pct),
            ("batch_rows_with_keywords", batch_kw_rows),
            ("batch_rows_with_keywords_pct", batch_rows_with_keywords_pct),
            ("total_rows", total_rows),
            ("total_rows_with_keywords", total_kw_rows),
            ("total_rows_with_keywords_pct", total_rows_with_keywords_pct),
            ("platforms", platforms),
            ("platforms_with_keywords", platforms_with_keywords),
            ("platforms_with_csam", platforms_with_csam)
        ]

    def summarize(self, frame: pl.DataFrame) -> _Summary:
        """Summarize the data frame."""
        for index, tag in enumerate(get_tags(frame)):
            with self._tagged_frame(tag, frame) as this:
                if index == 0:
                    this._summary_intro(frame, tag)

                this._spacer()
                this._spacer()
                t = humanize(tag)
                this._summary.append((_Tag(t), _Tag(t)))
                this._spacer()
                this._summarize_fields()

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
                if isinstance(var, _Tag):
                    # Delay formatting of tag for non-markdown output
                    # so that we can center it
                    assert isinstance(val, _Tag)
                    svar = (
                        f"***—————————— {humanize(str(var))} ——————————***"
                        if markdown else var
                    )
                elif var is _SPACER:
                    svar = "\u2800" if markdown else " "
                else:
                    svar = var

                if isinstance(val, _Tag):
                    assert isinstance(var, _Tag)
                    sval = "\u2800" if markdown else " "
                elif val is _SPACER:
                    sval = "\u2800" if markdown else " "
                elif val is None:
                    sval = "␀"
                elif (
                    var is not _SPACER
                    and not isinstance(var, _Tag)
                    and var.endswith("_pct")
                ):
                    sval = f"{val:.3f} %"
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
            if isinstance(var, _Tag) and not markdown:
                var = f" {humanize(str(var))} ".center(var_width + 2, "═")
                val = "═" * (val_width + 2)
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


# =================================================================================================


class Statistics:
    """
    Wrapper around statistics describing the DSA transparency database.
    Conceptually, the descriptive statistics form a single data frame. However,
    to support incremental collection of those statistics, this class may
    temporarily wrap more than one data frame, lazily materializing a single
    frame only on demand.
    """

    DEFAULT_RANGE: ClassVar[DateRange] = DateRange(
        dt.date(2023, 9, 25), dt.date.today() - dt.timedelta(days=3)
    )

    def __init__(self, file: str, *frames: pl.DataFrame) -> None:
        self._file = file
        self._frames = list(frames)
        self._collector = None
        # Cache monthly and total aggregations
        self._monthly = None
        self._total = None

    @classmethod
    def builtin(cls) -> Self:
        """Get the pre-computed statistics for the entire DSA database."""
        # Per spec, __package__ is the same as __spec__.parent, which
        source = files(__spec__.parent).joinpath("db.parquet")
        with as_file(source) as path:
            return cls.read(path)

    @classmethod
    def from_storage(cls, file: str, staging: Path, persistent: Path) -> Self:
        """
        Pick the more complete statistics from staging and the persistent root
        directory, i.e., archive or extract. This method assumes that if both
        files exist, they also start on the same date.
        """
        s1 = cls.read(staging / file) if (staging / file).exists() else None
        s2 = cls.read(persistent / file) if (persistent / file).exists() else None
        if s1 is None:
            return cls(file) if s2 is None else s2
        elif s2 is None:
            return s1

        r1 = s1.date_range()
        r2 = s2.date_range()
        if r1.first != r2.first:
            raise ValueError(
                f"inconsistent start dates {r1.first.isoformat()} "
                f"and {r2.first.isoformat()} for statistics coverage"
            )

        return s1 if r2.last < r1.last else s2

    @classmethod
    def read(cls, path: Path) -> Self:
        """
        Instantiate a new statistics frame from the given file path. This method
        assumes that the file exists and throws an exception otherwise.
        """
        frame = pl.read_parquet(
            path
        ).with_columns(
            # Cast to string so that replace matches platform names
            pl.col("platform").cast(str).replace(CanonicalPlatformNames)
        )

        # If the platform names are out-of-whack, the cast below will fail. But
        # Pola.rs error messages tend to be less than helpful. So instead we
        # check for unknown platform names ourselves. That way, we can initiate
        # largely automatic recovery as well.
        check_stats_platforms(path, frame)

        return cls(
            path.name,
            frame.cast(StatisticsSchema) # pyright: ignore[reportArgumentType]
        )

    def file(self) -> str:
        return self._file

    def __dataframe__(self) -> Any:
        return self.frame().__dataframe__()

    def frame(self, validate: bool = False) -> pl.DataFrame:
        """
        Materialize a single data frame with the summary statistics. If this
        method computes a new single data frame, it also updates the internal
        state with that frame, discarding the subsets used for creating that
        single frame in the first place. That implies that a subsequent call to
        this method, with no intervening calls to `collect` or `append` return
        the exact same data frame.
        """
        # Fast path: Data has already been reduced to a single frame
        if (
            len(self._frames) == 1
            and self._collector is None
            and not validate
        ):
            return self._frames[0]

        # Somewhat slower path: No data or data in collector only
        if len(self._frames) == 0:
            if self._collector is None:
                self._frames.append(pl.DataFrame([], schema=StatisticsSchema))
            else:
                self._frames.append(self._collector.frame())
                self._collector = None
            return self._frames[0]

        # Slow path: Concatenate 2+ frames
        if self._collector is not None:
            self._frames.append(self._collector.frame())
        frame = pl.concat(self._frames, how="vertical")

        # Take care of validation and grouping
        if validate:
            _validate_row_counts(frame)

        # Update internal state
        self._frames = [frame]
        self._collector = None
        return frame

    def monthly(self) -> pl.DataFrame:
        if self._monthly is None:
            self._monthly = self.frame().group_by(
                pl.col("start_date").dt.year().alias("year"),
                pl.col("start_date").dt.month().alias("month"),
                pl.col("tag", "platform", "column", "entity", "variant", "text"),
            ).agg(*aggregates())
        return self._monthly

    def total(self) -> pl.DataFrame:
        if self._total is None:
            self._total = self.frame().group_by(
                pl.col("tag", "platform", "column", "entity", "variant", "text"),
            ).agg(*aggregates())
        return self._total

    def is_empty(self) -> bool:
        """Determine whether this instance has no data."""
        return self.frame().height == 0

    def __contains__(self, date: None | dt.date | Daily) -> bool:
        """
        Determine whether the summary statistics contain data for the given
        date. This method recognizes summary statistics with either daily or
        monthly granularity.
        """
        if date is None:
            return False
        if isinstance(date, Daily):
            date = date.start_date

        # The threshold TRANSFORM_COUNT is the number of transforms that aren't
        # skipped. Since each such transform results in at least a row,
        # typically many more, that count also is a loose lower bound on the
        # number of rows added per time period.
        return TRANSFORM_COUNT < self.frame().filter(
            pl.col("start_date").le(date).and_(pl.col("end_date").ge(date))
        ).height

    def date_range(self) -> DateRange:
        """
        Determine the range from minimum start date to maximum end date
        covered by the summary statistics.
        """
        frame = self.frame()
        if frame.height == 0:
            raise ValueError("no statistics available")
        return date_range_of(frame)

    def collect(
        self,
        release: Release,
        frame: pl.DataFrame,
        tag: None | str = None,
        metadata_entry: None | MetadataEntry = None,
    ) -> None:
        """
        Add summary statistics for the frame with transparency database data.
        This method adds the given `tag` to collected summary statistics. Use
        `append()` for frames with already computed statistics.
        """
        if self._collector is None:
            self._collector = Collector()
            # Null out cached monthly and total aggregations
            self._monthly = None
            self._total = None
        self._collector.collect(release, frame, tag=tag, metadata_entry=metadata_entry)

    def append(self, frame: pl.DataFrame) -> None:
        """
        Append the data frame with summary statistics. Use `collect()` for
        frames with transparency database data.
        """
        # Null out cached monthly and total aggregations
        self._monthly = None
        self._total = None
        self._frames.append(
            frame.cast(StatisticsSchema) # pyright: ignore[reportArgumentType]
        )

    def summary(self, platform: None | str = None, markdown: bool = False) -> str:
        """Create a summary table formatted as Unicode text or Markdown."""
        summarizer = _Summarizer(platform=platform)
        summarizer.summarize(self.frame())
        return summarizer.formatted_summary(markdown)

    def write(self, directory: Path, should_finalize: bool = False) -> Self:
        """
        Write this statistics frame to the given directory. If `finalize` is
        `True`, this method groups and aggregates the frame at daily
        granularity, sorts the entries by date, and rechunks the memory consumed
        by the data frame before writing it out. The updated version also
        replaces the original version.
        """
        if should_finalize:
            self._frames = [finalize(self.frame())]

        path = directory / self.file()
        tmp = path.with_suffix(".tmp.parquet")
        self.frame().write_parquet(tmp)
        tmp.replace(path)

        return self

    @classmethod
    def copy(cls, file: str, source: Path, target: Path) -> None:
        """
        Copy the statistics file in the source directory to the target directory
        via an intermediate temporary file on the same file system as the target
        directory.
        """
        tmp = (target / file).with_suffix(".tmp.parquet")
        shutil.copy(source / file, tmp)
        tmp.replace(target / file)
