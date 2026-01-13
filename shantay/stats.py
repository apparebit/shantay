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
import filecmp
from importlib.resources import files, as_file
import math
from pathlib import Path
import re
import shutil
from typing import Any, cast, ClassVar, Self

import polars as pl

from .framing import (
    aggregates, finalize, get_quantity, NoArgumentProvided, NO_ARGUMENT_PROVIDED,
    NOT_NULL, predicate, Quantity
)
from .model import (
    Daily, DateRange, Filter, FilterKind, FullMetadataEntry, MetadataEntry, Release,
    ReleaseRange
)
from .schema import (
    CanonicalPlatformNames, CategoryValueType, check_stats_platforms, ColumnValueType,
    DurationTransform, EntityValueType, humanize, KeywordChildSexualAbuseMaterial,
    NULL, PlatformValueType, StatementCategoryProtectionOfMinors, StatisticsSchema,
    TagValueType, TRANSFORM_COUNT, TRANSFORMS, TransformType, ValueCountsPlusTransform,
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
        if (
            tag is not None
            and not tag.startswith("STATEMENT_CATEGORY_")
            and not tag.startswith("KEYWORD_")
        ):
            tags.append(tag)
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

    # Use a materialized or eager source frame, which ensures that data is
    # available and well-formed. Use lazy partial frames and only materialize
    # when needed to maximize opportunities for Pola.rs' optimizations.

    def __init__(
        self, stratify_by_category: bool = False, stratify_all_text: bool = False
    ) -> None:
        self._stratify_by_category = stratify_by_category
        self._stratify_all_text = stratify_all_text
        self._source: pl.DataFrame = pl.DataFrame()
        self._source_categories = pl.Series()
        self._source_platforms = pl.Series()
        self._tag = None
        self._release = None
        self._platform = None
        self._category = None
        self._partial_frames: list[pl.LazyFrame] = []
        self._full_frame: None | pl.DataFrame = None

    @contextmanager
    def source_data(
        self,
        frame: pl.DataFrame,
        release: Release,
        tag: None | str,
    ) -> Iterator[Self]:
        """
        Create a context for the release. This context manager precomputes the
        categories and platforms.
        """
        old_source, self._source = self._source, frame
        old_platforms, old_categories = self._source_platforms, self._source_categories
        self._source_platforms = frame.select(
            pl.col("platform_name").unique()
        ).get_column("platform_name")
        if self._stratify_by_category:
            self._source_categories = frame.select(
                pl.col("category").unique()
            ).get_column("category")
        old_tag, self._tag = self._tag, (tag if tag != "" else None)
        old_release, self._release = self._release, release
        try:
            yield self
        finally:
            self._source = old_source
            self._source_platforms = old_platforms
            self._source_categories = old_categories
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

    def _add_partial_frame(self, frame: pl.LazyFrame) -> None:
        if self._full_frame is not None:
            self._partial_frames.append(self._full_frame.lazy())
            self._full_frame = None
        self._partial_frames.append(frame)

    def add_rows(
        self,
        column: str,
        entity: None | str = None,
        variant: None | pl.Expr = None,
        value_counts: None | pl.Expr = None,
        text_value_counts: None | pl.Expr = None,
        frame: None | pl.LazyFrame = None,
        **kwargs: None | int | pl.Expr,
    ) -> None:
        """Add new rows."""
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
            # We must rename the source column eagerly. Otherwise, there is a
            # conflict between the "category" column in the source data and the
            # "category" column in the resulting statistics frame.
            effective_values.append(
                value_counts
                    .alias("variant")
                    .cast(pl.String)
                    .cast(VariantValueType)
                    .value_counts(sort=True)
                    .list.explode()
                    .struct.with_fields(
                        pl.field("count").cast(pl.Int64)
                    )
                    .struct.unnest()
            )

        if text_value_counts is None:
            effective_values.append(
                pl.lit(None, dtype=pl.String).alias("text")
            )
        else:
            effective_values.append(
                text_value_counts
                    .alias("text")
                    .value_counts(sort=True)
                    .list.explode()
                    .struct.with_fields(
                        pl.field("count").cast(pl.Int64)
                    )
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

        category = self._category if self._stratify_by_category else None

        assert self._release is not None
        frame = self._source.lazy() if frame is None else frame
        stats = frame.select(
            pl.lit(self._release.start_date, dtype=pl.Date).alias("start_date"),
            pl.lit(self._release.end_date, dtype=pl.Date).alias("end_date"),
            pl.lit(tag, dtype=TagValueType).alias("tag"),
            pl.lit(self._platform, dtype=PlatformValueType).alias("platform"),
            pl.lit(category, dtype=CategoryValueType).alias("category"),
            pl.lit(column, dtype=ColumnValueType).alias("column"),
            pl.lit(entity, dtype=EntityValueType).alias("entity"),
            *effective_values,
        )

        # Enforce canonical column order, so that frames can be concatenated!
        self._add_partial_frame(stats.select(
            pl.col(
                "start_date", "end_date",
                "tag", "platform", "category", "column", "entity", "variant", "text",
                "count", "min", "mean", "max"
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

        assert other_field.startswith("end_date")
        self.add_rows(
            field,
            entity="with_end_date",
            value_counts=values.cast(pl.String),
            frame=self._source.lazy().filter(
                pl.col(other_field).is_not_null()
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
            if not self._stratify_by_category and value is TransformType.CATEGORY_NAME:
                value = TransformType.VALUE_COUNTS
            if self._stratify_all_text and value is TransformType.TEXT_ROW_COUNT:
                value = TransformType.TEXT_VALUE_COUNTS

            match value:
                case TransformType.PLATFORM_NAME:
                    assert key == "platform_name"
                case TransformType.CATEGORY_NAME:
                    assert key == "category"
                case TransformType.SKIPPED_DATE:
                    pass
                case TransformType.ALL_ROWS_COUNT:
                    self.add_rows(key, count=pl.len())
                case TransformType.VALUE_COUNTS:
                    self.add_rows(key, value_counts=pl.col(key))
                case TransformType.TEXT_ROW_COUNT:
                    self.add_rows(
                        key, entity="rows_of_text",
                        count=pl.col(key).str.len_chars().gt(0).sum()
                    )
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
        """Collect statistics about categories."""
        for category in self._source_categories:
            with self.category_data(category) as this:
                this.collect_body_data()

    def collect_platform(self, platform) -> None:
        """Collect statistics for the given platform."""
        with self.platform_data(platform) as this:
            if self._stratify_by_category:
                this.collect_categories()
            else:
                this.collect_body_data()

    def collect_platforms(self) -> None:
        """Collect statistics about platforms. This method forces evaluation."""
        for name in self._source_platforms:
            self.collect_platform(name)

    def collect_header(
        self, tag: None | str, metadata_entry: None | MetadataEntry
    ) -> None:
        """Eagerly create a header frame with the given statistics."""
        pairs = {}
        md = cast(dict, metadata_entry or {})

        extract_rows_with_keywords = (
            self._source.select(
                pl.col("category_specification").is_null().not_().sum()
            ).item()
        )

        pairs["batch_count"] = md.get("batch_count")
        pairs["extract_rows"] = self._source.height
        pairs["extract_rows_with_keywords"] = extract_rows_with_keywords
        pairs["total_rows"] = (
            self._source.height if tag is None else md.get("total_rows")
        )
        pairs["total_rows_with_keywords"] = (
            extract_rows_with_keywords if tag is None
            else md.get("total_rows_with_keywords")
        )
        height = len(pairs)

        assert self._release is not None
        header = pl.LazyFrame({
            "start_date": height * [self._release.start_date],
            "end_date": height * [self._release.end_date],
            "tag": [
                (None if k in ("total_rows", "total_rows_with_keywords") else tag)
                for k in pairs.keys()
            ],
            "platform": height * [None],
            "category": height * [None],
            "column": [k for k in pairs.keys()],
            "entity": height * [None],
            "variant": height * [None],
            "text": height * [None],
            "count": [v for v in pairs.values()],
            "min": height * [None],
            "mean": height * [None],
            "max": height * [None],
        }, schema=StatisticsSchema)

        self._add_partial_frame(header)

    def collect(
        self,
        release: Release,
        frame: pl.DataFrame,
        filter: None | Filter = None,
        metadata_entry: None | MetadataEntry = None,
    ) -> None:
        """Collect all necessary data in partial data frames."""
        tag = None if filter is None else filter.tag()

        with self.source_data(frame, release, tag) as this:
            this.collect_header(tag, metadata_entry)
            if filter is None or filter.kind is not FilterKind.PLATFORM:
                this.collect_platforms()
            else:
                assert isinstance(filter.criterion, tuple)
                for platform in filter.criterion:
                    this.collect_platform(platform)

        if filter is not None and filter.is_category(
            StatementCategoryProtectionOfMinors
        ):
            frame = frame.filter(
                pl.col("category_specification").list.contains(
                    KeywordChildSexualAbuseMaterial
                )
            )

            with self.source_data(
                frame, release, KeywordChildSexualAbuseMaterial
            ) as this:
                this.collect_platforms()

    def frame(self) -> pl.DataFrame:
        """
        Combine the collected partial frames into one. This method forces
        evaluation.
        """
        # Fast path for single data frame
        if self._full_frame is not None:
            return self._full_frame
        if len(self._partial_frames) == 0:
            return pl.DataFrame([], schema=StatisticsSchema)

        # Slow path for combining more than one lazy frame
        self._full_frame = frame = pl.concat(
            self._partial_frames, how="vertical"
        ).collect()
        self._partial_frames = []
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
            # `frame` is statistics summary, hence `platform` is right column name!
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
        is_duration = isinstance(TRANSFORMS[column], DurationTransform)
        variable = column if entity is None or entity == "" else f"{column}.{entity}"
        if is_duration or quantity != "count":
            variable = f"{variable}.{quantity}"

        value = get_quantity(self._source, column, entity=entity, statistic=quantity)
        if (
            is_duration
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
                if text is None and entity != "rows_of_text":
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
                case TransformType.CATEGORY_NAME:
                    assert field_name == "category"
                    self._spacer()
                    self._collect_value_counts(field_name)
                case TransformType.SKIPPED_DATE:
                    pass
                case TransformType.ALL_ROWS_COUNT:
                    self._collect1("rows")
                    self._spacer()
                case TransformType.VALUE_COUNTS:
                    self._spacer()
                    self._collect_value_counts(field_name)
                case TransformType.TEXT_ROW_COUNT:
                    self._spacer()
                    self._collect1(field_name, "rows_of_text")
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
            # `frame` is statistics summary, hence `platform` is right column name!
            pl.col("platform").filter(pl.col("platform").is_not_null()).n_unique()
        ).item()

        platforms_with_keywords = frame.filter(
            predicate("category_specification", variant=NOT_NULL, tag=tag)
        ).select(
            # `frame` is statistics summary, hence `platform` is right column name!
            pl.col("platform").n_unique()
        ).item()

        platforms_with_csam = frame.filter(
            predicate(
                "category_specification",
                variant=KeywordChildSexualAbuseMaterial,
                tag=tag
            )
        ).select(
            # `frame` is statistics summary, hence `platform` is right column name!
            pl.col("platform").n_unique()
        ).item()

        extract_rows = get_quantity(frame, "extract_rows", entity=None, tag=tag)
        total_rows = get_quantity(frame, "total_rows", entity=None, tag=None)
        extract_kw_rows = get_quantity(frame, "extract_rows_with_keywords", entity=None, tag=tag)
        total_kw_rows = get_quantity(frame, "total_rows_with_keywords", entity=None, tag=None)
        assert tag is None or extract_rows is not None
        assert tag is None or extract_kw_rows is not None
        assert total_rows is not None
        assert total_kw_rows is not None

        extract_rows_pct = (
            (extract_rows or 0) / total_rows * 100 if total_rows != 0 else None
        )
        extract_rows_with_keywords_pct = (
            (extract_kw_rows or 0) / extract_rows * 100
            if extract_rows is not None and extract_rows != 0
            else None
        )
        total_rows_with_keywords_pct = (
            total_kw_rows / total_rows * 100 if total_rows != 0 else None
        )

        self._summary = [
            ("start_date", frame.select(pl.col("start_date").min()).item()),
            ("end_date", frame.select(pl.col("end_date").max()).item()),
            ("batch_count", get_quantity(frame, "batch_count", entity=None)),
            ("extract_rows", extract_rows),
            ("extract_rows_pct", extract_rows_pct),
            ("extract_rows_with_keywords", extract_kw_rows),
            ("extract_rows_with_keywords_pct", extract_rows_with_keywords_pct),
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
                t = "" if tag is None else humanize(tag)
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
                    sval = NULL
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
    temporarily wrap more than one data frames, lazily materializing a single
    frame on demand only.
    """

    DEFAULT_RANGE: ClassVar[DateRange] = DateRange(
        dt.date(2023, 9, 25), dt.date.today() - dt.timedelta(days=3)
    )

    # Collector performs elaborate computations on for each partial frame and
    # hence uses lazy partial frames to maximize opportunities for optimization.
    # By contrast, this class forces evaluation for its only collector instance
    # and otherwise only concatenates partial frames (optionally applying
    # finalization when writing). Hence partial frames are eager, too.

    def __init__(
        self,
        file: str,
        *frames: pl.DataFrame,
        stratify_by_category: bool = False,
        stratify_all_text: bool = False,
    ) -> None:
        self._file = file
        self._stratify_by_category = stratify_by_category
        self._stratify_all_text = stratify_all_text

        # If the instance wraps a full frame, the partial frames and collector
        # must be empty. Vice versa, if the instance wraps partial frames and/or
        # a collector, the full frame must be empty.
        self._full_frame: None | pl.DataFrame = None
        self._partial_frames: list[pl.DataFrame] = []
        self._collector = None

        match len(frames):
            case 0:
                pass
            case 1:
                self._full_frame = frames[0]
            case _:
                self._partial_frames = list(frames)

    @classmethod
    def builtin(cls) -> Self:
        """Get the pre-computed statistics for the entire DSA database."""
        # Per spec, __package__ is the same as __spec__.parent
        source = files(__spec__.parent).joinpath("builtin.parquet")
        with as_file(source) as path:
            return cls.read(path)

    @classmethod
    def pick(cls, file: str, staging: Path, persistent: Path) -> None | Self:
        """
        Pick the more complete statistics from staging and the persistent root
        directory, i.e., archive or extract. This method assumes that if both
        files exist, they also start on the same date.
        """
        try:
            staged_stats = cls.read(staging / file)
            if staged_stats.frame().is_empty():
                staged_stats = None
        except FileNotFoundError:
            staged_stats = None

        try:
            persistent_stats = cls.read(persistent / file)
            if persistent_stats.frame().is_empty():
                persistent_stats = None
        except FileNotFoundError:
            persistent_stats = None

        if staged_stats is None:
            return None if persistent_stats is None else persistent_stats
        elif persistent_stats is None:
            return staged_stats

        staged_range = staged_stats.date_range()
        persistent_range = persistent_stats.date_range()
        assert staged_range is not None and persistent_range is not None
        return (
            staged_stats if persistent_range.last < staged_range.last
            else persistent_stats
        )

    @classmethod
    def read_all(
        cls,
        directory: Path,
        *,
        glob: str = "*.parquet",
        file: None | str = None,
    ) -> Self:
        """
        Instantiate a new statistics frame from the parquet files in the given
        directory. By  default, the file name for the new statistics is derived
        from the directory stem. This method assumes that all frames have been
        created with the same stratification options. At the same time, frames
        may differ in their schemas as far as the enumeration of platform names
        is concerned.
        """
        if file is None:
            file = f"{directory.stem}.parquet"

        frames = []
        for path in sorted(directory.glob(glob)):
            frame = pl.read_parquet(path)
            frame = cls._check_platform_names(path, frame)
            frames.append(frame)

        frame = pl.concat(frames, how="vertical", rechunk=True)
        return cls(
            file,
            frame,
            **cls._extract_stratification(frame),
        )

    @classmethod
    def read(cls, path: Path) -> Self:
        """
        Instantiate a new statistics frame from the given file path. This method
        assumes that the file exists and throws an exception otherwise.
        """
        frame = pl.read_parquet(path)
        return cls(
            path.name,
            cls._check_platform_names(path, frame),
            **cls._extract_stratification(frame)
        )

    @classmethod
    def _check_platform_names(
        cls, path: str | Path, frame: pl.DataFrame
    ) -> pl.DataFrame:
        frame = frame.with_columns(
            # Cast to string so that replace matches platform names
            pl.col("platform").cast(str).replace(CanonicalPlatformNames)
        )
        # The statistics schema incorporates the platform names into an
        # enumeration type. Hence, if the frame includes unknown platforms, the
        # cast below will fail, with a Pola.rs error message that does NOT
        # identify the offending name(s). So instead we proactively check for
        # unknown platform names and initiate (mostly) automatic recovery.
        check_stats_platforms(path, frame)
        return frame.cast(StatisticsSchema) # pyright: ignore[reportArgumentType]

    @classmethod
    def _extract_stratification(cls, frame: pl.DataFrame) -> dict[str, bool]:
        if frame.is_empty():
            return {}

        non_null_categories, rows_of_text_entities = frame.select(
            pl.col("category").is_not_null().sum(),
            pl.col("entity").eq("rows_of_text").sum(),
        ).row(0)

        return {
            "stratify_by_category": non_null_categories != 0,
            "stratify_all_text": rows_of_text_entities == 0,
        }

    @property
    def file(self) -> str:
        """Access the file name."""
        return self._file

    @property
    def stratify_by_category(self) -> bool:
        """The flag for stratifying by category."""
        return self._stratify_by_category

    @property
    def stratify_all_text(self) -> bool:
        """The flag for stratifying all text."""
        return self._stratify_all_text

    @property
    def stratification(self) -> dict[str, bool]:
        """The stratification flags."""
        return {
            "stratify_by_category": self.stratify_by_category,
            "stratify_all_text": self.stratify_all_text,
        }

    def __dataframe__(self) -> Any:
        return self.frame().__dataframe__()

    def frame(self) -> pl.DataFrame:
        """
        Materialize a single data frame with the summary statistics. If this
        method computes a new single data frame, it also updates the internal
        state with that frame and discards the partial frames. Hence, any
        subsequent calls to this method, with no intervening calls to `collect`
        or `append` return the exact same data frame.
        """
        # Fast path: Just return full frame
        if self._full_frame is not None:
            return self._full_frame

        # Slow path: Concat partial frames
        partial_frames, self._partial_frames = self._partial_frames, []
        if self._collector is not None:
            frame = self._collector.frame()
            frame.shrink_to_fit(in_place=True)
            partial_frames.append(frame)
            self._collector = None

        if len(partial_frames) == 0:
            self._full_frame = frame = pl.DataFrame([], schema=StatisticsSchema)
            return frame

        self._full_frame = frame = pl.concat(
            partial_frames, how="vertical"
        )
        return frame

    def is_empty(self) -> bool:
        """Determine whether this instance has no data."""
        return self.frame().height == 0

    def __contains__(self, date: None | dt.date | Daily) -> bool:
        """
        Determine whether the summary statistics contain data for the given
        date. This method checks for that day's batch_count being present.
        """
        if date is None:
            return False
        if isinstance(date, Daily):
            date = date.start_date

        return self.frame().select(
            pl.col("column").eq("batch_count").and_(
                pl.col("tag").is_null()
            ).and_(
                pl.col("start_date").eq(date)
            ).and_(
                pl.col("end_date").eq(date)
            )
        ).height == 1

    def release_range(self) -> None | ReleaseRange[Daily]:
        """Determine the range of releases covered by the summary statistics."""
        range = self.date_range()
        return None if range is None else range.dailies()

    def date_range(self) -> None | DateRange:
        """Determine the range of dates covered by the summary statistics."""
        frame = self.frame()
        if frame.height == 0:
            return None

        return DateRange(*frame.select(
            pl.col("start_date").min(),
            pl.col("end_date").max(),
        ).row(0))

    def metadata(
        self,
        tag: NoArgumentProvided | None | str = NO_ARGUMENT_PROVIDED,
    ) -> list[FullMetadataEntry]:
        if tag == NO_ARGUMENT_PROVIDED:
            tags = get_tags(self.frame())
            if len(tags) == 1:
                tag = tags[0]
            else:
                lst = ", ".join((str(t) for t in tags))
                raise ValueError(
                    f"statistics include {lst} as tags; please pass one as argument"
                )

        selectors = []
        for column in ("batch_count", "extract_rows", "extract_rows_with_keywords"):
            if tag is None:
                selector = pl.col("tag").is_null()
            else:
                selector = pl.col("tag").eq(tag)

            selectors.append(
                pl.col("count").filter(
                    selector.and_(
                        pl.col("column").eq(column)
                    )
                ).alias(column)
            )

        for column in ("total_rows", "total_rows_with_keywords"):
            selectors.append(
                pl.col("count").filter(
                    pl.col("tag").is_null().and_(
                        pl.col("column").eq(column)
                    )
                ).alias(column)
            )

        frame = self.frame().filter(
            pl.col("start_date").eq(pl.col("end_date")).and_(
                pl.col("platform").is_null()
            ).and_(
                pl.col("category").is_null()
            ).and_(
                pl.col("entity").is_null()
            ).and_(
                pl.col("variant").is_null()
            )
        ).select(
            pl.col("start_date").alias("date"),
            *selectors,
        )

        entries = []
        for row in frame.rows():
            date, batches, extract_rows, extract_kw_rows, total_rows, total_kw_rows = (
                row
            )

            entries.append(dict(
                release=Release.of(date),
                batch_count=batches,
                extract_rows=extract_rows,
                extract_rows_with_keywords=extract_kw_rows,
                total_rows=total_rows,
                total_rows_with_keywords=total_kw_rows,
            ))

        return entries

    def collect(
        self,
        release: Release,
        frame: pl.DataFrame,
        filter: None | Filter = None,
        metadata_entry: None | MetadataEntry = None,
    ) -> None:
        """
        Add summary statistics for the frame with transparency database data.
        This method adds the given `tag` to collected summary statistics. Use
        `append()` for frames with already computed statistics.
        """
        if self._collector is None:
            self._collector = Collector(
                stratify_by_category=self._stratify_by_category,
                stratify_all_text=self._stratify_all_text,
            )

        self._collector.collect(
            release,
            frame,
            filter=filter,
            metadata_entry=metadata_entry
        )

    def append(self, frame: pl.DataFrame) -> None:
        """
        Append the data frame with summary statistics. Use `collect()` for
        frames with transparency database data.
        """
        self._partial_frames.append(
            frame.cast(StatisticsSchema) # pyright: ignore[reportArgumentType]
        )

    def summary(self, platform: None | str = None, markdown: bool = False) -> str:
        """Create a summary table formatted as Unicode text or Markdown."""
        summarizer = _Summarizer(platform=platform)
        summarizer.summarize(self.frame())
        return summarizer.formatted_summary(markdown)

    def write(
        self,
        directory: Path,
        *,
        should_finalize: bool = False,
    ) -> Self:
        """
        Write this statistics frame to the given directory. If `finalize` is
        `True`, this method groups and aggregates the frame at daily
        granularity, sorts the entries by date, and rechunks the memory consumed
        by the data frame before writing it out. The updated frame also becomes
        the internal version.
        """
        frame = self.frame()
        if should_finalize:
            self._full_frame = frame = finalize(frame)

        path = directory / self.file
        tmp = path.with_suffix(".tmp.parquet")
        frame.write_parquet(tmp)
        tmp.replace(path)

        return self

    def write_release(self, release: Release, directory: Path) -> None:
        assert release.date is not None

        self.frame().filter(
            pl.col("start_date").le(release.date).and_(
                pl.col("end_date").ge(release.date)
            )
        ).write_parquet(directory / f"{release}.parquet")

    @classmethod
    def copy_all(cls, stem: str, source: Path, target: Path) -> None:
        """Copy the directory of pre-release statistics and the combined file."""
        stats_dir = f"{stem}.stats"
        stats_file = f"{stem}.parquet"

        cls._compare_or_copy(source / stats_file, target / stats_file)

        target_dir = target / stats_dir
        target_dir.mkdir(exist_ok=True)

        for file in (source / stats_dir).glob("*.parquet"):
            cls._compare_or_copy(file, target_dir / file.name)

    @classmethod
    def _compare_or_copy(cls, source: Path, target: Path) -> None:
        """Copy the source file to the target file."""
        if target.exists():
            if not filecmp.cmp(source, target, shallow=False):
                raise ValueError(
                    f'"{source}" and "{target}" differ; please move '
                    'the latter out of the way'
                )
        else:
            tmp = target.with_suffix(".tmp.parquet")
            shutil.copy(source, tmp)
            tmp.replace(target)
