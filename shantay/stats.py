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
from pathlib import Path
import shutil
from typing import Any, ClassVar, Literal, Self

import polars as pl

from .framing import (
    aggregates, daily_groupies, get_quantity, monthly_groupies, predicate, Quantity
)
from .model import Daily, DateRange, Release
from .schema import (
    DurationTransform, StatisticsSchema, TRANSFORM_COUNT, TRANSFORMS, TransformType,
    ValueCountsPlusTransform, VariantValueType, VariantTooValueType
)
from .util import scale_time


_DECISION_OFFSET = len("decision_")

_DECISION_TYPES = (
    "decision_visibility",
    "decision_monetary",
    "decision_provision",
    "decision_account",
)


def _range_of(frame: pl.DataFrame) -> DateRange:
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
        "moderation_delay",
        "disclosure_delay",
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


# =================================================================================================


class Collector:
    """Analyze the data while also collecting the results."""

    def __init__(self) -> None:
        self._source = pl.DataFrame()
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

        if variant_too is None:
            effective_values.append(
                pl.lit(None, dtype=VariantTooValueType).alias("variant_too")
            )
        else:
            effective_values.append(
                variant_too
                    .cast(pl.String)
                    .cast(VariantTooValueType)
                    .alias("variant_too")
            )

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
            pl.lit(self._release.start_date).alias("start_date"),
            pl.lit(self._release.end_date).alias("end_date"),
            pl.lit(tag).alias("tag"),
            pl.lit(column).alias("column"),
            pl.lit(entity).alias("entity"),
            *effective_values,
        )

        if value_counts is not None:
            frame = frame.rename({
                column: "variant",
            })

        frame = frame.cast(StatisticsSchema) # pyright: ignore[reportArgumentType]

        # Enforce canonical column order, so that frames can be concatenated!
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

        if not _is_categorical(other_field):
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
            return

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

    def collect_header(self, metadata: None | pl.DataFrame = None) -> None:
        """Create a header frame with the given statistics."""
        pairs = {}
        if metadata is None:
            if isinstance(self._source, pl.LazyFrame):
                self._source = self._source.collect()

            pairs["batch_count"] = 1
            pairs["batch_rows"] = self._source.height
            pairs["batch_rows_with_keywords"] = (
                self._source.select(
                    pl.col("category_specification").is_null().not_().sum()
                ).item()
            )
            pairs["batch_memory"] = self._source.estimated_size()
            pairs["total_rows"] = pairs["batch_rows"]
            pairs["total_rows_with_keywords"] = pairs["batch_rows_with_keywords"]
        else:
            for name in (
                "batch_count", "batch_rows", "batch_rows_with_keywords",
                "batch_memory", "total_rows", "total_rows_with_keywords",
            ):
                if name in metadata.columns:
                    value = metadata.select(pl.col(name).sum()).item()
                else:
                    value = None

                pairs[name] = value

        assert self._release is not None
        height = len(pairs)

        # Pola.rs uses different code paths for pl.concat depending on whether
        # the first frame is lazy or not. Let's use the right one.
        Frame = pl.LazyFrame if isinstance(self._source, pl.LazyFrame) else pl.DataFrame
        header = Frame({
            "start_date": height * [self._release.start_date],
            "end_date": height * [self._release.end_date],
            "tag": height * [None],
            "column": [k for k in pairs.keys()],
            "entity": height * [None],
            "variant": height * [None],
            "variant_too": height * [None],
            "count": [v for v in pairs.values()],
            "min": height * [None],
            "mean": height * [None],
            "max": height * [None],
        }, schema=StatisticsSchema)

        self._frames.append(header)

    def collect(
        self,
        release: Release,
        frame: pl.DataFrame | pl.LazyFrame,
        tag: None | str = None,
        metadata: None | pl.DataFrame = None,
    ) -> None:
        """Collect all necessary data in partial data frames."""
        if tag is None:
            with self.source_data(frame=frame, release=release) as this:
                this.collect_header(metadata)
                this.collect_body()
        else:
            with self.source_data(frame=frame, release=release, tag=tag) as this:
                this.collect_body()

    def frame(
        self, validate: bool = False, group_by: None | Literal["day", "month"] = None
    ) -> pl.DataFrame:
        """Combine the collected partial frames into one."""
        frame = pl.concat(self._frames, how="vertical")
        if isinstance(frame, pl.LazyFrame):
            frame = frame.collect()
        frame = frame.cast(StatisticsSchema) # pyright: ignore[reportArgumentType]
        if validate:
            _validate_row_counts(frame)
        if group_by is not None:
            groupies = daily_groupies if group_by == "day" else monthly_groupies
            frame = frame.group_by(*groupies(), maintain_order=True).agg(*aggregates())
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


type _Summary = list[tuple[str | _Tag | _Spacer, Any]]


class _Summarizer:
    """Summarize analysis results."""

    def __init__(self) -> None:
        self._source = pl.DataFrame()
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
            pl.col("column", "entity", "variant", "variant_too")
        ).agg(
            *aggregates()
        )

        try:
            yield self
        finally:
            self._source = old_source
            self._tag = old_tag

    @contextmanager
    def spacer_on_demand(self) -> Iterator[None]:
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
                self.spacer(actual_summary)
                actual_summary.extend(self._summary)
            self._summary = actual_summary

    def spacer(self, summary: None | _Summary = None) -> None:
        """Add an empty row to the summary of summary statistics."""
        if summary is None:
            summary = self._summary
        summary.append((_SPACER, _SPACER))

    def collect1(
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
        if duration and quantity != "count" and value is not None:
            value = dt.timedelta(seconds=value // 1_000, milliseconds=value % 1_000)

        self._summary.append((variable, value))

    def collect_value_counts(self, column: str, entity: None | str = None) -> None:
        """Collect the given column's value counts."""
        for row in self._source.filter(
            predicate(column, entity=entity)
        ).select(
            pl.col("column", "entity", "variant", "variant_too", "count")
        ).sort(
            ["count", "variant", "variant_too"], descending=True
        ).rows():
            column, entity, variant, variant_too, count = row
            var = column

            if entity == "with_end_date":
                var = f"{var}.{entity}"

            if variant is None:
                var = f"{var}.is_null"
            else:
                var = f"{var}.{variant}"

            if (
                entity is not None
                and entity != "with_end_date"
                and entity.startswith("with_")
            ):
                if variant_too is None:
                    var = f"{var}.is_null"
                else:
                    var = f"{var}.{variant_too}"

            self._summary.append((var, count))

    def summarize_fields(self) -> None:
        """Summarize all fields of summary statistics."""
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
                    self.collect1(field_name, quantity="count")
                    self.collect1(field_name, quantity="min")
                    self.collect1(field_name, quantity="mean")
                    self.collect1(field_name, quantity="max")
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

    def summarize(self, frame: pl.DataFrame) -> _Summary:
        """Summarize the data frame."""
        with self.tagged_frame(tag=None, frame=frame) as this:
            platforms = frame.filter(
                predicate("platform_name", entity=None, tag=None)
            ).select(
                pl.col("variant").n_unique()
            ).item()

            platforms_with_keywords = frame.filter(
                predicate("platform_name", entity="with_category_specification", tag=None)
            ).filter(
                pl.col("variant_too").is_null().not_()
            ).select(
                pl.col("variant").n_unique()
            ).item()

            platforms_with_csam = frame.filter(
                predicate("platform_name", entity="with_category_specification", tag=None)
            ).filter(
                pl.col("variant_too").eq("KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL")
            ).select(
                pl.col("variant").n_unique()
            ).item()

            batch_rows = get_quantity(frame, "batch_rows", entity=None)
            total_rows = get_quantity(frame, "total_rows", entity=None)
            batch_kw_rows = get_quantity(frame, "batch_rows_with_keywords", entity=None)
            total_kw_rows = get_quantity(frame, "total_rows_with_keywords", entity=None)
            assert batch_rows is not None
            assert batch_kw_rows is not None
            assert total_rows is not None
            assert total_kw_rows is not None

            this._summary = [
                ("start_date", frame.select(pl.col("start_date").min()).item()),
                ("end_date", frame.select(pl.col("end_date").max()).item()),
                ("batch_count", get_quantity(frame, "batch_count", entity=None)),
                ("batch_rows", batch_rows),
                ("batch_rows_pct", batch_rows / total_rows * 100),
                ("batch_rows_with_keywords", batch_kw_rows),
                ("batch_rows_with_keywords_pct", batch_kw_rows / batch_rows * 100),
                ("batch_memory", get_quantity(frame, "batch_memory", entity=None)),
                ("total_rows", total_rows),
                ("total_rows_with_keywords", total_kw_rows),
                ("total_rows_with_keywords_pct", total_kw_rows / total_rows * 100),
                ("platforms", platforms),
                ("platforms_with_keywords", platforms_with_keywords),
                ("platforms_with_csam", platforms_with_csam)
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
                this._summary.append((_Tag(this._tag), _Tag(this._tag)))
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
                if isinstance(var, _Tag):
                    # Delay formatting of tag for non-markdown output
                    # so that we can center it
                    assert isinstance(val, _Tag)
                    svar = f"***——— {val} ———***" if markdown else var
                elif var is _SPACER:
                    svar = "\u2800" if markdown else " "
                else:
                    svar = var

                if isinstance(val, _Tag):
                    assert isinstance(var, _Tag)
                    sval = f"***— {val} —***" if markdown else val
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
                assert isinstance(val, _Tag)
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


# =================================================================================================


from ._platform import (
    MissingPlatformError as MissingPlatformError,
    check_new_platform_names as check_new_platform_names,
    update_new_platform_names as update_new_platform_names,
)


class Statistics:
    """
    Wrapper around statistics describing the DSA transparency database.
    Conceptually, the descriptive statistics form a single data frame. However,
    to support incremental collection of those statistics, this class may
    temporarily wrap more than one data frame, lazily materializing a single
    frame only on demand.
    """

    """The name of the Parquet file with the summary statistics."""
    FILE: ClassVar[str] = "statistics.parquet"

    """
    The default date range for summary statistics, which start with
    2023-09-25 and end two days before today.
    """
    DEFAULT_RANGE: ClassVar[DateRange] = DateRange(
        dt.date(2023, 9, 25), dt.date.today() + dt.timedelta(days=1)
    )

    def __init__(self, *frames: pl.DataFrame) -> None:
        self._frames = list(frames)
        self._collector = None

    @classmethod
    def from_storage(cls, staging: Path, persistent: Path) -> Self:
        """
        Pick the more complete statistics from staging and the persistent root
        directory, i.e., archive or working. This method assumes that if both
        files exist, they also start on the same date.
        """
        s1 = cls.read(staging) if (staging / cls.FILE).exists() else None
        s2 = cls.read(persistent) if (persistent / cls.FILE).exists() else None
        if s1 is None:
            return cls() if s2 is None else s2
        elif s2 is None:
            return s1

        r1 = s1.range()
        r2 = s2.range()
        if r1.first != r2.first:
            raise ValueError(
                f"inconsistent start dates {r1.first.isoformat()} "
                f"and {r2.first.isoformat()} for statistics coverage"
            )

        return s1 if r2.last < r1.last else s2

    @classmethod
    def read(cls, directory: Path) -> Self:
        """
        Instantiate a new statistics frame from the given directory. This method
        assumes that the file exists and throws an exception otherwise.
        """
        frame = pl.read_parquet(
            directory / cls.FILE
        ).cast(StatisticsSchema) # pyright: ignore[reportArgumentType]
        return cls(frame)

    def __dataframe__(self) -> Any:
        return self.frame().__dataframe__()

    def frame(
        self,
        validate: bool = False,
        group_by: None | Literal["day", "month"] = None,
    ) -> pl.DataFrame:
        """
        Materialize a single data frame with the summary statistics. If this
        method computes a new single data frame, it also updates the internal
        state with that frame, discarding the subsets used for creating that
        single frame in the first place. That implies that a subsequent call to
        this method, with no intervening calls to `collect` or `append` return
        the exact same data frame.
        """
        # Fast paths for repeated read-access to not yet built or finished frame.
        if len(self._frames) == 0:
            return pl.DataFrame([], schema=StatisticsSchema)
        if (
            len(self._frames) == 1
            and self._collector is None
            and not validate
            and group_by is None
        ):
            return self._frames[0]

        # Combine frame fragments into one frame
        all_frames = list(self._frames)
        if self._collector is not None:
            all_frames.append(self._collector.frame(group_by="day"))
        frame = pl.concat(all_frames, how="vertical")

        # Take care of validation and grouping
        if validate:
            _validate_row_counts(frame)
        if group_by is not None:
            groupies = daily_groupies if group_by == "day" else monthly_groupies
            frame = frame.group_by(
                *groupies(), maintain_order=True
            ).agg(
                *aggregates()
            )

        # Update internal state
        self._frames = [frame]
        self._collector = None
        return frame

    def is_empty(self) -> bool:
        """Determine whether this instance has no data."""
        frame = self.frame()
        return frame.height == 0

    def __contains__(self, date: None | dt.date | Daily) -> bool:
        """
        Determine whether the summary statistics contain data for the given
        date. This method recognizes summary statistics with either daily or
        monthly granularity, as collected by the summarize and analyze tasks,
        respectively.
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

    def range(self) -> DateRange:
        """
        Determine the range from minimum start date to maximum end date
        covered by the summary statistics.
        """
        frame = self.frame()
        if frame.height == 0:
            raise ValueError("no statistics available")
        return _range_of(frame)

    def collect(
        self,
        release: Release,
        frame: pl.DataFrame | pl.LazyFrame,
        tag: None | str = None,
        metadata: None | pl.DataFrame = None,
    ) -> None:
        """
        Add summary statistics for the frame with transparency database data.
        This method adds the given `tag` to collected summary statistics. Use
        `append()` for frames with already computed statistics.
        """
        if self._collector is None:
            self._collector = Collector()
        self._collector.collect(release, frame, tag=tag, metadata=metadata)

    def append(self, frame: pl.DataFrame) -> None:
        """
        Append the data frame with summary statistics. Use `collect()` for
        frames with transparency database data.
        """
        self._frames.append(
            frame.cast(StatisticsSchema) # pyright: ignore[reportArgumentType]
        )

    def summary(self, markdown: bool = False) -> str:
        """Create a summary table formatted as Unicode or Markdown."""
        summarizer = _Summarizer()
        summarizer.summarize(self.frame())
        return summarizer.formatted_summary(markdown)

    def write(self, directory: Path, rechunk: bool = False) -> Self:
        """Write this statistics frame to the given directory."""
        frame = self.frame()
        if rechunk:
            frame = frame.rechunk()
            self._frames = [frame]

        tmp = (directory / self.FILE).with_suffix(".tmp.parquet")
        frame.write_parquet(tmp)
        tmp.replace(directory / self.FILE)

        return self

    @classmethod
    def copy(cls, source: Path, target: Path) -> None:
        """
        Copy the statistics file in the source directory to the target directory
        via an intermediate temporary file on the same file system as the target
        directory.
        """
        tmp = (target / cls.FILE).with_suffix(".tmp.parquet")
        shutil.copy(source / cls.FILE, tmp)
        tmp.replace(target / cls.FILE)
