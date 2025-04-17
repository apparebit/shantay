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
from importlib import import_module
from typing import Literal

import polars as pl

from .metadata import FullMetadataEntry
from .model import ConfigError, DateRange, Period, QueryExpression


CSAM_TAG = "CSAM"


def collect_release_metadata(
    records: Iterator[FullMetadataEntry]
) -> tuple[DateRange, pl.DataFrame]:
    """
    Collect metadata release records into a data frame.

    This function does *not* depend on the particulars of the DSA SoR DB schema.

    The data frame uses `i64` for columns containing counts. The corresponding
    resolution is barely sufficient for the current use case and hence switching
    to `i128` is highly desirable. However, for now, that is impossible because
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


def get_frequency(frame: pl.DataFrame) -> Literal["daily", "monthly"]:
    """
    Determine the frequency for summary statistics, which can be daily or
    monthly
    """
    start_date, end_date, *_ = frame.row(0)
    return "daily" if start_date == end_date else "monthly"


class NoArgumentProvided:
    """See description of `predicate()`"""
    pass

NO_ARGUMENT_PROVIDED = NoArgumentProvided()


class NotNull:
    """See description of `predicate()`"""
    pass

NOT_NULL = NotNull()


def predicate(
    column: str | Sequence[str] | NotNull,
    entity: NoArgumentProvided | NotNull | None | str = NO_ARGUMENT_PROVIDED,
    variant: NoArgumentProvided | NotNull | None | str = NO_ARGUMENT_PROVIDED,
    variant_too: NoArgumentProvided | NotNull | None | str = NO_ARGUMENT_PROVIDED,
    tag: NoArgumentProvided | NotNull | None | str = NO_ARGUMENT_PROVIDED,
) -> pl.Expr:
    """
    Create the predicate over the "tag", "column", "entity", "variant", and
    "variant_too" columns. If the argument is a string or list of strings, the
    predicate tests that column for the literal string value(s). If it is None,
    the predicate tests for the column being null. If it is `NOT_NULL`, the
    predicate tests for it being not null. Finally, if it is
    `NO_ARGUMENT_PROVIDED`, the predicate does not test that column.
    """
    # We always query the tag and column
    if tag is None:
        predicate = pl.col("tag").is_null()
    elif isinstance(tag, NotNull):
        predicate = pl.col("tag").is_null().not_()
    elif tag is not NO_ARGUMENT_PROVIDED:
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
    elif entity is not NO_ARGUMENT_PROVIDED:
        predicate = predicate.and_(pl.col("entity").eq(entity))

    if variant is None:
        predicate = predicate.and_(pl.col("variant").is_null())
    elif isinstance(variant, NotNull):
        predicate = predicate.and_(pl.col("variant").is_null().not_())
    elif variant is not NO_ARGUMENT_PROVIDED:
        predicate = predicate.and_(pl.col("variant").eq(variant))

    if variant_too is None:
        predicate = predicate.and_(pl.col("variant_too").is_null())
    elif isinstance(variant_too, NotNull):
        predicate = predicate.and_(pl.col("variant_too").is_null().not_())
    elif variant_too is not NO_ARGUMENT_PROVIDED:
        predicate = predicate.and_(pl.col("variant_too").eq(variant_too))

    return predicate


type Quantity = Literal["count", "min", "mean", "max"]


def get_quantity(
    frame: pl.DataFrame,
    column: str,
    entity: NoArgumentProvided | None | str = NO_ARGUMENT_PROVIDED,
    variant: NoArgumentProvided | None | str = NO_ARGUMENT_PROVIDED,
    variant_too: NoArgumentProvided | NotNull | None | str = NO_ARGUMENT_PROVIDED,
    tag: NoArgumentProvided | NotNull | None | str = NO_ARGUMENT_PROVIDED,
    statistic: Quantity = "count"
) -> None | int:
    """Retrieve a quantity from the data frame."""
    frame = frame.filter(
        predicate(
            column,
            entity=entity,
            variant=variant,
            variant_too=variant_too,
            tag=tag
        )
    ).select(
        aggregates()
    ).select(
        pl.col(statistic)
    )

    assert frame.height <= 1
    return frame.item() if frame.height == 1 else None


def daily_groupies() -> list[pl.Expr]:
    """The Pola.rs expressions to `group_by` when computing daily statistics."""
    return [
        pl.col("start_date"),
        pl.col("end_date"),
        pl.col("tag"),
        pl.col("column"),
        pl.col("entity"),
        pl.col("variant"),
        pl.col("variant_too"),
    ]


def monthly_groupies() -> list[pl.Expr]:
    """The Pola.rs expressions to `group_by` when computing monthly statistics."""
    return [
        pl.col("start_date").dt.year().alias("year"),
        pl.col("start_date").dt.month().alias("month"),
    ]


def aggregates() -> list[pl.Expr]:
    """The Pola.rs expressions to `agg` by when computing daily statistics."""
    return [
        pl.col("count").sum(),
        pl.col("min").min(),
        (pl.col("mean") * pl.col("count")).sum() // pl.col("count").sum(),
        pl.col("max").max(),
    ]
