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
from importlib import import_module

import polars as pl

from .metadata import FullMetadataEntry
from .model import ConfigError, DateRange, Period, QueryExpression


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


class Collector:
    """
    Add named data frames per release, consume concatenated data frames by name.
    """

    def __init__(self) -> None:
        self._releases = {}

    def add_frames(self, release: object, **kwargs: pl.DataFrame) -> None:
        """Add named data frames for release."""
        frames = self._releases.setdefault(str(release), {})
        frames.update(kwargs)

    def consume_frames(self) -> Iterator[tuple[str, pl.DataFrame]]:
        """Iterate """
        all_frames = {}
        for release_data in self._releases.values():
            for name, frame in release_data.items():
                all_frames.setdefault(name, []).append(frame)

        for name, frames in all_frames.items():
            yield name, pl.concat(frames, how="vertical", rechunk=True)


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


def one_column_summary(frame: pl.DataFrame) -> pl.DataFrame:
    """
    Summarize the given data frame. This function expects a statistics data
    frame. It sums up numeric columns besides those containing maxima, which
    require the continuing maximization of values, and then transposes the one
    row into one column for easier readability.
    """
    columns = [c for c in frame.columns if c not in ("start_date", "end_date")]

    return frame.with_columns(
        pl.lit(1).alias("fake")
    ).group_by("fake").agg(
        pl.col("total_rows").sum(),
        pl.col("total_rows_with_keywords").sum(),
        pl.lit("").alias("•"),
        pl.col("batch_count").sum(),
        pl.lit("").alias("•"),
        pl.col("rows").sum(),
        pl.col("keywords").sum(),
        pl.col("rows_with_keywords").sum(),
        pl.col("max_keywords_per_row").max(),
        pl.lit("").alias("•"),
        pl.col("illegal_content").sum(),
        pl.col("incompatible_content").sum(),
        pl.lit("").alias("•"),
        pl.col("account_suspended_no_date").sum(),
        pl.col("account_suspended_until_date").sum(),
        pl.col("account_terminated_no_date").sum(),
        pl.col("account_terminated_until_date").sum(),
        pl.lit("").alias("•"),
        pl.col("csam").sum(),
        pl.lit("").alias("•"),
        pl.col("csam_illegal_content").sum(),
        pl.col("csam_incompatible_content").sum(),
        pl.col("csam_no_decision_ground").sum(),
        pl.lit("").alias("•"),
        pl.col("csam_account_suspended").sum(),
        pl.lit("").alias("•"),
        pl.col("csam_suspended_no_date").sum(),
        pl.col("csam_suspended_until_date").sum(),
        pl.col("csam_terminated_no_date").sum(),
        pl.col("csam_terminated_until_date").sum(),
        pl.col("csam_no_decision_account").sum(),
        pl.lit("").alias("•"),
        pl.col("csam_max_visibility_per_row").max(),
        pl.col("csam_visibility_values").sum(),
        pl.col("csam_rows_with_visibility").sum(),
        pl.col("csam_rows_null_visibility").sum(),
        pl.lit("").alias("•"),
        pl.col("csam_removed").sum(),
        pl.col("csam_disabled").sum(),
        pl.col("csam_demoted").sum(),
        pl.col("csam_age_restricted").sum(),
        pl.col("csam_interaction_restricted").sum(),
        pl.col("csam_labeled").sum(),
        pl.col("csam_other_visibility").sum(),
    ).transpose(
        include_header=True,
        header_name="variable",
        column_names=["value"]
    )
