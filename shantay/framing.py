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
from collections.abc import Sequence
from typing import Literal

import polars as pl

from .model import Period


CSAM_TAG = "CSAM"


def distill_category_from_parquet(glob: str) -> None | str:
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


# --------------------------------------------------------------------------------------


class NoArgumentProvided:
    """See description of `predicate()`"""
    pass

NO_ARGUMENT_PROVIDED = NoArgumentProvided()


class NotNull:
    """See description of `predicate()`"""
    pass

NOT_NULL = NotNull()


def predicate(
    column: (
        NoArgumentProvided | NotNull | None | str | Sequence[str]
    ) = NO_ARGUMENT_PROVIDED,
    entity: NoArgumentProvided | NotNull | None | str = NO_ARGUMENT_PROVIDED,
    variant: NoArgumentProvided | NotNull | None | str = NO_ARGUMENT_PROVIDED,
    tag: NoArgumentProvided | NotNull | None | str = NO_ARGUMENT_PROVIDED,
    platform: (
        NoArgumentProvided | NotNull | None | str | Sequence[str]
    ) = NO_ARGUMENT_PROVIDED,
) -> pl.Expr:
    """
    Create the predicate over the "tag", "platform", "column", "entity", and
    "variant" columns. If the argument is a string or list of strings, the
    predicate tests that column for the literal string value(s). If it is None,
    the predicate tests for the column being null. If it is `NOT_NULL`, the
    predicate tests for it being not null. Finally, if it is
    `NO_ARGUMENT_PROVIDED`, the predicate does not test that column.
    """
    result = None

    def filter(clause: pl.Expr) -> None:
        nonlocal result
        if result is None:
            result = clause
        else:
            result = result.and_(clause)

    for key, value in (
        ("tag", tag),
        ("platform", platform),
        ("column", column),
        ("entity", entity),
        ("variant", variant),
    ):
        if value is None:
            filter(pl.col(key).is_null())
        elif isinstance(value, NotNull):
            filter(pl.col(key).is_null().not_())
        elif isinstance(value, str):
            filter(pl.col(key).eq(value))
        elif isinstance(value, Sequence):
            filter(pl.col(key).is_in(value))
        elif isinstance(value, NoArgumentProvided):
            pass
        else:
            raise AssertionError("unreachable")

    assert result is not None
    return result


type Quantity = Literal["count", "min", "mean", "max"]


def get_quantity(
    frame: pl.DataFrame,
    column: str,
    entity: NoArgumentProvided | None | str = NO_ARGUMENT_PROVIDED,
    variant: NoArgumentProvided | None | str = NO_ARGUMENT_PROVIDED,
    tag: NoArgumentProvided | NotNull | None | str = NO_ARGUMENT_PROVIDED,
    platform: NoArgumentProvided | NotNull | None | str = NO_ARGUMENT_PROVIDED,
    statistic: Quantity = "count",
) -> None | int:
    """Retrieve a quantity from the data frame."""
    frame = frame.filter(
        predicate(
            column,
            entity=entity,
            variant=variant,
            tag=tag,
            platform=platform,
        )
    ).select(
        aggregates()
    ).select(
        pl.col(statistic)
    )

    assert frame.height <= 1
    return frame.item() if frame.height == 1 else None


def aggregates() -> list[pl.Expr]:
    """The Pola.rs expressions to `agg` by when computing daily statistics."""
    return [
        pl.col("count").sum(),
        pl.col("min").min(),

        # The following formula started out as:
        #
        #     (pl.col("mean") * pl.col("count")).sum() // pl.col("count").sum()
        #
        # But that version is prone to overflowing the numerator. So we made two
        # significant changes: First, we refactored the division of a sum into a
        # sum of divisions. Second, we changed the resolution of duration from
        # milliseconds to seconds. We also rewrote the formula using Pola.rs
        # expressions instead of Python operators. Note that the first change,
        # by distributing the division, makes the expression more expensive to
        # compute.

        pl.col("mean")
        .mul(pl.col("count"))
        .floordiv(pl.col("count").sum())
        .sum(),

        pl.col("max").max(),
    ]


def finalize(frame: pl.DataFrame) -> pl.DataFrame:
    """
    Prepare a data frame for production use. This function regroups, sorts, and
    rechunks the data frame.
    """
    return frame.group_by(
        "start_date", "end_date", "tag", "platform",
        "column", "entity", "variant", "text",
        maintain_order=True
    ).agg(
        *aggregates()
    ).sort(
        pl.col("start_date", "platform"), maintain_order=True
    ).rechunk()
