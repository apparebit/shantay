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
import datetime as dt
from importlib import import_module

import polars as pl

from .metadata import FullMetadataEntry
from .model import ConfigError, DateRange, Period, QueryExpression, Release


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


class Collector[R: Release]:
    """
    Add named data frames per release, consume concatenated data frames by name.
    """

    def __init__(self) -> None:
        self._first: None | R = None
        self._last: None | R = None
        self._releases = {}

    @property
    def start_date(self) -> dt.date:
        assert self._first is not None
        return self._first.start_date

    @property
    def end_date(self) -> dt.date:
        assert self._last is not None
        return self._last.end_date

    def add_frames(self, release: R, **kwargs: pl.DataFrame) -> None:
        """Add named data frames for release."""
        if self._first is None or release < self._first:
            self._first = release
        if self._last is None or self._last < release:
            self._last = release

        frames = self._releases.setdefault(str(release), {})
        frames.update(kwargs)

    def consume_frames(self) -> Iterator[tuple[str, pl.DataFrame]]:
        """Sort by release and iterate over concatenated, named frames."""
        all_frames = {}
        for release in sorted(self._releases.keys()):
            release_data = self._releases[release]
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


def _percent(*columns: str) -> pl.Expr:
    assert 2 <= len(columns)
    numerator, *denominator = columns
    return (
        pl.col(numerator).sum() / pl.sum_horizontal(*denominator).sum() * 100
    ).alias(f"{numerator}_pct")


def one_column_summary(frame: pl.DataFrame) -> pl.DataFrame:
    """
    Summarize the given data frame. This function expects a statistics data
    frame. It sums up numeric columns besides those containing maxima, which
    require the continuing maximization of values, and then transposes the one
    row into one column for easier readability.

    Beware: The data frame generated by this method contains empty divider rows,
    which have an empty variable field and a null value field.
    """
    frame = frame.with_columns(
        pl.lit(1).alias("fake")
    ).group_by("fake").agg(
        # ACHTUNG: This aggregation inserts spacer columns with null values and
        # distinct names consisting of Unicode spaces.
        pl.col("total_rows").sum(),
        pl.col("total_rows_with_keywords").sum(),
        pl.lit(None).alias("\u2800"),
        pl.col("batch_count").sum(),
        pl.lit(None).alias(" \u2800"),
        pl.col("rows").sum(),
        pl.col("keywords").sum(),
        pl.col("rows_with_keywords").sum(),
        pl.col("max_keywords_per_row").max(),
        pl.lit(None).alias("  \u2800"),
        pl.col("null_decision").sum(),
        pl.col("visibility_decision_only").sum(),
        pl.col("monetary_decision_only").sum(),
        pl.col("provision_decision_only").sum(),
        pl.col("account_decision_only").sum(),
        pl.col("visibility_monetary_decision").sum(),
        pl.col("visibility_provision_decision").sum(),
        pl.col("visibility_account_decision").sum(),
        pl.col("monetary_provision_decision").sum(),
        pl.col("monetary_account_decision").sum(),
        pl.col("provision_account_decision").sum(),
        pl.col("monetary_provision_account_decision").sum(),
        pl.col("visibility_provision_account_decision").sum(),
        pl.col("visibility_monetary_account_decision").sum(),
        pl.col("visibility_monetary_provision_decision").sum(),
        pl.col("all_kinds_decision").sum(),
        pl.lit(None).alias("   \u2800"),
        pl.col("max_visibility_per_row").max(),
        pl.col("visibility_values").sum(),
        pl.col("rows_with_visibility").sum(),
        pl.col("null_visibility_decision").sum(),
        pl.lit(None).alias("    \u2800"),
        pl.col("content_removed").sum(),
        pl.col("content_disabled").sum(),
        pl.col("content_demoted").sum(),
        pl.col("content_age_restricted").sum(),
        pl.col("content_interaction_restricted").sum(),
        pl.col("content_labeled").sum(),
        pl.col("other_visibility").sum(),
        pl.lit(None).alias("     \u2800"),
        pl.col("monetary_suspension").sum(),
        pl.col("monetary_termination").sum(),
        pl.col("monetary_other").sum(),
        pl.col("null_monetary_decision").sum(),
        pl.lit(None).alias("      \u2800"),
        pl.col("provision_partial_suspension").sum(),
        pl.col("provision_total_suspension").sum(),
        pl.col("provision_partial_termination").sum(),
        pl.col("provision_total_termination").sum(),
        pl.col("null_provision_decision").sum(),
        pl.lit(None).alias("       \u2800"),
        pl.col("account_suspended").sum(),
        pl.col("account_suspended_null_date").sum(),
        pl.col("account_suspended_until_date").sum(),
        _percent(
            "account_suspended_until_date",
            "account_suspended_until_date", "account_suspended_null_date"
        ),
        pl.col("account_terminated").sum(),
        pl.col("account_terminated_null_date").sum(),
        pl.col("account_terminated_until_date").sum(),
        pl.col("null_account_decision").sum(),
        pl.lit(None).alias("        \u2800"),
        pl.col("account_type_business").sum(),
        pl.col("account_type_private").sum(),
        pl.col("null_account_type").sum(),
        pl.lit(None).alias("         \u2800"),
        pl.col("illegal_content").sum(),
        pl.col("incompatible_content").sum(),
        pl.col("null_decision_ground").sum(),
        pl.col("incompatible_content_illegal_yes").sum(),
        pl.col("incompatible_content_illegal_no").sum(),
        pl.col("null_incompatible_content_illegal").sum(),
        pl.lit(None).alias("          \u2800"),
        pl.col("source_article_16").sum(),
        pl.col("source_trusted_flagger").sum(),
        pl.col("source_other_notification").sum(),
        pl.col("source_voluntary").sum(),
        pl.col("null_source_type").sum(),
        pl.lit(None).alias("           \u2800"),
        pl.col("automated_detection_yes").sum(),
        pl.col("automated_detection_no").sum(),
        pl.col("null_automated_detection").sum(),
        pl.col("automated_decision_fully").sum(),
        pl.col("automated_decision_partially").sum(),
        pl.col("automated_decision_not_automated").sum(),
        pl.col("null_automated_decision").sum(),
        pl.lit(None).alias("            \u2800"),
        pl.col("csam").sum(),
        _percent("csam", "rows"),
        pl.lit(None).alias("             \u2800"),
        pl.col("csam_null_decision").sum(),
        pl.col("csam_visibility_decision_only").sum(),
        pl.col("csam_monetary_decision_only").sum(),
        pl.col("csam_provision_decision_only").sum(),
        pl.col("csam_account_decision_only").sum(),
        pl.col("csam_visibility_monetary_decision").sum(),
        pl.col("csam_visibility_provision_decision").sum(),
        pl.col("csam_visibility_account_decision").sum(),
        pl.col("csam_monetary_provision_decision").sum(),
        pl.col("csam_monetary_account_decision").sum(),
        pl.col("csam_provision_account_decision").sum(),
        pl.col("csam_monetary_provision_account_decision").sum(),
        pl.col("csam_visibility_provision_account_decision").sum(),
        pl.col("csam_visibility_monetary_account_decision").sum(),
        pl.col("csam_visibility_monetary_provision_decision").sum(),
        pl.col("csam_all_kinds_decision").sum(),
        pl.lit(None).alias("              \u2800"),
        pl.col("csam_max_visibility_per_row").max(),
        pl.col("csam_visibility_values").sum(),
        pl.col("csam_rows_with_visibility").sum(),
        pl.col("csam_null_visibility_decision").sum(),
        pl.lit(None).alias("               \u2800"),
        pl.col("csam_content_removed").sum(),
        pl.col("csam_content_disabled").sum(),
        pl.col("csam_content_demoted").sum(),
        pl.col("csam_content_age_restricted").sum(),
        pl.col("csam_content_interaction_restricted").sum(),
        pl.col("csam_content_labeled").sum(),
        pl.col("csam_other_visibility").sum(),
        pl.lit(None).alias("                \u2800"),
        pl.col("csam_monetary_suspension").sum(),
        pl.col("csam_monetary_termination").sum(),
        pl.col("csam_monetary_other").sum(),
        pl.col("csam_null_monetary_decision").sum(),
        pl.lit(None).alias("                 \u2800"),
        pl.col("csam_provision_partial_suspension").sum(),
        pl.col("csam_provision_total_suspension").sum(),
        pl.col("csam_provision_partial_termination").sum(),
        pl.col("csam_provision_total_termination").sum(),
        pl.col("csam_null_provision_decision").sum(),
        pl.lit(None).alias("                  \u2800"),
        pl.col("csam_account_suspended").sum(),
        pl.col("csam_account_suspended_null_date").sum(),
        pl.col("csam_account_suspended_until_date").sum(),
        pl.col("csam_account_terminated").sum(),
        pl.col("csam_account_terminated_null_date").sum(),
        pl.col("csam_account_terminated_until_date").sum(),
        pl.col("csam_null_account_decision").sum(),
        pl.lit(None).alias("                   \u2800"),
        pl.col("csam_account_type_business").sum(),
        pl.col("csam_account_type_private").sum(),
        pl.col("csam_null_account_type").sum(),
        pl.lit(None).alias("                    \u2800"),
        pl.col("csam_illegal_content").sum(),
        pl.col("csam_incompatible_content").sum(),
        pl.col("csam_null_decision_ground").sum(),
        pl.col("csam_incompatible_content_illegal_yes").sum(),
        pl.col("csam_incompatible_content_illegal_no").sum(),
        pl.col("csam_null_incompatible_content_illegal").sum(),
        pl.lit(None).alias("                     \u2800"),
        pl.col("csam_source_article_16").sum(),
        pl.col("csam_source_trusted_flagger").sum(),
        pl.col("csam_source_other_notification").sum(),
        pl.col("csam_source_voluntary").sum(),
        pl.col("csam_null_source_type").sum(),
        pl.lit(None).alias("                      \u2800"),
        pl.col("csam_automated_detection_yes").sum(),
        pl.col("csam_automated_detection_no").sum(),
        pl.col("csam_null_automated_detection").sum(),
        pl.col("csam_automated_decision_fully").sum(),
        pl.col("csam_automated_decision_partially").sum(),
        pl.col("csam_automated_decision_not_automated").sum(),
        pl.col("csam_null_automated_decision").sum(),
    )

    return frame.drop(
        ["fake"]
    ).transpose(
        include_header=True,
        header_name="Variable",
        column_names=["Value"]
    ).with_columns(
        pl.when(pl.col("Variable").str.contains("_pct").not_())
        .then(pl.col("Value").cast(pl.UInt64))
        .otherwise(pl.col("Value"))
    ).with_columns(
        pl.when(pl.col("Variable").str.contains("\u2800"))
        .then(pl.lit(""))
        .otherwise(pl.col("Variable"))
        .alias("Variable"),
    )


def validate_statistics(frame: pl.DataFrame) -> None:
    all = frame.select(pl.col("rows").sum()).item()

    assert all == frame.select(
        pl.col("null_decision").sum()
        + pl.col("visibility_decision_only").sum()
        + pl.col("monetary_decision_only").sum()
        + pl.col("provision_decision_only").sum()
        + pl.col("account_decision_only").sum()
        + pl.col("visibility_monetary_decision").sum()
        + pl.col("visibility_provision_decision").sum()
        + pl.col("visibility_account_decision").sum()
        + pl.col("monetary_provision_decision").sum()
        + pl.col("monetary_account_decision").sum()
        + pl.col("provision_account_decision").sum()
        + pl.col("monetary_provision_account_decision").sum()
        + pl.col("visibility_provision_account_decision").sum()
        + pl.col("visibility_monetary_account_decision").sum()
        + pl.col("visibility_monetary_provision_decision").sum()
        + pl.col("all_kinds_decision").sum()
    ).row(0)[0]

    assert all == frame.select(
        pl.col("rows_with_visibility").sum()
        + pl.col("null_visibility_decision").sum()
    ).row(0)[0]

    visibility_values = frame.select(
        pl.col("visibility_values").sum()
    ).item()

    assert visibility_values == frame.select(
        pl.col("content_removed").sum()
        + pl.col("content_disabled").sum()
        + pl.col("content_demoted").sum()
        + pl.col("content_age_restricted").sum()
        + pl.col("content_interaction_restricted").sum()
        + pl.col("content_labeled").sum()
        + pl.col("other_visibility").sum()
    ).row(0)[0]

    assert all == frame.select(
        pl.col("monetary_suspension").sum()
        + pl.col("monetary_termination").sum()
        + pl.col("monetary_other").sum()
        + pl.col("null_monetary_decision").sum()
    ).row(0)[0]

    assert all == frame.select(
        pl.col("provision_partial_suspension").sum()
        + pl.col("provision_total_suspension").sum()
        + pl.col("provision_partial_termination").sum()
        + pl.col("provision_total_termination").sum()
        + pl.col("null_provision_decision").sum()
    ).row(0)[0]

    assert all == frame.select(
        pl.col("account_suspended").sum()
        + pl.col("account_terminated").sum()
        + pl.col("null_account_decision").sum()
    ).row(0)[0]

    account_suspended = frame.select(
        pl.col("account_suspended").sum()
    ).item()

    assert account_suspended == frame.select(
        pl.col("account_suspended_null_date").sum()
        + pl.col("account_suspended_until_date").sum()
    ).row(0)[0]

    account_terminated = frame.select(
        pl.col("account_terminated").sum()
    ).item()

    assert account_terminated == frame.select(
        pl.col("account_terminated_null_date").sum()
        + pl.col("account_terminated_until_date").sum()
    ).row(0)[0]

    assert all == frame.select(
        pl.col("account_type_business").sum()
        + pl.col("account_type_private").sum()
        + pl.col("null_account_type").sum()
    ).row(0)[0]

    assert all == frame.select(
        pl.col("illegal_content").sum()
        + pl.col("incompatible_content").sum()
        + pl.col("null_decision_ground").sum()
    ).row(0)[0]

    assert all == frame.select(
        pl.col("incompatible_content_illegal_yes").sum()
        + pl.col("incompatible_content_illegal_no").sum()
        + pl.col("null_incompatible_content_illegal").sum()
    ).row(0)[0]

    assert all == frame.select(
        pl.col("source_article_16").sum()
        + pl.col("source_trusted_flagger").sum()
        + pl.col("source_other_notification").sum()
        + pl.col("source_voluntary").sum()
        + pl.col("null_source_type").sum()
    ).row(0)[0]

    assert all == frame.select(
        pl.col("automated_detection_yes").sum()
        + pl.col("automated_detection_no").sum()
        + pl.col("null_automated_detection").sum()
    ).row(0)[0]

    assert all == frame.select(
        pl.col("automated_decision_fully").sum()
        + pl.col("automated_decision_partially").sum()
        + pl.col("automated_decision_not_automated").sum()
        + pl.col("null_automated_decision").sum()
    ).row(0)[0]

    # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~

    csam = frame.select(pl.col("csam").sum()).item()

    assert csam == frame.select(
        pl.col("csam_null_decision")
        + pl.col("csam_visibility_decision_only").sum()
        + pl.col("csam_monetary_decision_only").sum()
        + pl.col("csam_provision_decision_only").sum()
        + pl.col("csam_account_decision_only").sum()
        + pl.col("csam_visibility_monetary_decision").sum()
        + pl.col("csam_visibility_provision_decision").sum()
        + pl.col("csam_visibility_account_decision").sum()
        + pl.col("csam_monetary_provision_decision").sum()
        + pl.col("csam_monetary_account_decision").sum()
        + pl.col("csam_provision_account_decision").sum()
        + pl.col("csam_monetary_provision_account_decision").sum()
        + pl.col("csam_visibility_provision_account_decision").sum()
        + pl.col("csam_visibility_monetary_account_decision").sum()
        + pl.col("csam_visibility_monetary_provision_decision").sum()
        + pl.col("csam_all_kinds_decision").sum()
    ).row(0)[0]

    assert csam == frame.select(
        pl.col("csam_rows_with_visibility").sum()
        + pl.col("csam_null_visibility_decision").sum()
    ).row(0)[0]

    csam_visibility_values = frame.select(
        pl.col("csam_visibility_values").sum()
    ).item()

    assert csam_visibility_values == frame.select(
        pl.col("csam_content_removed").sum()
        + pl.col("csam_content_disabled").sum()
        + pl.col("csam_content_demoted").sum()
        + pl.col("csam_content_age_restricted").sum()
        + pl.col("csam_content_interaction_restricted").sum()
        + pl.col("csam_content_labeled").sum()
        + pl.col("csam_other_visibility").sum()
    ).row(0)[0]

    assert csam == frame.select(
        pl.col("csam_monetary_suspension").sum()
        + pl.col("csam_monetary_termination").sum()
        + pl.col("csam_monetary_other").sum()
        + pl.col("csam_null_monetary_decision").sum()
    ).row(0)[0]

    assert csam == frame.select(
        pl.col("csam_provision_partial_suspension").sum()
        + pl.col("csam_provision_total_suspension").sum()
        + pl.col("csam_provision_partial_termination").sum()
        + pl.col("csam_provision_total_termination").sum()
        + pl.col("csam_null_provision_decision").sum()
    ).row(0)[0]

    assert csam == frame.select(
        pl.col("csam_account_suspended").sum()
        + pl.col("csam_account_terminated").sum()
        + pl.col("csam_null_account_decision").sum()
    ).row(0)[0]

    csam_account_suspended = frame.select(
        pl.col("csam_account_suspended").sum()
    ).item()

    assert csam_account_suspended == frame.select(
        pl.col("csam_account_suspended_null_date").sum()
        + pl.col("csam_account_suspended_until_date").sum()
    ).row(0)[0]

    csam_account_terminated = frame.select(
        pl.col("csam_account_terminated").sum()
    ).item()

    assert csam_account_terminated == frame.select(
        pl.col("csam_account_terminated_null_date").sum()
        + pl.col("csam_account_terminated_until_date").sum()
    ).row(0)[0]

    assert csam == frame.select(
        pl.col("csam_account_type_business").sum()
        + pl.col("csam_account_type_private").sum()
        + pl.col("csam_null_account_type").sum()
    ).row(0)[0]

    assert csam == frame.select(
        pl.col("csam_illegal_content").sum()
        + pl.col("csam_incompatible_content").sum()
        + pl.col("csam_null_decision_ground").sum()
    ).row(0)[0]

    assert csam == frame.select(
        pl.col("csam_incompatible_content_illegal_yes").sum()
        + pl.col("csam_incompatible_content_illegal_no").sum()
        + pl.col("csam_null_incompatible_content_illegal").sum()
    ).row(0)[0]

    assert csam == frame.select(
        pl.col("csam_source_article_16").sum()
        + pl.col("csam_source_trusted_flagger").sum()
        + pl.col("csam_source_other_notification").sum()
        + pl.col("csam_source_voluntary").sum()
        + pl.col("csam_null_source_type").sum()
    ).row(0)[0]

    assert csam == frame.select(
        pl.col("csam_automated_detection_yes").sum()
        + pl.col("csam_automated_detection_no").sum()
        + pl.col("csam_null_automated_detection").sum()
    ).row(0)[0]

    assert csam == frame.select(
        pl.col("csam_automated_decision_fully").sum()
        + pl.col("csam_automated_decision_partially").sum()
        + pl.col("csam_automated_decision_not_automated").sum()
        + pl.col("csam_null_automated_decision").sum()
    ).row(0)[0]


def format_summary(frame: pl.DataFrame, as_markdown: bool = True) -> str:
    """
    Format the one column summary for fixed-width display.

    The non-Markdown version uses box drawing characters whereas the Markdown
    version emits the necessary ASCII characters for cell delimiters, while
    using U+2800, Braille empty pattern, in the variable and value columns for
    empty rows. That ensures that Markdown table formatting logic recognizes
    these cells as non-empty without actually displaying anything.
    """
    rows = []

    var_width = frame.select(pl.col("Variable").str.len_chars().max()).item()
    val_width = int(frame.select(pl.col("Value").log10().max()).item() + 1)
    val_width += val_width // 3

    for variable, value in frame.rows():
        if variable == "":
            variable = "\u2800" if as_markdown else " "
        variable = variable.ljust(var_width)
        if value is None:
            value = "\u2800" if as_markdown else " "
        elif "pct" in variable:
            value = f"{value:.3f}"
        else:
            value = f"{int(value):,}"
        value = value.rjust(val_width)
        rows.append([variable, value])

    if as_markdown:
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

    bar = "|" if as_markdown else "│"
    for variable, value in rows:
        lines.append(f"{bar} {variable} {bar} {value} {bar}")
    if not as_markdown:
        lines.append(f"└─{'─' * var_width}─┴─{'─' * val_width}─┘")

    return "\n".join(lines)
