from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from typing import Self

from IPython.display import display, Markdown, HTML

import altair as alt
import polars as pl

from .framing import collect_release_metadata, is_row_within_period
from .metadata import Metadata
from .model import KEYWORDS_FILE, PLATFORMS_FILE, ReleaseRange, STATISTICS_FILE
from .schema import KEYWORDS_MINOR_PROTECTION_PLUS, SCHEMA, StatementCategory
from .util import scale, to_markdown_table


TIMELINE_WIDTH = 1_000


# --------------------------------------------------------------------------------------
# Observable's [color palette](https://observablehq.com/blog/crafting-data-colors)


BLUE = "#4269d0"
ORANGE = "#efb118"
RED = "#ff725c"
CYAN = "#6cc5b0"
GREEN = "#3ca951"
PINK = "#ff8ab7"
PURPLE = "#a463f2"
LIGHT_BLUE = "#97bbf5"
BROWN = "#9c6b4e"
GRAY = "#9498a0"

PALETTE = [
    BLUE,
    ORANGE,
    RED,
    CYAN,
    GREEN,
    PINK,
    PURPLE,
    LIGHT_BLUE,
    BROWN,
    GRAY,
]

KEYWORD_PALETTE = [
    LIGHT_BLUE, BLUE, PURPLE, RED, ORANGE, GREEN, PINK, CYAN, BROWN, GRAY
]


# --------------------------------------------------------------------------------------
# Visualization


def visualize(working_root: Path, staging_root: Path) -> None:
    Visualization(working_root, staging_root).run()


class Visualization:

    def __init__(
        self, working_root: Path, staging_root: Path, with_extras: bool = False
    ) -> None:
        self._working_root = working_root
        self._staging_root = staging_root
        self._with_extras = with_extras

    @staticmethod
    def configure_display() -> None:
        alt.theme.enable("default")

        pl.Config.set_tbl_rows(100)
        pl.Config.set_float_precision(3)
        pl.Config.set_thousands_separator(",")
        pl.Config.set_fmt_str_lengths(
            (max(len(s) for s in StatementCategory.categories) // 10 + 2) * 10
        )

    def run(self) -> None:
        self.configure_display()
        self.ingest()
        self.render_heading()
        self.render_overview()
        self.render_timelines()

    def ingest(self) -> None:
        range, metadata = collect_release_metadata(
            Metadata.read_json(self._working_root).records
        )
        statistics = pl.read_parquet(self._working_root / STATISTICS_FILE)
        keywords = pl.read_parquet(self._working_root / KEYWORDS_FILE)
        platforms = pl.read_parquet(self._working_root / PLATFORMS_FILE)

        # Restrict rendered data to *full* months. That essentially drops the
        # first week of data from the DSA SoR DB.
        range = range.to_release_range()
        self._range = ReleaseRange(
            range.first.to_full_first_month(),
            range.last.to_full_last_month()
        )

        within_range = is_row_within_period(range)
        self._metadata = metadata.filter(within_range)
        self._statistics = statistics.filter(within_range)
        self._keywords = keywords.filter(within_range)
        self._platforms = platforms.filter(within_range)
        self._summary = Summary.of(self._metadata, self._platforms)

        # Determine global keyword usage and keywords with at least 1% use.
        self._keyword_usage = (
            self._keywords.group_by("keyword")
            .agg(pl.col("count").sum())
            .with_columns(
                (pl.col("count") / pl.col("count").sum() * 100).alias("pct")
            )
            .sort(pl.col("count"), descending=True)
        )

        frequent_keywords = (
            self._keyword_usage
            .filter(1 <= pl.col("pct"))
            .get_column("keyword")
        )

        self._short_keywords = {
            k: KEYWORDS_MINOR_PROTECTION_PLUS[k] for k in frequent_keywords
        }

    def render_heading(self) -> None:
        display(HTML("<h1>The DSA Transparency Database: Protection of Minors</h1>"))

    def render_overview(self) -> None:
        display(HTML("<h2>Summary</h2>"))
        display(self._summary.markdown())

        display(HTML("<h2>Table Schemas</h2>"))
        display(format_schema(SCHEMA, title="Source Data"))
        display(format_schema(self._metadata, title="meta.json"))
        display(format_schema(self._statistics, title="meta-statistics.parquet"))
        display(format_schema(self._keywords, title="meta-keywords.parquet"))
        display(format_schema(self._platforms, title="meta-platforms.parquet"))

        display(HTML("<h2>Keywords</h2>"))
        display(self._keyword_usage)
        pie = self.total_keyword_usage_minor_prot()
        display(pie)
        pie.save(self._staging_root / "keyword_pie.svg")

        display(HTML("<h2>Platforms</h2>"))
        table = self._platforms.select(
            pl.col("platform").unique().sort(descending=False)
        )
        display(table.with_row_index())

    def render_timelines(self) -> None:
        display(HTML("<h2>Timelines</h2>"))

        timelines = []
        timelines.append(self.daily_sor_counts_minor_prot())
        timelines.append(self.daily_sor_percentage_minor_prot())
        timelines.append(self.daily_keywords_percent_minor_prot())
        timelines.append(self.monthly_platform_counts_minor_prot())
        timelines.append(self.monthly_keyword_usage_minor_prot())
        timelines.append(self.monthly_decision_grounds_for_csam())
        timelines.append(self.monthly_account_decisions_for_csam())
        timelines.append(self.monthly_visibility_changes_for_csam())

        graph = alt.vconcat(*timelines).resolve_scale(
            x="shared",
            color="independent",
        ).configure_range(
            category={"scheme": PALETTE},
        )

        display(graph)
        graph.save(self._staging_root / "timelines.svg")

    def daily_sor_counts_minor_prot(self) -> alt.Chart:
        table = self._metadata.select(
            pl.col("start_date"),
            pl.col("batch_rows") / 1_000,
        )

        return (
            alt.Chart(
                table,
                title="Statements of Reasons: Protection of Minors — Daily Counts",
            ).mark_bar(
                tooltip=True,
                color=GREEN,
            ).encode(
                alt.X("start_date:T"),
                alt.Y("batch_rows:Q").title("thousand rows"),
            ).properties(
                width=TIMELINE_WIDTH,
            ).interactive()
        )

    def daily_sor_percentage_minor_prot(self) -> alt.Chart:
        table = self._metadata.select(
            pl.col("start_date"),
            (
                pl.col("batch_rows") / pl.col("total_rows") * 100
            )
            .alias("protection_of_minors")
        )

        return (
            alt.Chart(
                table,
                title="Statements of Reasons: Protection of Minors — Daily Percentage",
            ).mark_bar(
                tooltip=True,
                color=GREEN,
            ).encode(
                alt.X("start_date:T"),
                alt.Y("protection_of_minors:Q").title("percent"),
            )
            .properties(width=TIMELINE_WIDTH)
            .interactive()
        )

    def daily_keywords_percent_minor_prot(self) -> alt.Chart:
        table = self._metadata.select(
            pl.col("start_date"),
            (pl.col("batch_rows_with_keywords") / pl.col("batch_rows") * 100)
            .alias("Protection of Minors Only"),
            (pl.col("total_rows_with_keywords") / pl.col("total_rows") * 100)
            .alias("All SoRs"),
        ).unpivot(
            index=["start_date"],
            on=["Protection of Minors Only", "All SoRs"],
            variable_name="kind",
            value_name="pct",
        )

        return (
            alt.Chart(
                table,
                title="Statements of Reasons With Keywords — Daily Percentage",
            ).mark_line(
                tooltip=True,
            ).encode(
                alt.X("start_date:T"),
                alt.Y("pct:Q").title("percent"),
                alt.Color("kind:N").scale(
                    domain=["Protection of Minors Only", "All SoRs"],
                    range=[PINK, BLUE],
                ),
                #order=alt.Order("kind", sort="ascending")
            ).properties(
                width=TIMELINE_WIDTH,
            )
            .interactive()
        )

    def monthly_platform_counts_minor_prot(self) -> alt.Chart:
        table = self._platforms.with_columns(
            (pl.col("start_date") + dt.timedelta(days=15)).alias("mid_date"),
        ).group_by(
            pl.col("mid_date").dt.year().alias("year"),
            pl.col("mid_date").dt.month().alias("month"),
        ).agg(
            pl.col("mid_date").first(),
            (
                pl.col("platform")
                .filter(pl.col("has_keyword"))
                .unique().len()
                .alias("Platforms w/ Keywords")
            ),
            pl.col("platform").unique().len().alias("All Platforms"),
        ).unpivot(
            index=["mid_date"],
            on=["Platforms w/ Keywords", "All Platforms"],
            variable_name="kind",
            value_name="count",
        )
        return (
            alt.Chart(
                table, title="Platforms Submitting Protection of Minors SoRs — Monthly Counts"
            ).mark_line(
                tooltip=True
            ).encode(
                alt.X("mid_date:T"),
                alt.Y("count:Q"),
                alt.Color("kind:N").scale(
                    domain=["Platforms w/ Keywords", "All Platforms"],
                    range=[RED, GRAY],
                ),
            )
            .properties(width=TIMELINE_WIDTH)
            .interactive()
        )

    def monthly_keyword_usage_minor_prot(self) -> alt.Chart | alt.LayerChart:
        table = (
            self._keywords.filter(
                pl.col("keyword").is_in(self._short_keywords)
            ).group_by(
                pl.col("start_date").dt.year().alias("year"),
                pl.col("start_date").dt.month().alias("month"),
                pl.col("keyword")
            ).agg(
                pl.col("start_date").min() + dt.timedelta(days=5),
                pl.col("end_date").max() - dt.timedelta(days=5),
                pl.col("count").sum(),
            ).with_columns(
                pl.col("keyword").cast(pl.String).replace(self._short_keywords)
            )
        )

        chart = (
            alt.Chart(
                table, title="Keywords in Protection of Minors SoRs — Monthly Counts"
            ).mark_bar(
                tooltip=True,
                #size=45,
                #width=alt.RelativeBandSize(0.9),
                #width={"band": 200},
            ).encode(
                alt.X("start_date:T"),
                alt.X2("end_date:T"),
                alt.Y("sum(count):Q"),
                alt.Color("keyword:N").scale(
                    domain=[*self._short_keywords.values()],
                    range=KEYWORD_PALETTE[:len(self._short_keywords)],
                ),
            )
            .properties(width=TIMELINE_WIDTH)
            .interactive()
        )

        if self._with_extras:
            validation = self._metadata.group_by(
                pl.col("start_date").dt.year().alias("year"),
                pl.col("start_date").dt.month().alias("month"),
            ).agg(
                pl.col("start_date").min(),
                pl.col("end_date").max(),
                pl.col("batch_rows_with_keywords").sum().alias("keyed"),
            )

            chart = chart + alt.Chart(validation).mark_line(
                shape="stroke",
                strokeWidth=2.5,
                color=GRAY,
            ).encode(
                alt.X("start_date:T"),
                alt.X2("end_date:T"),
                alt.Y("keyed:Q"),
            ).properties(width=TIMELINE_WIDTH)

        return chart

    def monthly_decision_grounds_for_csam(self) -> alt.Chart | alt.LayerChart:
        table = self._statistics.group_by(
            pl.col("start_date").dt.year().alias("year"),
            pl.col("start_date").dt.month().alias("month"),
        ).agg(
            pl.col("start_date").min() + dt.timedelta(days=5),
            pl.col("end_date").max() - dt.timedelta(days=5),
            pl.col("csam_illegal_content").sum(),
            pl.col("csam_incompatible_content").sum(),
            pl.col("csam_no_decision_ground").sum(),
        ).rename({
            "csam_illegal_content": "Illegal Content",
            "csam_incompatible_content": "Incompatible Content",
            "csam_no_decision_ground": "⸺none⸺",
        }).unpivot(
            index=["start_date", "end_date"],
            on=["Illegal Content", "Incompatible Content", "⸺none⸺"],
            variable_name="Decision Ground",
            value_name="count",
        )

        no_decision = table.filter(
            pl.col("Decision Ground").eq("⸺none⸺")
        ).select(
            pl.col("count").sum()
        ).item()
        assert no_decision == 0, "decision_ground is required"

        chart = (
            alt.Chart(
                table, title="Decision Grounds for CSAM - Monthly Counts"
            ).mark_bar(
                tooltip=True,
            ).encode(
                alt.X("start_date:T"),
                alt.X2("end_date:T"),
                alt.Y("sum(count):Q"),
                alt.Color("Decision Ground:N").scale(
                    domain=["Illegal Content", "Incompatible Content"],
                    range=[RED, LIGHT_BLUE],
                ),
            ).properties(width=TIMELINE_WIDTH).interactive()
        )

        if self._with_extras:
            validation = self._statistics.group_by(
                pl.col("start_date").dt.year().alias("year"),
                pl.col("start_date").dt.month().alias("month"),
            ).agg(
                pl.col("start_date").min(),
                pl.col("end_date").max(),
                pl.col("csam").sum(),
            )

            chart = chart + alt.Chart(validation).mark_line(
                shape="stroke",
                strokeWidth=2.5,
                color=GRAY,
            ).encode(
                alt.X("start_date:T"),
                alt.X2("end_date:T"),
                alt.Y("csam:Q"),
            ).properties(width=TIMELINE_WIDTH)

        return chart

    def monthly_account_decisions_for_csam(self) -> alt.Chart | alt.LayerChart:
        table = self._statistics.group_by(
            pl.col("start_date").dt.year().alias("year"),
            pl.col("start_date").dt.month().alias("month"),
        ).agg(
            pl.col("start_date").min() + dt.timedelta(days=5),
            pl.col("end_date").max() - dt.timedelta(days=5),
            pl.col("csam_account_suspended").sum(),
            pl.col("csam_account_terminated").sum(),
            pl.col("csam_no_decision_account").sum(),
        ).rename({
            "csam_account_suspended": "Suspended",
            "csam_account_terminated": "Terminated",
            "csam_no_decision_account": "⸺none⸺",
        }).unpivot(
            index=["start_date", "end_date"],
            on=["Suspended", "Terminated", "⸺none⸺"],
            variable_name="Account Decision",
            value_name="count",
        )

        return (
            alt.Chart(
                table, title="Account Decisions for CSAM - Monthly Counts"
            ).mark_bar(
                tooltip=True,
            ).encode(
                alt.X("start_date:T"),
                alt.X2("end_date:T"),
                alt.Y("sum(count):Q"),
                alt.Color("Account Decision:N").scale(
                    domain=["Suspended", "Terminated", "⸺none⸺"],
                    range=[RED, ORANGE, GRAY],
                ),
            ).properties(width=TIMELINE_WIDTH).interactive()
        )

    def monthly_visibility_changes_for_csam(self) -> alt.Chart | alt.LayerChart:
        table = self._statistics.group_by(
            pl.col("start_date").dt.year().alias("year"),
            pl.col("start_date").dt.month().alias("month"),
        ).agg(
            pl.col("start_date").min() + dt.timedelta(days=5),
            pl.col("end_date").max() - dt.timedelta(days=5),
            pl.col("csam_removed").sum(),
            pl.col("csam_disabled").sum(),
            pl.col("csam_demoted").sum(),
            pl.col("csam_age_restricted").sum(),
            pl.col("csam_interaction_restricted").sum(),
            pl.col("csam_labeled").sum(),
            pl.col("csam_other_visibility").sum(),
        ).rename({
            "csam_removed": "Removed",
            "csam_disabled": "Disabled",
            "csam_demoted": "Demoted",
            "csam_age_restricted": "Age Restricted",
            "csam_interaction_restricted": "Interaction Restricted",
            "csam_labeled": "Labeled",
            "csam_other_visibility": "⸺other⸺",
        }).unpivot(
            index=["start_date", "end_date"],
            on=["Removed", "Disabled", "⸺other⸺"],
            variable_name="Visibility Decision",
            value_name="count",
        )

        return (
            alt.Chart(
                table, title="Visibility Decisions for CSAM - Monthly Counts"
            ).mark_bar(
                tooltip=True,
            ).encode(
                alt.X("start_date:T"),
                alt.X2("end_date:T"),
                alt.Y("sum(count):Q"),
                alt.Color("Visibility Decision:N").scale(
                    domain=["Removed", "Disabled", "⸺other⸺"],
                    range=[RED, ORANGE, GRAY],
                ),
            ).properties(width=TIMELINE_WIDTH).interactive()
        )

    def total_keyword_usage_minor_prot(self) -> alt.Chart:
        table = self._keyword_usage.filter(
            pl.col("keyword").is_in(self._short_keywords)
        ).with_columns(
            pl.col("keyword").cast(pl.String).replace(self._short_keywords)
        )

        return (
            alt.Chart(
                table, title="Keywords in Protection of Minors SoRs"
            ).mark_arc(
                tooltip=True,
            ).encode(
                alt.Theta("count:Q"),
                alt.Color("keyword:N").scale(
                    domain=[*self._short_keywords.values()],
                    range=KEYWORD_PALETTE[:len(self._short_keywords)],
                ),
            ).interactive()
        )

# --------------------------------------------------------------------------------------
# Data Summary


@dataclass(frozen=True, slots=True)
class Summary:
    """A concise, one-dimensional summary of the metadata."""

    start_date: dt.date
    end_date: dt.date
    batch_rows: int
    batch_memory: int
    mean_minor_prot_pct: float
    total_rows: int
    mean_batch_keywords_pct: float
    mean_total_keywords_pct: float
    max_platforms: int
    max_platforms_keywords: int

    @classmethod
    def of(cls, metadata: pl.DataFrame, platforms: pl.DataFrame) -> Self:
        """Create the summary."""
        start_date, end_date, batch_rows, batch_memory, mean_minor_prot_pct = (
            metadata.select(
                pl.col("start_date").min(),
                pl.col("end_date").max(),
                pl.col("batch_rows").sum(),
                pl.col("batch_memory").sum(),
                (
                    pl.col("batch_rows") / pl.col("total_rows") * 100
                )
                .mean()
                .alias("mean_minor_prot_pct"),
            ).row(0)
        )

        mean_batch_keywords_pct, mean_total_keywords_pct, total_rows = (
            metadata.select(
                (pl.col("batch_rows_with_keywords") / pl.col("batch_rows") * 100)
                .mean()
                .alias("mean_batch_keywords_pct"),
                (pl.col("total_rows_with_keywords") / pl.col("total_rows") * 100)
                .mean()
                .alias("mean_total_keywords_pct"),
                pl.col("total_rows").sum(),
            ).row(0)
        )

        max_platforms = platforms.select(pl.col("platform").unique().len()).item()
        max_platforms_keywords = platforms.filter(
            pl.col("has_keyword")
        ).select(
            pl.col("platform").unique().len()
        ).item()

        return cls(
            start_date=start_date,
            end_date=end_date,
            batch_rows=batch_rows,
            batch_memory=batch_memory,
            mean_minor_prot_pct=mean_minor_prot_pct,
            total_rows=total_rows,
            mean_batch_keywords_pct=mean_batch_keywords_pct,
            mean_total_keywords_pct=mean_total_keywords_pct,
            max_platforms=max_platforms,
            max_platforms_keywords=max_platforms_keywords,
        )

    def markdown(self) -> Markdown:
        row_size, row_unit = scale(self.batch_rows)
        total_rows, total_unit = scale(self.total_rows)
        mem_size, mem_unit = scale(self.batch_memory)
        return Markdown(to_markdown_table(
            [
                "Dates",
                f"{self.start_date} to {self.end_date} (inclusive)"
            ],
            [
                "Platforms reporting Protection of Minors SoRs",
                f"{self.max_platforms_keywords} out of {self.max_platforms} "
                "may include keywords"
            ],
            [
                "Protection of Minors SoRs",
                f"{row_size:,.1f} {row_unit}rows "
                f"out of {total_rows:,.1f} {total_unit}rows or "
                f"{self.batch_rows / self.total_rows * 100:.1f}%"
            ],
            [
                "Protection of Minors SoRs with keywords",
                f"{self.mean_batch_keywords_pct:.1f}% of category vs "
                f"{self.mean_total_keywords_pct:.1f}% of all SoRs"
            ],
            [
                "Size of in-memory frames",
                f"{mem_size:,.1f} {mem_unit}byte"
            ],
            columns=["Attribute", "Value"],
        ))


# --------------------------------------------------------------------------------------
# Schema Rendering


def format_schema(object: pl.DataFrame | pl.Schema, title: None | str = None) -> Markdown:
    """Render the schema for the data frame as a markdown table."""
    schema = object.schema if isinstance(object, pl.DataFrame) else object
    return Markdown(to_markdown_table(
        *([k, v] for k, v in schema.items()),
        columns=["Column", "Type"],
        title=title,
    ))
