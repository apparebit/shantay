from dataclasses import dataclass
import datetime as dt
from pathlib import Path
from typing import Any

from IPython.display import display, Markdown, HTML

import altair as alt
import polars as pl

from shantay.metadata import Metadata
from shantay.model import Daily, MonthlyRange, PLATFORMS_FILE
from shantay.schema import StatementCategory
from shantay.util import scale, to_markdown_table


TIMELINE_WIDTH = 1_000


# Observable's [April 2024 color
# palette](https://observablehq.com/blog/crafting-data-colors)

BLUE = "#4269d0"
ORANGE = "#efb118"
RED = "#ff725c"
CYAN = "#6cc5b0"
GREEN = "#3ca951"
PINK = "#ff8ab7"
PURPLE = "#a463f2"
LIGHT_BLUE = "#97bbf5"
BROWN = "#9c6b34e"
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

alt.theme.enable("default")

pl.Config.set_tbl_rows(100)
pl.Config.set_thousands_separator(",")
pl.Config.set_fmt_str_lengths(
    (max(len(s) for s in StatementCategory.categories) // 10 + 2) * 10
)

# --------------------------------------------------------------------------------------
# Schema Rendering


def schema(frame: pl.DataFrame, title: None | str = None) -> Markdown:
    """Render the schema for the data frame as a markdown table."""
    return Markdown(to_markdown_table(
        *([k, v] for k, v in frame.schema.items()),
        columns=["Column", "Type"],
        title=title,
    ))


# --------------------------------------------------------------------------------------
# Summary


@dataclass(frozen=True, slots=True)
class Summary:
    """A concise, one-dimensional summary of the metadata."""

    min_day: dt.date
    max_day: dt.date
    batch_rows: int
    batch_memory: int
    mean_minor_prot_pct: float
    total_rows: int
    mean_batch_keywords_pct: float
    mean_total_keywords_pct: float
    max_platforms: int
    max_platforms_keywords: int

    def markdown(self) -> Markdown:
        row_size, row_unit = scale(self.batch_rows)
        total_rows, total_unit = scale(self.total_rows)
        mem_size, mem_unit = scale(self.batch_memory)
        return Markdown(to_markdown_table(
            [
                "Dates",
                f"{self.min_day} to {self.max_day} (inclusive)"
            ],
            [
                "Platforms reporting Protection of Minors SoRs",
                f"{self.max_platforms_keywords} out of {self.max_platforms} "
                "may include keywords"
            ],
            [
                "Protection of Minors SoRs",
                f"{row_size:,.1f} {row_unit}rows out of {total_rows:,.1f{total_unit}}unit or "
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
            title="Summary"
        ))


def summarize(metadata: pl.DataFrame, platforms: pl.DataFrame) -> Summary:
    """Create the summary."""
    min_day, max_day, batch_rows, batch_memory, mean_minor_prot_pct, total_rows = (
        metadata.select(
            pl.col("release").min().alias("min"),
            pl.col("release").max().alias("max"),
            pl.col("batch_rows").sum(),
            pl.col("batch_memory").sum(),
            (pl.col("batch_rows") / pl.col("total_rows") * 100).mean().alias("mean_minor_prot_pct"),
            pl.col("total_rows").sum(),
        ).row(0)
    )

    mean_batch_keywords_pct, mean_total_keywords_pct = (
        metadata.select(
            (pl.col("batch_rows_with_keywords") / pl.col("batch_rows") * 100)
            .mean()
            .alias("mean_batch_keywords_pct"),
            (pl.col("total_rows_with_keywords") / pl.col("total_rows") * 100)
            .mean()
            .alias("mean_total_keywords_pct"),
        ).row(0)
    )

    max_platforms = platforms.select(pl.col("platform_name").unique().len()).item()
    max_platforms_keywords = platforms.filter(
        pl.col("has_keyword")
    ).select(
        pl.col("platform_name").unique().len()
    ).item()

    return Summary(
        min_day=min_day,
        max_day=max_day,
        batch_rows=batch_rows,
        batch_memory=batch_memory,
        mean_minor_prot_pct=mean_minor_prot_pct,
        total_rows=total_rows,
        mean_batch_keywords_pct=mean_batch_keywords_pct,
        mean_total_keywords_pct=mean_total_keywords_pct,
        max_platforms=max_platforms,
        max_platforms_keywords=max_platforms_keywords,
    )


# --------------------------------------------------------------------------------------
# Timelines


def daily_sor_counts_minor_prot(metadata: pl.DataFrame) -> Any:
    f1 = metadata.select(
        pl.col("release"),
        pl.col("batch_rows") / 1_000,
    )

    return (
        alt.Chart(
            f1,
            title="Statements of Reason: Protection of Minors — Daily Counts",
        ).mark_bar(
            tooltip=True,
            color=GREEN,
        ).encode(
            alt.X("release:T"),
            alt.Y("batch_rows:Q").title("thousand rows"),
        ).properties(
            width=TIMELINE_WIDTH,
        ).interactive()
    )

def daily_sor_percentage_minor_prot(metadata: pl.DataFrame) -> Any:
    f2 = metadata.select(
        pl.col("release"),
        (pl.col("batch_rows") / pl.col("total_rows") * 100).alias("protection_of_minors")
    )

    return (
        alt.Chart(
            f2,
            title="Statements of Reason: Protection of Minors — Daily Percentage",
        ).mark_bar(
            tooltip=True,
            color=GREEN,
        ).encode(
            alt.X("release:T"),
            alt.Y("protection_of_minors:Q").title("percent"),
        )
        .properties(width=TIMELINE_WIDTH)
        .interactive()
    )

def daily_keywords_percent_minor_prot(metadata:pl.DataFrame) -> Any:
    f3 = metadata.select(
        pl.col("release"),
        (pl.col("batch_rows_with_keywords") / pl.col("batch_rows") * 100)
        .alias("Protection of Minors Only"),
        (pl.col("total_rows_with_keywords") / pl.col("total_rows") * 100)
        .alias("All SoRs"),
    ).unpivot(
        index=["release"],
        on=["Protection of Minors Only", "All SoRs"],
        variable_name="kind",
        value_name="pct",
    )

    return (
        alt.Chart(
            f3,
            title="Statements of Reason: With Keywords — Daily Percentage",
        ).mark_line(
            tooltip=True,
        ).encode(
            alt.X("release:T"),
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


def monthly_platform_counts_minor_prot(platforms: pl.DataFrame) -> Any:
    f4 = platforms.group_by("year", "month", maintain_order=True).agg(
        pl.col("date").first(),
        pl.col("platform_name").filter(pl.col("has_keyword")).unique().len().alias("Platforms w/ Keywords"),
        pl.col("platform_name").unique().len().alias("All Platforms"),
    # ).select(
    #     pl.exclude("year", "month")
    ).unpivot(
        index=["date"],
        on=["Platforms w/ Keywords", "All Platforms"],
        variable_name="kind",
        value_name="count",
    )

    return (
        alt.Chart(f4, title="Protection of Minors SoRs: Platforms — Monthly Counts").mark_line(tooltip=True).encode(
            alt.X("date:T"),
            alt.Y("count:Q"),
            alt.Color("kind:N").scale(
                domain=["Platforms w/ Keywords", "All Platforms"],
                range=[RED, GRAY],
            ),
        )
        .properties(width=TIMELINE_WIDTH)
        .interactive()
    )

def monthly_keyword_counts_minor_prot(metadata: pl.DataFrame) -> Any:
    return


# --------------------------------------------------------------------------------------
# Schema Rendering


def render(root: Path) -> None:
    metadata = Metadata.read_json(root).to_frame()
    platforms = pl.read_parquet(root / PLATFORMS_FILE)
    summary = summarize(metadata, platforms)

    range = MonthlyRange(
        Daily.of(summary.min_day).to_full_first_month(),
        Daily.of(summary.max_day).to_full_last_month(),
    )

    metadata = metadata.filter(
        ((range.first.year < pl.col("release").dt.year())
         | ((range.first.year == pl.col("release").dt.year())
          & (range.first.month <= pl.col("release").dt.month())))
        & ((pl.col("release").dt.year() < range.last.year)
         | ((pl.col("release").dt.year() == range.last.year)
          & (pl.col("release").dt.month() <= range.last.month)))
    )

    platforms = platforms.filter(
        ((range.first.year < pl.col("year"))
         | ((range.first.year == pl.col("year"))
          & (range.first.month <= pl.col("month"))))
        & ((pl.col("year") < range.last.year)
         | ((pl.col("year") == range.last.year)
          & (pl.col("month") <= range.last.month)))
    )

    display(HTML("<h1>The DSA Transparency Database</h1>"))
    display(HTML("<p>With a Focus on the Protection of Minors</p>"))

    # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
    display(HTML("<h2>Summary</h2>"))
    display(summary.markdown())

    # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
    display(HTML("<h2>Table Schemas</h2>"))
    display(schema(metadata, title="meta.json"))
    display(schema(platforms, title="meta-platforms.parquet"))

    # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
    display(HTML("<h2>Timelines</h2>"))

    timelines = []
    timelines.append(daily_sor_counts_minor_prot(metadata))
    timelines.append(daily_sor_percentage_minor_prot(metadata))
    timelines.append(daily_keywords_percent_minor_prot(metadata))
    timelines.append(monthly_platform_counts_minor_prot(platforms))

    graph = alt.vconcat(*timelines).resolve_scale(
        x="shared",
        color="independent",
    ).configure_range(
        category={"scheme": PALETTE},
    )

    display(graph)
