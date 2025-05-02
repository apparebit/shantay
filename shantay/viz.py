from abc import ABCMeta, abstractmethod
import datetime as dt
from pathlib import Path
import re
from typing import Any, Literal

import altair as alt
import mistune
import polars as pl

from .color import (
    BLUE, GRAY, GREEN, KEYWORD_PALETTE, ORANGE, PINK, PURPLE, RED
)
from .framing import (
    aggregates, collect_release_metadata, get_frequency, is_row_within_period, NOT_NULL,
    predicate
)
from .metadata import Metadata
from .model import ConfigError, Coverage, StatSource, Storage
from .schema import (
    AutomatedDecision, AutomatedDetection,
    ContentType, DecisionAccount, DecisionGroundAndLegality, DecisionMonetary,
    DecisionProvision, DecisionType, DecisionVisibility,
    KeywordsMinorProtection, MetricDeclaration, ProcessingDelay, SCHEMA,
    StatementCount,
)
from .stats import Statistics
from .util import to_markdown_table


TIMELINE_WIDTH = 600
TIMELINE_HEIGHT = 400

HTML_HEADLINE = re.compile(r"<h([1-3])>([^<]*)</h[1-3]>")

FRAME_BORDER = re.compile(r' border="1"')
FRAME_CLASS = re.compile(r' class="dataframe"')
FRAME_QUOT = re.compile(r"&quot;")
FRAME_SHAPE = re.compile(r"<small>shape:[^<]*</small>")
FRAME_STYLE = re.compile(r"<style>[^<]*</style>")

SVG_ATTRIBUTES = re.compile(r' class="marks" width="[0-9]+" height="[0-9]+"')

DOC_HEADER = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>The DSA Transparency Database</title>
<meta property="og:article:published_time" content="{0}">
"""

DOC_HEADER_TOO = """\
<style>
/* ----------------------------------- General ----------------------------------- */
*::before, *, *::after {
    box-sizing: inherit;
}
:root {
    box-sizing: border-box;
    font-family: -apple-system, BlinkMacSystemFont, avenir next, avenir, segoe ui,
        helvetica neue, Cantarell, Ubuntu, roboto, noto, helvetica, arial, sans-serif;
    line-height: 1.5;
    --black: #1d1d20;
    --white: #f5f5f8;
}
body {
    margin: 3em;
}
p, table, svg {
    margin-bottom: 3em;
}
svg {
    width: 100%;
}

/* ----------------------------------- Table ----------------------------------- */
table {
    border-collapse: separate;
    border-spacing: 0;
    line-height: 1.2;
}
table caption {
    font-size: 0.8em;
    text-align: left;
    font-style: italic;
    padding: 0.45em 0;
}
table caption > :where(cite, dfn, em, i) {
    font-style: normal;
}
tr > th:first-child, tr > td:first-child {
    text-align: left;
}
th {
    font-weight: normal;
}
thead th {
    font-weight: bold;
}
th, td {
    padding: 0.25em 0.5em;
}
thead > tr:first-of-type {
    background: #e0e0e0;
}
thead > tr {
    background: #f0f0f0;
}
thead > tr:last-of-type > :where(th, td) {
    padding-bottom: 0.35em;
}
tbody > tr:first-of-type > :where(th, td) {
    border-top: solid 0.15em var(--black);
    padding-top: 0.35em;
}
tbody > tr:nth-child(even) {
    background: #f0f0f0
}
td {
    font-variant-numeric: tabular-nums;
    text-align: right;
}
th {
    text-align: right;
}
.alltext th, .alltext td {
    text-alight: left;
}
</style>
</head>
<body>
<main>
"""

DOC_FOOTER = """\
</main>
</body>
</html>
"""


# --------------------------------------------------------------------------------------


def visualize(
    storage: Storage,
    coverage: Coverage,
    notebook: bool = False,
    stat_source: StatSource = None,
) -> None:
    charts = storage.staging_root / "charts"
    charts.mkdir(exist_ok=True)

    renderer = NotebookRenderer(charts) if notebook else PlainTextRenderer(charts)
    visualizer = Visualizer(storage, coverage, renderer, stat_source)
    visualizer.run()


# --------------------------------------------------------------------------------------

type Chart = alt.Chart | alt.LayerChart | alt.VConcatChart

class Renderer(metaclass=ABCMeta):

    def __init__(self, charts: Path) -> None:
        self._charts = charts

    @property
    def charts(self) -> Path:
        return self._charts

    @property
    @abstractmethod
    def plain(self) -> bool: ...

    @abstractmethod
    def html(self, markup: str) -> None: ...

    @abstractmethod
    def md(self, markdown: str) -> None: ...

    @abstractmethod
    def frame(self, frame: pl.DataFrame) -> None: ...

    @abstractmethod
    def chart(self, name: str, chart: Chart) -> None: ...


TAG = re.compile(r"<[^>]+>")

class PlainTextRenderer(Renderer):

    @property
    def plain(self) -> bool:
        return True

    def html(self, markup: str) -> None:
        print(TAG.sub("", markup))
        print()

    def md(self, markdown: str) -> None:
        print(markdown)
        print()

    def frame(self, frame: pl.DataFrame) -> None:
        print(frame)
        print()

    def chart(self, name: str, chart: Chart) -> None:
        chart.save(self._charts / f"{name}.svg")


try:
    from IPython.display import display, HTML, Markdown
except ImportError:
    display = HTML = Markdown = None

if display is None:
    NotebookRenderer = None # pyright: ignore[reportAssignmentType]
else:
    class NotebookRenderer(Renderer):

        @property
        def plain(self) -> bool:
            return False

        def html(self, markup: str) -> None:
            display(HTML(markup)) # pyright: ignore[reportOptionalCall]

        def md(self, markdown: str) -> None:
            display(Markdown(markdown)) # pyright: ignore[reportOptionalCall]

        def frame(self, frame: pl.DataFrame) -> None:
            display(frame) # pyright: ignore[reportOptionalCall]

        def chart(self, name: str, chart: Chart) -> None:
            display(chart) # pyright: ignore[reportOptionalCall]
            chart.save(self._charts / f"{name}.svg")


# --------------------------------------------------------------------------------------


class Visualizer:

    def __init__(
        self,
        storage: Storage,
        coverage: Coverage,
        renderer: Renderer,
        stat_source: StatSource,
        with_extras: bool = False,
    ) -> None:
        self._storage = storage
        self._coverage = coverage
        self._with_extras = with_extras
        self._renderer = renderer
        self._timelines = False
        self._timestamp = dt.datetime.now()
        self._stat_source = stat_source or "working"

    def has_all_sors(self) -> bool:
        return self._stat_source == "archive"

    def is_monthly(self) -> bool:
        return self._frequency == "monthly"

    @property
    def persistent_root(self) -> Path:
        return (
            self._storage.archive_root if self._stat_source == "archive"
            else self._storage.working_root
        )

    @staticmethod
    def configure_display() -> None:
        alt.theme.enable("default")

        from .tool import configure_printing
        configure_printing()

    def html(self, markup: str) -> None:
        if self._renderer.plain and (hn := HTML_HEADLINE.fullmatch(markup)) is not None:
            self._renderer.md(f"{'#' * int(hn.group(1))} {hn.group(2)}")
        else:
            self._renderer.html(markup)

        assert self._document is not None
        self._document.write(markup)
        self._document.write("\n\n")

    def markdown(
        self,
        markdown: str,
        render: bool = True,
        disclosure: bool = False,
    ) -> None:
        if render:
            self._renderer.md(markdown)

        assert self._document is not None
        html = str(mistune.html(markdown))
        hn = HTML_HEADLINE.match(html)
        if not disclosure or hn is None:
            self._document.write(html)
            self._document.write("\n\n")
            return

        summary = hn.group(2)
        html = html[len(hn.group(0)):]
        self._document.write("<details>\n")
        self._document.write(f"<summary>{summary}</summary>\n")
        self._document.write(html)
        self._document.write("</details>\n\n")

    def frame(self, frame: pl.DataFrame, all_text: bool = False) -> None:
        self._renderer.frame(frame)

        assert self._document is not None
        html = frame._repr_html_()
        html = FRAME_BORDER.sub("", html)
        html = FRAME_CLASS.sub(' class="alltext"' if all_text else "", html)
        html = FRAME_QUOT.sub("", html)
        html = FRAME_SHAPE.sub("", html)
        html = FRAME_STYLE.sub("", html)

        self._document.write(html)
        self._document.write("\n\n")

    def chart(self, name: str, chart: Chart) -> None:
        self._renderer.chart(name, chart)

        path = self._renderer.charts / f"{name}.svg"
        with open(path, mode="r", encoding="utf8") as file:
            svg = file.read()

        if name != "keyword-pie":
            svg = SVG_ATTRIBUTES.sub("", svg)

        assert self._document is not None
        self._document.write(svg)
        self._document.write("\n\n")

    def run(self) -> None:
        path = self._storage.staging_root / "overview.html"
        self.configure_display()
        self.ingest()

        with open(path, mode="w", encoding="utf8") as document:
            try:
                self._document = document
                document.write(DOC_HEADER.format(self._timestamp.isoformat()))
                document.write(DOC_HEADER_TOO)

                self.render_heading()
                self.render_overview()
                self.render_charts()

                document.write(DOC_FOOTER)
            finally:
                self._document = None

    def ingest(self) -> None:
        _, metadata = collect_release_metadata(
            Metadata.read_json(self.persistent_root).records
        )
        statistics = Statistics.read(self.persistent_root)
        self._frequency = get_frequency(statistics.frame())
        self._tags = statistics.frame().select(
            pl.col("tag").drop_nulls().unique()
        ).get_column(
            "tag"
        ).to_list()
        date_range = statistics.range().intersection(
            self._coverage.to_date_range(), empty_ok=False
        ).monthlies().date_range() # Restrict to full months

        within_range = is_row_within_period(date_range)
        self._metadata = metadata.filter(within_range)
        self._statistics = Statistics(statistics.frame().filter(within_range))
        if self._statistics.frame().height == 0:
            raise ConfigError("cannot visualize less than a full month of data")

        # Determine global keyword usage and keywords with at least 1% use.
        self._keyword_usage = self._statistics.frame().filter(
            predicate("category_specification", entity=None)
        ).group_by(
            "variant"
        ).agg(
            pl.col("count").sum()
        ).rename({
            "variant": "keyword"
        }).with_columns(
            pl.when(
                pl.col("keyword").is_null()
            ).then(
                pl.col("count")
                / pl.col("count").sum()
                * 100
            ).otherwise(
                pl.col("count")
                / pl.col("count").filter(pl.col("keyword").is_not_null()).sum()
                * 100
            ).alias("pct")
        ).sort(
            pl.col("count"), descending=True
        )

        frequent_keywords = (
            self._keyword_usage
            .filter(0.1 <= pl.col("pct"))
            .get_column("keyword")
        )

        if "CSAM" in self._tags:
            self._keyword_names = {
                k: KeywordsMinorProtection.variants[k][0]
                for k in frequent_keywords
                if k is not None
            }
        else:
            self._keyword_names = {
                k: k
                for k in frequent_keywords
                if k is not None
            }

    def render_heading(self) -> None:
        self.html(
            '<h1><a href="https://transparency.dsa.ec.europa.eu">The DSA '
            'Transparency Database</a></h1>'
        )
        if "CSAM" in self._tags:
            self.html('<h2>Focus on Protection of Minors</h2>')
        self.html(
            f'<p>Created on {self._timestamp.date().isoformat()} '
            f'at {self._timestamp.time().isoformat()}</p>'
        )

    def render_overview(self) -> None:
        self.html("<h2>Summary</h2>")
        self.markdown(self._statistics.summary(markdown=True))

        self.html("<h2>Table Schemas</h2>")
        remark = (
            '\nAlso see [the official '
            'documentation](https://transparency.dsa.ec.europa.eu/page/api-documentation)'
        )
        self.markdown(
            format_schema(SCHEMA, title="Source Data") + remark,
            disclosure=True,
            render=not self._renderer.plain
        )
        self.markdown(
            format_schema(self._metadata, title="meta.json"),
            disclosure=True,
        )
        self.markdown(
            format_schema(self._statistics.frame(), title=Statistics.FILE),
            disclosure=True,
        )

        self.html("<h2>Keywords</h2>")
        self.html(
            '''\
<p>The percentage for the "null" keyword denotes the fraction of <em>all</em> SoRs,
whereas all other percentages denote fractions of SoRs with keywords only.</p>
            ''')
        self.frame(self._keyword_usage)
        pie = self.overall_keyword_usage()
        self.chart("keyword-pie", pie)

        self.html("<h2>Platforms</h2>")
        table = self._statistics.frame().filter(
            predicate("platform_name", entity=None)
        ).group_by(
            "variant"
        ).agg(
            pl.col("count").sum()
        ).sort(
            "count", descending=True
        ).with_row_index(
            offset=1
        )

        self.frame(table, all_text=True)

    def render_charts(self) -> None:
        self.html("<h2>Timelines</h2>")

        charts: list[alt.Chart | alt.LayerChart] = [
            self.daily_statements_of_reasons(),
            self.daily_statements_of_reasons(rolling_mean_days=7),
        ]

        if not self.has_all_sors():
            charts.extend([
                self.daily_sor_fraction_with_keywords(),
                self.daily_sor_fraction_with_keywords(rolling_mean_days=7),
            ])

        self.chart("sor-counts", alt.vconcat(*charts).resolve_scale(
            x="shared",
            color="independent",
        ))

        self.render_standard_timelines()

        self.html("<h2>Platforms</h2>")
        self.chart("platform-counts", self.cumulative_platform_counts())

        self.chart("platform-statements", alt.vconcat(
            self.overall_statements_by_platform(),
            self.overall_statements_by_platform(
                threshold=10_000_000 if self.has_all_sors() else 50_000
            ),
        ).resolve_scale(
            color="shared",
        ).configure_scale(
            barBandPaddingInner=0.05,
        ))

        if not self.has_all_sors():
            self.chart("platform-keywords", alt.vconcat(
                self.overall_keyword_usage_by_platform(percent=True),
                self.overall_keyword_usage_by_platform(percent=False),
            ).resolve_scale(color='independent'))

        if "CSAM" in self._tags:
            self.html("<h3>CSAM SoRs</h3>")
            self.render_standard_timelines("CSAM")

    def render_standard_timelines(self, tag: None | str = None) -> None:
        if self.is_monthly():
            name = f"{tag.lower()}-monthlies" if tag else "monthlies"
        else:
            name = f"{tag.lower()}-dailies" if tag else "more-dailies"

        self.chart(name, alt.vconcat(
            self.render_timeline(ProcessingDelay, tag),
            self.render_timeline(StatementCount, tag),
            self.render_timeline(ContentType, tag),
            self.timeline_chart(
                self.decision_ground(tag), DecisionGroundAndLegality, tag
            ),
            self.render_timeline(DecisionType, tag),
            self.render_timeline(DecisionVisibility, tag),
            self.render_timeline(DecisionProvision, tag),
            self.render_timeline(DecisionMonetary, tag),
            self.render_timeline(DecisionAccount, tag),
            self.render_timeline(AutomatedDetection, tag),
            self.render_timeline(AutomatedDecision, tag),
        ).resolve_scale(
            x="shared",
            color="independent",
        ))

    # ==================================================================================

    def daily_statements_of_reasons(
        self,
        *,
        rolling_mean_days: None | int = None,
        percentage: bool = False,
    ) -> alt.Chart:
        table = self._metadata.select(
            pl.col("start_date"),
            pl.col("batch_rows") / pl.col("total_rows") * 100 if percentage
            else pl.col("batch_rows") / 1_000,
        )

        if rolling_mean_days is not None:
            table = table.with_columns(
                pl.col("batch_rows").mean().rolling(
                    index_column="start_date", period=f"{rolling_mean_days}d"
                )
            )

        title = "Statements of Reasons — "
        if rolling_mean_days is None:
            title += "Daily Percentage" if percentage else "Daily Counts"
        else:
            title += f"{rolling_mean_days}-Day Rolling "
            title += "Percentage" if percentage else "Mean"

        if rolling_mean_days is None:
            chart = alt.Chart(table, title=title).mark_bar(
                tooltip=True,
                color=GREEN,
            )
        else:
            chart = alt.Chart(table, title=title).mark_line(
                tooltip=True,
                color=GREEN,
            )

        return chart.encode(
            alt.X("start_date:T").title("Date"),
            alt.Y("batch_rows:Q").title("Statements of Reasons (Thousands)"),
        ).properties(
            height=TIMELINE_HEIGHT,
            width=TIMELINE_WIDTH,
        ).interactive()

    def daily_sor_fraction_with_keywords(
        self,
        *,
        rolling_mean_days: None | int = None,
    ) -> alt.Chart | alt.LayerChart:
        title = "Statements of Reasons With Keywords — Daily Percentage"
        table = self._metadata.select(
            pl.col("start_date"),
            (pl.col("batch_rows_with_keywords") / pl.col("batch_rows") * 100)
            .alias("Protection of Minors Only"),
            (pl.col("total_rows_with_keywords") / pl.col("total_rows") * 100)
            .alias("All SoRs"),
        )

        if rolling_mean_days is not None:
            title = (
                "Statements of Reasons With Keywords - "
                f"{rolling_mean_days}-Day Rolling Min/Mean/Max (Percent)"
            )
            table = table.with_columns(
                pl.col("Protection of Minors Only")
                .rolling_min(window_size=rolling_mean_days).alias("band_min"),
                pl.col("Protection of Minors Only")
                .rolling_max(window_size=rolling_mean_days).alias("band_max"),
                pl.col("Protection of Minors Only", "All SoRs")
                .rolling_mean(window_size=rolling_mean_days),
            )

        long_table = table.unpivot(
            index=["start_date"],
            on=["Protection of Minors Only", "All SoRs"],
            variable_name="Kind",
            value_name="pct",
        )

        chart = alt.Chart(
            long_table,
            title=title,
        ).mark_line(
            tooltip=True,
        ).encode(
            alt.X("start_date:T").title("Date"),
            alt.Y("pct:Q").title("Percent"),
            alt.Color("Kind:N").scale(
                domain=["Protection of Minors Only", "All SoRs"],
                range=[PINK, BLUE],
            ),
        ).properties(
            height=TIMELINE_HEIGHT,
            width=TIMELINE_WIDTH,
        ).interactive()

        if rolling_mean_days is not None:
            band = alt.Chart(table).mark_errorband().encode(
                alt.X("start_date:T").title("Date"),
                alt.Y("band_min:Q").title(""),
                alt.Y2("band_max:Q"),
                color=alt.value(PINK),
            ).properties(
                height=TIMELINE_HEIGHT,
                width=TIMELINE_WIDTH,
            )

            chart = chart + band

        return chart

    def render_timeline(
        self,
        spec: MetricDeclaration,
        tag: None | str = None,
    ) -> alt.Chart:
        table = self.timeline_data(spec, tag)
        return self.timeline_chart(table, spec, tag)

    def timeline_data(
        self,
        spec: MetricDeclaration,
        tag: None | str = None,
    ) -> pl.DataFrame:
        filters: dict[str, Any] = dict(
            column=spec.field,
            tag=tag,
        )
        if spec.selector != "entity":
            filters["entity"] = None
        if not spec.has_null_variant() and spec.selector not in filters:
            filters[spec.selector] = NOT_NULL

        table = self._statistics.frame().filter(
            predicate(**filters)
        )

        if self.is_monthly():
            table = table.group_by(
                pl.col("start_date").dt.year().alias("year"),
                pl.col("start_date").dt.month().alias("month"),
                *spec.groupings(),
            ).agg(
                pl.col("start_date").min() + dt.timedelta(days=5),
                pl.col("end_date").max() - dt.timedelta(days=5),
                *aggregates()
            )
        else:
            table = table.group_by(
                pl.col("start_date"),
                *spec.groupings(),
            ).agg(
                *aggregates(),
            )

        if spec.has_variants():
            table = table.with_columns(
                pl.col(spec.selector).cast(pl.String).replace(spec.replacements())
            )
        if spec.quantity != "count" and spec.label == "Delays":
            table = table.with_columns(
                pl.col(spec.quantity) / (24 * 60 * 60 * 1_000)
            )

        return table

    def timeline_chart(
        self,
        table: pl.DataFrame,
        spec: MetricDeclaration,
        tag: None | str = None,
    ) -> alt.Chart:
        """
        Generate the standard timeline chart. The data frame may contain daily
        or monthly summary statistics.
        """
        quantity = {
            "count": "Counts",
            "min": "Minima",
            "mean": "Means",
            "max": "Maxima",
        }[spec.quantity]

        bar_area_props: dict[str, Any] = dict(
            tooltip=True,
        )
        if not spec.has_variants():
            bar_area_props["color"] = GRAY

        chart = alt.Chart(
            table,
            title=(
                f"{spec.label}{f" for {tag}" if tag else ""} "
                f"— {"Monthly" if self.is_monthly() else "Daily"} {quantity}"
            )
        )

        if self.is_monthly():
            chart = chart.mark_bar(**bar_area_props)
        elif not spec.has_variants():
            chart = chart.mark_line(**bar_area_props)
        else:
            chart = chart.mark_area(**bar_area_props)

        encoding: list[Any] = [
            alt.X("start_date:T")
            .title("Month" if self.is_monthly() else "Day"),
        ]
        if self.is_monthly():
            encoding.append(alt.X2("end_date:T").title(""))

        yaxis = alt.Y(f"sum({spec.quantity}):Q").title(spec.quant_label)
        if self.has_all_sors() and spec.quantity == "count":
            # This does cut off some daily numbers between January and March 2024,
            # but it also ensures that smaller categories are visible by and large
            yaxis = yaxis.scale(domain=(0, 120_000_000))
        encoding.append(yaxis)

        if spec.has_variants():
            encoding.append(
                alt.Color(f"{spec.selector}:N").scale(
                    domain=spec.variant_labels(),
                    range=spec.variant_colors(),
                ).title(spec.label),
            )

        chart = chart.encode(
            *encoding
        ).properties(
            height=TIMELINE_HEIGHT,
            width=TIMELINE_WIDTH,
        ).interactive()

        return chart

    def decision_ground(self, tag: None | str = None) -> pl.DataFrame:
        return self.timeline_data(
            DecisionGroundAndLegality, tag
        ).pivot(
            on="variant",
            values="count",
            index=["start_date"] + (["end_date"] if self.is_monthly() else [])
        ).with_columns(
            # Remove incompatible & illegal from incompatible
            (
                pl.col("Incompatible") - pl.col("Incompatible & Illegal")
            )
            .alias("Incompatible")
        ).unpivot(
            on=["Incompatible", "Illegal", "Incompatible & Illegal"],
            variable_name="variant",
            value_name="count",
            index=["start_date"] + (["end_date"] if self.is_monthly() else [])
        )

    def cumulative_platform_counts(self) -> alt.Chart:
        ALL = "All Platforms"
        KEY = "Platforms w/ Keywords"
        CSAM = "Platforms w/ CSAM"

        table = self._statistics.frame().lazy().with_columns(
            (pl.col("start_date") + dt.timedelta(days=15)).alias("mid_date"),
        ).group_by(*[
            pl.col("mid_date").dt.year().alias("year"),
            pl.col("mid_date").dt.month().alias("month"),
        ] + [] if self.is_monthly() else [
            pl.col("mid_date").dt.day().alias("day")
        ]).agg(
            pl.col("mid_date").first(),
            pl.col("variant").filter(pl.col("column").eq("platform_name")).alias(ALL),
            pl.col("variant").filter(
                pl.col("column").eq("platform_name")
                .and_(pl.col("entity").eq("with_category_specification"))
                .and_(pl.col("variant_too").is_null().not_())
            ).alias(KEY),
            pl.col("variant").filter(
                pl.col("column").eq("platform_name")
                .and_(pl.col("entity").eq("with_category_specification"))
                .and_(pl.col("variant_too").eq("KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL"))
            ).alias(CSAM),
        ).sort(
            "mid_date"
        ).with_columns(
            pl.col(ALL, KEY, CSAM).cumulative_eval(
                pl.element().explode().unique().implode().list.len()
            )
        ).unpivot(
            index=["mid_date"],
            on=[KEY, ALL, CSAM],
            variable_name="Kind",
            value_name="Count",
        ).collect()

        freq = "Monthly" if self.is_monthly() else "Daily"
        return (
            alt.Chart(
                table,
                title="Platforms Submitting SoRs with Keywords — "
                f"Cumulative {freq} Counts"
            ).mark_line(
                tooltip=True
            ).encode(
                alt.X("mid_date:T")
                .title("Month" if self.is_monthly() else "Day"),
                alt.Y("Count:Q").title("Number of Platforms"),
                alt.Color("Kind:N").scale(
                    domain=[CSAM, KEY, ALL],
                    range=[RED, ORANGE, GRAY],
                ),
            ).properties(
                height=TIMELINE_HEIGHT,
                width=TIMELINE_WIDTH,
            ).interactive()
        )

    def overall_statements_by_platform(
        self, threshold: None | int = None
    ) -> alt.Chart | alt.LayerChart:
        table = self._statistics.frame().lazy().filter(
            pl.col("column").eq("platform_name").and_(pl.col("entity").is_null())
        ).group_by(
            "variant"
        ).agg(
            pl.col("count").sum()
        ).sort(
            "count"
        ).filter(
            pl.col("count") >= (threshold if threshold else 1)
        ).collect()

        quantity = "SoRs" if self.has_all_sors() else "Protection of Minors SoRs"

        if threshold:
            base = alt.Chart(
                table,
                title=f"{quantity}: {table.height} Platforms ≥ "
                f"{threshold:,} SoRs — Total Counts"
            ).encode(
                alt.X("variant:N", sort="y")
                .axis(labelAngle=-45, labelFontSize=10)
                .title("Platform"),
                alt.Y("count:Q")
                .scale(type="log", domain=(
                    10_000,
                    30_000_000_000 if self.has_all_sors() else 100_000_000
                ), clamp=True)
                .title("log(Statements of Reasons)"),
                alt.Text("count:Q", format=",d"),
            )
        else:
            base = alt.Chart(
                table, title=f"{quantity} by Platform — Total Counts"
            ).encode(
                alt.X("variant:N", sort="y")
                .axis(labelAngle=-45, labelFontSize=5)
                .title("Platform"),
                alt.Y("count:Q")
                .title("Statements of Reasons"),
                alt.Text("count:Q", format=",d"),
            )

        chart = base.mark_bar(
            tooltip=True,
            color=f"{PURPLE}90" if threshold else PURPLE,
        ).properties(
            width=TIMELINE_WIDTH,
            height=TIMELINE_HEIGHT,
        )

        if threshold is None or threshold < 50_000:
            return chart
        else:
            text = base.mark_text(
                yOffset=30,
                angle=315,
                fontSize=8,
                fontWeight="bold",
            )

            return chart + text

    def overall_keyword_usage_by_platform(self, percent: bool) -> alt.Chart:
        frame = self._statistics.frame().lazy().filter(
            predicate(
                "platform_name",
                entity="with_category_specification",
                variant_too=NOT_NULL
            )
        ).group_by(
            pl.col("variant", "variant_too"),
        ).agg(
            *aggregates()
        ).with_columns(
            pl.col("variant_too")
            .cast(pl.String)
            .replace(self._keyword_names)
        ).collect()

        title = "Platforms' Overall Keyword Usage — "
        if percent:
            title += "Percentage Fractions"

            frame = frame.join(
                frame.group_by(
                    "variant"
                ).agg(
                    pl.col("count").sum().alias("platform_total")
                ),
                on="variant",
                how="left",
            ).with_columns(
                (pl.col("count").cast(pl.Float64) / pl.col("platform_total") * 100)
                .alias("percent")
            )
        else:
            title += "Total Counts"

        y_data = "sum(percent):Q" if percent else "sum(count):Q"
        y_title = "Percent Fraction" if percent else "Statements of Reasons"

        color = alt.Color("variant_too:N")
        if "CSAM" in self._tags:
            color = color.scale(
                domain=KeywordsMinorProtection.variant_labels(),
                range=KeywordsMinorProtection.variant_colors(),
            )
        color = color.title("Keyword")

        return alt.Chart(
            frame, title=title
        ).mark_bar(
            size=30,
            tooltip=True,
        ).encode(
            alt.X("variant:N", axis=alt.Axis(labelAngle=-45)).title("Platform"),
            alt.Y(y_data).title(y_title),
            color,
        ).properties(
            height=TIMELINE_HEIGHT,
            width=TIMELINE_WIDTH,
        )

    def overall_keyword_usage(self) -> alt.Chart:
        table = self._keyword_usage.filter(
            pl.col("keyword").is_in(self._keyword_names)
        ).with_columns(
            pl.col("keyword").cast(pl.String).replace(self._keyword_names)
        )

        return (
            alt.Chart(
                table, title="Keywords Appearing in > 0.1% of SoRs"
            ).mark_arc(
                tooltip=True,
            ).encode(
                alt.Theta("count:Q"),
                alt.Color("keyword:N").scale(
                    domain=[*self._keyword_names.values()],
                    range=KEYWORD_PALETTE[:len(self._keyword_names)],
                ),
            ).interactive()
        )


# --------------------------------------------------------------------------------------
# Schema Rendering


def format_schema(object: pl.DataFrame | pl.Schema, title: None | str = None) -> str:
    """Render the schema for the data frame as a markdown table."""
    schema = object.schema if isinstance(object, pl.DataFrame) else object
    return to_markdown_table(
        *([k, v] for k, v in schema.items()),
        columns=["Column", "Type"],
        title=title,
    )
