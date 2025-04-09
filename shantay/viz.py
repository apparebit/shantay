from abc import ABCMeta, abstractmethod
from collections.abc import Sequence
from dataclasses import dataclass
import datetime as dt
from pathlib import Path
import re
from typing import Any, Self

import altair as alt
import mistune
import polars as pl

from .color import (
    BLUE, GRAY, GREEN, KEYWORD_PALETTE, ORANGE, PINK, PURPLE, RED
)
from .framing import (
    aggregates, collect_release_metadata, formatted_summary, is_row_within_period,
    NOT_NULL, predicate
)
from .metadata import Metadata
from .model import ConfigError, Coverage, ReleaseRange, STATISTICS_FILE, Storage
from .schema import (
    AutomatedDecision, AutomatedDetection,
    ContentType, DecisionAccount, DecisionGroundAndLegality, DecisionMonetary,
    DecisionProvision, DecisionType, DecisionVisibility,
    KeywordsMinorProtection, MetricDeclaration, ProcessingDelay, SCHEMA,
    StatementCategory, StatementCount,
)
from .util import scale, to_markdown_table


TIMELINE_WIDTH = 600
TIMELINE_HEIGHT = 400

QUANT_WIDTH = 500
QUANT_HEIGHT = 250

HTML_HEADLINE = re.compile(r"<h([1-3])>([^<]*)</h[1-3]>")

FRAME_BORDER = re.compile(r' border="1"')
FRAME_CLASS = re.compile(r' class="dataframe"')
FRAME_QUOT = re.compile(r"&quot;")
FRAME_SHAPE = re.compile(r"<small>shape:[^<]*</small>")
FRAME_STYLE = re.compile(r"<style>[^<]*</style>")

SVG_ATTRIBUTES = re.compile(r' class="marks" width="[0-9]*" height="[0-9]*"')

DOC_HEADER = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>The DSA Transparency Database</title>
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
/*thead > tr:first-of-type > :where(th, td) {
    padding-top: 0.35em;
}*/
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


def visualize(storage: Storage, coverage: Coverage, notebook: bool = False) -> None:
    charts = storage.staging_root / "charts"
    charts.mkdir(exist_ok=True)

    renderer = NotebookRenderer(charts) if notebook else PlainTextRenderer(charts)
    visualizer = Visualizer(
        storage.working_root, storage.staging_root, coverage, renderer
    )
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
        working_root: Path,
        staging_root: Path,
        coverage: Coverage,
        renderer: Renderer,
        with_extras: bool = False
    ) -> None:
        self._working_root = working_root
        self._staging_root = staging_root
        self._coverage = coverage
        self._with_extras = with_extras
        self._renderer = renderer
        self._timelines = False

    @staticmethod
    def configure_display() -> None:
        alt.theme.enable("default")

        pl.Config.set_tbl_rows(100)
        pl.Config.set_float_precision(3)
        pl.Config.set_thousands_separator(",")
        pl.Config.set_tbl_cell_numeric_alignment("RIGHT")
        pl.Config.set_fmt_str_lengths(
            (max(len(s) for s in StatementCategory) // 10 + 2) * 10
        )
        pl.Config.set_tbl_cols(20)

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

        if "timeline" in name or "breakdown" in name:
            svg = SVG_ATTRIBUTES.sub("", svg)

        assert self._document is not None
        self._document.write(svg)
        self._document.write("\n\n")

    def run(self) -> None:
        path = self._staging_root / "overview.html"
        self.configure_display()
        self.ingest()

        with open(path, mode="w", encoding="utf8") as document:
            try:
                self._document = document
                document.write(DOC_HEADER)

                self.render_heading()
                self.render_overview()
                self.render_timelines()

                document.write(DOC_FOOTER)
            finally:
                self._document = None

    def ingest(self) -> None:
        range, metadata = collect_release_metadata(
            Metadata.read_json(self._working_root).records
        )
        statistics = pl.read_parquet(self._working_root / STATISTICS_FILE)

        # Restrict rendered data to *full* months. That essentially drops the
        # first week of data from the DSA SoR DB.
        range = range.intersect(self._coverage.to_date_range()).to_release_range()
        self._range = ReleaseRange(
            range.first.to_first_full_month(),
            range.last.to_last_full_month()
        )

        within_range = is_row_within_period(range)
        self._metadata = metadata.filter(within_range)
        self._statistics = statistics.filter(within_range)
        if self._statistics.height == 0:
            raise ConfigError("cannot visualize less than a full month of data")

        self._summary = Summary.of(self._metadata, self._statistics)

        # Determine global keyword usage and keywords with at least 1% use.
        self._keyword_usage = self._statistics.filter(
            predicate("category_specification", entity=None)
        ).group_by(
            "variant"
        ).agg(
            pl.col("count").sum()
        ).rename({
            "variant": "keyword"
        }).with_columns(
            (pl.col("count") / pl.col("count").sum() * 100).alias("pct"),
        ).sort(
            pl.col("count"), descending=True
        )

        frequent_keywords = (
            self._keyword_usage
            .filter(1 <= pl.col("pct"))
            .get_column("keyword")
        )

        self._short_keywords = {
            k: KeywordsMinorProtection.variants[k][0]
            for k in frequent_keywords
            if k is not None
        }

    def render_heading(self) -> None:
        self.html(
            '<h1>The <a href="https://transparency.dsa.ec.europa.eu">DSA '
            'Transparency Database</a>: Protection of Minors</h1>'
        )

    def render_overview(self) -> None:
        self.html("<h2>Summary</h2>")
        self.markdown(self._summary.to_markdown())
        self.markdown(formatted_summary(self._statistics))

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
            format_schema(self._statistics, title=STATISTICS_FILE),
            disclosure=True,
        )

        self.html("<h2>Keywords</h2>")
        self.frame(self._keyword_usage)
        pie = self.overall_keyword_usage()
        self.chart("keyword-pie", pie)

        self.html("<h2>Platforms</h2>")
        table = self._statistics.filter(
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

    def render_timelines(self) -> None:
        self.html("<h2>Timelines</h2>")

        self.chart("timelines", alt.vconcat(
            self.daily_statements_of_reasons(),
            self.daily_statements_of_reasons(rolling_mean_days=7),
            self.daily_statements_of_reasons(percentage=True),
            self.daily_statements_of_reasons(rolling_mean_days=7, percentage=True),
            self.daily_sor_fraction_with_keywords(),
            self.daily_sor_fraction_with_keywords(rolling_mean_days=7),
            self.monthly_statistic(ProcessingDelay),
            self.monthly_statistic(ContentType),
            self.monthly_cumulative_platform_counts(),
            self.monthly_statistic(KeywordsMinorProtection),
        ).resolve_scale(
            x="shared",
            color="independent",
        ))

        self.html("<h3>SoRs by Platform</h3>")
        self.chart("platform-breakdown", alt.vconcat(
            self.overall_statements_by_platform(),
            self.overall_statements_by_platform(threshold=50_000),
        ).resolve_scale(
            x="independent",
        ).configure_scale(
            barBandPaddingInner=0.05,
        ))

        self.html("<h3>CSAM SoRs</h3>")
        self.html(
            "<p>The following timelines exclusively cover Statements of Reasons "
            "with CSAM as a keyword. Since even possession of CSAM is illegal "
            "across the EU (as well as the US), one would expect that platforms "
            "alway remove the content. Furthermore, given the legal and reputational "
            "risks, one would also expect that platforms close offending accounts. "
            "Alas, that is not reflected in the SoRs. However, only 8 out of 44 "
            "platforms have submitted SoRs with CSAM as keyword. So findings about "
            "CSAM may <em>not</em> generalize.</p>"
        )

        self.chart("csam-breakdown", alt.vconcat(
            self.overall_keyword_usage_by_platform(percent=True),
            self.overall_keyword_usage_by_platform(percent=False),
            self.monthly_statistic(ProcessingDelay, "CSAM"),
        ).resolve_scale(color='independent'))

        self.chart("csam-timelines", alt.vconcat(
            self.monthly_statistic(StatementCount, "CSAM"),
            self.monthly_statistic(ContentType, "CSAM"),
            self.monthly_chart(
                self.decision_ground("CSAM"), DecisionGroundAndLegality, "CSAM"
            ),
            self.monthly_statistic(DecisionType, "CSAM"),
            self.monthly_statistic(DecisionVisibility, "CSAM"),
            self.monthly_statistic(DecisionProvision, "CSAM"),
            self.monthly_statistic(DecisionMonetary, "CSAM"),
            self.monthly_statistic(DecisionAccount, "CSAM"),
            self.monthly_statistic(AutomatedDecision, "CSAM"),
            self.monthly_statistic(AutomatedDetection, "CSAM"),
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

    def monthly_statistic(
        self,
        spec: MetricDeclaration,
        tag: None | str = None,
    ) -> alt.Chart:
        table = self.monthly_data(spec, tag)
        return self.monthly_chart(table, spec, tag)

    def monthly_data(
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
        if not spec.has_null_variant():
            filters[spec.selector] = NOT_NULL

        table = self._statistics.filter(
            predicate(**filters)
        ).group_by(
            pl.col("start_date").dt.year().alias("year"),
            pl.col("start_date").dt.month().alias("month"),
            *spec.groupings(),
        ).agg(
            pl.col("start_date").min() + dt.timedelta(days=5),
            pl.col("end_date").max() - dt.timedelta(days=5),
            *aggregates()
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

    def monthly_chart(
        self,
        table: pl.DataFrame,
        spec: MetricDeclaration,
        tag: None | str = None,
    ) -> alt.Chart:
        quantity = {
            "count": "Counts",
            "min": "Minima",
            "mean": "Means",
            "max": "Maxima",
        }[spec.quantity]

        bar_props: dict[str, Any] = dict(
            tooltip=True,
        )
        if not spec.has_variants():
            bar_props["color"] = GRAY

        color_coding = []
        if spec.has_variants():
            color_coding.append(
                alt.Color(f"{spec.selector}:N").scale(
                    domain=spec.variant_labels(),
                    range=spec.variant_colors(),
                ).title(spec.label),
            )

        return alt.Chart(
            table,
            title=f"{spec.label}{f" for {tag}" if tag else ""} — Monthly {quantity}"
        ).mark_bar(
            **bar_props,
        ).encode(
            alt.X("start_date:T").title("Month"),
            alt.X2("end_date:T").title(""),
            alt.Y(f"sum({spec.quantity}):Q").title(spec.quant_label),
            *color_coding,
        ).properties(
            height=TIMELINE_HEIGHT,
            width=TIMELINE_WIDTH,
        ).interactive()

    def decision_ground(self, tag: None | str = None) -> pl.DataFrame:
        return self.monthly_data(
            DecisionGroundAndLegality, tag
        ).pivot(
            on="variant",
            values="count",
            index=["start_date", "end_date"]
        ).with_columns(
            pl.col("Incompatible") - pl.col("Incompatible & Illegal")
        ).unpivot(
            on=["Incompatible", "Illegal", "Incompatible & Illegal"],
            variable_name="variant",
            value_name="count",
            index=["start_date", "end_date"]
        )

    def monthly_cumulative_platform_counts(self) -> alt.Chart:
        ALL = "All Platforms"
        KEY = "Platforms w/ Keywords"
        CSAM = "Platforms w/ CSAM"

        table = self._statistics.lazy().with_columns(
            (pl.col("start_date") + dt.timedelta(days=15)).alias("mid_date"),
        ).group_by(
            pl.col("mid_date").dt.year().alias("year"),
            pl.col("mid_date").dt.month().alias("month"),
        ).agg(
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

        return (
            alt.Chart(
                table,
                title="Platforms Submitting Protection of Minors SoRs — "
                "Cumulative Monthly Counts"
            ).mark_line(
                tooltip=True
            ).encode(
                alt.X("mid_date:T").title("Month"),
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
        table = self._statistics.lazy().filter(
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

        if threshold:
            base = alt.Chart(
                table,
                title=f"Protection of Minors SoRs: {table.height} Platforms ≥ "
                f"{threshold:,} SoRs — Total Counts"
            ).encode(
                alt.X("variant:N", sort="y")
                .axis(labelAngle=-45)
                .title("Platform"),
                alt.Y("count:Q")
                .scale(type="log", domain=(10_000, 100_000_000), clamp=True)
                .title("log(Statements of Reasons)"),
                alt.Text("count:Q", format=",d"),
            )
        else:
            base = alt.Chart(
                table, title="Protection of Minors SoRs by Platform — Total Counts"
            ).encode(
                alt.X("variant:N", sort="y")
                .axis(labelAngle=-45, labelFontSize=8)
                .title("Platform"),
                alt.Y("count:Q")
                .title("Statements of Reasons"),
                alt.Text("count:Q", format=",d"),
            )

        chart = base.mark_bar(
            tooltip=True,
            color=f"{PURPLE}90" if threshold else PURPLE,
        ).properties(
            width=QUANT_WIDTH,
            height=QUANT_HEIGHT,
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
        frame = self._statistics.lazy().filter(
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
            .replace(KeywordsMinorProtection.replacements())
        ).collect()

        title = "Keywords Used by Platforms Reporting CSAM — "
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

        return alt.Chart(
            frame, title=title
        ).mark_bar(
            size=30,
            tooltip=True,
        ).encode(
            alt.X("variant:N", axis=alt.Axis(labelAngle=-45)).title("Platform"),
            alt.Y(y_data).title(y_title),
            alt.Color("variant_too:N").scale(
                domain=KeywordsMinorProtection.variant_labels(),
                range=KeywordsMinorProtection.variant_colors(),
            ).title("Keyword")
        ).properties(
            height=TIMELINE_HEIGHT,
            width=TIMELINE_WIDTH,
        )

    def overall_keyword_usage(self) -> alt.Chart:
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
    max_platforms_csam: int

    @classmethod
    def of(cls, metadata: pl.DataFrame, statistics: pl.DataFrame) -> Self:
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

        max_platforms = statistics.filter(
            predicate("platform_name", entity=None, tag=None)
        ).select(
            pl.col("variant").n_unique()
        ).item()

        frame = statistics.filter(
            predicate("platform_name", entity="with_category_specification", tag=None)
        ).with_columns(
            pl.col("variant").cast(pl.String).str.split(by="‖")
        )

        max_platforms_with_keywords = frame.filter(
            pl.col("variant").list.last().ne("is_null")
        ).select(
            pl.col("variant").list.first().n_unique()
        ).item()

        max_platforms_with_csam = frame.filter(
            pl.col("variant").list.last().eq("KEYWORD_CHILD_SEXUAL_ABUSE_MATERIAL")
        ).select(
            pl.col("variant").list.first().n_unique()
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
            max_platforms_keywords=max_platforms_with_keywords,
            max_platforms_csam=max_platforms_with_csam,
        )

    def to_frame(self) -> pl.DataFrame:
        return pl.DataFrame(self._rows(), orient="row", schema=["Attribute", "Value"])

    def to_markdown(self) -> str:
        return to_markdown_table(*self._rows(), columns=["Attribute", "Value"])

    def _rows(self) -> list[list[str]]:
        row_size, row_unit = scale(self.batch_rows)
        total_rows, total_unit = scale(self.total_rows)
        mem_size, mem_unit = scale(self.batch_memory)

        return [
            [
                "Dates",
                f"{self.start_date} to {self.end_date} (inclusive)"
            ],
            [
                "Platforms reporting Protection of Minors SoRs",
                f"{self.max_platforms_keywords} out of {self.max_platforms} "
                f"include keywords, {self.max_platforms_csam} include CSAM"
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
        ]

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
