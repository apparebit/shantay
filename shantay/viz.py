from abc import ABCMeta, abstractmethod
from dataclasses import dataclass
import datetime as dt
from pathlib import Path
import re
from typing import Self

import altair as alt
import mistune
import polars as pl

from .framing import (
    collect_release_metadata, formatted_summary, is_row_within_period, predicate
)
from .metadata import Metadata
from .model import ConfigError, Coverage, ReleaseRange, STATISTICS_FILE, Storage
from .schema import KEYWORDS_MINOR_PROTECTION_PLUS, SCHEMA, StatementCategory
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
            (pl.col("count") / pl.col("count").sum() * 100).alias("pct")
        ).sort(
            pl.col("count"), descending=True
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
        pie = self.total_keyword_usage_minor_prot()
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
            self.daily_sor_counts_minor_prot(),
            self.daily_sor_counts_minor_prot(rolling_mean_days=7),
            self.daily_sor_percentage_minor_prot(),
            self.daily_sor_percentage_minor_prot(rolling_mean_days=7),
            self.daily_keywords_percent_minor_prot(),
            self.daily_keywords_percent_minor_prot(rolling_mean_days=7),
            self.monthly_delays(label="Protection of Minors"),
            #self.monthly_content_types(prefix=""),
            self.monthly_platform_counts_minor_prot(),
            self.monthly_keyword_usage_minor_prot(),
        ).resolve_scale(
            x="shared",
            color="independent",
        ))

        self.html("<h3>SoRs by Platform</h3>")
        self.chart("platform-breakdown", alt.vconcat(
            self.total_sors_by_platform_minor_prot(),
            self.total_sors_by_platform_minor_prot(threshold=50_000),
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
            self.monthly_share_of_csam_per_platform(percent=True),
            self.monthly_share_of_csam_per_platform(percent=False),
            self.monthly_delays(prefix="csam_", label="CSAM"),
        ).resolve_scale(color='independent'))

        self.chart("csam-timelines", alt.vconcat(
            self.monthly_csam_sors(),
            #self.monthly_content_types(prefix="csam_"),
            self.monthly_decision_grounds_for_csam(),
            self.monthly_decision_kinds_for_csam(),
            #self.monthly_visibility_changes_for_csam(),
            self.monthly_provision_decisions_for_csam(),
            self.monthly_monetary_decisions_for_csam(),
            self.monthly_account_decisions_for_csam(),
            self.monthly_automated_detection_for_csam(),
            self.monthly_automated_decision_for_csam(),
        ).resolve_scale(
            x="shared",
            color="independent",
        ))


    # ==================================================================================
    # Timelines: Overview


    def daily_sor_counts_minor_prot(
        self,
        *,
        rolling_mean_days: None | int = None,
    ) -> alt.Chart:
        table = self._metadata.select(
            pl.col("start_date"),
            pl.col("batch_rows") / 1_000,
        )

        if rolling_mean_days is None:
            chart = alt.Chart(
                table,
                title="Statements of Reasons: Protection of Minors — Daily Counts",
            ).mark_bar(
                tooltip=True,
                color=GREEN,
            )
        else:
            chart = alt.Chart(
                table.with_columns(
                    pl.col("batch_rows")
                    .mean()
                    .rolling(index_column="start_date", period=f"{rolling_mean_days}d")
                ),
                title="Statements of Reasons: Protection of Minors — "
                f"{rolling_mean_days}-Day Rolling Mean",
            ).mark_line(
                tooltip=True,
                color=GREEN,
            )

        return (
            chart.encode(
                alt.X("start_date:T").title("Date"),
                alt.Y("batch_rows:Q").title("Statements of Reasons (Thousands)"),
            ).properties(
                height=TIMELINE_HEIGHT,
                width=TIMELINE_WIDTH,
            ).interactive()
        )

    def daily_sor_percentage_minor_prot(
        self,
        *,
        rolling_mean_days: None | int = None,
    ) -> alt.Chart:
        table = self._metadata.select(
            pl.col("start_date"),
            (
                pl.col("batch_rows") / pl.col("total_rows") * 100
            )
            .alias("protection_of_minors")
        )

        if rolling_mean_days is None:
            chart = alt.Chart(
                table,
                title="Statements of Reasons: Protection of Minors — Daily Percentage",
            ).mark_bar(
                tooltip=True,
                color=GREEN,
            )
        else:
            chart = alt.Chart(
                table.with_columns(
                    pl.col("protection_of_minors")
                    .mean()
                    .rolling(index_column="start_date", period=f"{rolling_mean_days}d")
                ),
                title="Statements of Reasons: Protection of Minors — "
                f"{rolling_mean_days}-Day Rolling Mean (Percent)",
            ).mark_line(
                tooltip=True,
                color=GREEN,
            )

        return (
            chart.encode(
                alt.X("start_date:T").title("Date"),
                alt.Y("protection_of_minors:Q")
                .title("Percent of All Statements of Reasons"),
            ).properties(
                height=TIMELINE_HEIGHT,
                width=TIMELINE_WIDTH
            ).interactive()
        )

    def monthly_delays(self, prefix: str = "", label: str = "") -> alt.Chart:
        table = self._statistics.with_columns(
            (pl.col(*(c for c in self._statistics.columns if c.endswith("_delay")))
            / (24 * 60 * 60 * 1_000)).cast(pl.Float64)
        )
        table = self.extract_table3(table, "Mean Delay", {
            f"{prefix}mean_moderation_delay": "Moderation",
            f"{prefix}mean_reporting_delay": "Reporting",
        })

        return self.create_chart(
            f"{label} SoRs: Moderation & Reporting Delays — Monthly Means",
            table,
            variable="Mean Delay",
            var_label="Days",
            domain=["Moderation", "Reporting"],
            range=[LIGHT_BLUE, RED],
        )

    def daily_keywords_percent_minor_prot(
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

        chart = (
            alt.Chart(
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
            )
            .interactive()
        )

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

    def monthly_platform_counts_minor_prot(self) -> alt.Chart:
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
            pl.col("platform_value_counts")
            .list.explode().struct.field("platform_name").unique().alias(ALL),
            pl.col("platform_with_keyword_value_counts")
            .list.explode().struct.field("platform_name").unique().alias(KEY),
            pl.col("platform_with_csam_value_counts")
            .list.explode().struct.field("platform_name").unique().alias(CSAM),
        ).sort(
            "mid_date"
        ).with_columns(
            pl.col(ALL, KEY, CSAM).cumulative_eval(
                pl.element().explode().unique().implode()
            )
        ).with_columns(
            pl.col(ALL, KEY, CSAM).list.len()
        ).unpivot(
            index=["mid_date"],
            on=[KEY, ALL, CSAM],
            variable_name="Kind",
            value_name="Count",
        )

        return (
            alt.Chart(
                table.collect(),
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

    def total_sors_by_platform_minor_prot(
        self, threshold: None | int = None
    ) -> alt.Chart | alt.LayerChart:
        table = self._statistics.lazy().select(
            pl.col("platform_value_counts").list.explode().struct.unnest()
        ).group_by(
            "platform_name"
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
                alt.X("platform_name:N", sort="y")
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
                alt.X("platform_name:N", sort="y")
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

    def monthly_keyword_usage_minor_prot(self) -> alt.Chart | alt.LayerChart:
        table = self._statistics.select(
            pl.col("start_date", "end_date", "keyword_value_counts")
        ).explode(
            "keyword_value_counts"
        ).with_columns(
            pl.col("keyword_value_counts").struct.unnest()
        ).rename({
            "category_specification": "keyword"
        }).filter(
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
            .alias("Keyword")
        )

        chart = (
            alt.Chart(
                table, title="Keywords in Protection of Minors SoRs — Monthly Counts"
            ).mark_bar(
                tooltip=True,
            ).encode(
                alt.X("start_date:T").title("Month"),
                alt.X2("end_date:T").title(""),
                alt.Y("sum(count):Q").title("Number of Keywords"),
                alt.Color("Keyword:N").scale(
                    domain=[*self._short_keywords.values()],
                    range=KEYWORD_PALETTE[:len(self._short_keywords)],
                ),
            ).properties(
                height=TIMELINE_HEIGHT,
                width=TIMELINE_WIDTH,
            ).interactive()
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
            ).properties(
                height=TIMELINE_HEIGHT,
                width=TIMELINE_WIDTH,
            )

        return chart

    # ==================================================================================
    # Timelines: CSAM

    def monthly_share_of_csam_per_platform(self, percent: bool) -> alt.Chart:
        frame = self._statistics.lazy()
        frame = frame.select(
            pl.col("platform_value_counts").list.explode().struct.unnest()
        ).group_by(
            "platform_name"
        ).agg(
            pl.col("count").sum()
        ).join(
            frame.select(
                pl.col("platform_with_keyword_value_counts").list.explode().struct.unnest()
            ).group_by(
                "platform_name"
            ).agg(
                pl.col("count").sum()
            ),
            on="platform_name",
            how="right",
        ).rename({
            "count_right": "with_keyword"
        }).join(
            frame.select(
                pl.col("platform_with_csam_value_counts").list.explode().struct.unnest()
            ).group_by(
                "platform_name"
            ).agg(
                pl.col("count").sum()
            ),
            on="platform_name",
            how="right",
        ).rename({
            "count_right": "with_csam",
        }).with_columns(
            pl.col("with_keyword", "with_csam").fill_null(0)
        ).with_columns(
            (pl.col("with_keyword") - pl.col("with_csam")).alias("other_keyword"),
            (pl.col("count") - pl.col("with_keyword")).alias("no_keyword"),
        ).sort(
            "platform_name"
        )

        title = "Keywords Used by Platforms Reporting CSAM — "
        if percent:
            title += "Percentage Fractions"
            frame = frame.select(
                pl.col("platform_name"),
                (pl.col("with_csam") / pl.col("count") * 100).alias("with_csam"),
                (pl.col("other_keyword") / pl.col("count") * 100).alias("other_keyword"),
                (pl.col("no_keyword") / pl.col("count") * 100).alias("no_keyword"),
            )
        else:
            title += "Total Counts"

        frame = frame.rename({
            "platform_name": "Platform",
            "with_csam": "CSAM",
            "other_keyword": "Other Keyword",
            "no_keyword": "—none—",
        }).unpivot(
            index=["Platform"],
            on=["CSAM", "Other Keyword", "—none—"],
            variable_name="Kind",
            value_name="Percent" if percent else "SoRs",
        ).collect()

        y_data = "sum(Percent):Q" if percent else "sum(SoRs):Q"
        y_title = "Percent Fraction" if percent else "Statements of Reasons"

        return alt.Chart(
            frame, title=title
        ).mark_bar(
            size=30,
            tooltip=True,
        ).encode(
            alt.X("Platform:N", axis=alt.Axis(labelAngle=-45)),
            alt.Y(y_data).title(y_title),
            alt.Color("Kind:N").scale(
                domain=["CSAM", "Other Keyword", "—none—"],
                range=["#efb118", "#ff725c", "#9498a0"],
            )
        ).properties(
            height=TIMELINE_HEIGHT,
            width=TIMELINE_WIDTH,
        )

    def monthly_csam_sors(self) -> alt.Chart:
        table = self._statistics.group_by(
            pl.col("start_date").dt.year().alias("year"),
            pl.col("start_date").dt.month().alias("month"),
        ).agg(
            pl.col("start_date").min() + dt.timedelta(days=5),
            pl.col("end_date").max() - dt.timedelta(days=5),
            pl.col("csam").sum(),
        ).rename({
            "csam": "CSAM",
        })

        return (
            alt.Chart(
                table, title="Statements of Reasons with CSAM as Keyword - Monthly Counts"
            ).mark_bar(
                tooltip=True,
                color=GRAY,
            ).encode(
                alt.X("start_date:T").title("Month"),
                alt.X2("end_date:T").title(""),
                alt.Y("sum(CSAM):Q").title("Statements of Reasons"),
            ).properties(
                height=TIMELINE_HEIGHT,
                width=TIMELINE_WIDTH,
            ).interactive()
        )

    def extract_table2(self, variable: str, columns: dict[str, str]) -> pl.DataFrame:
        """Extract a long table from statistics."""
        return self.extract_table3(self._statistics, variable, columns)

    def extract_table3(
        self,
        frame: pl.DataFrame,
        column: str,
        variants: dict[str, str],
    ) -> pl.DataFrame:
        """Extract a long table from an arbitrary data frame."""
        return frame.filter(
            predicate(column, entity=None)
        ).group_by(
            pl.col("start_date").dt.year().alias("year"),
            pl.col("start_date").dt.month().alias("month"),
            pl.col("column"),
            pl.col("variant"),
        ).agg(
            pl.col("start_date").min() + dt.timedelta(days=5),
            pl.col("end_date").max() - dt.timedelta(days=5),
            pl.col("count").sum(),
        ).with_columns(
            pl.col("variant").replace(variants)
        )

    def create_chart(
        self,
        title: str,
        table: pl.DataFrame,
        *,
        variable: str,
        domain: list[str],
        range: list[str],
        var_label: str = "",
    ) -> alt.Chart:
        if not var_label:
            var_label = "Statements of Reasons"

        return alt.Chart(
            table, title=title,
        ).mark_bar(
            tooltip=True,
        ).encode(
            alt.X("start_date:T").title("Month"),
            alt.X2("end_date:T").title(""),
            alt.Y("sum(Count):Q").title(var_label),
            alt.Color(f"{variable}:N").scale(
                domain=domain,
                range=range,
            ),
        ).properties(
            height=TIMELINE_HEIGHT,
            width=TIMELINE_WIDTH,
        ).interactive()

    # def monthly_content_types(self, prefix: str) -> alt.Chart:
    #     table = self.extract_table2("Content Type", {
    #         f"{prefix}content_type_app": "App",
    #         f"{prefix}content_type_audio": "Audio",
    #         f"{prefix}content_type_image": "Image",
    #         f"{prefix}content_type_product": "Product",
    #         f"{prefix}content_type_synthetic_media": "Synthetic Media",
    #         f"{prefix}content_type_text": "Text",
    #         f"{prefix}content_type_video": "Video",
    #         f"{prefix}content_type_other": "Other",
    #     })

    #     return self.create_chart(
    #         "Content Types for CSAM - Monthly Counts",
    #         table,
    #         variable="Content Type",
    #         domain=["Audio", "Image", "Product", "Synthetic Media", "Text", "Video", "Other"],
    #         range=[LIGHT_BLUE, BLUE, ORANGE, RED, PINK, PURPLE, GRAY],
    #     )

    def monthly_decision_grounds_for_csam(self) -> alt.Chart | alt.LayerChart:
        assert 0 == self._statistics.select(
            pl.col("csam_null_decision_ground").sum()
        ).item()

        table = self._statistics.group_by(
            pl.col("start_date").dt.year().alias("year"),
            pl.col("start_date").dt.month().alias("month"),
        ).agg(
            pl.col("start_date").min() + dt.timedelta(days=5),
            pl.col("end_date").max() - dt.timedelta(days=5),
            pl.col("csam_illegal_content").sum(),
            pl.col("csam_incompatible_content_illegal_yes").sum(),
            (
                pl.col("csam_incompatible_content").sum()
                - pl.col("csam_incompatible_content_illegal_yes").sum()
            ).alias("Incompatible"),
        ).rename({
            "csam_illegal_content": "Illegal",
            "csam_incompatible_content_illegal_yes": "Illegal & Incompatible",
        }).unpivot(
            index=["start_date", "end_date"],
            on=["Illegal", "Illegal & Incompatible", "Incompatible"],
            variable_name="Decision Ground",
            value_name="Count",
        )

        chart = self.create_chart(
            "Decision Grounds for CSAM - Monthly Counts",
            table,
            variable="Decision Ground",
            domain=["Illegal", "Illegal & Incompatible", "Incompatible"],
            range=[BLUE, LIGHT_BLUE, RED],
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
            ).properties(
                height=TIMELINE_HEIGHT,
                width=TIMELINE_WIDTH,
            )

        return chart

    def monthly_decision_kinds_for_csam(self) -> alt.Chart:
        table = self.extract_table2("Decision Kind", {
            "csam_visibility_decision_only": "Visibility",
            "csam_provision_decision_only": "Provision",
            "csam_account_decision_only": "Account",
            "csam_visibility_provision_account_decision": "All Three",
        })

        return self.create_chart(
            "Kinds of Decisions for CSAM SoRs - Monthly Counts",
            table,
            variable="Decision Kind",
            domain=["Visibility", "Provision", "Account", "All Three"],
            range=[BLUE, LIGHT_BLUE, PURPLE, RED],
        )

    def monthly_provision_decisions_for_csam(self) -> alt.Chart | alt.LayerChart:
        provision_decision_columns = {
            "csam_provision_partial_suspension": "Partial Suspension",
            "csam_provision_total_suspension": "Total Suspension",
            "csam_provision_partial_termination": "Partial Termination",
            "csam_provision_total_termination": "Total Termination",
            "csam_null_provision_decision": "—none—",
        }

        table = self.extract_table2("Provision Decision", provision_decision_columns)
        return self.create_chart(
            "Provision Decisions for CSAM - Monthly Counts",
            table,
            variable="Provision Decision",
            domain=[*provision_decision_columns.values()],
            range=[LIGHT_BLUE, BLUE, ORANGE, RED, GRAY],
        )

    def monthly_monetary_decisions_for_csam(self) -> alt.Chart | alt.LayerChart:
        table = self.extract_table2("Monetary Decision", {
            "csam_monetary_suspension": "Suspended",
            "csam_monetary_termination": "Terminated",
            "csam_monetary_other": "Other",
            "csam_null_monetary_decision": "—none—",
        })

        return self.create_chart(
            "Monetary Decisions for CSAM - Monthly Counts",
            table,
            variable="Monetary Decision",
            domain=["Suspended", "Terminated", "Other", "—none—"],
            range=[ORANGE, RED, PINK, GRAY],
        )

    def monthly_account_decisions_for_csam(self) -> alt.Chart | alt.LayerChart:
        table = self.extract_table2("Account Decision", {
            "csam_account_suspended": "Suspended",
            "csam_account_terminated": "Terminated",
            "csam_null_account_decision": "—none—",
        })

        return self.create_chart(
            "Account Decisions for CSAM - Monthly Counts",
            table,
            variable="Account Decision",
            domain=["Suspended", "Terminated", "—none—"],
            range=[ORANGE, RED, GRAY],
        )

    # def monthly_visibility_changes_for_csam(self) -> alt.Chart | alt.LayerChart:
    #     table = self._statistics.select(
    #         pl.col("start_date", "end_date", "csam_visibility_decision_value_counts")
    #     ).explode(
    #         "visibility_decision_value_counts"
    #     )
    #     .group_by(
    #         pl.col("start_date").dt.year().alias("year"),
    #         pl.col("start_date").dt.month().alias("month"),
    #     ).agg(
    #         pl.col("start_date").min() + dt.timedelta(days=5),
    #         pl.col("end_date").max() - dt.timedelta(days=5),
    #         *(
    #             pl.col(c).sum() for c in columns.keys()
    #         )
    #     ).rename(
    #         columns
    #     ).unpivot(
    #         index=["start_date", "end_date"],
    #         on=[*columns.values()],
    #         variable_name=variable,
    #         value_name="Count",
    #     )


    #     table = self.extract_table2("Visibility Decision", {
    #         "csam_content_removed": "Removed",
    #         "csam_content_disabled": "Disabled",
    #         "csam_content_demoted": "Demoted",
    #         "csam_content_age_restricted": "Age Restricted",
    #         "csam_content_interaction_restricted": "Interaction Restricted",
    #         "csam_content_labeled": "Labeled",
    #         "csam_other_visibility": "Other",
    #         "csam_null_visibility_decision": "—none—",
    #     })

    #     return self.create_chart(
    #         "Visibility Decisions for CSAM - Monthly Counts",
    #         table,
    #         variable="Visibility Decision",
    #         domain=["Removed", "Disabled", "Other", "—none—"],
    #         range=[LIGHT_BLUE, RED, BLUE, GRAY],
    #     )

    def monthly_automated_detection_for_csam(self) -> alt.Chart:
        table = self.extract_table2("Automated Detection", {
            "csam_automated_detection_yes": "Automated",
            "csam_automated_detection_no": "Not Automated",
            "csam_null_automated_detection": "—none—",
        })

        return self.create_chart(
            "Automation of Detection for CSAM - Monthly Counts",
            table,
            variable="Automated Detection",
            domain=["Automated", "Not Automated", "—none—"],
            range=[LIGHT_BLUE, PURPLE, RED],
        )

    def monthly_automated_decision_for_csam(self) -> alt.Chart:
        table = self.extract_table2("Automated Decision", {
            "csam_automated_decision_fully": "Fully Automated",
            "csam_automated_decision_partially": "Partially Automated",
            "csam_automated_decision_not_automated": "Not Automated",
            "csam_null_automated_decision": "—none—",
        })

        return self.create_chart(
            "Automation of Decision for CSAM - Monthly Counts",
            table,
            variable="Automated Decision",
            domain=["Fully Automated", "Partially Automated", "Not Automated", "—none—"],
            range=[CYAN, BLUE, GREEN, RED],
        )


    # ==================================================================================

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
