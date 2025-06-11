from abc import ABCMeta, abstractmethod
import datetime as dt
import logging
from pathlib import Path
import re
from typing import Any, cast

import altair as alt
import mistune
import polars as pl

from .color import (
    BLUE, GRAY, GREEN, ORANGE, PINK, PURPLE, RED
)
from .framing import (
    aggregates, is_row_within_period, NOT_NULL, predicate
)
from .model import ConfigError, Coverage, Storage
from .schema import (
    AutomatedDecision, AutomatedDetection, CategoryMetric, ContentType, DecisionAccount,
    DecisionGroundAndLegality, DecisionMonetary, DecisionProvision, DecisionType,
    DecisionVisibility, humanize, KeywordChildSexualAbuseMaterial,
    make_metric, MetaPlatforms, MetricDeclaration, PlatformValueType, ProcessingDelay,
    SCHEMA, StatementCategoryProtectionOfMinors, StatementCount, TextColumns
)
from .stats import get_tags, Statistics
from .util import minify, to_markdown_table


TIMELINE_WIDTH = 600
TIMELINE_HEIGHT = 400
SPACING = 30

HTML_HEADLINE = re.compile(r"<h([1-3])(?: id=[^>]+)?>([^<]*)</h[1-3]>")
HTML_TABLEROW = re.compile(
    r'<tr>\n  <td style="text-align:left"><em><strong>(—+)([^—]+)(—+)</strong></em></td>'
    r'\n  <td style="text-align:right">⠀</td>'
)

FRAME_BORDER = re.compile(r' border="1"')
FRAME_CLASS = re.compile(r'<table class="dataframe">')
FRAME_QUOT = re.compile(r"&quot;")
FRAME_SHAPE = re.compile(r"<small>shape:[^<]*</small>")
FRAME_STYLE = re.compile(r"<style>[^<]*</style>")
FRAME_EOL = re.compile(
    r"(<thead>|<tbody>|<tr>|</th>|</td>|</tr>|</thead>|</tbody>|</table>)"
)

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
    margin: 3rem;
}
main {
    margin-left: auto;
    margin-right: auto;
    max-width: 60rem;
}
h2 {
    margin-top: 3rem;
}
svg + :where(div, svg, table) {
    margin-top: 1.5rem;
}

/* ----------------------------------- Table ----------------------------------- */
table {
    border-collapse: separate;
    border-spacing: 0;
    line-height: 1.2;
    margin-bottom: 3rem;
}
table caption {
    font-size: 1.2em;
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
/*tbody > tr.highlight {
    background: #fff9cf;
}*/
td {
    font-variant-numeric: tabular-nums;
    text-align: right;
}
th {
    text-align: right;
}
.col2left tr > :where(td, th):nth-child(2) {
    text-align: left;
}
tbody > tr.highlight > td {
    text-align: center;
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


_logger = logging.getLogger(__spec__.parent)


# --------------------------------------------------------------------------------------


def visualize(
    storage: Storage,
    coverage: Coverage,
    notebook: bool = False,
    with_cutoff: bool = True,
) -> pl.DataFrame:
    charts = storage.staging_root / "charts"
    charts.mkdir(exist_ok=True)

    renderer = NotebookRenderer(charts) if notebook else PlainTextRenderer(charts)
    visualizer = Visualizer(
        storage,
        coverage,
        renderer,
        with_cutoff=with_cutoff
    )
    return visualizer.run()


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
        with_extras: bool = False,
        with_cutoff: bool = False,
    ) -> None:
        self._storage = storage
        self._coverage = coverage
        self._with_extras = with_extras
        self._with_cutoff = with_cutoff
        self._renderer = renderer
        self._timelines = False
        self._timestamp = dt.datetime.now()
        self._section_num = 0
        self._is_meta = False

    def has_all_sors(self) -> bool:
        return self._coverage.category is None

    def is_monthly(self) -> bool:
        return self._frequency == "monthly"

    @property
    def persistent_root(self) -> Path:
        """
        Get root directory for the current visualization. If neither the archive
        nor extract root are available, the staging root will do as well.
        """
        root = (
            self._storage.archive_root
            if self.has_all_sors()
            else self._storage.extract_root
        )
        return root or self._storage.staging_root

    @staticmethod
    def configure_display() -> None:
        alt.theme.enable("default")

        from .tool import configure_printing
        configure_printing()

    def secno(self) -> int:
        self._section_num += 1
        return self._section_num

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

        def replace(match: re.Match) -> str:
            return (
                f'<tr class=highlight>\n  <td colspan=2><em><strong>{match.group(1)}'
                f'{match.group(2)}{match.group(3)}</strong></em></td>'
            )
        html = HTML_TABLEROW.sub(replace, html)

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

    def frame(
        self,
        frame: pl.DataFrame,
        caption: None | str = None,
        klass: None | str = None,
    ) -> None:
        self._renderer.frame(frame)

        assert self._document is not None
        html = frame._repr_html_()
        html = FRAME_BORDER.sub("", html)
        table_head = '<table>' if klass is None else f'<table class="{klass}">\n'
        if caption is not None:
            table_head += f'<caption>{caption}</caption>\n'
        html = FRAME_CLASS.sub(table_head, html)
        html = FRAME_QUOT.sub("", html)
        html = FRAME_SHAPE.sub("", html)
        html = FRAME_STYLE.sub("", html)
        html = html.replace("<td>", "  <td>").replace("<th>", "  <th>")
        html = FRAME_EOL.sub(r"\1\n", html)
        html = html.replace("<td>null</td>", "<td></td>")

        self._document.write(html)
        self._document.write("\n\n\n")

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

    # ==================================================================================

    def run(self) -> pl.DataFrame:
        path = (self._storage.staging_root / f"{self._coverage.stem()}.html")
        self.configure_display()
        self.ingest()

        with open(path, mode="w", encoding="utf8") as document:
            try:
                self._document = document
                document.write(DOC_HEADER.format(self._timestamp.isoformat()))
                document.write(DOC_HEADER_TOO)

                self.render_heading()
                self.render_charts()
                self.render_tables()

                document.write(DOC_FOOTER)
            finally:
                self._document = None

        return self._statistics.frame()

    def ingest(self) -> None:
        # Ingest summary statistics
        path = self.persistent_root / f"{self._coverage.stem()}.parquet"
        if not path.exists():
            _logger.info('ingesting built-in statistics')
            statistics = Statistics.builtin()
        else:
            _logger.info('ingesting statistics file="%s"', path)
            statistics = Statistics.read(path)

        # Capture frequency, tags, date range
        self._frequency = self._coverage.frequency()
        self._tags = get_tags(statistics.frame())
        self._date_range = statistics.range().intersection(
            self._coverage.to_date_range(), empty_ok=False
        ).monthlies().date_range() # Restrict to full months

        within_range = is_row_within_period(self._date_range)
        self._statistics = Statistics(
            f"{self._coverage.stem()}.parquet", statistics.frame().filter(within_range)
        )
        if self._statistics.frame().height == 0:
            raise ConfigError("cannot visualize less than a full month of data")

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        # Determine keyword ranking
        _logger.debug('analyze keyword usage')
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
            pl.col("count"), descending=True, maintain_order=True
        )

        self._keyword_metric = make_metric(
            "category_specification",
            "Keywords",
            self._keyword_usage.get_column("keyword"),
            quant_label="SoRs with Keywords"
        )

        self._frequent_keywords = (
            self._keyword_usage
            .drop_nulls()
            .filter(1 <= pl.col("pct"))
            .get_column("keyword")
        )

        # ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~ ~
        _logger.debug(
            "determine Meta's platforms and top-3 platforms other than Meta's"
        )

        meta_data = self._statistics.frame().filter(
            pl.col("platform").is_in(MetaPlatforms)
        )

        main_tag = self._tags[0]
        base_filter = (
            pl.col("tag").is_null() if main_tag is None else pl.col("tag").eq(main_tag)
        )

        meta_platforms = []
        for platform in MetaPlatforms:
            if 0 < meta_data.filter(
                base_filter.and_(pl.col("platform").eq(platform))
            ).height:
                meta_platforms.append(platform)

        self._meta = Statistics(
            f"{self._coverage.stem()}-meta.parquet",
            meta_data.group_by(
                pl.col(
                    "start_date", "end_date",
                    "tag", "column", "entity", "variant", "text"
                )
            ).agg(
                pl.lit(None, dtype=PlatformValueType).alias("platform"),
                *aggregates()
            )
        )

        top_num = 3 # The targeted number of non-Meta platforms
        top = self._statistics.frame().filter(
            predicate("rows", entity=None)
        ).group_by(
            pl.col("platform")
        ).agg(
            pl.col("count").sum()
        ).sort(
            "count", descending=True, maintain_order=True
        ).head(
            # Thanks to the len(MetaPlatforms) term, this selection must contain
            # at least top_num non-Meta platforms
            top_num + len(MetaPlatforms)
        ).get_column(
            "platform"
        ).to_list()

        # Remove Meta's platforms, leaving at least top_num non-Meta platforms
        for platform in meta_platforms:
            if platform in top:
                del top[top.index(platform)]

        # Compose complete list
        self._top_platforms = top[:top_num] + ["Meta", *meta_platforms]

    def render_heading(self) -> None:
        _logger.debug('render heading')

        main_tag = self._tags[0]
        description = "All Data" if main_tag is None else humanize(main_tag)
        self.html(f'<h1>The DSA Transparency Database: {description}</h1>')

        tag_toc = "\n            ".join(
            f'<li><a href="#{t}">{humanize(cast(str, t))}</a></li>'
            for t in self._tags[1:]
        )
        platform_toc = "\n            ".join(
            f'<li><a href="#{p.lower().replace(" ", "_")}">Focus on {p}</a></li>'
            for p in self._top_platforms
        )
        self.html(
            f"""
            <ol>
            <li><a href="#dailies">Daily Statements of Reasons</a></li>
            <li><a href="#platforms">The Platforms Filing SoRs</a></li>
            <li><a href="#sors">The Statements of Reasons</a></li>
            {tag_toc}
            {platform_toc}
            <li><a href="#data">Data Summary</a></li>
            <li><a href="#platform-ranking">Platform Ranking</a></li>
            <li><a href="#keyword-ranking">Keyword Ranking</a></li>
            <li><a href="#schemas">Schemas</a></li>
            </ol>
            """
        )

        self.html(
            """
            <p><strong>Platform-focused sections</strong> comprise the top-three
            non-Meta platforms by SoR volume, all of Meta's platforms together,
            and Meta's platforms individually. Meta platforms that have
            submitted SoRs to the DSA transparency database are Facebook,
            Instagram, Threads, WhatsApp, and some other Meta product(s).
            Seriously, the database entries for the latter are attributed to
            "Other Meta Platforms Ireland Limited-offered Products". For
            category-specific data, not all of Meta's platforms may be included
            in this report.</p>
            """
        )
        self.html(
            """
            <p><strong>Bars marked ⚠️</strong> represent outliers that go beyond
            the coordinate grid. Thusly clamping the y-axis enures that even
            subcategories remain easily distinguishable in other bars.</p>
            """
        )
        self.html(
            f"""
            <p><strong><a
            href="https://github.com/apparebit/shantay">Shantay</a></strong>
            created this document on {self._timestamp.date().isoformat()} at
            {self._timestamp.time().isoformat(timespec="seconds")} based on data
            from the <a href="https://transparency.dsa.ec.europa.eu">DSA
            transparency database</a>.</p>
            """
        )

    def render_tables(self) -> None:
        _logger.debug('render data tables')

        self.html(f"<h2 id=data>{self.secno()}. The Data</h2>")
        self.markdown(self._statistics.summary(markdown=True))

        self.html(f"<h2 id=platform-ranking>{self.secno()}. Platform Ranking</h2>")
        table = self._statistics.frame().filter(
            predicate("rows", entity=None, tag=self._tags[0])
        ).group_by(
            "platform"
        ).agg(
            pl.col("count").sum()
        ).sort(
            "count", descending=True
        ).with_row_index(
            offset=1
        )

        self.frame(table, klass="col2left")

        self.html(f"<h2 id=keyword-ranking>{self.secno()}. Keyword Ranking</h2>")
        self.html(
            '''\
<p>The percentage for the "null" keyword denotes the fraction of <em>all</em> SoRs,
whereas all other percentages denote fractions of SoRs with keywords only.</p>
            ''')
        self.frame(self._keyword_usage.with_row_index(offset=1), klass="col2left")
        pie = self.overall_keyword_usage()
        self.chart("keyword-pie", pie)

        self.html(f"<h2 id=schemas>{self.secno()}. Schemas</h2>")
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
            format_schema(self._statistics.frame(), title=self._statistics.file()),
            disclosure=True,
            render=not self._renderer.plain,
        )

    def render_charts(self) -> None:
        main_tag = self._tags[0]
        _logger.debug('render charts tag="%s"', "" if main_tag is None else main_tag)

        if main_tag is None:
            title = f"<h2 id=dailies>{self.secno()}. Daily Statements of Reasons</h2>"
        else:
            title = (
                f"<h2 id=dailies>{self.secno()}. Daily Statements of Reasons: "
                f"{humanize(main_tag)}</h2>"
            )
        self.html(title)
        self.chart("01a-daily-sors", alt.vconcat(
            self.daily_statements_of_reasons(tag=main_tag),
            self.daily_statements_of_reasons(tag=main_tag, rolling_mean_days=7),
            self.daily_statements_of_reasons(tag=main_tag, rolling_mean_days=30),
            spacing=SPACING,
        ).resolve_scale(
            x="shared",
            color="independent",
        ))

        self.chart("01b-daily-sors-with-keywords", alt.vconcat(
            self.chart_sor_fraction_with_keywords(
                tag=main_tag, with_total=main_tag is not None
            ),
            self.chart_sor_fraction_with_keywords(
                tag=main_tag, with_monthly_mean=True
            ),
            spacing=SPACING,
        ).resolve_scale(
            x="shared",
            color="independent",
        ))

        self.html(f"<h2 id=platforms>{self.secno()}. The Platforms Filing SoRs</h2>")
        self.chart("02a-platforms", self.cumulative_platform_counts())

        self.chart("02b-sors-by-platform", alt.vconcat(
            self.overall_statements_by_platform(tag=main_tag),
            self.overall_statements_by_platform(
                tag=main_tag,
                threshold=10_000_000 if self.has_all_sors() else 50_000
            ),
            spacing=SPACING,
        ).resolve_scale(
            color="shared",
        ).configure_scale(
            barBandPaddingInner=0.05,
        ))

        if not self.has_all_sors():
            self.chart("02c-keywords-by-platform", alt.vconcat(
                self.overall_keyword_usage_by_platform(percent=True, tag=main_tag),
                self.overall_keyword_usage_by_platform(percent=False, tag=main_tag),
                spacing=SPACING,
            ).resolve_scale(color='independent'))

        self.html(f"<h2 id=sors>{self.secno()}. The Statements of Reasons</h2>")
        self.render_standard_timelines("03", tag=main_tag)

        base = 4
        for index, tag in enumerate(self._tags[1:]):
            assert tag is not None
            _logger.debug('render charts tag="%s"', tag)
            self.html(f"<h2 id={tag}>{self.secno()}. Focus on {humanize(tag)}</h2>")
            self.render_standard_timelines(
                f"{base + index:02d}", tag=tag
            )

        base += len(self._tags) - 1
        for index, platform in enumerate(self._top_platforms):
            self.render_platform(base + index, platform)

    def render_platform(self, index, platform: str) -> None:
        main_tag = self._tags[0]
        _logger.debug(
            'render charts tag="%s", platform="%s"',
            "" if main_tag is None else main_tag,
            platform
        )

        platform_id = platform.lower().replace(" ", "_")
        self.html(f"<h2 id={platform_id}>{self.secno()}. Focus on {platform}</h2>")

        # Meta stands for combination of Facebook, Instagram, Other Meta
        # Product, Threads, and WhatsApp.
        stats = None
        cutoff = None
        effective_platform = platform
        if platform == "Meta":
            stats, self._statistics = self._statistics, self._meta
            cutoff, self._with_cutoff = self._with_cutoff, False
            self._is_meta = True
            effective_platform = None

        try:
            self.chart(
                f"{index:02d}a",
                self.daily_statements_of_reasons(
                    tag=main_tag,
                    platform=platform,
                    use_rows_as_source=platform == "Meta",
                )
            )
            self.render_standard_timelines(
                f"{index:02d}b", tag=main_tag, platform=effective_platform
            )
        finally:
            if platform == "Meta":
                assert stats is not None
                self._statistics = stats
                self._with_cutoff = cutoff
                self._is_meta = False

    def render_standard_timelines(
        self, prefix: str, tag: None | str = None, platform: None | str = None
    ) -> None:
        self.chart(f"{prefix}a-sor-attributes", alt.vconcat(
            self.render_timeline(StatementCount, tag, platform),
            self.render_timeline(self._keyword_metric, tag, platform),
            spacing=SPACING,
        ).resolve_scale(
            x="shared",
            color="independent",
        ))

        self.frame(
            self.text_usage("category_specification_other", tag, platform),
            caption="Keyword: Other"
        )

        if tag is None:
            self.chart(
                f"{prefix}b-sor-attributes",
                self.render_timeline(CategoryMetric, tag, platform)
            )

        self.chart(
            f"{prefix}c-sor-attributes",
            self.render_timeline(ContentType, tag, platform)
        )

        self.frame(
            self.text_usage("content_type_other", tag, platform),
            caption="Content Type: Other"
        )

        dg = self.decision_ground(tag, platform)
        self.chart(f"{prefix}d-sor-attributes", alt.vconcat(
            self.timeline_chart(dg, DecisionGroundAndLegality, tag, platform),
            self.render_timeline(DecisionType, tag, platform),
            self.render_timeline(DecisionVisibility, tag, platform),
            spacing=SPACING,
        ).resolve_scale(
            x="shared",
            color="independent",
        ))

        self.frame(
            self.text_usage("decision_visibility_other", tag, platform),
            caption="Visibility Decision: Other"
        )

        self.chart(f"{prefix}e-sor-attributes", alt.vconcat(
            self.render_timeline(DecisionProvision, tag, platform),
            self.render_timeline(DecisionMonetary, tag, platform),
            spacing=SPACING,
        ).resolve_scale(
            x="shared",
            color="independent",
        ))

        self.frame(
            self.text_usage("decision_monetary_other", tag, platform),
            caption="Monetary Decision: Other"
        )

        self.chart(f"{prefix}f-sor-attributes", alt.vconcat(
            self.render_timeline(DecisionAccount, tag, platform),
            self.render_timeline(AutomatedDetection, tag, platform),
            self.render_timeline(AutomatedDecision, tag, platform),
            self.render_timeline(ProcessingDelay, tag, platform),
            spacing=SPACING,
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
        tag: None | str = None,
        platform: None | str = None,
        use_rows_as_source: bool = False,
    ) -> alt.Chart:
        if use_rows_as_source:
            source = "rows"
            filter = predicate("rows", tag=tag)
        elif platform is not None:
            source = "rows"
            filter = predicate("rows", tag=tag, platform=platform)
        elif tag is None:
            source = "total_rows"
            filter = predicate("total_rows", tag=tag)
        else:
            source = "batch_rows"
            filter = pl.col("column").eq("total_rows").and_(
                pl.col("tag").is_null()
            ).or_(
                pl.col("column").eq("batch_rows").and_(
                    pl.col("tag").eq(tag)
                )
            )

        table = self._statistics.frame().filter(
            filter
        ).pivot(
            on="column",
            index="start_date",
            values="count"
        ).select(
            pl.col("start_date"),
            pl.col(source) / pl.col("total_rows") * 100 if percentage
            else pl.col(source) / 1_000,
        )

        if rolling_mean_days is not None:
            table = table.with_columns(
                pl.col(source).mean().rolling(
                    index_column="start_date", period=f"{rolling_mean_days}d"
                )
            )

        title = "Statements of Reasons — "
        if platform is not None:
            title = f"{platform}: {title}"
        if rolling_mean_days is None:
            title += "Daily Percentage" if percentage else "Daily Counts"
        else:
            title += f"{rolling_mean_days}-Day Rolling "
            title += "Percentage" if percentage else "Mean"

        if rolling_mean_days is None:
            chart = alt.Chart(table, title=title).mark_bar(
                tooltip=True,
                color=GREEN,
                size=1.3,
            )
        else:
            chart = alt.Chart(table, title=title).mark_line(
                tooltip=True,
                color=GREEN,
                size=1.5,
            )

        return chart.encode(
            alt.X("start_date:T").scale(domain=self._date_range.to_tuple()).title("Date"),
            alt.Y(f"{source}:Q").title("Statements of Reasons (Thousands)"),
        ).properties(
            height=TIMELINE_HEIGHT,
            width=TIMELINE_WIDTH,
        ).interactive()

    def sor_fraction_with_keywords_data(
        self,
        *,
        tag: None | str = None,
        rolling_mean_days: None | int = None,
        with_total: bool = False,
        with_monthly_mean: bool = False,
    ) -> pl.DataFrame:
        """
        Create the suitable data frame for visualizing the SoR fraction with
        keywords.
        """
        # Filter out unneeded data and then pivot to needed columns
        if tag is None or with_total:
            expr1 = (
                pl.col("column").is_in(["total_rows_with_keywords", "total_rows"])
                .and_(pl.col("tag").is_null())
            )
        else:
            expr1 = None

        if tag is not None:
            expr2 = (
                pl.col("column").is_in(["batch_rows_with_keywords", "batch_rows"])
                .and_(pl.col("tag").eq(tag))
            )
        else:
            expr2 = None

        if expr1 is None and expr2 is None:
            raise AssertionError("unreachable statement")
        elif expr1 is None:
            expr = expr2
        elif expr2 is None:
            expr = expr1
        else:
            expr = expr1.or_(expr2)

        assert expr is not None
        base_frame = self._statistics.frame().filter(
            expr
        ).pivot(
            on="column",
            index=["start_date", "end_date"],
            values="count",
        )

        # Handle monthly aggregation
        if with_monthly_mean:
            if tag is None:
                columns = ["total_rows_with_keywords", "total_rows"]
            else:
                columns = ["batch_rows_with_keywords", "batch_rows"]

            frame = base_frame.group_by(
                pl.col("start_date").dt.year().alias("year"),
                pl.col("start_date").dt.month().alias("month"),
                maintain_order=True,
            ).agg(
                pl.col("start_date").first().dt.month_start(),
                pl.col("start_date").first().dt.month_end().alias("end_date"),
                pl.col(*columns).sum(),
            )
        else:
            frame = base_frame

        # Convert to percentage fractions
        def percent_fraction(prefix: str) -> pl.Expr:
            expr = (
                pl.col(f"{prefix}_rows_with_keywords") / pl.col(f"{prefix}_rows") * 100
            )
            if rolling_mean_days is not None:
                expr = expr.rolling_mean(window_size=rolling_mean_days)
            expr = expr.alias(
                "All SoRs" if prefix == "total" else humanize(cast(str, tag))
            )
            return expr

        column_names = []
        fractions = []
        if tag is None or with_total:
            column_names.append("All SoRs")
            fractions.append(percent_fraction("total"))
        if tag is not None:
            column_names.append(humanize(tag))
            fractions.append(percent_fraction("batch"))

        return frame.select(
            pl.col("start_date", "end_date"),
            *fractions
        ).unpivot(
            index=["start_date", "end_date"],
            on=column_names,
            variable_name="Kind",
            value_name="pct",
        )

    def chart_sor_fraction_with_keywords(
        self,
        *,
        rolling_mean_days: None | int = None,
        with_total: bool = False,
        with_monthly_mean: bool = False,
        tag: None | str = None,
    ) -> alt.Chart | alt.LayerChart:
        daily_frame = self.sor_fraction_with_keywords_data(
            tag=tag,
            rolling_mean_days=rolling_mean_days,
            with_total=with_total,
            with_monthly_mean=False,
        )

        title = "Statements of Reasons With Keywords — "
        if rolling_mean_days is None and not with_monthly_mean:
            title += "Daily Percentage"
        elif rolling_mean_days is not None:
            title += f"{rolling_mean_days}-Day Rolling Mean (Percent)"
        else:
            title += f"Daily Percentage vs Monthly Mean"

        column_names = []
        if tag is not None:
            column_names.append(humanize(tag))
        if tag is None or with_total:
            column_names.append("All SoRs")

        daily_chart = alt.Chart(
            daily_frame,
            title=title,
        ).mark_line(
            tooltip=True,
            size=1 if with_monthly_mean else 1.5,
        ).encode(
            alt.X("start_date:T").title("Date"),
            alt.Y("pct:Q").title("Percent"),
            alt.Color("Kind:N").scale(
                domain=column_names,
                range=[PINK, BLUE],
            ),
        ).properties(
            height=TIMELINE_HEIGHT,
            width=TIMELINE_WIDTH,
        )

        if not with_monthly_mean:
            return daily_chart

        monthly_frame = self.sor_fraction_with_keywords_data(
            tag=tag,
            rolling_mean_days=None,
            with_monthly_mean=True,
        )

        monthly_chart = alt.Chart(
            monthly_frame,
        ).mark_bar(
            tooltip=True,
            color=f"{PURPLE}50",
        ).encode(
            alt.X("start_date:T"),
            alt.X2("end_date:T"),
            alt.Y("sum(pct):Q"),
        )

        if tag is None:
            text = ["Monthly Mean", "All SoRs", "With Keywords"]
        else:
            text = ["Monthly Mean", humanize(tag), "SoRs with Keywords"]

        label = alt.Chart(
            pl.DataFrame({"pct": [0]})
        ).encode(
            alt.Y("pct:Q"),
        ).mark_text(
            x="width",
            dx=6,
            dy=-30,
            align="left",
            baseline="bottom",
            text=text,
            color=PURPLE,
        )

        chart = monthly_chart + daily_chart + label
        return chart

    # ----------------------------------------------------------------------------------

    def render_timeline(
        self,
        spec: MetricDeclaration,
        tag: None | str = None,
        platform: None | str = None,
    ) -> alt.Chart | alt.LayerChart:
        table = self.timeline_data(spec, tag, platform)
        return self.timeline_chart(table, spec, tag, platform)

    def timeline_data(
        self,
        spec: MetricDeclaration,
        tag: None | str = None,
        platform: None | str = None,
    ) -> pl.DataFrame:
        filters: dict[str, Any] = dict(
            column=spec.field,
            tag=tag,
        )
        if spec.selector != "entity":
            filters["entity"] = None
        if not spec.has_null_variant() and spec.selector not in filters:
            filters[spec.selector] = NOT_NULL
        if platform is not None:
            filters["platform"] = platform

        table = self._statistics.frame().filter(
            predicate(**filters)
        )

        if self.is_monthly():
            table = table.group_by(
                pl.col("start_date").dt.year().alias("year"),
                pl.col("start_date").dt.month().alias("month"),
                *spec.groupings(),
            ).agg(
                pl.col("start_date").first().dt.month_start().dt.offset_by("5d"),
                pl.col("start_date").first().dt.month_end().dt.offset_by("-5d")
                .alias("end_date"),
                *aggregates(),
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
                pl.col(spec.quantity) / (24 * 60 * 60)
            )
        return table

    def timeline_chart(
        self,
        table: pl.DataFrame,
        spec: MetricDeclaration,
        tag: None | str = None,
        platform: None | str = None,
    ) -> alt.Chart | alt.LayerChart:
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

        encoding: list[Any] = [
            alt.X("start_date:T").scale(domain=self._date_range.to_tuple())
            .title("Month" if self.is_monthly() else "Day"),
        ]
        if self.is_monthly():
            encoding.append(alt.X2("end_date:T").title(""))

            if spec is StatementCount:
                table = self.data_signage(table, encoding)

        yaxis = alt.Y(f"sum({spec.quantity}):Q").title(spec.quant_label)

        cutoff = None
        signage = None
        if self._with_cutoff and platform is None and spec.quantity == "count":
            if self.is_monthly() and tag is None:
                cutoff = 2_000_000_000
            elif (
                self.is_monthly() and
                tag == StatementCategoryProtectionOfMinors
            ):
                cutoff = 5_000_000

            if cutoff is not None:
                yaxis = yaxis.scale(domain=(0, cutoff), clamp=True)
                signage = self.warning_signage(cutoff, table)

        encoding.append(yaxis)

        if spec.has_variants():
            encoding.append(
                alt.Color(f"{spec.selector}:N").scale(
                    domain=spec.variant_labels(),
                    range=spec.variant_colors(),
                ).title(spec.label),
            )

        title = spec.label
        if platform is not None:
            title = f"{platform}: {title}"
        elif self._is_meta:
            title = f"Meta: {title}"
        if tag is not None:
            title += f" for {humanize(tag)}"
        title += f" — {"Monthly" if self.is_monthly() else "Daily"} {quantity}"

        base = alt.Chart(
            table,
            title=title,
        ).encode(
            *encoding
        ).properties(
            height=TIMELINE_HEIGHT,
            width=TIMELINE_WIDTH,
        )

        if self.is_monthly():
            chart = base.mark_bar(**bar_area_props)

            if spec is StatementCount:
                labels = base.encode(
                    alt.X("mid_date:T")
                ).mark_text(
                    dy=-8,
                    align="center",
                    fontSize=10,
                )
                chart = chart + labels

        elif not spec.has_variants():
            chart = base.mark_line(**bar_area_props)
        else:
            chart = base.mark_area(**bar_area_props)

        if cutoff is not None:
            assert signage is not None
            warnings = alt.Chart(
                signage
            ).encode(
                alt.X("mid_date:T"),
                alt.Y("cutoff:Q"),
                alt.Text("warning:N"),
            ).mark_text(
                baseline="line-top",
                dy=3,
                align="center",
                fontSize=16,
            )

            chart = chart + warnings

        if spec is ProcessingDelay:
            chart = chart + self.processing_delay_signage(tag, platform)

        return chart

    def data_signage(
        self, frame: pl.DataFrame, encoding: list[alt.FieldChannelMixin]
    ) -> pl.DataFrame:
        encoding.append(alt.Text("label"))
        return frame.with_columns(
            pl.col("start_date").dt.offset_by("10d").alias("mid_date"),
            pl.col("count").map_elements(minify, return_dtype=pl.String).alias("label"),
        )

    def warning_signage(self, cutoff: int, frame: pl.DataFrame) -> pl.DataFrame:
        if self.is_monthly():
            signage = frame.group_by(
                pl.col("start_date").dt.year().alias("year"),
                pl.col("start_date").dt.month().alias("month"),
            ).agg(
                pl.col("start_date").first().dt.offset_by("10d").alias("mid_date"),
                pl.col("count").sum().alias("total"),
            )
        else:
            signage = frame.group_by(
                pl.col("start_date"),
            ).agg(
                pl.col("start_date").first().alias("mid_date"),
                pl.col("count").sum().alias("total"),
            )

        return signage.with_columns(
            pl.lit(cutoff).alias("cutoff"),
            pl.when(
                pl.col("total").gt(cutoff)
            ).then(
                pl.lit("⚠️")
            ).otherwise(
                pl.lit("")
            ).alias("warning"),
        )

    def processing_delay_signage(
        self, tag: None | str = None, platform: None | str = None
    ) -> alt.LayerChart:
        constraints = {
            "entity": None,
            "tag": tag,
        }

        if platform is not None:
            constraints["platform"] = platform

        weighted_mean = (
            pl.col("mean")
            .mul(pl.col("count"))
            .floordiv(pl.col("count").sum())
            .sum()
            / (24 * 60 * 60)
        )

        total = pl.concat([
            self._statistics.frame().filter(
                predicate(column="moderation_delay", **constraints)
            ).select(
                weighted_mean.alias("moderation")
            ),
            self._statistics.frame().filter(
                predicate(column="disclosure_delay", **constraints)
            ).select(
                weighted_mean.alias("disclosure")
            ),
        ], how="horizontal")

        base = alt.Chart(total)
        moderation_rule = base.mark_rule(
            color=BLUE,
            size=2.5,
        ).encode(
            alt.Y("moderation:Q")
        )

        moderation_label = moderation_rule.mark_text(
            x="width",
            dx=6,
            dy=0,
            align="left",
            baseline="bottom",
            text=["Mean Moderation", f"Delay: {total.item(0, 0):.1f} Days"],
            color=BLUE,
        )

        disclosure_rule = base.mark_rule(
            color=RED,
            size=2.5,
        ).encode(
            alt.Y("disclosure:Q")
        )

        disclosure_label = disclosure_rule.mark_text(
            x="width",
            dx=6,
            dy=0,
            align="left",
            baseline="bottom",
            text=["Mean Disclosure", f"Delay {total.item(0, 1):.1f} Days"],
            color=RED,
        )

        return (
            disclosure_rule + disclosure_label + moderation_rule + moderation_label
        )

    def decision_ground(
        self, tag: None | str = None, platform: None | str = None
    ) -> pl.DataFrame:
        frame = self.timeline_data(
            DecisionGroundAndLegality, tag, platform
        ).pivot(
            on="variant",
            values="count",
            index=["start_date"] + (["end_date"] if self.is_monthly() else [])
        )

        # Some platforms do not report all three quantities, so we add them here
        for column in ("Incompatible", "Illegal", "Incompatible & Illegal"):
            if column not in frame.columns:
                frame = frame.with_columns(
                    pl.lit(0).alias(column)
                )

        return frame.with_columns(
            # Subtract incompatible & illegal from incompatible
            pl.col("Incompatible") - pl.col("Incompatible & Illegal")
        ).unpivot(
            on=["Incompatible", "Illegal", "Incompatible & Illegal"],
            variable_name="variant",
            value_name="count",
            index=["start_date"] + (["end_date"] if self.is_monthly() else [])
        )

    # ----------------------------------------------------------------------------------

    def cumulative_platform_counts(self, keyword: None | str = None) -> alt.Chart:
        ALL = "All Platforms"
        KEY = "Platforms w/ Keywords"
        metrics = [ALL, KEY]
        if keyword is not None:
            metrics.append(keyword)

        table = self._statistics.frame().lazy().filter(
            predicate("category_specification", entity=None, tag=self._tags[0])
        )

        if self.is_monthly():
            table = table.group_by(
                pl.col("start_date").dt.year().alias("year"),
                pl.col("start_date").dt.month().alias("month"),
                maintain_order=True,
            )
            mid_date = (
                pl.col("start_date")
                .first()
                .dt.month_start()
                .dt.offset_by("14d")
                .alias("mid_date")
            )
        else:
            table = table.group_by(
                pl.col("start_date"),
                maintain_order=True,
            )
            mid_date = (
                pl.col("start_date")
                .first()
                .alias("mid_date")
            )

        aggregates = [
            mid_date,
            pl.col("platform").unique().alias(ALL),
            pl.col("platform").filter(
                pl.col("variant").is_null().not_()
            ).alias(KEY),
        ]

        if keyword is not None:
            aggregates.append(
                pl.col("platform").filter(
                    pl.col("variant").eq(keyword)
                ).alias(keyword)
            )

        table = table.agg(
            *aggregates
        ).with_columns(
            pl.col(*metrics).cumulative_eval(
                pl.element().explode().unique().implode().list.len()
            )
        ).unpivot(
            index=["mid_date"],
            on=metrics,
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
                    domain=metrics,
                    range=[GRAY, ORANGE, RED],
                ),
            ).properties(
                height=TIMELINE_HEIGHT,
                width=TIMELINE_WIDTH,
            ).interactive()
        )

    def overall_statements_by_platform(
        self, threshold: None | int = None, tag: None | str = None
    ) -> alt.Chart | alt.LayerChart:
        table = self._statistics.frame().lazy().filter(
            predicate("rows", entity=None, tag=tag)
        ).group_by(
            "platform"
        ).agg(
            pl.col("count").sum()
        ).sort(
            "count"
        ).filter(
            pl.col("count") >= (threshold if threshold else 1)
        ).with_columns(
            pl.col("count").map_elements(minify, return_dtype=pl.String).alias("label")
        ).collect()

        if tag is None:
            quantity = "SoRs"
        else:
            quantity = f"{humanize(tag)} SoRs"

        if threshold:
            base = alt.Chart(
                table,
                title=f"{quantity}: {table.height} Platforms ≥ "
                f"{threshold:,} SoRs — Total Counts"
            ).encode(
                alt.X("platform:N", sort="y")
                .axis(labelAngle=-45, labelFontSize=10)
                .title("Platform"),
                alt.Y("count:Q")
                .scale(type="log", domain=(
                    10_000,
                    30_000_000_000 if self.has_all_sors() else 100_000_000
                ), clamp=True)
                .title("log(Statements of Reasons)"),
                alt.Text("label"),
            )
        else:
            base = alt.Chart(
                table, title=f"{quantity} by Platform — Total Counts"
            ).encode(
                alt.X("platform:N", sort="y")
                .axis(labelAngle=-45, labelFontSize=5)
                .title("Platform"),
                alt.Y("count:Q")
                .title("Statements of Reasons"),
                alt.Text("label"),
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
                fontWeight="bold",
            )

            return chart + text

    def overall_keyword_usage_by_platform(
        self, percent: bool, tag: None | str = None
    ) -> alt.Chart:
        frame = self._statistics.frame().lazy().filter(
            predicate(
                "category_specification",
                entity=None,
                variant=NOT_NULL,
                tag=tag,
            )
        ).group_by(
            pl.col("platform", "variant"),
        ).agg(
            *aggregates()
        ).with_columns(
            pl.col("variant")
            .cast(pl.String)
            .replace(self._keyword_metric.replacements())
        ).collect()

        title = "Platforms' Overall Keyword Usage — "
        if percent:
            title += "Percentage Fractions"

            frame = frame.join(
                frame.group_by(
                    "platform"
                ).agg(
                    pl.col("count").sum().alias("platform_total")
                ),
                on="platform",
                how="left",
            ).with_columns(
                (pl.col("count").cast(pl.Float64) / pl.col("platform_total") * 100)
                .alias("percent")
            )
        else:
            title += "Total Counts"

        y_data = "sum(percent):Q" if percent else "sum(count):Q"
        y_title = "Percent Fraction" if percent else "Statements of Reasons"

        color = alt.Color("variant:N")
        if KeywordChildSexualAbuseMaterial in self._tags:
            color = color.scale(
                domain=self._keyword_metric.variant_labels(),
                range=self._keyword_metric.variant_colors(),
            )
        color = color.title("Keyword")

        return alt.Chart(
            frame, title=title
        ).mark_bar(
            size=30,
            tooltip=True,
        ).encode(
            alt.X("platform:N", axis=alt.Axis(labelAngle=-45)).title("Platform"),
            alt.Y(y_data).title(y_title),
            color,
        ).properties(
            height=TIMELINE_HEIGHT,
            width=TIMELINE_WIDTH,
        )

    def overall_keyword_usage(self) -> alt.Chart:
        table = self._keyword_usage.filter(
            pl.col("keyword").is_in(self._frequent_keywords)
        ).with_columns(
            pl.col("keyword")
            .cast(pl.String)
            .replace(self._keyword_metric.replacements())
        )

        return (
            alt.Chart(
                table, title="Keywords Appearing in > 1% of SoRs"
            ).mark_arc(
                tooltip=True,
            ).encode(
                alt.Theta("count:Q"),
                alt.Color("keyword:N").scale(
                    domain=self._keyword_metric.variant_labels(),
                    range=self._keyword_metric.variant_colors(),
                ).title("Keyword")
            ).interactive()
        )

    def text_usage(
        self,
        column: None | str = None,
        tag: None | str = None,
        platform: None | str = None,
    ) -> pl.DataFrame:
        if column is None:
            filter = pl.col("column").is_in(TextColumns)
        else:
            filter = pl.col("column").eq(column)

        if tag is None:
            filter = filter.and_(pl.col("tag").is_null())
        else:
            filter = filter.and_(pl.col("tag").eq(tag))

        if platform == "Meta":
            source = self._meta.frame()
        else:
            source = self._statistics.frame()
            if platform is not None:
                filter = filter.and_(pl.col("platform").eq(platform))

        return source.filter(filter).group_by(
            pl.col("column", "text")
        ).agg(
            pl.col("count").sum()
        ).sort(
            ["column", "count"],
            descending=True,
        ).with_columns(
            pl.col("text").fill_null("␀")
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
