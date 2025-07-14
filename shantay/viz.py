from abc import ABCMeta, abstractmethod
from collections.abc import Iterable
import datetime as dt
from io import StringIO
import logging
from pathlib import Path
import re
import shutil
from typing import Any, cast

import altair as alt
import mistune
import polars as pl

from .color import (
    BLUE, CYAN, GRAY, GREEN, LIGHT_BLUE, ORANGE, PURPLE, RED
)
from .framing import (
    aggregates, is_row_within_period, NO_ARGUMENT_PROVIDED, NOT_NULL, predicate
)
from .model import ConfigError, Coverage, file_stem_for, Storage
from .schema import (
    AccountTypeMetric, AutomatedDecisionMetric, AutomatedDetectionMetric,
    ContentLanguageMetric, CategoryMetric, ContentTypeMetric, DecisionAccountMetric,
    DecisionGroundMetric, DecisionMonetaryMetric, DecisionProvisionMetric,
    DecisionTypeMetric, DecisionVisibilityMetric, humanize,
    IncompatibleContentIllegalMetric, InformationSourceMetric,
    KeywordChildSexualAbuseMaterial, make_metric, MetaPlatforms, MetricDeclaration,
    ModerationDelayMetric, PlatformValueType, ProcessingDelayMetric, SCHEMA,
    StatementCategoryProtectionOfMinors, StatementCountMetric, TerritorialScopeMetric,
    TextColumns
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
FRAME_HEAD = re.compile(r"<thead>.*?</thead>")
FRAME_EOL = re.compile(
    r"(<thead>|<tbody>|<tr>|</th>|</td>|</tr>|</thead>|</tbody>|</table>)"
)

SVG_ATTRIBUTES = re.compile(r' class="marks" width="[0-9]+" height="[0-9]+"')

DOC_HEAD = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<title>{title}</title>
<meta property="og:type" content="object">
<meta property="og:image" content="https://repository-images.githubusercontent.com/937380800/32d2e3d9-26a4-4fc1-b79c-497865a5176d">
<meta property="og:image:alt" content="Leigh Bowery wearing a floral dress, matching face mask, and Pickelhaube">
<meta property="og:title" content="{title}">
<meta property="og:description" content="A comprehensive report on the contents of the EU's DSA transparency database">
<meta property="og:article:published_time" content="{time}">
"""

DOC_SCRIPTS = """\
<script src="https://cdn.jsdelivr.net/npm/vega@6"></script>
<script src="https://cdn.jsdelivr.net/npm/vega-lite@6"></script>
<script src="https://cdn.jsdelivr.net/npm/vega-embed@7"></script>
"""

DOC_STYLE = """\
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
    margin: 3rem 0.5rem;
}

svg {
    display: block;
}
.vega-embed {
    display: block !important;
}

main > :where(details, div, h1, h2, h3, ol, p, svg, table, ul, .vega-embed) {
    margin-left: auto;
    margin-right: auto;
}

main > :where(details, div, h1, h2, h3, ol, p, ul) {
    max-width:  75rch;
}
main > :where(table) { max-width:  90rch; }
main > :where(.vega-embed) { width: 100rch; }
main > :where(svg)   { max-width: 100rch; }

h2 {
    margin-top: 3rem;
}

:where(div, script, svg, table) + :where(div, svg, table) {
    margin-top: 2.5rem;
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
    border-top: solid 0.15em var(--black);
}
tbody > tr:first-of-type > :where(th, td) {
    padding-top: 0.35em;
}
tbody > tr:nth-child(even) {
    background: #f0f0f0
}
td {
    font-variant-numeric: tabular-nums;
}
:where(.left-except-2) :where(th, td) {
    text-align: left;
}
:where(.left-except-2) :where(th, td):nth-child(2) {
    text-align: right;
}
:where(.right-except-2, .right-except-2-3) :where(th, td) {
    text-align: right;
}
:where(.right-except-2, .right-except-2-3) :where(th, td):nth-child(2) {
    text-align: left;
}
:where(.right-except-2-3) :where(th, td):nth-child(3) {
    text-align: left;
}
tbody > tr.highlight > td {
    text-align: center;
}
</style>
"""

CHART_OPTIONS = '{"renderer": "canvas", "actions": true}'

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
    charts = storage.staging_root / "charts" / coverage.stem()
    if charts.exists():
        shutil.rmtree(charts)
    charts.mkdir(parents=True)

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
    def chart(self, name: str, chart: str | Chart) -> None: ...


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

    def chart(self, name: str, chart: str | Chart) -> None:
        if isinstance(chart, str):
            (self._charts / name).write_text(chart, encoding="utf8")
        else:
            chart.save(self._charts / name)


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

        def chart(self, name: str, chart: str | Chart) -> None:
            display(chart) # pyright: ignore[reportOptionalCall]
            if isinstance(chart, str):
                (self._charts / name).write_text(chart, encoding="utf8")
            else:
                chart.save(self._charts / name)


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
        self._chart_num = 0
        self._is_meta = False

    def emit_interactive_charts(self) -> bool:
        return False

    def timeline_width(self) -> int | str:
        return TIMELINE_WIDTH

    def timeline_height(self, short: bool = False) -> int:
        return TIMELINE_HEIGHT * (2 // 3 if short else 1)

    def has_all_sors(self) -> bool:
        return self._coverage.category is None

    def is_monthly(self) -> bool:
        return self._frequency == "monthly"

    @staticmethod
    def configure_display() -> None:
        alt.theme.enable("default")

        from .tool import configure_printing
        configure_printing()

    def secno(self) -> int:
        self._section_num += 1
        return self._section_num

    def chartno(self) -> str:
        self._chart_num += 1
        return f"{self._chart_num:03d}"

    def html(self, markup: str) -> None:
        if self._renderer.plain and (hn := HTML_HEADLINE.fullmatch(markup)) is not None:
            self._renderer.md(f"{'#' * int(hn.group(1))} {hn.group(2)}")
        else:
            self._renderer.html(FRAME_STYLE.sub("", markup))

        assert self._document is not None
        self._document.write(markup)
        self._document.write("\n\n\n")

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
            self._document.write("\n\n\n")
            return

        summary = hn.group(2)
        html = html[len(hn.group(0)):]
        self._document.write("<details>\n")
        self._document.write(f"<summary>{summary}</summary>\n")
        self._document.write(html)
        self._document.write("</details>\n\n\n")

    def frame(
        self,
        frame: pl.DataFrame,
        caption: None | str = None,
        klass: None | str = None,
        with_index: bool = True,
        with_head: bool = True,
    ) -> None:
        if with_index:
            frame = frame.with_row_index(offset=1)
        self._renderer.frame(frame)

        assert self._document is not None
        html = frame._repr_html_().strip()
        if html.startswith("<div>") and html.endswith("</div>"):
            html = html[len("<div>"): -len("</div>")]
        html = FRAME_BORDER.sub("", html)
        table_head = '<table>' if klass is None else f'<table class="{klass}">\n'
        if caption is not None:
            table_head += f'<caption>{caption}</caption>\n'
        html = FRAME_CLASS.sub(table_head, html)
        html = FRAME_QUOT.sub("", html)
        html = FRAME_SHAPE.sub("", html)
        html = FRAME_STYLE.sub("", html)
        if not with_head:
            html = FRAME_HEAD.sub("", html)
        html = html.replace("<td>", "  <td>").replace("<th>", "  <th>")
        html = FRAME_EOL.sub(r"\1\n", html)
        html = html.replace("<td>null</td>", "<td></td>")

        self._document.write(html)
        self._document.write("\n\n\n")

    def chart(self, name: str, chart: Chart) -> None:
        if self.emit_interactive_charts():
            self.dynamic_chart(name, chart)
        else:
            self.svg_chart(name, chart)

    def svg_chart(self, name: str, chart: Chart) -> None:
        buffer = StringIO()
        chart.save(buffer, format="svg")
        svg = buffer.getvalue()

        filename = f"{self.chartno()}-{name}.svg"
        self._renderer.chart(filename, svg)

        if name != "keyword-pie":
            svg = SVG_ATTRIBUTES.sub("", svg)

        assert self._document is not None
        self._document.write(svg)
        self._document.write("\n\n\n")

    def dynamic_chart(self, name: str, chart: Chart) -> None:
        filename = f"{self.chartno()}-{name}.svg"
        self._renderer.chart(filename, chart)

        id = f"chart-{self.chartno()}-{name}"
        assert self._document is not None
        self._document.write(f'<div id={id}></div>\n')
        self._document.write('<script>\n')
        self._document.write(
            f'vegaEmbed("#{id}", {chart.to_json()}, {CHART_OPTIONS});\n'
        )
        self._document.write('</script>\n\n\n')

    # ==================================================================================

    def run(self) -> pl.DataFrame:
        self.configure_display()
        self.ingest()

        path = (self._storage.staging_root / f"{self._coverage.stem()}.html")
        with open(path, mode="w", encoding="utf8") as document:
            try:
                self._document = document
                title = self.render_head()
                self.render_intro(title)
                self.render_charts()
                self.render_tables()
                document.write(DOC_FOOTER)
            finally:
                self._document = None

        return self._statistics.frame()

    def ingest(self) -> None:
        path = self._storage.best_available_root / f"{self._coverage.stem()}.parquet"
        try:
            statistics = Statistics.read(path)
            _logger.info('using statistics file="%s"', path)
        except FileNotFoundError:
            _logger.info('using built-in statistics')
            statistics = Statistics.builtin()
            self._coverage = Coverage.of(statistics.date_range().dailies())

        # Capture frequency, tags, date range
        self._frequency = self._coverage.frequency()
        self._tags = get_tags(statistics.frame())
        self._date_range = statistics.date_range().intersection(
            self._coverage.date_range(), empty_ok=False
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
        self._keyword_usage = self._statistics.frame().lazy().filter(
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
        ).collect()

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
                ),
                maintain_order=True,
            ).agg(
                pl.lit(None, dtype=PlatformValueType).alias("platform"),
                *aggregates()
            )
        )

        # We want to show top_num platforms in addition to Meta's and selected ones
        top_num = 5
        select_platforms = ("X", "YouTube")

        top = self._statistics.frame().lazy().filter(
            predicate("rows", entity=None)
        ).group_by(
            pl.col("platform")
        ).agg(
            pl.col("count").sum()
        ).sort(
            "count", descending=True, maintain_order=True
        ).head(
            # Thanks to the len(...) terms, this selection must contain at least
            # top_num platforms in addition to Meta's and select platforms.
            top_num + len(MetaPlatforms) + len(select_platforms)
        ).collect(
        ).get_column(
            "platform"
        ).to_list()

        # Remove Meta's platforms, leaving at least top_num non-Meta platforms
        for platform in [*meta_platforms, *select_platforms]:
            if platform in top:
                del top[top.index(platform)]

        # Compose complete list
        self._top_platforms = (
            top[:top_num] + ["Meta", *meta_platforms, *select_platforms]
        )

    def render_head(self) -> str:
        _logger.debug('render HTML <head>')

        main_tag = self._tags[0]
        description = "All Data" if main_tag is None else humanize(main_tag)
        title = f"The DSA Transparency Database: {description}"

        self.html(DOC_HEAD.format(
            time=self._timestamp.isoformat(),
            title=title,
        ))
        if self.emit_interactive_charts():
            self.html(DOC_SCRIPTS)
        self.html(DOC_STYLE)
        self.html("</head>")

        return title

    def render_intro(self, title: str) -> None:
        _logger.debug('render introduction')
        main_tag = self._tags[0]

        self.html(f'<body>\n<main>')
        self.html(f'<h1>{title}</h1>')

        row = self._statistics.frame().lazy().select(
            pl.col("count").filter(predicate("batch_rows", tag=main_tag)).sum()
            .alias("batch_rows"),
            pl.col("count").filter(predicate("total_rows", tag=None)).sum()
            .alias("total_rows"),
            pl.col("platform").n_unique(),
            pl.col("end_date").max() - pl.col("start_date").min() + dt.timedelta(days=1),
            pl.col("text").filter(predicate(tag=main_tag)).is_null().not_().sum()
            .alias("other_entries"),
            pl.col("text").filter(predicate(tag=main_tag)).n_unique()
            .alias("unique_other_entries"),
        ).collect().row(0)
        batch_rows, total_rows, platform, days, other_entries, unique_other_entries = (
            row
        )

        tag_toc = "\n            ".join(
            f'<li><a href="#{t}">{humanize(cast(str, t))}</a></li>'
            for t in self._tags[1:]
        )
        platform_toc = "\n            ".join(
            f'<li><a href="#{p.lower().replace(" ", "_")}">{p}</a></li>'
            for p in self._top_platforms
        )
        self.html(
            f"""
            <ol>
            <li><a href="#intro">Introduction</a></li>
            <li><a href="#dailies">Daily Statements of Reasons</a></li>
            <li><a href="#platforms">The Platforms Filing SoRs</a></li>
            <li><a href="#sors">Monthly Categorizations</a></li>
            <li><a href="#outages">Per-Platform Outages</a></li>
            {tag_toc}
            {platform_toc}
            <li><a href="#data">Data Summary</a></li>
            <li><a href="#platform-ranking">Platform Ranking</a></li>
            <li><a href="#keyword-ranking">Keyword Ranking</a></li>
            <li><a href="#schemas">Schemas</a></li>
            </ol>
            """
        )

        secno = self.secno()
        self.html(f"<h2 id=intro>{secno}. Introduction</h2>")

        self.frame(
            pl.DataFrame({
                "Description": [
                    "covers",
                    "out of",
                    "submitted by",
                    "over",
                    "including",
                    "with",
                ],
                "Quantity": [
                    batch_rows,
                    total_rows,
                    platform,
                    days.days,
                    other_entries,
                    unique_other_entries,
                ],
                "Entity": [
                    "statements of reasons",
                    "statements of reasons",
                    "platforms",
                    "days",
                    "free-text entries",
                    "unique values",
                ],
            }),
            caption="This Report…",
            klass="left-except-2",
            with_index=False,
            with_head=False,
        )

        self.html(
            f"""
            <p><strong><a
            href="https://github.com/apparebit/shantay">Shantay</a></strong>
            created this report on {self._timestamp.date().isoformat()} at
            {self._timestamp.time().isoformat(timespec="seconds")}<br>
            based on data from the <a
            href="https://transparency.dsa.ec.europa.eu">DSA transparency
            database</a>.</p>

            <h3>{secno}.1. Platforms</h3>

            <ul>

            <li><p><strong>Platform-focused sections</strong> include a manually
            curated selection of platforms, i.e., all of Meta's platforms
            together, Meta's platforms individually, as well as X and YouTube,
            in sections 11&#8209;18. They also include the top five platforms by
            SoR volume ignoring the already included platforms in sections
            6&#8209;10.</p></li>

            <li><p><strong>Meta's platforms</strong> are Facebook, Instagram,
            Threads, WhatsApp, and some other Meta product(s). Seriously, the
            database entries for the latter are attributed to "Other Meta
            Platforms Ireland Limited-offered Products".</p></li>

            </ul>

            <h3>{secno}.2. Charts</h3>

            <ul>

            <li><p><strong>Bars are stacked</strong> by number of statements of
            reasons (SoRs) per category, with the category with the most SoRs at
            the bottom. The legend follows the opposite order from category with
            the most SoRs at the top.</p></li>

            <li><p><strong>Only categories with counts greater zero</strong> are
            included in bar charts. If a category is listed in the legend but
            not visible amongst bars, its counts are too small.</p></li>

            <li><p>Except for keywords and delays, <strong>all bar charts have
            the same x and y axis dimensions</strong>. That way, they are easier
            to compare. (This does not hold for the y-axis of the bottom panel
            in two-panel charts; see next item.)</p></li>

            <li><p><strong>Charts with two panels</strong> visualize the same
            breakdown of categories in both panels, except that the bottom panel
            omits the top-one to top-three categories and has its own y-axis
            range. That way, the bottom panel shows the bottom permille or
            percent of categories when the top panel does not.</p></li>

            <li><p><strong>Bars marked ⚠️</strong> represent outliers that go
            beyond the coordinate grid. To avoid charts with a few very large
            and many small, hardly readable bars, Shantay clamps the y-axis
            under certain circumstances. However, in practice, the second panel
            tends to be more effective at ensuring that most categories are
            visible.</p></li>

            </ul>
            """
        )

    def render_tables(self) -> None:
        _logger.debug('render data tables')

        self.html(f"<h2 id=data>{self.secno()}. The Data</h2>")
        self.markdown(self._statistics.summary(markdown=True))

        self.html(f"<h2 id=platform-ranking>{self.secno()}. Platform Ranking</h2>")
        table = self._statistics.frame().lazy().filter(
            predicate("rows", entity=None, tag=self._tags[0])
        ).group_by(
            "platform"
        ).agg(
            pl.col("count").sum()
        ).sort(
            "count", descending=True
        ).collect()

        self.frame(table, klass="right-except-2")

        self.html(f"<h2 id=keyword-ranking>{self.secno()}. Keyword Ranking</h2>")
        self.html(
            '''\
<p>The percentage for the "null" keyword denotes the fraction of <em>all</em> SoRs,
whereas all other percentages denote fractions of SoRs with keywords only.</p>
            ''')
        self.frame(self._keyword_usage, klass="right-except-2")
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
        self.chart("daily-sors", self.daily_statements_of_reasons(tag=main_tag))
        self.chart(
            "daily-and-monthly-sors",
            self.daily_statements_of_reasons(tag=main_tag, with_monthly_sum=True),
        )
        self.chart("daily-sors-rolling-mean", alt.vconcat(
            self.daily_statements_of_reasons(tag=main_tag, rolling_mean_days=7),
            self.daily_statements_of_reasons(tag=main_tag, rolling_mean_days=30),
            spacing=SPACING,
        ).resolve_scale(
            x="shared",
            color="independent",
        ))

        self.chart("daily-sors-with-keywords", alt.vconcat(
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
        self.chart("platforms", self.cumulative_platform_counts())

        self.chart("sors-by-platform", alt.vconcat(
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
            self.chart("keywords-by-platform", alt.vconcat(
                self.overall_keyword_usage_by_platform(percent=True, tag=main_tag),
                self.overall_keyword_usage_by_platform(percent=False, tag=main_tag),
                spacing=SPACING,
            ).resolve_scale(color='independent'))

        self.html(f"<h2 id=sors>{self.secno()}. The Statements of Reasons</h2>")
        self.render_standard_timelines(None, tag=main_tag)

        self.html(f"<h2 id=outages>{self.secno()}. Outages</h2>")
        self.render_outages()

        for tag in self._tags[1:]:
            assert tag is not None
            _logger.debug('render charts tag="%s"', tag)
            self.html(f"<h2 id={tag}>{self.secno()}. Focus on {humanize(tag)}</h2>")
            self.render_standard_timelines(
                file_stem_for(tag), tag=tag
            )

        for platform in self._top_platforms:
            self.render_platform(platform)

    def render_platform(self, platform: str) -> None:
        main_tag = self._tags[0]
        _logger.debug(
            'render charts tag="%s", platform="%s"',
            "" if main_tag is None else main_tag,
            platform
        )

        platform_id = platform.lower().replace(" ", "_")
        self.html(f"<h2 id={platform_id}>{self.secno()}. {platform}</h2>")

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

        filter = {"tag": main_tag}
        if effective_platform is not None:
            filter["platform"] = effective_platform

        if self._statistics.frame().filter(predicate(**filter)).height == 0:
            _logger.debug('due to lack of data, skipping platform="%s"', platform)
            self.html("<p>No data available for platform</p>")
            return

        try:
            self.chart(
                f"{file_stem_for(platform)}-daily-sors",
                self.daily_statements_of_reasons(
                    tag=main_tag,
                    platform=platform,
                    use_rows_as_source=platform == "Meta",
                )
            )
            self.render_standard_timelines(
                file_stem_for(platform), tag=main_tag, platform=effective_platform
            )
        finally:
            if platform == "Meta":
                assert stats is not None
                self._statistics = stats
                self._with_cutoff = cutoff
                self._is_meta = False

    def render_standard_timelines(
        self, prefix: None | str, tag: None | str = None, platform: None | str = None
    ) -> None:
        metrics: list[MetricDeclaration | str | tuple[str, str]] = [
            StatementCountMetric
        ]
        if tag is None:
            metrics.append(CategoryMetric)
        metrics.extend([
            "keywords",
            ("Keyword: Other", "category_specification_other"),
            ContentLanguageMetric,
            TerritorialScopeMetric,
            ContentTypeMetric,
            ("Content Type: Other", "content_type_other"),
            DecisionGroundMetric,
            IncompatibleContentIllegalMetric,
            DecisionTypeMetric,
            DecisionVisibilityMetric,
            ("Visibility Decision: Other", "decision_visibility_other"),
            DecisionProvisionMetric,
            DecisionMonetaryMetric,
            ("Monetary Decision: Other", "decision_monetary_other"),
            AccountTypeMetric,
            DecisionAccountMetric,
            InformationSourceMetric,
            AutomatedDetectionMetric,
            AutomatedDecisionMetric,
            ProcessingDelayMetric,
            "moderation-delays",
        ])

        for metric in metrics:
            name = None

            if isinstance(metric, tuple):
                caption, column = metric
                self.frame(
                    self.text_usage(column, tag, platform),
                    caption=caption,
                    klass="right-except-2-3",
                )
                continue

            elif metric == "keywords":
                metric = self._keyword_metric.without_null()
                chart = self.render_timeline(metric, tag, platform, allow_cutoff=False)

            elif metric == "moderation-delays":
                metric = ModerationDelayMetric
                chart = self.render_moderation_delays(tag, platform)

            else:
                assert isinstance(metric, MetricDeclaration)
                chart = self.render_timeline(metric, tag, platform, allow_cutoff=True)

            if chart is None:
                self.html(
                    f"<p><em>No Data Available on {metric.label}!</em></p>"
                )
                continue

            if metric is ProcessingDelayMetric:
                name = "delays"
            else:
                assert isinstance(metric.field, str)
                name = metric.field.replace("_", "-")
            if prefix is not None:
                name = f"{prefix}-{name}"
            self.chart(name, chart)

    # ==================================================================================

    def daily_statements_of_reasons(
        self,
        *,
        rolling_mean_days: None | int = None,
        percentage: bool = False,
        tag: None | str = None,
        platform: None | str = None,
        use_rows_as_source: bool = False,
        with_monthly_sum: bool = False,
    ) -> alt.Chart | alt.LayerChart:
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
            filter = predicate("total_rows", tag=None).or_(
                predicate("batch_rows", tag=tag)
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

        monthly_table = None
        if with_monthly_sum:
            monthly_table = table.group_by(
                pl.col("start_date").dt.year().alias("year"),
                pl.col("start_date").dt.month().alias("month"),
                maintain_order=True,
            ).agg(
                pl.col("start_date").first().dt.month_start().dt.offset_by("5d"),
                pl.col("start_date").first().dt.month_end().dt.offset_by("-5d").alias("end_date"),
                pl.col(source).sum(),
            )

        title = "Statements of Reasons — "
        if platform is not None:
            title = f"{platform}: {title}"
        if rolling_mean_days is None:
            if percentage:
                title += "Daily Percentage"
            elif with_monthly_sum:
                title += "Daily and Monthly Counts"
            else:
                title += "Daily Counts"
        else:
            title += f"{rolling_mean_days}-Day Rolling "
            title += "Percentage" if percentage else "Mean"

        if rolling_mean_days is None and not with_monthly_sum:
            chart = alt.Chart(table, title=title).mark_bar(color=GREEN, size=1.3)
        else:
            chart = alt.Chart(table, title=title).mark_line(color=GREEN, size=1.5)

        daily_axis = "Statements of Reasons (Thousands)"
        if with_monthly_sum:
            daily_axis = "Daily " + daily_axis
        chart = chart.encode(
            alt.X("start_date:T").scale(domain=self._date_range.to_limits()).title("Date"),
            alt.Y(f"{source}:Q").title(daily_axis),
        ).properties(
            width=self.timeline_width(),
            height=self.timeline_height(),
        )

        if with_monthly_sum:
            assert monthly_table is not None
            monthly = alt.Chart(monthly_table, title=title).mark_bar(
                color=f"{CYAN}A0",
            ).encode(
                alt.X("start_date:T"),
                alt.X2("end_date:T"),
                alt.Y(f"sum({source}):Q").title(
                    "Monthly Statements of Reasons (Thousands)"
                ),
            )

            chart = alt.layer(monthly, chart).resolve_scale(
                y="independent",
                color="independent"
            )

        return chart

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
            alt.Y("pct:Q").title("Percent (Statements of Reasons)"),
            alt.Color("Kind:N").scale(
                domain=column_names,
                range=[BLUE, RED],
            ),
        ).properties(
            width=self.timeline_width(),
            height=self.timeline_height(),
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
            color=LIGHT_BLUE,
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
            color=BLUE,
        )

        chart = monthly_chart + daily_chart + label
        return chart

    # ----------------------------------------------------------------------------------

    def render_timeline(
        self,
        spec: MetricDeclaration,
        tag: None | str = None,
        platform: None | str = None,
        allow_cutoff: bool = True,
    ) -> None | alt.Chart | alt.LayerChart | alt.VConcatChart:
        table = self.timeline_data(spec, tag=tag, platform=platform)

        if not spec.has_variants():
            return self.timeline_chart(
                spec,
                table,
                tag=tag,
                platform=platform,
                allow_cutoff=allow_cutoff
            )

        ranking = self.variant_ranking(spec, table)
        categories = ranking.height
        if categories == 0:
            return None

        pct = ranking.get_column("cum_pct")
        if categories > 1 and pct[0] > 95:
            cut = 1
        elif categories > 2 and pct[1] > 90:
            cut = 2
        elif categories > 3 and pct[2] > 75:
            cut = 3
        else:
            cut = None

        spec2, table2 = self.apply_ranking(
            spec, table, ranking.get_column(spec.selector)
        )
        chart = self.timeline_chart(
            spec2,
            table2,
            tag=tag,
            platform=platform,
            allow_cutoff=allow_cutoff,
            with_x_title=cut is None,
        )

        if cut is None:
            return chart

        spec2, table2 = self.apply_ranking(
            spec, table, ranking.get_column(spec.selector).to_list()[cut:]
        )
        chart2 = self.timeline_chart(
            spec2,
            table2,
            tag=tag,
            platform=platform,
            allow_cutoff=False,
            with_title=False,
            with_full_height=False,
        )

        return alt.vconcat(chart, chart2, spacing=0).resolve_scale(
            x="shared",
            color="shared",
        )

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

        table = self._statistics.frame().lazy().filter(
            predicate(**filters)
        )

        if self.is_monthly():
            table = table.group_by(
                pl.col("start_date").dt.year().alias("year"),
                pl.col("start_date").dt.month().alias("month"),
                *spec.groupings(),
                maintain_order=True,
            ).agg(
                pl.col("start_date").first().dt.month_start().dt.offset_by("5d"),
                pl.col("start_date").first().dt.month_start().dt.offset_by("14d")
                .alias("mid_date"),
                pl.col("start_date").first().dt.month_end().dt.offset_by("-5d")
                .alias("end_date"),
                *aggregates(),
            )

            total_counts = table.group_by(
                pl.col("year", "month"),
                maintain_order=True,
            ).agg(
                pl.col("count").sum().alias("total_count")
            )

            table = table.join(
                total_counts, on=["year", "month"], how="left"
            )
        else:
            table = table.group_by(
                pl.col("start_date"),
                *spec.groupings(),
                maintain_order=True,
            ).agg(
                *aggregates(),
            )

            total_counts = table.group_by(
                pl.col("start_date"),
                maintain_order=True,
            ).agg(
                pl.col("count").sum().alias("total_count")
            )

            table = table.join(
                total_counts, on="start_date", how="left"
            )

        if spec.has_variants():
            table = table.with_columns(
                pl.col(spec.selector)
                .cast(pl.String)
                .replace(spec.replacements())
                .alias("variant_label")
            )
        elif spec.quantity == "count":
            table = table.with_columns(
                pl.col("count")
                .map_elements(minify, return_dtype=pl.String)
                .alias("data_label")
            )

        if spec.quantity == "mean" and spec.is_duration():
            table = table.with_columns(
                pl.col(spec.quantity) / (24 * 60 * 60)
            )

        return table.collect()

    def variant_ranking(
        self,
        spec: MetricDeclaration,
        table: pl.DataFrame,
    ) -> pl.DataFrame:
        """Rank variants by overall popularity."""
        assert spec.has_variants()

        ranking = table.lazy().group_by(
            spec.selector,
            maintain_order=True,
        ).agg(
            *aggregates()
        )

        if spec.quantity == "count":
            ranking = ranking.filter(
                pl.col(spec.quantity).gt(0)
            )
        else:
            ranking = ranking.filter(
                pl.col(spec.quantity).is_not_null()
            )

        return ranking.sort(
            spec.quantity, descending=True
        ).with_columns(
            pl.col(spec.quantity).sum().alias("total"),
        ).with_columns(
            (pl.col(spec.quantity) / pl.col("total") * 100).alias("pct"),
        ).with_columns(
            pl.col("pct").cum_sum().alias("cum_pct")
        ).collect()

    def apply_ranking(
        self, spec: MetricDeclaration, table: pl.DataFrame, names: Iterable[None | str]
    ) -> tuple[MetricDeclaration, pl.DataFrame]:
        """Apply the ranking to the metric and its data."""
        spec2 = spec.with_variants(names, use_palette=spec.has_many_variants())

        actual_names = [n for n in names if n is not None]
        filter = pl.col(spec.selector).is_in(actual_names)
        if spec2.has_null_variant():
            filter = filter.or_(pl.col(spec.selector).is_null())
        table2 = table.filter(filter)
        return spec2, table2

    def timeline_chart(
        self,
        spec: MetricDeclaration,
        table: pl.DataFrame,
        tag: None | str = None,
        platform: None | str = None,
        allow_cutoff: bool = True,
        with_title: bool = True,
        with_x_title: bool = True,
        with_full_height: bool = True,
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

        # X-Axis
        xtitle = ("Month" if self.is_monthly() else "Day") if with_x_title else None
        encoding: list[Any] = [
            alt.X("start_date:T").scale(domain=self._date_range.to_limits())
            .title(xtitle),
        ]
        if self.is_monthly():
            encoding.append(alt.X2("end_date:T").title(""))

        # Variant Order
        order = spec.variant_labels() if spec.has_variants() else alt.Undefined

        # Y-Axis
        yaxis = alt.Y(f"sum({spec.quantity}):Q", sort=order).title(spec.quant_label)

        cutoff = None
        signage = None
        if (
            self._with_cutoff
            and allow_cutoff
            and self.is_monthly()
            and spec.quantity == "count"
            and platform is None
        ):
            if tag is None:
                cutoff = 2_000_000_000
            elif tag == StatementCategoryProtectionOfMinors:
                cutoff = 5_000_000

            if cutoff is not None:
                yaxis = yaxis.scale(domain=(0, cutoff), clamp=True)
                signage = self.warning_signage(cutoff, table)

        encoding.append(yaxis)

        # (Variant) Colors
        mark_props = {}
        if spec.has_variants():
            encoding.append(
                alt.Color(f"variant_label:N", sort=order).scale(
                    domain=spec.variant_labels(),
                    range=spec.variant_colors(),
                ).title(spec.label),
            )
            encoding.append(
                alt.Order("color_variant_label_sort_index:Q")
            )
        else:
            mark_props["color"] = GRAY

        # Title
        title = spec.label
        if platform is not None:
            title = f"{platform}: {title}"
        elif self._is_meta:
            title = f"Meta: {title}"
        if tag is not None:
            title += f" for {humanize(tag)}"
        title += f" — {"Monthly" if self.is_monthly() else "Daily"} {quantity}"

        # Base Chart
        base = alt.Chart(
            table,
            title=title if with_title else alt.Undefined,
        ).encode(
            *encoding
        ).properties(
            width=self.timeline_width(),
            height=self.timeline_height(not with_full_height),
        )

        # Charts
        if self.is_monthly():
            chart = base.mark_bar(**mark_props)

            if spec is StatementCountMetric:
                labels = base.encode(
                    alt.X("mid_date:T"),
                    alt.Text("data_label")
                ).mark_text(
                    dy=-8,
                    align="center",
                    fontSize=10,
                )
                chart = chart + labels
        elif not spec.has_variants():
            chart = base.mark_line(**mark_props)
        else:
            chart = base.mark_area(**mark_props)

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

        if with_full_height and spec.quantity == "mean" and spec.label == "Delays":
            chart = chart + self.processing_delay_signage(tag, platform)

        return chart

    def warning_signage(self, cutoff: int, frame: pl.DataFrame) -> pl.DataFrame:
        return frame.with_columns(
            pl.lit(cutoff).alias("cutoff"),
            pl.when(
                pl.col("total_count").gt(cutoff)
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

        table = self._statistics.frame().lazy()
        total = pl.concat([
            table.filter(
                predicate(column="moderation_delay", **constraints)
            ).select(
                weighted_mean.alias("moderation")
            ),
            table.filter(
                predicate(column="disclosure_delay", **constraints)
            ).select(
                weighted_mean.alias("disclosure")
            ),
        ], how="horizontal").collect()

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

    # ----------------------------------------------------------------------------------

    def render_moderation_delays(
        self,
        tag: None | str = None,
        platform: None | str = None,
    ) -> None | alt.VConcatChart:
        table = self._statistics.frame().lazy().filter(
            predicate(
                "moderation_delay",
                entity=None,
                variant=None,
                tag=tag,
                platform=NO_ARGUMENT_PROVIDED if platform is None else platform,
            )
        ).group_by(
            pl.col("start_date").dt.year().alias("year"),
            pl.col("start_date").dt.month().alias("month"),
            maintain_order=True,
        ).agg(
            pl.col("start_date").first().dt.month_start().dt.offset_by("5d"),
            pl.col("start_date").first().dt.month_start().dt.offset_by("14d")
            .alias("mid_date"),
            pl.col("start_date").first().dt.month_end().dt.offset_by("-5d")
            .alias("end_date"),
            *aggregates(),
        ).with_columns(
            pl.col("min", "mean", "max") / (24 * 60 * 60)
        ).collect()

        if table.select(pl.col("mean").sum()).item() == 0:
            return None

        title = "Moderation Delay"
        if platform is not None:
            title = f"{platform}: {title}"
        elif self._is_meta:
            title = f"Meta: {title}"
        if tag is not None:
            title += f" for {humanize(tag)}"
        title += " — Monthly Mean & Maximum"

        max = alt.Chart(table, title=title).encode(
            alt.X("start_date:T").scale(domain=self._date_range.to_limits()).title(None),
            alt.X2("end_date:T"),
            alt.Y("sum(max):Q").title("Maximum Days"),
        ).mark_bar(
            color=BLUE,
        ).properties(
            width=self.timeline_width(),
            height=self.timeline_height() * 2 // 3,
        )

        mean = alt.Chart(table).encode(
            alt.X("start_date:T").title("Month"),
            alt.X2("end_date:T"),
            alt.Y("sum(mean):Q").title("Mean Days"),
        ).mark_bar(
            color=LIGHT_BLUE,
        ).properties(
            width=self.timeline_width(),
            height=self.timeline_height() // 2,
        )

        return alt.vconcat(max, mean, spacing=5).resolve_scale(
            x="shared",
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
                width=self.timeline_width(),
                height=self.timeline_height(),
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
            width=self.timeline_width(),
            height=self.timeline_height(),
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
        y_title = (
            "Percent (Statements of Reasons)" if percent else "Statements of Reasons"
        )

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
            width=self.timeline_width(),
            height=self.timeline_height(),
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

    def render_outages(self) -> None:
        outages = self._statistics.frame().filter(
            pl.col("column").eq("rows")
        ).group_by(
            pl.col("start_date"),
            pl.col("platform"),
            maintain_order=True,
        ).agg(
            pl.col("count").sum()
        ).sort(
            pl.col("start_date")
        ).group_by(
            pl.col("platform"),
            maintain_order=True,
        ).agg(
            pl.len().alias("days_with_sors"),
            pl.col("start_date").filter(
                pl.col("start_date")
                .ne(pl.col("start_date").shift(-1).dt.offset_by("-1d"))
            ).alias("before_outage"),
            pl.col("start_date").filter(
                pl.col("start_date")
                .ne(pl.col("start_date").shift(1).dt.offset_by("1d"))
            ).alias("after_outage"),
        ).explode(
            ["before_outage", "after_outage"]
        ).drop_nulls(
        ).with_columns(
            (pl.col("after_outage") - pl.col("before_outage") - dt.timedelta(days=1))
            .alias("duration")
        )

        summary = outages.group_by(
            pl.col("platform")
        ).agg(
            pl.len().alias("outage_count"),
            pl.col("duration").sum().dt.total_days().alias("days_without_sors"),
            pl.col("days_with_sors").first(),
        ).with_columns(
            (pl.col("days_without_sors") / pl.col("days_with_sors") < 0.2)
            .alias("has_real_outages")
        ).with_columns(
            pl.col("platform").cast(pl.String).str.to_lowercase().alias("sortkey")
        ).sort(
            pl.col("sortkey")
        ).select(
            pl.exclude("sortkey")
        )

        outages = outages.join(
            summary.select(pl.col("platform", "has_real_outages")),
            on="platform",
            how="left"
        ).filter(
            pl.col("has_real_outages") & pl.col("duration").dt.total_days().gt(1)
        ).with_columns(
            pl.col("platform").cast(pl.String).str.to_lowercase().alias("sortkey")
        ).sort(
            pl.col("sortkey", "before_outage")
        ).select(
            pl.exclude("sortkey")
        )

        self.html(
            """
            <p><strong>An outage</strong> is a period of at least a day for
            which a platform did not report any SoRs, despite reporting SoRs
            before and after that period as well as having at least five times
            more days with SoRs than without.</p>
            """
        )

        self.frame(
            summary,
            caption="Platforms and Days with/without SoRs",
            klass="right-except-2"
        )
        self.frame(
            outages,
            caption="Outages of More Than One Day",
            klass="right-except-2"
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
