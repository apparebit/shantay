# Version History for Shantay

## v0.4.0 (July 14, 2025)

In addition to simplifying and refactoring the implementation, this release
makes the following changes:

  - When summarizing a category-specific subset of the DSA transparency
    database, stage the extracted Parquet files before processing them.
  - When summarizing the entire DSA transparency database, also generate a
    metadata file.
  - When distilling the database or when summarizing (part of) the database,
    consistently write the metadata and summary statistics to the archive or
    extract root only after completing the task.
  - Do not require metadata to check whether a release has already been
    distilled.
  - Remove table header from overview table at beginning of report introduction.
  - Add support for detail panel to timelines with moderation and disclosure
    delays; also add timeline with mean and maximum moderation delays.
  - Use more readable colors for timelines that show both daily and monthly
    statements of reasons, with or without keywords.
  - Extend the `info` task to emit the coverage of metadata and summary
    statistics in the staging directory as well.
  - Remove `batch_memory` entry from summary statistics.
    [fixmem.py](script/fixmem.py) patches existing metadata and summary
    statistics.
  - Add tests for summarizing the entire database as well as a category-specific
    subset. Ensure that tests work even if the list of platforms has been
    updated.


## v0.3.0 (June 24, 2025)

Shantay now collects richer statistics and produces far more compelling
visualizations with a simpler interface. It can either process the full
transparency database or a category-specific subset for all the categories
supported by the database.


### Simpler Command Line Interface

Shantay now has only two primary tasks, `summarize` to collect statistics about
the full database or some category-specific subset and `visualize` to illustrate
previously collected statistics in HTML reports. The `summarize` task also
downloads daily distributions and distills category-specific data as needed.

Shantay now stores daily distributions in the `--archive` directory and
category-specific data in the `--extract` directory.

For fine-grained control and data recovery, Shantay also supports the `download`
(new), `distill` (née `extract`), `info` (new), and `recover` tasks. The
`--offline` (new) and `--workers` (née `--multiproc`) options control resource
consumption. The `--first` and `--last` options restrict date coverage. See the
[project readme](https://github.com/apparebit/shantay/blob/boss/README.md) or
the `--help` output for further details.

The `--daily`, `--filter`, `--monthly`, `--root`, `--with-archive`, and
`--with-working` options have been removed. The`analyze` task has been subsumed
by the `summarize` task.


### More compelling visualization

Amongst other changes, the HTML document produced by `visualize` is now named
after the category, e.g.,
[`protection-of-minors.html`](https://apparebit.github.io/shantay/protection-of-minors.html),
when covering a subset and
[`db.html`](https://apparebit.github.io/shantay/db.html) when covering the full
database. It now starts with an outline, includes graphs tracking the volume of
daily statements of reasons (SoRs) and breaking down the various attributes for
(by default) monthly SoRs. Where needed, bars are annotated with their numeric
quantities and means; others cut off outliers (clearly marked with ⚠️) to ensure
good readability of the majority of (stacked) bar graphs. Each timeline only
includes categories that are actually present in the data and they are always
ordered from category with the most SoRs to least SoRs. Each summary repeats the
same set of timelines for overall SoRs, Meta's platforms, YouTube, as well as
the top-five platforms by SoRs not already mentioned. The appearance of the
document has also been improved. Alas, all that goodness adds quite a bit of
heft to the self-contained HTML document, which weighs in at 6-10 MB.


### Richer statistics

The summary statistics driving the visualization are collected with a daily
resolution and cover all transparency database properties with fixed, categorial
values as well as several properties with in theory arbitrary but in practice
ad-hoc categorical text. To minimize the storage required for summary
statistics, Shantay makes extensive use of [Pola.rs'
enumerations](https://docs.pola.rs/user-guide/expressions/categorical-data-and-enums/).

While generally straight-forward, including platform names in enumerations is a
bit tricky, since their number has been growing by almost 10 platforms per
month. To avoid putting releases on the critical path of users, Shantay
dynamically detects new platform names and updates its list of valid platform
names accordingly. The corresponding functionality is implemented by the
[`shantay._platform`](https://github.com/apparebit/shantay/blob/boss/shantay/_platform.py)
module, but should be accessed through the
[`shantay.schema`](https://github.com/apparebit/shantay/blob/boss/shantay/schema.py)
module. The file with the up-to-date platform names is
`~/.shantay/platforms.json` on Linux/macOS and uses an equivalent path on
Windows.

Shantay's Python package ships with a copy of the **summary statistics for the
entire DSA transparency database**. This version covers the database from its
first day, 2023-09-25, through 2025-06-09, inclusive. Likewise, platform names
are current as of that last date, 2025-06-09.


## v0.2.0 (May 1, 2025)

Shantay now supports analyzing either the entire transparency database, with the
`summarize` command, or a category-specific or otherwise filtered view, with the
`prepare` and `analyze` commands. In either case, the `visualize` command
produces timeline graphs about the collected statistics. Multiprocessing mode,
enabled with `--multiproc`, significantly speeds up `summarize` and `prepare`.

The underlying implementation leverages declarative data structures for
describing the schema of the transparency database, the computation of summary
statistics, and the visualization of the statistics. The implementation also
makes use of self-modifying code to incorporate new platforms without requiring
new package releases.


## v0.1.0 (March 1, 2025)

Initial release that extracts and analyzes a category-specific view of the
transparency database.
