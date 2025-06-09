# Version History for Shantay

## v0.3.0 (TBD)

Shantay now collects richer statistics and produces more compelling
visualizations with a simpler interface. It can either process the full
transparency database or a category-specific subset for all the categories
supported by the database.


### Simpler Command Line Interface

Shantay now supports three primary tasks:

  - `extract` to prepare category-specific subsets of the transparency database.
  - `summarize` to collect statistics about the full database or
    category-specific subset.
  - `visualize` to prepare helpful charts illustrating previously collected
    statistics.

When processing the entire database, you provide the `--archive` directory for
storing original daily releases and global statistics. When processing a
category-specific subset, you provide the `--archive` as well as `--working`
directories, with the latter storing data and statistics for the subset. The
first invocation of `extract` requires the `--category`, too.

Shantay covers all available data by default. You can also restrict the date
range with `--first` and `--last`.

The `--filter`, `--root`, `--with-archive`, and `--with-working` options have
been removed. The `prepare` task is now called `extract` and the `analyze` task
has been subsumed by the `summarize` task.


### More compelling visualization

Amongst other changes, the HTML document produced by `visualize` is now named
after the category, e.g., `protection-of-minors.html`, when covering a subset
and `all-data.html` when covering the full database. It now starts with an
outline, includes graphs tracking the volume of daily statements of reasons
(SoRs) and breaking down the various attributes for (by default) monthly SoRs.
Where needed, bars are annotated with their numeric quantities and means; others
cut off outliers (clearly marked with ⚠️) to ensure good readability of the
majority of (stacked) bar graphs. Each summary repeats the same set of timelines
for overall SoRs, the top-three non-Meta platforms by SoRs, and Meta's platforms
(both in aggregate and individually). The visual appearance of the document has
also been improved.


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
first day, 2023-09-25, through 2025-06-04, inclusive. Likewise, platform names
are current as of that last date, 2025-06-04.


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
