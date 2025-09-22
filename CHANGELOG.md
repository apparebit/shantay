# Version History for Shantay


## v0.6.0 (September ??, 2025)

Shantay now covers all transparency DB columns in its summary statistics,
including the `content_id_ean` column introduced with the schema change of
2025-07-01. It also providing more control over the extent of transparency DB
extracts and the granularity of summary statistics. The implementation has been
restructured to require much less memory. Growth over time is much slower as
well.

### Cover All Transparency DB Columns

Shantay now also captures the free-text columns `content_id_ean`,
`decision_ground_reference_url`, `illegal_content_legal_ground`,
`illegal_content_explanation`, `incompatible_content_ground`,
`incompatible_content_explanation`, `decision_facts`, and `source_identity` in
its summary statistics, thus covering *all* DSA transparency DB columns.

In practice, most values in these string-valued columns are repeated many times,
just as for enum-valued columns. However, values in string-valued columns are
not known a-priori and their distribution has a very long tail of values that
are hardly repeated or even unique. As a result, memory requirements for summary
statistics quadruple when including value counts for the newly added columns.
Since the `content_id_ean`, `illegal_content_legal_ground`,
`illegal_content_explanation`, `incompatible_content_ground`,
`incompatible_content_explanation`, and `decision_facts` columns include the
most text while providing hardly any additional information, Shantay only tracks
the number of non-empty rows for these columns.

### Configure DB Extract and Summary Statistics

In addition to distilling the transparency DB by `--category`, Shantay now also
supports generating extracts for one or more `--platform`s or with an arbitrary
Pola.rs `--filter` expression. The latter may use the customary `pl` for
Pola.rs' namespace, but no other bindings, including for Python's builtins, are
available. Still, it would be folly to use an untrusted string as the `--filter`
argument.

With `--stratify-by-category`, Shantay collects summary statistics broken down
by platform and statement category instead of only by platform. With
`--stratify-all-text`, Shantay collects value counts for _all_ string-valued
columns, including the five columns explicitly exempted by default. Each of
these two options significantly increases Shantay's memory requirements. As a
result, you may have to reduce the date range or the number of worker processes.

### Lower Memory Requirements

Shantay used to create summary statistics by creating several small data frame
for each release and then combining them with the data frame with statistics for
all previous releases into one data frame. To ensure good performance, Shantay
also rechunked the data frame at that time, which ensures contiguous memory
allocation. Unfortunately, this approach resulted in fairly high memory
requirements, with worker processes requiring 40-60 GB of RAM for processing
half a year of transparency data.

With this release, Shantay uses a different approach that avoids having to
reallocate one very large memory segment. Now, worker processes quickly ramp up
to around 10 GB and then slow increase to around 17 GB after processing 22
months of transparency data. Under the new approach, Shantay processes each
batch belonging to a daily release independently, saving the result to disk, and
then combines all batch statistics into one data frame for each release, again
saving the result to disk. Once all releases have been processed, Shantay
combines the per-release frames into a complete data frame as well. For now, it
also preserves the per-release frames, which simplify incremental computation
including recovery from failures.

(The same measurements show that per-batch latency slowly increases as well,
from about 10s to 15s over 22 months. That strongly suggests to restart worker
processes every n processed releases.)

### Minor Improvements and Bug Fixes

This release also contains several minor improvements and bug fixes:

  * `--platform` can also be used to override Shantay's choice of platforms when
    visualizing results. By default, Shantay selects them by popularity.
  * Mark staging root directories for multiprocessing workers as `<name>.done`
    upon completion. For debuggability, they are not deleted at the time; though
    they are removed upon Shantay's next run with the same staging root.
  * Fix labelling of mean moderation and disclosure delay lines. Move mean
    disclosure delay to second, lower panel.
  * Fix crashing errors when cleaning up directories during archive download,
    when forwarding cancellations during multiprocessing, when just downloading
    archives with multiprocessing, and when the archive already contains the
    requested statistics.
  * Fix name of statistics file in tool help.


## v0.5.0 (July 28, 2025)

When given the new `--interactive-report` command line option, Shantay emits
JavaScript code for interactive charts instead of static SVG markup.

When given the new `--clamp-outliers` command line option, if one or two months
have an unusually large number of SoRs—currently, 1.9× more SoRs than the month
with the next largest number—Shantay clamps the monthly bars for the outlier
months. Clipped bars are clearly marked with a ⚠️.

Pola.rs v1.31.0 changed the behavior of `scan_csv()`. That broke Shantay's
second pass over CSV files while extracting category-specific data. While that
second pass seems unavoidable, this release dispenses with CSV parsing during
the second pass altogether and extracts the same statistics with a simple
textual scan. That makes for a more robust and, hopefully, faster implementation
as well. The first pass is not impacted by this change because it transparently
falls back onto a different CSV parser already.

This release further improves coverage and robustness of Shantay's testing
harness. It also enables CI with GitHub actions. The documentation of internal
APIs has been somewhat improved and can be automatically generated with
[Sphinx](https://www.sphinx-doc.org/en/master/).


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
