# Version History for Shantay

## v0.3.0 (TBD)

This release substantially improves support for collecting and visualizing
summary statistics about the entire DSA transparency database as well as
specific statement categories. It also features a new, more compact schema for
summary statistics. The schema uses several explicit enumerations, as they are
easier to use and also more compact than Pola.rs' category types or strings.
However, one of those enumerations includes the platform names, which do churn
quite a bit. Currently, Shantay updates its local installation with newly
encountered names and asks the user to rerun Shantay with the same command line
options. Updates are atomic and additive only, so ordering and batching make no
difference, still yielding the same list.

Shantay's Python package ships with a copy of the summary statistics for the
entire DSA transparency database.

Improved handling of summary statistics:
  - Collect statistics with daily resolution for full dataset and subsets alike
  - User can select `--daily` instead of the default `--monthly` resolution when
    running `visualize`
  - The parquet files with the summary statistics for subsets now have distinct
    names that depend on the statement catgory used for deriving the subset,
    e.g., `protection-of-minors.parquet` for
    `STATEMENT_CATEGORY_PROTECTION_OF_MINORS`.

Simplified command line options:
  - The `--root` option has been removed. The `--archive` and `--working`
    options now are the only way of specifying root directories; they also
    are optional now, though most tasks require at least one of them.
  - The `--filter` option has been removed; the `--category` option is now the
    only option for selecting a subset of database entries.
  - The `--first` and `--last` options now work for all tasks; they also are
    validated to be within a realistic range
  - While users can still select `--with-archive` or `--with-working` for
    visualization, just providing the `--archive` or `--working` path suffices.

Improved handling of platform names:
  - Map some of the more unwieldy platform names to shorter versions. For
    instance, "Apple Books (ebooks)" becomes "Apple Books", "Quora Ireland
    Limited" becomes "Quora", and "eDarling, EliteSingles, SilverSingles, Zoosk"
    becomes "Spark Networks".
  - Reapply latest canonical mapping for platform names when reading statistics
  - Enforce that new platform names are well-formed
  - Do not rely on `eval()` when updating `_platform` module
  - Platform names are current as of 2025-05-31, including several that are mapped
    to simpler versions

Improved visualizations:
  - Increase threshold for frequently used keywords to 1%
  - Display all keywords when charting platforms' keyword use in percent for
    working data
  - Actually chart total statement counts and their rolling mean for full archive
  - Cut off outliers to ensure that the rest remains readable

Other, miscellaneous changes:
  - Merge metadata from all three root directories (if they exist)
  - Filter out negative durations and track their number


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
