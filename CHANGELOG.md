# Version History for Shantay

## v0.x.x (TBD)

Improved handling of platform names:
  - Use canonical mapping to remove parentheses from "Apple Books ~~(ebooks)~~"
  - Reapply latest canonical mapping for platform names when reading statistics
  - Enforce that new platform names are well-formed
  - Do not rely on `eval()` when updating `_platform` module

Improved visualizations:
  - Increase threshold for frequently used keywords to 1%
  - Display all keywords when charting platforms' keyword use in percent for
    working data
  - Actually chart total statement counts and their rolling mean for full archive
  - Lower cut-off for statement counts to 90,000,000 statements per day in
    detail charts

Other, miscellaneous changes:
  - Merge metadata from all three root directories (if they exist)
  - Filter out negative durations and track their number
  - Remove hard-coded assumptions about statistics being daily or monthly


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
