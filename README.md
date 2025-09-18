# Shantay

*Shantay* is a [permissively
licensed](https://github.com/apparebit/shantay/blob/boss/LICENSE), open-source,
Python-based command line tool for analyzing the European Commission's [DSA
transparency database](https://transparency.dsa.ec.europa.eu/). That database
collects the anonymized statements of reasons for online platforms' content
moderation decision. Even though the database is huge, almost 2 TB and growing,
*shantay* runs on consumer hardware. All you need is a USB drive, such as the 2
TB Samsung T7, that is large enough to store the full database and, for some
tasks, patience, as they may take a day, or two.

I've written [a blog post about my initial
impressions](https://apparebit.com/blog/2025/sashay-shantay) of the DSA
transparency database. Let's just say that, Brussels, we've got, uhm, problems
(plural)!


## 1. Getting Started

*Shantay*'s Python package is [distributed through
PyPI](https://pypi.org/project/shantay/). Hence, you can use a Python tool
runner such as [pipx](https://github.com/pypa/pipx) or
[uvx](https://docs.astral.sh/uv/guides/tools/) for executing *shantay* without
even installing it:

```bash
$ pipx shantay -h
```
or
```bash
$ uvx shantay -h
```

In either case, *shantay* will output its help message, which describes command
line options and tasks in detail. But to get you acquainted, here are some
choice examples.

The EU started operating the database on the 25th of September, 2023. To
download the daily releases for that year and determine their summary
statistics, execute:

```
$ uvx shantay --archive <directory> --last 2023-12-31 summarize
```

Except, you want to replace `<directory>` with the path to a directory suitable
for storing the complete database.

The previous command will run for quite a while, downloading and analyzing
release after release after release. Depending on your hardware, using more than
one process for downloading and analyzing the data may be faster. The following
invocation, for example, uses three worker processes for downloading and
analyzing data:

```
$ uvx shantay --archive <directory> --last 2023-12-31 --multiproc 3 summarize
```

Don't forget to replace `<directory>` with the actual path.

When running with parallel worker processes, *shantay*'s original process serves
as coordinator. Notably, it updates the status display on the console and writes
log entries to a file, by default `shantay.log` in the current working
directory.

Once *shantay* is done downloading and summarizing the daily releases for 2023,
you'll find a `db.parquet` file in the archive's root directory. It contains the
summary statistics at day-granularity. To visualize that same data, execute:

```
$ uvx shantay --archive <directory> visualize
```

Once finished, you'll find an
[`db.html`](https://apparebit.github.io/shantay/db.html) document with all
charts in the default staging directory `dsa-db-staging`. (The linked version
covers far more data.)

Alas, three months of data from the beginning of the DSA transparency database
aren't particularly satisfying. Shantay ships with a copy of the summary
statistics for the entire database. To visualize them, execute:

```
$ uvx shantay visualize
```

Now look at the [`db.html`](https://apparebit.github.io/shantay/db.html) again:
Much better!


## 2. Using Shantay

The `summarize` and `visualize` tasks cover almost all of Shantay's
functionality. Nonetheless, Shantay supports a few more tasks for fine-grained
control and data recovery. Here are all of them:

  - **download** makes sure that daily distributions are locally available,
    retrieving them as necessary. This task lets your prepare for future
    `--offline` operation by downloading archives as expediently as possible and
    not performing any other processing.

  - **distill** extracts a filtered subset from daily distributions. It requires
    both the `--archive` and `--extract` directories. For a new extract, it also
    requires one of these options:

      - `--category` to select a statement category such as
        `STATEMENT_CATEGORY_PROTECTION_OF_MINORS`.
      - `--platform` to select a platform; use further `--platform` options for
        selecting more than one.
      - `--filter` to select all statements matching a [Pola.rs filter
        expression](https://docs.pola.rs/user-guide/concepts/expressions-and-contexts/#filter);
        `pl` is the only available binding. Still, it would be folly to pass an
        untrusted string as filter argument.

    The filter and other metadata are stored in a JSON file in the extract
    directory.

  - **recover** scans the `--extract` directory to validate the files and
    restore (some of the) metadata.

  - **summarize** collects summary statistics either for the full database or a
    category-specific subset, depending on whether `--archive` only (for the
    full database) or both `--archive` and `--extract` (for a subset) are
    specified. If you specify neither, Shantay materializes the builtin copy of
    the summary statistics in staging. You can customize the summary statistics
    with the following options:

      - `--stratify-by-category` instructs Shantay to break down all statistics
        by both statement category and platform instead of platform only.
      - `--stratify-all-text` instructs Shantay to determine value counts for
        the DSA transparency DB's `illegal_content_legal_ground`,
        `illegal_content_explanation`, `incompatible_content_ground`,
        `incompatible_content_explanation`, and `decision_facts` columns instead
        of simply counting non-empty rows.

    *Beware*: Enabling either option by itself significantly increases the
    memory requirements for collected data, in case of `--stratify-all-text`
    quadrupling the size of the statistics. You may need to restrict the covered
    data range and/or the number of worker processes.

  - **info** prints helpful information about Shantay, key dependencies, the
    Python runtime, and operating system, as well as the `--archive` and
    `--extract` directories and their contents. If you specify neither, Shantay
    prints information about the builtin copy of the summary statistics.

  - **visualize** generates an HTML document that visualizes summary statistics.
    `--archive` and `--extract` determine the scope of the visualization, just
    as for `summarize`. If you specify neither, Shantay visualizes the builtin
    copy of the summary statistics. In addition to generating an HTML report,
    Shantay also saves all charts as SVG graphics.

    The HTML report includes subsections with platform-specific statistics. By
    default, Shantay automatically selects those platforms based on popularity.
    However, if an extract was configured with the `--platform` option, Shantay
    uses the platforms of the extract. In either case, `--platform` can be
    used to override this default behavior.

Unless the `--offline` option is specified, the `distill` and `summarize` tasks
download daily distributions as needed.

You can restrict the date range with `--first` and `--last`. By default, the
`--first` date is 2023-09-25, the day the DSA transparency database became
operational, and the `--last` date is three days before today—one day to allow
for the Americas being six to nine hours behind Europe and another two days to
allow for some posting delay.

Depending on available memory and processor cores, `--workers` may speed up
distillation and/or summarization. It takes the number of worker processes as
its only argument. Since Pola.rs is rather aggressive in its use of hardware
resources, the number of workers should probably be small, 2 or 3, no more.

Summary statistics are stored in `db.parquet` for the full database and in a
file named after the category, platform, or filter for distilled data. For
example, the summary statistics for the extract with category
`STATEMENT_CATEGORY_PROTECTION_OF_MINORS` are stored in
`protection-of-minors.parquet`. JSON metadata and HTML visualizations follow the
same naming convention.

Shantay's log distinguishes between `summarize-all`, `summarize-extract`, and
`summarize-builtin` when identifying tasks. Furthermore, even when executing a
category-specific `summarize` task, Shantay's log distinguishes `distill` from
`summarize-extract`. For multiprocessing, it schedules both tasks separately.


## 3. Organization of Storage

The screenshot below shows an example directory hierarchy under the `--extract`
root. It illustrates the directory levels discussed in 3.2 as well as the files
with digests and summary statistics discussed in 3.3.

![The extract root hierarchy](https://raw.githubusercontent.com/apparebit/shantay/boss/viz/screenshot/hierarchy.png)


### 3.1 Three Root Directories: Staging, Archive, Extract

*Shantay* distinguishes between three primary directories, `--staging` as
temporary storage, `--archive` for the original distributions, and `--extract`
for a filtered subset:

  - **Staging** stores data currently being processed, e.g., by uncompressing,
    converting, and filtering it. You wouldn't be wrong if you called this
    directory *temp* or *tmp* instead. This directory must be on a fast, local
    file system; it should not be on an external disk, particularly not if the
    disk is connected with USB.
  - **Archive** stores the original, daily ZIP files and their SHA1 digests. It
    is treated as append-only storage and holds the ground truth. This directory
    must be on a large file system, e.g., 2 TB just about holds all data from
    2023-09-25 into May 2025. This directory may be on an external drive (such
    as the already mentioned T7).
  - **Extract** stores parquet files with a (much) smaller subset of the
    database. Like *archive*, *extract* is treated as append-only storage.
    Unlike *archive*, which is unique, different runs of *shantay* may use
    different *extract* directories representing different subsets of the
    database.


### 3.2 Three Levels of Nested Directories: Year, Month, Day

Under the three root directories, *shantay* arranges files into a hierarchy of
directories, e.g., resulting in paths like
`2025/03/14/2025-03-14-00000.parquet`. The top level is named for years,
followed by two-digit months one level down, followed by two-digit days another
level down. Finally, daily archive files have their original names, whereas
files with distilled data are named after the date and a zero-based five-digit
index (as illustrated earlier in this paragraph).

For the extract root, *shantay* maintains a per-day digest file named
`sha256.txt`. It contains the SHA-256 digests for every parquet file in the
directory: Each line contains one hexadecimal ASCII digest, a space,and the
file's name.


### 3.3 Summary Statistics

In addition to yearly directories, *shantay* also stores the following two files
inside root directories.

  - A JSON file named after the filter with, for example,
    `protection-of-minors.json` containing an object whose `filter` property
    identifies the category `STATEMENT_CATEGORY_PROTECTION_OF_MINORS`. That same
    object also has a `releases` property with per-release metadata, including:

      - `batch_count` for the number of daily data files
      - `batch_rows` for the number of statements included in the statistics
      - `total_rows` for the number of statements before filtering
      - `sha256` for the (recursive) digest of the digests in the `sha256.txt`
        file, one for each daily data file

  - `db.parquet` contains the summary statistics about the full database.
    Statistics for distilled subsets are named after their filters. Each file
    basically is a non-tidy, long data frame that uses up to seven columns for
    identifying variables and up to four columns for identifying values. While
    an encoding with fewer columns is eminently feasible, the schema is
    optimized for being easy to work with (e.g., aggregations are trivial) and
    compact to store (e.g., a column with mostly nulls requires almost no
    space).

    The individual columns are:

      - `start_date` and `end_date` denote the date coverage of a row.
      - `tag` is the filter for distilled source data.
      - `platform` is the online platform making the disclosures.
      - `category` is the coarse statement category.
      - `column` is the original transparency database column, with a few
        virtual column names added.
      - `entity` describes the metric contained in that row.
      - `variant` captures values from the original database, encoded as a very
        large enumeration.
      - `text` does the same for transparency database columns with arbitrary
        text.
      - `count`, `min`, `mean`, and `max` contain the eponymous descriptive
        statistics.

    If `mean` contains a value, then `count` also contains a value, thus
    enabling correct aggregation with a weighted average.

    In theory, transparency database columns with arbitrary text let platforms
    provide granular detail about content moderation decisions. In practice,
    content moderation at scale pushes platforms towards standardizing free-form
    text as well. Alibaba appears to be the one exception, with mostly unique
    entries for these columns. Alas, that is the result of the firm including
    case-specific identifiers in the text. Since that causes Alibaba to account
    for 2/3 of rows in daily summary statistics, Shantay does not track
    Alibaba's entries for the worst offending transparency database columns.


## 4. Big Data in the Small

Unlike most big data tools, Shantay is designed to run on consumer-level
hardware. A reasonably fast laptop or desktop with an external flash drive, such
as the Samsung T7, should do. And it does do: My primary development machine is
a four-year-old x86 iMac, albeit with 10 processor cores and 128 GB RAM, and all
data is stored on a 4 TB Samsung T7 drive.

Shantay targets consumer-level hardware because transparency as an
accountability mechanism mustn't be limited to people who have access to compute
clusters, whether locally or in the cloud. No, for a transparency database to be
effective, anyone with a reasonable computer should be able to do their own
analysis.

That seeming limitation also is a blessing in disguise. Notably, the [EU's
official tool](https://code.europa.eu/dsa/transparency-database/dsa-tdb) uses
the [Apache Spark engine](https://spark.apache.org), which has excellent
scalability but also very high resource requirements for every cluster node. In
other words, while the EU's tool does run on individual machines, it also runs
very slowly. In contrast, Shantay builds on the [Pola.rs](https://pola.rs) data
frame library, which is much simpler and faster when running on a single
computer. In addition, Shantay makes the most of available resources and
supports parallel execution across a (small) number of processes, which does
make a difference in my experience.

### 4.1 Reliability Challenges

Fundamentally, big data in the small is only possible if both data and
computation can be broken into units small enough to be feasible on consumer
hardware. For the DSA transparency database, daily releases are still too big.
Thankfully, the EU already breaks them down further, distributing each daily
release as a zip file of zip files of CSV files. With each nested zip file of
CSV files maxing out at 100,000 rows or statements of reasons, such "chunks"
*are* manageable. Hence, Shantay sticks to that same data partitioning when
distilling or summarizing transparency data.

While that sounds straight-forward enough, doing so reliably can be challenging.
That may not sound surprising, since reliability also is a major challenge for
cluster-based systems. But whereas clusters, by their very design, can leverage
redundancy towards reliability, that isn't possible when targeting a single
computer.

Worse, some of the targeted hardware adds to the reliability challenges.
Notably, external USB drives are a cost-effective solution for providing the
necessary bulk storage. But those drives are at their most reliable and
performant for bulk-reads and -writes only. That is the primary reason for
Shantay using a *staging directory* that is is distinct from long-term, external
storage and should be located on the computer's internal drive.

However, since such internal drives are not dedicated to storing transparency
data, they also may only have 100-200 GB of free space. But with a 3.5 GB daily
release archive easily expanding into 70 GB of CSV data, it would take only 2 to
3 worker processes fully expanding daily releases to completely fill the
internal drive, which will trigger dire operating system warnings at best and a
crash at worst. Similarly, when processing transparency data, each worker needs
to handle a rapidly increasing amount of storage and can easily consume all
virtual memory—which does trigger an operating system crash.

These are not theoretical challenges. I have encountered them all during
development of Shantay.

### 4.2 The Implementation

To keep separate concerns actually separate, Shantay's implementation
distinguishes between code that orchestrates the data processing and code that
performs the actual data analysis:

 1. The higher-level orchestration code has its own data structures defined in
    `shantay.model` and `shantay.metadata`, a conventional single-threaded,
    single-process implementation in `shantay.processor`, and a multi-threaded,
    multi-process version in `shantay.multiprocessor` that delegates to the
    single-process version as much as possible. This code largely treats the
    data being processed as opaque blobs.

 2. Other than ingesting CSV files (with two different parsers), the lower-level
    data wrangling code handles data frames and hence makes liberal use of
    Pola.rs. It is spread over three modules, with `shantay.dsa_sor` ingesting
    transparency DB releases, `shantay.stats` extracting summary statistics from
    the transparency data, and `shantay.viz` turning extracted statistics into
    detailed, illustration-heavy reports. In support, `shantay.schema` defines
    the necessary schemas and `shantay.framing` provides some commonly needed
    helper functions.

While both `shantay.stats` and `shantay.viz` do contain plenty of bespoke,
application-specific code, much of the extraction of summary statistics and
their visualization through timeseries graphs is rather mechanical and
repetitive. That makes it possible to concisely specify extraction and
visualization through type declarations. `shantay.schema.TRANSFORM` defines how
to process each column of transparency data and `shantay.schema.FIELDS` defines
how to visualize each metric. The `stats` and `viz` modules implement the
corresponding interpreters.

----

(C) 2025 by Robert Grimm. The Python source code in this repository has been
released as open source under the [Apache
2.0](https://github.com/apparebit/shantay/blob/boss/LICENSE) license.
