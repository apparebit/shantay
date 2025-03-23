# Shantay

*Shantay* is a [permissively
licensed](https://github.com/apparebit/shantay/blob/boss/LICENSE), open-source,
Python-based command line tool for analyzing the European Commission's [DSA
transparency database](https://transparency.dsa.ec.europa.eu/). That database
contains granular records about online platforms' content moderation actions. To
support the broadest possible user base in exploring this dataset, which already
comprises 1.5 TB of compressed CSV files, *shantay* punts on traditional
techniques for scaling data processing. Instead, it is designed for minimizing
resource requirements. The tool makes do with what you can comfortably spare. In
other words, a regular laptop/desktop with a 2 TB Samsung T7 drive for long-term
storage will do (for now). While *shantay* is not concerned with traditional
notions of big data scalability, single node performance and developer
experience do matter. For that reason, *shantay* uses [Pola.rs](https://pola.rs)
as its data frame.

To calibrate expectations: Assuming that all data has already been downloaded,
my five-year-old iMac with one of those T7 drives takes maybe a day to extract
relevant records from the full dataset. For my use case, the Protection of
Minors, which comprise 0.3% of all records, it takes less than two minutes to
analyze the extracted working set. Finally, visualization of the analysis
results is nearly instantaneous, taking seconds at most.

Since extraction does not saturate my iMac's CPU or memory bus, I did integrate
process-based multiprocessing with *shantay*. It is enabled with the
`--multiproc` command line option. For the prepare task processing the archives
for 2024/5/2 and 5/3 either serially or in parallel, the serial version took 4.5
minutes and the parallel one took 2.6 minutes, yielding a speedup of 1.7x.

(I picked the two days because they both yield the same number of batch files
(39) with about the same number of rows (300,000) requiring roughly the same
memory (< 390 MB). The time for the parallel version does not include the time
to create the pool.)

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
> pipx shantay -h
```
or
```bash
> uvx shantay -h
```

In either case, *shantay* will output something like this:

```
usage: shantay [-h] [--root ROOT] [--archive ARCHIVE] [--working WORKING]
               [--staging STAGING] [--first FIRST] [--last LAST]
               [--filter FILTER] [--category CATEGORY] [--logfile LOGFILE]
               [--quiet] [--multiproc MULTIPROC]
               {recover,prepare,analyze,visualize}

positional arguments:
  {recover,prepare,analyze,visualize}
                        select the task to execute: recover validates parquet
                        files and restores metadata; prepare downloads
                        distributions and extracts working data; analyze
                        processes the working data; visualize graphs the
                        analysis results

options:
  -h, --help            show this help message and exit
  --multiproc MULTIPROC
                        use several processes for downloading archives and
                        extracting working data

data storage:
  --root ROOT           set directories for `archive` and working `data` to
                        the eponymous subdirectories
  --archive ARCHIVE     set directory for downloaded archives (`./dsa-db-
                        archive` by default)
  --working WORKING     set directory for parquet files with working data
                        (`./dsa-db-working` by default)
  --staging STAGING     set directory for temporary files (`./dsa_db-staging`
                        by default)

coverage of working set:
  --first FIRST         set the start date (2023-09-25 by default)
  --last LAST           set the stop date (the day before yesterday by
                        default)
  --filter FILTER       set the module name, colon, and global variable name
                        for the Pola.rsexpression filtering out all but the
                        data of interest
  --category CATEGORY   set category to filter (may omit the
                        STATEMENT_CATEGORY_ prefix and/oruse lower case)

logging:
  --logfile LOGFILE     set file receiving log output (`./shantay.log` by
                        default)
  --quiet               disable verbose logging, which is the default
```

You can use *shantay* as is, with the prepare task, for downloading daily
distributions and selecting a category of your choice. However, to analyze or
visualize the data in a category other than *Protection of Minors*, you'll need
to write your own domain-specific code. However, the
[`dsa_sor`](https://github.com/apparebit/shantay/blob/boss/shantay/dsa_sor.py)
and
[`framing`](https://github.com/apparebit/shantay/blob/boss/shantay/framing.py)
modules include a fair number of helper functions that are generally useful for
analyzing Statements of Reasons and hence reusable. The
[`viz`](https://github.com/apparebit/shantay/blob/boss/shantay/viz.py) module
also contains helper functions for generating charts. But so far, I have found
fewer opportunities for cleanly abstracting over common patterns.

The next two sections explain the organization of storage and the different
workflow tasks.


## 2. Organization of Storage

The screenshot below shows an example directory hierarchy under the *working*
root. It illustrates the three directory levels discussed in 2.2 as well as the
files with digests and summary statistics discussed in 2.3.

![The working root hierarchy](https://raw.githubusercontent.com/apparebit/shantay/boss/viz/screenshot/hierarchy.png)


### 2.1 Three Root Directories: Staging, Archive, Working

*Shantay* distinguishes between three primary directories, *staging* as
temporary storage, *archive* for the original distributions, and *working* for a
practical subset:

 1. *Staging* stores data currently being processed, e.g., by uncompressing,
    converting, and filtering it. You wouldn't be wrong if you called this
    directory *temp* or *tmp* instead. This directory must be on a fast, local
    file system; it should not be on an external disk, particularly not if the
    disk is connected with USB.
 2. *Archive* stores the original, daily ZIP files and their SHA1 codes. It
    strictly is append-only storage and holds the ground truth. This directory
    must be on a large file system. 2 TB is a minimum. It may be on an external
    drive (such as the T7 mentioned above).
 3. *Working* stores parquet files with a (much) smaller subset of the full
    database that is currently being analyzed. Like *archive*, *working* is
    treated as append-only storage. Unlike *archive*, which is unique, different
    runs of *shantay* may use different *working* directories representing
    different subsets of the database.


### 2.2 Three Levels of Nested Directories: Year, Month, Day

Under the three root directories, *shantay* arranges files into a hierarchy of
directories, e.g., resulting in paths like
`2025/03/14/2025-03-14-00000.parquet`. The top level is named for years,
followed by two-digit months one level down, and followed by two-digit days
another level down. Finally, batch files have a zero-based five-digit index.

In addition to the data files, *shantay* maintains a per-day digest file named
`sha256.txt`, which contains the SHA-256 digests for every batch file in the
directory: Each line contains one hexadecimal ASCII digest, a space,and the
batch file's name.


### 2.3 Summary Statistics: meta.json and Three Parquet Files

*Shantay* also maintains the following files inside the root directory with
working data:

  - `meta.json` contains an object with the `filter` used for selecting the
    working data and some statistics about `releases`. `batch_count` must be the
    number of batch files and `sha256` must be the (recursive) digest of the
    digests in the `sha256.txt` file.

The `batch_count` and `sha256` properties can be automatically recovered from
the directory hierarchy. Simply run *shantay*'s recover task. It performs a good
number of consistency checks to ensure that the directory hierarchy is
well-formed. Futhermore, whereas other tasks are fail-fast and stop upon the
first error, the recover task only fails after completing its file system
traversal.

Three more files contain summary statistics about the batch file contents:

  - `meta-statistics.parquet` contains the same data as `meta.json` plus counts
    collected during analysis.
  - `meta-keywords.parquet` contains data about the use of keywords.
  - `meta-platforms.parquet` contains data about the composition of platforms.


## 3. Workflow

*Shantay* proceeds in three distinct phases of processing, with each phase
processing (much) less data and executing (much) faster:

 0. The __recover__ task re-generates critical metadata in the `meta.json` file
    stored in the working root and used by all other tasks. This task walks the
    directory hierarchy under the working root. All along, it performs detailed
    checks that directories and batch files are correctly named and organized,
    while also keeping track of the `batch_count` and `sha256` properties for
    each release.
 1. The __prepare__ task generates the working parquet files. It downloads the
    original ZIP files, uncompresses and parses the included CSV files, extracts
    the data of interest, and writes that data to parquet files. This phase may
    require a day or two to run.
 2. The __analyze__ task processes the parquet files. This phase probably
    requires you pluggin in your own code, unless you want to repeat the
    analysis I've been performing. This phase takes less than a minute to run
    for all records about Protection of Minors (0.3% of all records).
 3. The __visualize__ task produces summary tables and production-quality graphs
    from the analysis results. In addition to either printing plain text or
    generating Markdown and HTML output for Jupyter, this task also generates a
    self-contained HTML document in the staging directory called
    [overview.html](https://apparebit.github.io/shantay/overview.html)

The analysis task currently processes records at month granularity and also
produces statistics at month granularity. This one-to-one correspondence between
batch size and analysis resolution mostly is a simplifying convenience. However,
it *is* critical for performance that the batch size be as large as possible.


----

(C) 2025 by Robert Grimm. The Python source code in this repository has been
released as open source under the [Apache
2.0](https://github.com/apparebit/shantay/blob/boss/LICENSE) license.
