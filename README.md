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
my five-year-old iMac with one of those T7 drives takes two days and nights to
extract records of interest from the full dataset. For my use case, the
Protection of Minors, which comprise 0.3% of all records, it takes less than one
minute to analyze the working set. Finally, visualization of the analysis
results is nearly instantaneous, taking seconds at most. Since extraction seems
to be bottlenecked on shuffling data but fails to saturate my iMac's CPU or
memory bus, I am currently working towards parallelizing the pipeline.

I've written [a blog post about my initial
impressions](https://apparebit.com/blog/2025/sashay-shantay) of the DSA
transparency database. Let's just say that, Brussels, we've got, uhm, problems
(plural)!


## Organization of Storage

*Shantay* distinguishes between three primary directories:

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


## Workflow

*Shantay* proceeds in three distinct phases of processing, with each phase
processing (much) less data and executing (much) faster:

 1. The *prepare* phase generates the working parquet files. It downloads the
    original ZIP files, uncompresses and parses the included CSV files, extracts
    the data of interest, and writes that data to parquet files. This phase may
    require a day or two to run.
 2. The *analyze* phase processes the parquet files. This phase probably
    requires you pluggin in your own code, unless you want to repeat the
    analysis I've been performing. This phase takes less than a minute to run
    for all records about Protection of Minors (0.3% of all records).
 3. The *visualize* phase produces production-quality graphs from the analysis
   results. It currently is implemented by a separate IPython workbook, though I
   plan to integrate that code with *shantay* as well.

The analysis phase currently processes records one month at a time. It reads all
parquet files for the entire month and it aggregates statistics for the entire
month as well. While that one-to-one correspondence does simplify analysis, it
is not a requirement.


## Getting Started

Using a Python tool runner such as [pipx](https://github.com/pypa/pipx) or
[uvx](https://docs.astral.sh/uv/guides/tools/), running *shantay* is as simple
as executing:
```bash
> pipx shantay -h
```
or
```bash
> uvx shantay -h
```

The `-h` option causes *shantay* to print instructions about proper invocation.
That's as far as documentation goes right now. I'll be sure to add more detailed
instructions as *shantay* matures.

----

(C) 2025 by Robert Grimm. The Python source code in this repository has been
released as open source under the [Apache
2.0](https://github.com/apparebit/shantay/blob/boss/LICENSE) license.
