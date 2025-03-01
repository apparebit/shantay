# Hello, Shantay!

After analyzing American companies' [transparency disclosures about online child
sexual exploitation](https://github.com/apparebit/diaphanous), I am now turning
to Europe, specifically the [transparency
database](https://transparency.dsa.ec.europa.eu/) created by the Europen Union's
(EU's) Digital Services Act (DSA). It collects so-called *statements of reasons*
(SoR) for the content moderation decisions of digital service providers. In
theory, said database should be a wellspring of insight into content moderation
by the tech industry. After all, providers need to report every individual
action on content as well as accounts, and they need to do so "without undue
delay," i.e., order of days.


## Brussels, We Have, Uhm, Problems!

In practice, well, let's review:

 1. *Lack of documentation*: Somehow, the EU has managed not to document the
    most important part of its database schema, the categorization of content
    moderation decisions into coarse and fine labels, i.e., those starting with
    `STATEMENT_CATEGORY` and `KEYWORD`, respectively. For instance, what exactly
    are "unsafe challenges"? According [to the
    documentation](https://transparency.dsa.ec.europa.eu/page/additional-explanation-for-statement-attributes),
    they are related to the protection of minors. But that's the extent of the
    documentation. Nothing else. WTF? 🙁
 2. *Incomplete database entries*: Every service provider must submit a record
    to the database for every content moderation decision, including when the
    moderation resulted in demotion instead of content removal. They also must
    do so in a timely manner. That's great. But the required information is
    incomplete. Notably, fine labels are entirely optional and only 3.5% of
    entries include them. That renders 96.5% of database entries less than
    useful. 😣
 3. *Closed source development practices*: The European Commission is only
    pretending to develop the necessary software in the open. Its only [GitHub
    repository](https://github.com/digital-services-act/transparency-database)
    has issues disabled, and its only [GitLab
    repository](https://code.europa.eu/dsa/transparency-database/dsa-tdb) has
    two contributors who made 13 commits over 4+ months of the repository's
    existence. By comparison, I made 206 commits over the exact same time range
    on [*one* of my projects](https://github.com/apparebit/prettypretty). 😫
 4. *Inappropriate license*: The EU's Python-based command line tool doesn't
    look terrible, but [its
    license](https://code.europa.eu/dsa/transparency-database/dsa-tdb/-/blob/main/LICENSE)
    gives me serious pause. The EUPL is the EU's very own copyleft license,
    roughly comparable to the
    [GPLv2](https://en.wikipedia.org/wiki/GNU_General_Public_License). Given
    that the majority of projects nowadays use the far more permissive MIT or
    Apache 2.0 licenses, using a relatively obscure copyleft license for tooling
    seems like a poor choice. 😭
 5. *Buggy distribution*: Three CSV files in the distribution of zipped archives
    of zipped CSV files, `sor-global-2024-08-29-full-00030-00002.csv`,
    `sor-global-2024-09-13-full-00011-00000.csv`, and
    `sor-global-2024-09-14-full-00046-00001.csv`, contain severe violations of
    the CSV format. For instance, the first file not only intermingles two
    different conventions for escaping quotes, `""` and `\"`, but also manages
    to get both of them wrong. Not surprisingly, that trips up Pola.rs' and
    PyArrow's CSV parsers. 😡
 6. *Limited data availability*: According to the EU's data retention policy,
    "after 18 months (540 days), the daily dumps are removed from the data
    download section and are archived in a cold storage." It is unclear what
    motivates the choice of 18 months, which seems short for a public resource.
    Alas, the database started operations on 2023-09-25, and so the first
    archives are about to vanish. 🤯

Oof. That doesn't sound so good. *Sashay away!*


## Introducing Shantay

To better explore the DSA SoR DB, I wrote my own Python tool, *shantay*. It
automatically downloads daily CSV archives, extracts the SoRs I care about into
parquet files, and then analyzes the extracted data. The EU's implementation of
that functionality is fairly general, based on YAML configuration files, and
relies on Spark for data wrangling. That does ensure scalability—if you can
afford the cluster and stomach the attendant complexity. By contrast, shantay is
positively scrappy and makes do with what you can comfortably spare. A laptop
and 2 TB Samsung T7 drive for long-term storage will do. Shantay also doesn't
scale. It may take a few days and nights to download all of the DSA SoR DB and
extract all working data. That works for me.


### Making Do With Less

At the same time, making do with less presents its own challenges. The first are
interruptions. Maybe, the humming of a computer busy saturating both disk and
network is getting to you. Maybe, shantay has a bug or two. In either case,
shantay's download of the DSA SoR DB is almost certain to be interrupted,
probably even more than once. Hence, shantay is careful not to lose (too much)
work upon such interruptions. It clearly separates the current working
directory, "staging," from the directories used for storing zip archives and
parquet files. It performs incremental updates only in staging and otherwise
bulk copies files in and out of long-term-storage directories. While shantay
keeps helpful metadata in a `meta.json` file, it also knows how to recover
mission-critical information by scanning the file system and merging partial
contents of that file.

The second challenge is keeping humans in the loop. That includes satisfying
quick checks on the tool's progress as well as more thorough post-mortem
inspections to determine how far the tool got and what went wrong. For the quick
check variety, shantay keeps updating a status line on the console. For longer
lasting tasks, such as downloading a release archive, it includes a progress bar
on the status line. It even computes the download speed and includes it next to
the progress bar. For the more thorough inspection variety, shantay keeps a
persistent log on disk. The log format represents a compromise: It is structured
and regular enough to be easily parsed by software, but also unstructured enough
to still be human-readable. For example, with `-v` for verbose mode, shantay
logged:

```
2025-02-28 06:23:44 [DEBUG] extracted rows=709, using="Pola.rs with glob", file="sor-global-2025-01-31-full-00056.csv.zip"
```

Apparently, shantay was busy in the early morning, extracting 709 rows of data
using the fast path with Pola.rs parsing all CSV files contains in the named
archive in one operation.

The third challenge is fully utilizing what is available in terms of hardware,
notably, CPU and RAM. That excludes not only Java-based Spark but also
Python-based Pandas. The latter certainly is popular. But it isn't particularly
fast and also suffers from an unwieldy and inconsistent interface. Instead,
shantay builds on [Pola.rs](https://pola.rs). This data frame library has a
simpler and cleaner interface than Spark or Pandas. Its core is written in Rust,
which ensures good performance. With a little help from
[PyO3](https://pyo3.rs/), it also integrates seamlessly with Python. Using
Pola.rs, shantay chugs through a month's single digit gigabytes of working data,
loaded from hundreds of parquet files with a single line of Pola.rs using two
wildcards, and takes maybe 30 seconds to process 1.5 years worth of data. Not
bad. Not bad, indeed.


### A Sharp Edge and a Papercut

In my experience, there is one Pola.rs feature that just isn't worth the
trouble, namely automatic schema inference when reading CSV files and also when
directly instantiating data frames. The feature isn't even unique to Pola.rs.
Pandas also performs schema inference and Pandas' inference has also tripped me
up. Alas, Pola.rs' schema inference seems more brittle, and its error messages,
without any line numbers when reading CSV files, are positively rude. Pola.rs'
inference also doesn't handle nested list values, which appear in several DSA
SoR DB columns.

The solution has been to disable automatic inference when reading CSV files,
setting `infer_schema` to `False`, while also specifying types for unproblematic
columns with `schema_overrides` and using Pola.rs' string operators to clean up
more problematic columns before explicitly converting them to their proper
types. While this approach does require a bit more code even for columns that
don't require cleanup, it also has proven to be robust. It also simplified
integration of a second CSV parser into shantay because the code necessary for
converting the CSV parser's output to a typed representation was already
written.

Integrating a fallback CSV parser became necessary when Pola.rs' CSV parser
choked on the encoding errors described above. While looking for a work-around,
I tested other CSV parsers on the first offending file. PyArrow's CSV parser
failed as well. However, the Python standard library's CSV parser succeeded. It
also preserved all of the original text, minus the bad escape sequences. I had
my fallback and implemented the following three-level CSV parsing strategy: By
default, shantay uses Pola.rs' CSV parser with a wildcard in the file name,
reading several CSV files in one operation. If that fails, shantay tries to read
the CSV files again, with the same parser but now one file at a time. When that
fails on *one* file, shantay falls back to the Python standard library's CSV
parser. Once implemented, shantay processed the archive with the first broken
CSV file without a hitch. I only learned of the other two files from its log.


### Five EU DSA SoR DB Timelines

The proof of the pudding is, of course, in the eating. So here's shantay's
latest output, five timelines of content moderation, each spanning the exact
same time period but illuminating a different aspect of the database and its
contents.

 1. Doing content moderation well is hard at today's scales. On busy days,
    protection of minors alone may account for 500,000 plus content moderation
    actions across Europe alone.

    ![EU DSA SoR DB timeline 1](https://raw.githubusercontent.com/apparebit/shantay/boss/vis/2025-03-01/timeline1.svg)

 2. At the same time, protection of minors is not a particularly prominent
    category for content moderation. Out of 15 categories currently used by the
    DSA transparency database, protection of minors accounts for only 0.3% of
    all content moderation actions.

    ![EU DSA SoR DB timeline 2](https://raw.githubusercontent.com/apparebit/shantay/boss/vis/2025-03-01/timeline2.svg)

 3. Digital service providers largely treat optional compliance requirements as
    non-existent compliance requirements. Overall, only 3.5% of all SoRs include
    keywords. Interestingly, for protection of minors, that fraction, at 9.5%,
    is almost three times larger and seems to be increasing still.

    ![EU DSA SoR DB timeline 3](https://raw.githubusercontent.com/apparebit/shantay/boss/vis/2025-03-01/timeline3.svg)

 4. The platform determines whether to include keywords or not. Out of the 35
    platforms that have contributed SoRs concerning the protection of minors,
    only a third, i.e., 12 platforms, filed SoRs with keywords. That also means
    that any analysis that focuses on SoRs with keywords only is likely to yield
    biased results.

    ![EU DSA SoR DB timeline 4](https://raw.githubusercontent.com/apparebit/shantay/boss/vis/2025-03-01/timeline4.svg)

 5. At least for protection of minors, there are vast differences between, say,
    twelve- or thirteen-year-olds sneaking onto social media before their time,
    keyword *age-specific restrictions*, teenagers sexting each other, keyword
    *CSAM*, and adults exchanging child pornography, also keyword *CSAM*. If
    anything, the current keywords are not fine-grained enough, hence also the
    large fraction of SoRs with `KEYWORD_OTHER`.

    ![EU DSA SoR DB timeline 5](https://raw.githubusercontent.com/apparebit/shantay/boss/vis/2025-03-01/timeline5.svg)

In summary, the data that is available today clearly demonstrates a consistent
need for more fine-grained data. Making existing keywords mandatory is a step in
the right direction but won't be enough.


## Sashay? Shantay!

Me and shantay will be mining the data for more insight for sure. Hopefully, we
can work around some of the limitations with what's currently available. I might
have an idea or two already. Along the way, I'm going to continue improving
shantay. In fact, I just made [the first
release](https://pypi.org/project/shantay/). Its support for extracting
categories other than protection of minors is still very raw and shaky. But with
data retention expiring towards the end of the month, releasing early seemed
called for.

Meanwhile the EU would be well-advised to up its transparency game. The current
effort unfortunately reeks of transparency theater, producing large volumes of
daily data that end up meaning not that much. My recommendation is to start with
mandating keywords. Without them, the DSA transparency database is doomed to
failure. The second priority should be documenting categories and keywords in
more detail. Not having done so is a grave oversight. Once those two priorities
have been addressed, a consultation on additional keywords would be great. Then
there is the question of open source development...

Come on, EU. You can do better. And now that the US is dismantling its own
government might be a good time to do better if not shine.


----

(C) 2025 by Robert Grimm. The Python source code in this repository has been
released as open source under the [Apache
2.0](https://github.com/apparebit/prettypretty/blob/main/LICENSE) license.
