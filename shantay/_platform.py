# ==============================
#    If you regularly call
#     the stats module's
#    sync_web_platforms(),
#    check_db_platforms(),
#  and check_stats_platforms(),
#   this module automatically
#      maintains itself.
#
#     PLEASE DO NOT EDIT!
# ===========================

"""
Shantay uses Pola.rs enumerations for all categorical columns of the summary
statistics; hence all variants must be declared in advance. Pola.rs categorical
types do not require declaration but a global registry. To support
multiprocessing and categorical types, that registry would have to be
out-of-process, which is not currently supported by Pola.rs. Furthermore, even
if it was possible to implement such a registry, its performance would likely be
prohibitive. Finally, strings take up too much in-memory storage.

Declaring all variants in advance actually isn't too hard for almost all
columns. Alas, one of the columns includes platform names, the one entity that
is seeing constant churn. Updating Shantay on a regular basis puts releases on
the critical path of every users, which doesn't seem desirable. So a more
dynamic update mechanism is needed.

Shantay actually implements *two* mechanisms because neither strikes us as a
guaranteed solution. Hopefully, the combination of the two is effective in
practice. Both mechanisms update the source code of this module. Since **updates
are atomic**, thanks to Shantay's use of appropriate file system operations,
updates are guaranteed to always result in a usable module (barring bugs in this
module), even for several concurrent Shantay runs off the same installation.
However, since Shantay does not use transactions for reading this module,
notably when importing it, and later on updating it, there is a theoretical risk
of read-write conflicts due to concurrent tool runs. That is acceptable for the
following reasons:

  * Shantay always checks Parquet files with transparency data or summary
    statistics for currently unknown platform names. If it detects any, it
    terminates the current run.
  * By proactively validating data during loading, Shantay also knows which
    platform names are missing and updates this module with those names. Updates
    are strictly additive. It never deletes platform names.
  * This module is loaded early on during a run and unknown platform names are
    usually detected towards the end of what may be a dayslong run. That is too
    long a window for read-write conflicts. Hence Shantay re-reads the list of
    platform names just before updating its source code.
  * The only source of unknown platform names also is the only source of data
    for concurrent runs of Shantay. In other words, if some names are missing,
    concurrent runs are likely to encounter the same names in the same order.
  * Updating this module and rerunning Shantay is effective but a bit annoying,
    especially if you end up manually monitoring your runs. So Shantay also
    updates platform names by scraping the search webpage for the DSA
    transparency database, which enumerates almost all of them, every seven
    days.

Alas, should you be able to observe update thrashing between two concurrent
processes that make no forward progress as a result, I'd love to hear about it.
"""

from collections.abc import Iterable
import logging
from pathlib import Path
import re
import sys
import time
from types import MappingProxyType
from typing import Literal
from urllib.request import Request, urlopen

import polars as pl


PlatformNames = (
    "ADEO",
    "Adobe Lightroom",
    "Adobe Photoshop Express",
    "Adobe Stock",
    "AGODA",
    "Akciós-újság.hu",
    "AliExpress",
    "Amazon",
    "Amazon Store",
    "App Store",
    "Apple Books",
    "Apple Podcasts",
    "Auctronia",
    "AutoRevue",
    "AutoScout24",
    "Azar",
    "Badoo",
    "Behance",
    "BigBang.si",
    "BlaBlaCar",
    "bolha.com",
    "Booking.com",
    "Bumble",
    "Campfire",
    "Canva",
    "Catawiki",
    "Cdiscount",
    "Chrome Web Store",
    "Conrad",
    "Course Hero",
    "daft.ie",
    "Dailymotion",
    "DATEV Marktplatz",
    "DATEV SmartExperts",
    "Deliveroo Ireland",
    "Delivery Hero",
    "Discord",
    "Doctolib",
    "DoneDeal.ie",
    "e15",
    "eBay",
    "eJobs",
    "EMAG.BG",
    "EMAG.HU",
    "EMAG.RO",
    "eMimino.cz",
    "Eventbrite",
    "Facebook",
    "Fashiondays.ro",
    "Flights",
    "Flourish",
    "G2.com",
    "Garmin",
    "Gastrojobs",
    "Glassdoor",
    "Google Maps",
    "Google Play",
    "Google Shopping",
    "GroupMe",
    "Groupon",
    "gutefrage.net",
    "Habbo",
    "happn",
    "Használtautó.hu",
    "Hinge",
    "Hornbach",
    "Hostelworld.com",
    "Hotel Hideaway",
    "Hotels",
    "Hírstart",
    "Idealista.com",
    "Idealo",
    "IMDb",
    "imobiliare.ro",
    "Imovirtual",
    "ingatlan.com",
    "Ingatlanbazár",
    "Instagram",
    "irishjobs.ie",
    "JetBrains",
    "jobs.ie",
    "Joom",
    "Kaggle",
    "Kleinanzeigen",
    "Knowunity",
    "La Redoute",
    "leboncoin",
    "Ligaportal",
    "LinkedIn",
    "ManoMano",
    "Meetic",
    "Microsoft Operations",
    "Microsoft Store",
    "Microsoft Teams",
    "Milanuncios.com",
    "Mimiaukce",
    "Mindmegette",
    "mobile.de",
    "nebenan.de",
    "Nebius AI",
    "Njuškalo.hr",
    "Nosalty",
    "OKCupid",
    "OLX",
    "Other Meta Product",
    "OTTO",
    "Parship",
    "PC Games Store",
    "Pexels",
    "PHAISTOS NETWORKS",
    "Pinterest",
    "Plenty of Fish",
    "Pornhub",
    "Profesia",
    "profession.hu",
    "Pub.dev",
    "Quora",
    "Rajče",
    "Rakuten",
    "Reddit",
    "rentalia.com",
    "ResearchGate",
    "rezeptwelt.de",
    "Roblox",
    "Samsung PENUP",
    "SAP",
    "SE LOGER",
    "SFDC",
    "Shein",
    "Shopify",
    "SME Blog",
    "Snapchat",
    "SoundCloud",
    "Spark Networks",
    "Standvirtual",
    "Startlap",
    "Stepstone",
    "Streamate.com",
    "Stripchat",
    "Studydrive",
    "TAZZ",
    "Telia Yhteisö",
    "Temu",
    "Tenor",
    "The League",
    "TheFork",
    "Threads",
    "TikTok",
    "Tinder",
    "Tripadvisor",
    "Trustpilot",
    "Twitch",
    "Uber",
    "Udemy",
    "Vacation Rentals",
    "Vareni.cz",
    "Vestiaire Collective",
    "Viator",
    "Vimeo",
    "Vinted",
    "VSCO",
    "Wallapop",
    "Waze",
    "WhatsApp",
    "willhaben",
    "Wizz",
    "X",
    "Xbox Store",
    "Xbox.com",
    "YouTube",
    "Yubo",
    "Zalando",
    "Živě.cz",
)


CanonicalPlatformNames = MappingProxyType({
    "ADEO MARKETPLACE SERVICES": "ADEO",
    "Adobe Photoshop Lightroom": "Adobe Lightroom",
    "Apple Books (ebooks)": "Apple Books",
    "Apple Podcasts Subscriptions": "Apple Podcasts",
    "Discord Netherlands B.V.": "Discord",
    "eDarling, EliteSingles, SilverSingles, Zoosk": "Spark Networks",
    "foodora, Glovo, efood, foody": "Delivery Hero",
    "Garmin Nederland B.V.": "Garmin",
    "HORNBACH Marktplatz, Smart Home by HORNBACH": "Hornbach",
    "Hostelworld.com Limited": "Hostelworld.com",
    "Meetic SAS": "Meetic",
    "Microsoft Ireland Operations Limited": "Microsoft Operations",
    "Microsoft Store on Windows (PC App Store)": "Microsoft Store",
    "Microsoft Teams personal": "Microsoft Teams",
    "Other Meta Platforms Ireland Limited-offered Products": "Other Meta Product",
    "OTTO Market": "OTTO",
    "Quora Ireland Limited": "Quora",
    "www.rentalia.com": "rentalia.com",
    "SAP Community": "SAP",
    "SFDC Ireland Limited": "SFDC",
    'SIA "JOOM"': "Joom",
    "SIA &quot;JOOM&quot;": "Joom",
    "Vinted UAB": "Vinted",
    "WhatsApp Channels": "WhatsApp",
    "willhaben internet service GmbH & Co KG": "willhaben",
    "willhaben internet service GmbH &amp; Co KG": "willhaben",
    "www.gutefrage.net": "gutefrage.net",
    "Xbox Console Store": "Xbox Store",
    "Xbox.com Website Store": "Xbox.com",
})

class MissingPlatformError(Exception):
    """An exception indicating that previously unknown platform names."""


_KNOWN_PLATFORM_NAMES: frozenset[str] = frozenset(PlatformNames)
_logger = logging.getLogger(__spec__.parent)
_ONE_WEEK = 7 * 24 * 60 * 60

def sync_web_platforms() -> Literal["skipped", "mtime", "disk", "memory"]:
    """
    If this module's file modification time is older than a week, this function
    scrapes the list of platform names from the EU's DSA transparency database
    website and updates this module if necessary. Otherwise, it does nothing and
    returns `skipped`. See `update_platforms` for the meaning of the other three
    return values. In particular, unless this function returns `memory`, the
    current tool run must be terminated.
    """
    file = Path(__file__)
    now = time.time()
    if now - file.stat().st_mtime < _ONE_WEEK:
        return "skipped"

    new_names = _scrape_platforms()
    unknown_names = new_names - _KNOWN_PLATFORM_NAMES
    if len(unknown_names) == 0:
        file.touch(exist_ok=True)
        return "mtime"

    return update_platforms(unknown_names)


def check_db_platforms(release: str, batch: int, frame: pl.DataFrame) -> None:
    """
    Check a data frame with DSA transparency data for previously unknown
    platform names. This function raises an `UnknownPlatformError` if the data
    frame uses any unknown names. The three arguments to that error are the
    release, batch number, and the set of unknown names.

    This function does not update this module's state, whether on disk or in
    memory.
    """
    used_names = frame.select(
        pl.col("platform_name").unique()
    ).get_column("platform_name")

    unknown_names = _canonical_platforms(used_names) - _KNOWN_PLATFORM_NAMES
    if len(unknown_names) == 0:
        return
    for name in unknown_names:
        _logger.warning(
            'new platform in release="%s", batch=%d, name="%s"', release, batch, name
        )

    raise MissingPlatformError(unknown_names, release, batch)


def check_stats_platforms(frame: pl.DataFrame) -> None:
    """
    Check a data frame with summary statistics for unknown platform names. This
    function raises an `UnknownPlatformError` if the data frame uses any unknown
    names. The three arguments to that error are the release, batch number, and
    the set of unknown names.

    This function does not update this module's state, whether on disk or in
    memory.
    """
    used_names = frame.select(
        pl.col("variant").filter(pl.col("column").eq("platform_name")).unique()
    ).get_column("variant")

    unknown_names = _canonical_platforms(used_names) - _KNOWN_PLATFORM_NAMES
    if len(unknown_names) == 0:
        return
    for name in unknown_names:
        _logger.warning(
            'new platform in summary statistics, name="%s"', name
        )

    raise MissingPlatformError(unknown_names)


_PAGE_PATTERN = re.compile(
    r"""
    <select[ ]name="platform_id\[\]"[ ]id="platform_id"[^>]*>
        \s*
        (
            (?: (?: <option[^>]*>[^<]*</option>) \s* )*
        )
    </select>
    """, re.VERBOSE
)


_OPTION_PATTERN = re.compile(r'<option[^>]*>([^<]*)</option>')


class DownloadFailed(Exception):
    """A download ended in a status code other than 200."""


def _scrape_platforms() -> frozenset[str]:
    """
    Scrape the list of current platfrom names from the EU's DSA transparency
    database website. This function returns canonical names.
    """
    url = "https://transparency.dsa.ec.europa.eu/statement"

    with urlopen(Request(url, None, {})) as response:
        if response.status != 200:
            _logger.error(
                'failed to download type="web page", status=%d, url="%s"',
                response.status, url
            )
            raise DownloadFailed(
                f'download of web page "{url}" failed with status {response.status}'
            )

        page = response.read().decode("utf8")

    match = _PAGE_PATTERN.search(page)
    assert match is not None, f"failed to scrape platform names from {url}"

    platforms = _canonical_platforms(_OPTION_PATTERN.findall(match.group(1)))
    return platforms


def _canonical_platforms(names: Iterable[str]) -> frozenset[str]:
    """Convert the given names to their canonical versions."""
    return frozenset(CanonicalPlatformNames.get(p, p) for p in names)


def _sorted_platforms(names: Iterable[str]) -> list[str]:
    """Return the given canonical platform names in their canonical order."""
    return sorted(names, key=lambda n: n.casefold())


def _imported_unsafe_modules() -> bool:
    """
    Determine if any of the unsafe modules (model, schema, or stats) have
    already been loaded. If that is the case, this module's in-memory state must
    not be updated.
    """
    pkg = __spec__.parent
    for mod in ("model", "schema", "stats"):
        if f"{pkg}.{mod}" in sys.modules:
            return True
    return False


_MODULE_PARTS = re.compile(
    r"""
    ^
    (?P<prefix>.*?)
    PlatformNames [ ][=][ ][(][\n]
        (?P<names>.*?)
    [\n][)]
    (?P<suffix>.*)
    $
    """,
    re.VERBOSE | re.DOTALL
)


def update_platforms(names: Iterable[str]) -> Literal["mtime", "disk", "memory"]:
    """
    Update the source code of this module with the given platform names. This
    function merges the given platform names with the already known platform
    names.

     1. If the merged list does not contain any previously unknown platform
        names, this function updates this module's modified time and returns
        `mtime`.
     2. Otherwise, it updates this module's source code to include the
        previously unknown platform names. If the model, schema, or stats
        modules have already been loaded, this function returns `disk`.

        The on-disk and in-memory state for this module are inconsistent and
        Shantay must be restarted.
     3. If the model, schema, or stats modules have not been loaded, this
        function also updates the in-memory state and returns `memory`.

    Before merging, this function validates the given platform names. In
    particular, names must not contain backslashes or double quotes.
    """
    global PlatformNames, _KNOWN_PLATFORM_NAMES

    # Validate names.
    for name in names:
        if '\\' in name:
            raise ValueError(f"platform name '{name}' contains backslash")
        if '"' in name:
            raise ValueError(f"platform name '{name}' contains double quote")

    # Missing platform names become more likely the more recent the DSA entries
    # being processed. Since a single run of shantay may take a few days, that
    # implies that this module may have been imported days ago, leaving plenty
    # of time for another invocation of shantay to make modifications. So before
    # applying any update, we re-ingest the list from the module source code and
    # update that version.
    file = Path(__file__)
    tmp = file.with_suffix(".tmp.py")
    source_code = file.read_text(encoding="utf8")

    # Break the module into its parts.
    parts = _MODULE_PARTS.match(source_code)
    assert parts is not None
    prefix = parts.group("prefix")
    suffix = parts.group("suffix")

    # Ingest the list of platform names (without eval).
    old_names: set[str] = set()
    for line in parts.group("names").splitlines():
        assert line.startswith('    "')
        assert line.endswith('",')
        old_names.add(line[5:-2])

    # Check whether we need to rewrite this module.
    new_names = old_names | _canonical_platforms(names)
    if old_names == new_names:
        file.touch(exist_ok=True)
        return "mtime"

    # Rewrite the source code
    sorted_names = _sorted_platforms(new_names)
    s = "\n".join(f'    "{n}",' for n in sorted_names)
    tmp.write_text(f"{prefix}PlatformNames = (\n{s}\n){suffix}", encoding="utf8")
    tmp.replace(file)

    # Update module state if it hasn't been imported yet.
    if _imported_unsafe_modules():
        return "disk"

    PlatformNames = sorted_names
    _KNOWN_PLATFORM_NAMES = frozenset(sorted_names)
    return "memory"
