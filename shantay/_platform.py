"""
There are three options for encoding categorical data with Pola.rs. Strings are
stored verbatim over and over again and hence take up too much space.
Categorical types avoid the storage overhead but require a process-wide
registry. There is no interface for coordinating between several processes and,
even if that existed, the performance overhead would likely be too big. Finally,
enumeration types avoid the storage overhead and don't require dynamic
registration. They do, however, require up-front declaration.

Shantay uses enumeration types whereever possible. The only real complication
are platform names, whose number has been growing at a rate of almost 10 names
per month from summer 2024 to summer 2025. Hence we cannot hardcode the list, as
that would put new releases on the critical path of all users. Instead, it
stores the list in a subdirectory of the user's home directory.

To detect new platform names, Shantay checks data frames right after reading
them and also scrapes the EU's website once a week. To avoid write/write
conflicts by several concurrent runs of Shantay, it updates the file system
atomically. It cannot avoid read after write conflicts entirely but minimizes
them by reading the file with platform names just before updating.

We believe that is acceptable for the following reasons:

  - Shantay never removes names, only adds them. As a result, the order and
    grouping of names does not matter.
  - Shantay always tries to update the file when encountering an unknown
    platform name and terminates right after updating the file. As long as the
    user keeps restarting the tool, it keeps trying.
  - The list of platform names up to and including some date is always fixed. In
    other words, there is a well-defined end-state.

Alas, should you be able to observe update thrashing between two concurrent
processes that make no forward progress as a result, I'd love to hear about it.
"""

from collections.abc import Iterable
import datetime as dt
import json
import logging
import os
from pathlib import Path
import re
import sys
import time
from types import MappingProxyType
from typing import Any, Literal
from urllib.request import Request, urlopen


MetaPlatforms = (
    "Facebook",
    "Instagram",
    "Other Meta Product",
    "Threads",
    "WhatsApp",
)


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
    Scrape the list of platform names from the EU's DSA transparency database
    website and update the local list accordingly.
    """
    now = time.time()
    mtime = _PLATFORM_FILE.stat().st_mtime

    if now - mtime < _ONE_WEEK:
        ts = dt.datetime.fromtimestamp(mtime, dt.timezone.utc)
        _logger.info(
            'skip scraping of platform names for path="%s", mtime="%s"',
            _PLATFORM_FILE, ts.isoformat()
        )
        return "skipped"

    _logger.info('scraping platform names')
    new_names = _scrape_platforms()
    return update_platforms(new_names)


def check_db_platforms(release: str, batch: int, frame: Any) -> None:
    """
    Check the data frame with transparency data for previously unknown platform
    names and raise a missing platform error with any unknown names.
    """
    import polars as pl
    used_names = frame.select(
        pl.col("platform_name").unique()
    ).get_column("platform_name")

    unknown_names = to_canonical_platforms(used_names) - _KNOWN_PLATFORM_NAMES
    if len(unknown_names) == 0:
        return
    for name in unknown_names:
        _logger.warning(
            'new platform in release="%s", batch=%d, name="%s"', release, batch, name
        )

    raise MissingPlatformError(unknown_names, release, batch)


def check_stats_platforms(frame: Any) -> None:
    """
    Check the data frame with summary statistics for previously unknown platform
    names and raise a missing platform error with any unknown names.
    """
    import polars as pl
    used_names = frame.select(
        pl.col("variant").filter(pl.col("column").eq("platform_name")).unique()
    ).get_column("variant")

    unknown_names = to_canonical_platforms(used_names) - _KNOWN_PLATFORM_NAMES
    if len(unknown_names) == 0:
        return
    for name in unknown_names:
        _logger.warning(
            'new platform in summary statistics with name="%s"', name
        )

    raise MissingPlatformError(unknown_names)


_PLATFORM_FILE = Path.home() / ".shantay" / "platforms.json"


def update_platforms(names: Iterable[str]) -> Literal["mtime", "disk", "memory"]:
    """
    Update the persistent list of platform names with the given names. After
    converting the given names to their canonical versions, this function read
    the list of known platform names from persistent storage, merges the two
    lists, and writes out the combined list if it is any different.

    The result indicates the extent of this function changes:
      - `mtime` means that only the last modified time of the platform file was
        updated. In other words, the given names were already included in the
        platform file. However, since the names were new to this run of Shantay,
        it must be restarted.
      - `disk` means that the platform file was updated. However, the in-memory
        version is still outdated and hence Shantay must be restarted.
      - `memory` means that the platform file and the in-memory version were
        updated. It is safe to continue running.
    """
    global PlatformNames, _KNOWN_PLATFORM_NAMES

    names = to_canonical_platforms(names)

    old_names = set(_read_platforms())
    new_names = old_names | names
    if new_names == old_names:
        _PLATFORM_FILE.touch(exist_ok=True)
        return "mtime"

    sorted_names = _to_sorted_platforms(new_names)
    _write_platforms(sorted_names)

    if _did_import_unsafe_modules():
        return "disk"

    PlatformNames = tuple(sorted_names)
    _KNOWN_PLATFORM_NAMES = frozenset(sorted_names)
    return "memory"


def _read_platforms() -> list[str]:
    with open(_PLATFORM_FILE, mode="r", encoding="utf8") as file:
        return json.load(file)


def _write_platforms(names: list[str] | tuple[str, ...]) -> None:
    _PLATFORM_FILE.parent.mkdir(exist_ok=True)

    tmp = _PLATFORM_FILE.with_suffix(f".tmp.{os.getpid()}.json")
    with open(tmp, mode="w", encoding="utf8") as file:
        json.dump(names, file, indent=0, ensure_ascii=False)
    tmp.replace(_PLATFORM_FILE)


try:
    PlatformNames = tuple(_read_platforms())
    _KNOWN_PLATFORM_NAMES = frozenset(PlatformNames)
except FileNotFoundError:
    _write_platforms(PlatformNames)


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


def _scrape_platforms() -> list[str]:
    """
    Scrape the list of current platfrom names from the EU's DSA transparency
    database website. This function returns raw names.
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

    return _OPTION_PATTERN.findall(match.group(1))


def to_canonical_platforms(names: Iterable[str]) -> set[str]:
    """
    Convert the given names to their canonical versions, while also validating
    that they do not contain backslashes or double quotes.
    """
    canonical_names = set()

    for name in names:
        if '\\' in name:
            raise ValueError(f"platform name '{name}' contains backslash")
        if '"' in name:
            raise ValueError(f"platform name '{name}' contains double quote")

        canonical_names.add(CanonicalPlatformNames.get(name, name))

    return canonical_names


def _to_sorted_platforms(names: Iterable[str]) -> list[str]:
    """Return the given canonical platform names in their canonical order."""
    return sorted(names, key=lambda n: n.casefold())


def _did_import_unsafe_modules() -> bool:
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
