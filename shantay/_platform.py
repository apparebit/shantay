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

from collections.abc import Iterable, Mapping
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
    "ABOUT YOU",
    "Acast",
    "ADEO",
    "Adobe Lightroom",
    "Adobe Photoshop Express",
    "Adobe Stock",
    "AGODA",
    "Airbnb",
    "Akciós-újság.hu",
    "AliExpress",
    "Amazon",
    "Amazon Store",
    "App Store",
    "Apple Books",
    "Apple Podcasts",
    "Atmoskop",
    "Auctronia",
    "AutoRevue",
    "AutoScout24",
    "Autoweek",
    "Azar",
    "Back Market",
    "Badoo",
    "bazar.at",
    "Behance",
    "BigBang.si",
    "BlaBlaCar",
    "bol.com",
    "bolha.com",
    "Booking.com",
    "Bumble",
    "Campfire",
    "Canva",
    "Casamundo, Wimdu, Eurorelais",
    "Catawiki",
    "Cdiscount",
    "Chrome Web Store",
    "CLIP STUDIO PAINT",
    "Conrad",
    "Course Hero",
    "daft.ie",
    "Dailymotion",
    "DATEV Marktplatz",
    "DATEV SmartExperts",
    "De Morgen Shop",
    "Deliveroo Ireland",
    "Delivery Hero",
    "Discord",
    "Doctolib",
    "DoneDeal.ie",
    "e15",
    "eBay",
    "eJobs",
    "ElitePartner",
    "EMAG.BG",
    "EMAG.HU",
    "EMAG.RO",
    "eMimino.cz",
    "Eventbrite",
    "Facebook",
    "Fashiondays.ro",
    "Ferryhopper",
    "finden.at",
    "Flights",
    "Flourish",
    "G2.com",
    "Garmin",
    "Gastrojobs",
    "GIPHY",
    "GitHub",
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
    "Heureka Group",
    "Hinge",
    "HLN Shop",
    "HolidayCheck",
    "HomeExchange.com",
    "Hornbach",
    "Hostelworld.com",
    "Hotel Hideaway",
    "Hotelcareer",
    "Hotels",
    "Hírstart",
    "Idealista.com",
    "Idealo",
    "IMDb",
    "immo.kurier.at",
    "immobilien.derstandard.at",
    "imobiliare.ro",
    "Imovirtual",
    "Indeed",
    "Infojobs.net",
    "ingatlan.com",
    "Ingatlanbazár",
    "Instagram",
    "InterNations",
    "irishjobs.ie",
    "JetBrains",
    "JetBrains Marketplace",
    "Jobat",
    "jobs.cz",
    "jobs.derstandard.at",
    "jobs.ie",
    "Joom",
    "Kaggle",
    "Kleinanzeigen",
    "Knowunity",
    "kununu",
    "Költözzbe.hu",
    "La Redoute",
    "Lasso Moderation",
    "LeasingMarkt.de",
    "leboncoin",
    "Ligaportal",
    "LinkedIn",
    "Livios Forum",
    "LOVOO",
    "ländleauto.at",
    "ManoMano",
    "Marktplaats",
    "MATY",
    "Meetic",
    "Meinestadt",
    "METRO Markets",
    "Microsoft Operations",
    "Microsoft Store",
    "Microsoft Teams",
    "Mijnvergelijker",
    "Milanuncios.com",
    "Mimiaukce",
    "Mimibazar",
    "Mindmegette",
    "Miravia",
    "mobile.de",
    "MORE.COM",
    "mydealz, Pepper, Preisjäger",
    "nebenan.de",
    "Nebius AI",
    "Njuskalo Turizam",
    "Njuškalo.hr",
    "Nosalty",
    "NPM",
    "OKCupid",
    "OLX",
    "Opinio",
    "Other Meta Product",
    "OTTO",
    "Parship",
    "PC Games Store",
    "Peloton",
    "Pexels",
    "PHAISTOS NETWORKS",
    "Pinterest",
    "Platomics",
    "Plenty of Fish",
    "Pornhub",
    "Profesia",
    "profession.hu",
    "Práce za rohem",
    "Práce.cz",
    "Pub.dev",
    "Quora",
    "Rajče",
    "Rakuten",
    "Reddit",
    "rentalia.com",
    "ResearchGate",
    "rezeptwelt.de",
    "Roblox",
    "Salonkee",
    "Samsung Galaxy Store",
    "Samsung PENUP",
    "SAP",
    "SE LOGER",
    "Seduo",
    "SFDC",
    "Shein",
    "SHOPFLIX",
    "Shopify",
    "shöpping.at",
    "SME Blog",
    "Snapchat",
    "SoundCloud",
    "Spaargids Forum",
    "Spark Networks",
    "Standvirtual",
    "Startlap",
    "StayFriends",
    "Stepstone",
    "Streamate.com",
    "Stripchat",
    "Studydrive",
    "Takeaway.com",
    "TAZZ",
    "Telegram",
    "Telia Yhteisö",
    "Temu",
    "Tenor",
    "The League",
    "TheFork",
    "Threads",
    "TikTok",
    "Tinder",
    "Trendyol",
    "Tripadvisor",
    "Trustpilot",
    "Tweakers",
    "Twitch",
    "Uber",
    "Udemy",
    "Upwork",
    "Vacation Rentals",
    "Vareni.cz",
    "Veepee",
    "Vestiaire Collective",
    "Viator",
    "Videa",
    "Videakid",
    "Vimeo",
    "Vinted",
    "Vrbo.com",
    "VSCO",
    "Wallapop",
    "Wattpad",
    "Waze",
    "WhatsApp",
    "Wikimedia",
    "Wikipower",
    "willhaben",
    "Wizz",
    "Wolt",
    "X",
    "Xbox Store",
    "Xbox.com",
    "XING",
    "XNXX",
    "XVideos",
    "YouTube",
    "Yubo",
    "YVES ROCHER FRANCE",
    "Zalando",
    "Zboží.cz",
    "Zenga",
    "Živě.cz",
    "ΣΚΡΟΥΤΖ",
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
    "Mijnvergelijker / Comparateur": "Mijnvergelijker",
    "Microsoft Ireland Operations Limited": "Microsoft Operations",
    "Microsoft Store on Windows (PC App Store)": "Microsoft Store",
    "Microsoft Teams personal": "Microsoft Teams",
    "MORE.COM ΗΛΕΚΤΡΟΝΙΚΕΣ ΥΠΗΡΕΣΙΕΣ": "MORE.COM",
    "Other Meta Platforms Ireland Limited-offered Products": "Other Meta Product",
    "OTTO Market": "OTTO",
    "Quora Ireland Limited": "Quora",
    "www.rentalia.com": "rentalia.com",
    "Samsung Galaxy App Store": "Samsung Galaxy Store",
    "SAP Community": "SAP",
    "SFDC Ireland Limited": "SFDC",
    'SIA "JOOM"': "Joom",
    "SIA &quot;JOOM&quot;": "Joom",
    "Takeaway.com Central Core B.V.": "Takeaway.com",
    "Trendyol B.V.": "Trendyol",
    "Vinted UAB": "Vinted",
    "WhatsApp Channels": "WhatsApp",
    "willhaben internet service GmbH & Co KG": "willhaben",
    "willhaben internet service GmbH &amp; Co KG": "willhaben",
    "www.gutefrage.net": "gutefrage.net",
    "X (formerly Twitter)":"X",
    "Xbox Console Store": "Xbox Store",
    "Xbox.com Website Store": "Xbox.com",
    "ΣΚΡΟΥΤΖ Α.Ε.": "ΣΚΡΟΥΤΖ",
})


def _create_lookup_table() -> Mapping[str, str]:
    return MappingProxyType({
        p.casefold(): p for p in PlatformNames
    } | {
        k.casefold(): v for k, v in CanonicalPlatformNames.items()
    })


PlatformLookupTable = _create_lookup_table()


class MissingPlatformError(Exception):
    """An exception indicating unknown platform names."""


_KNOWN_PLATFORM_NAMES: frozenset[str] = frozenset(PlatformNames)
_logger = logging.getLogger(__spec__.parent)
_ONE_WEEK = 7 * 24 * 60 * 60
_ONE_WEEK_NS = _ONE_WEEK * 1_000_000_000


def sync_web_platforms(
    force: bool = False
) -> Literal["skipped", "mtime", "disk", "memory"]:
    """
    Scrape the list of platform names from the EU's DSA transparency database
    website and update the local list accordingly. By default, this function
    uses the last-modified-time of the platforms file to avoid querying the
    website more than once a week. However, if `force` is true, it ignores the
    timestamp and always queries the server.
    """
    if not force:
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


def check_db_platforms(path: Path, frame: Any) -> None:
    """
    Check the data frame with transparency data for previously unknown platform
    names and raise a missing platform error with any unknown names.
    """
    _check_platforms(path, frame, "platform_name")


def check_stats_platforms(path: str | Path, frame: Any) -> None:
    """
    Check the data frame with summary statistics for previously unknown platform
    names and raise a missing platform error with any unknown names.
    """
    _check_platforms(path, frame, "platform")


def _check_platforms(path: str | Path, frame: Any, column: str) -> None:
    import polars as pl
    used_names = frame.select(pl.col(column).drop_nulls().unique()).get_column(column)

    unknown_names = to_canonical_platforms(used_names) - _KNOWN_PLATFORM_NAMES
    if len(unknown_names) == 0:
        return
    for name in unknown_names:
        _logger.warning(
            'new platform in column="%s", path="%s", name="%s"', column, path, name
        )

    raise MissingPlatformError(unknown_names)


_PLATFORM_FILE = Path.home() / ".shantay" / "platforms.json"


def update_platforms(names: Iterable[str]) -> Literal["mtime", "disk", "memory"]:
    """
    Update the (persistent) list of platform names with the given names.

    After mapping the given names to there canonical versions, this function
    re-reads the list of known platform names from storage, merges the two
    lists, and writes out the combined list to storage if it is any different.

    The result indicates the extent of this function's changes:

      - `mtime` means that only the last-modified-time of the platform file was
        updated. That does imply that the given names were already included in
        the file with platform names. However, since this function is only
        called upon detection of a missing platform name, the in-memory list of
        platform names was outdated and Shantay must be restarted.
      - `disk` means that the platform file was updated. However, the in-memory
        version could not be updated, i.e., is still outdated, and hence Shantay
        must be restarted.
      - `memory` means that the platform file and the in-memory version were
        updated. It is safe to continue running.
    """
    global PlatformLookupTable, PlatformNames, _KNOWN_PLATFORM_NAMES

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
    PlatformLookupTable = _create_lookup_table()
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


def predate_platforms() -> None:
    """
    Predate the last modified time of the file with platform names. Running this
    function forces an update of the platform names upon the next run of
    Shantay. At the same time, this function is safe to call concurrently
    because the file system will serialize the competing updates, any of which
    has the intended effect.
    """
    ns = time.time_ns() - 2 * _ONE_WEEK_NS
    os.utime(_PLATFORM_FILE, ns=(ns, ns))


try:
    PlatformNames = tuple(_read_platforms())
    _KNOWN_PLATFORM_NAMES = frozenset(PlatformNames)
    PlatformLookupTable = _create_lookup_table()
except FileNotFoundError:
    # Create the platforms file in the first place, but predate its last
    # modified time so that the platform names are immediately updated.
    _write_platforms(PlatformNames)
    predate_platforms()


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
    that they do not contain backslashes or double quotes. This function also
    normalizes capitalization.
    """
    canonical_names = set()

    for name in names:
        if '\\' in name:
            raise ValueError(f"platform name '{name}' contains backslash")
        if '"' in name:
            raise ValueError(f"platform name '{name}' contains double quote")

        canonical_names.add(PlatformLookupTable.get(name.casefold(), name))

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


def _update_self() -> None:
    """
    Update this module's source code with the current platform names. This
    function always updates this module's source code. However, unless the
    platform names have been updated since the last distribution of Shantay,
    doing so effectively is a no-op.

    DO NOT EVEN THINK OF INVOKING THIS FUNCTION!
    """
    # Read this module's source code
    path = Path(__file__)
    source = path.read_text(encoding="utf8")
    parts = _MODULE_PARTS.match(source)
    assert parts is not None

    # Prepare the updated source code
    prefix = parts.group("prefix")
    listing = "\n".join(f'    "{n}",' for n in PlatformNames)
    suffix = parts.group("suffix")

    # Write out this module's source code
    tmp = path.with_suffix(".tmp.py")
    tmp.write_text(f"{prefix}PlatformNames = (\n{listing}\n){suffix}", encoding="utf8")
    tmp.replace(path)


if __name__ == "__main__":
    # Configure logging
    logging.Formatter.default_msec_format = "%s.%03d"
    logging.basicConfig(
        format='%(asctime)s︙%(process)d︙%(name)s︙%(levelname)s︙%(message)s',
        filename="shantay.log",
        encoding="utf8",
        level=logging.DEBUG,
    )

    # Sync platform names
    action = sync_web_platforms(force=True)
    assert action != "disk"

    # Update this module's source code
    _update_self()
