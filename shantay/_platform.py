# ===========================
#  This module automatically
#      maintains itself.
#
#     PLEASE DO NOT EDIT!
# ===========================

from collections.abc import Sequence
import logging
from pathlib import Path
import re
from types import MappingProxyType
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
    pass


_KNOWN_PLATFORM_NAMES = frozenset(PlatformNames)
_logger = logging.getLogger(__spec__.parent)


# release is a string to avoid dependency on .model module
def check_new_platform_names(release: str, batch: int, frame: pl.DataFrame) -> None:
    """
    Check for previously unknown platform names. This function raises an
    UnknownPlatformError if the data frame uses any unknown names. The three
    arguments to that error are the release, batch number, and list of unknown
    names.
    """
    used_names = frame.select(
        pl.col("platform_name").unique()
    ).get_column("platform_name")

    unknown_names = []
    for name in used_names:
        if name not in _KNOWN_PLATFORM_NAMES:
            unknown_names.append(name)

    if len(unknown_names) == 0:
        return
    for name in unknown_names:
        _logger.warning(
            'new platform in release="%s", batch=%d, name="%s"', release, batch, name
        )

    raise MissingPlatformError(release, batch, unknown_names)


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


def update_new_platform_names(names: Sequence[str]) -> bool:
    """
    Update the source code of this module with the given platform names.

    To minimize the possibility of conflicting writes, this function must not be
    called from a worker process. However, concurrent tool runs can still result
    in conflicting writes. That is acceptable for three reasons:

     1. The update to the file itself is an atomic file replace operation. Since
        that implies whole file updates, it also ensures that the source code is
        always well-formed (barring bugs in this function).
     2. This function only adds names to the list of platform names. Hence any
        order and combination of updates for the same names will eventually
        converge on the same final result.
     3. While concurrent updates may make names appear and disappear again, the
        restarted run of shantay will likely fail on re-encountering deleted
        names. On the off-chance that a name is deleted after it has already
        been counted, the concatenation of summary statistics will fail as it
        re-applies the statistics schema.

    If concatenation fails, human intervention becomes necessary. It entails
    manually adding new platform names to this module. In other words, in the
    unlikely worst-case, users have to wait for a new tool release. However,
    without this module updating itself, waiting for a new tool release is the
    only option. In other words, the use of self-modifying code improves the
    user experience but does not obviate the need for maintaining an up-to-date
    list of platform names.

    The above does assume that a complete list of platform names is required for
    running Shantay. Without it, we couldn't declare an enum type for the
    summary statistics' variant column. Since Pola.rs' categorical type
    dynamically determines all members, we might consider that as an option. But
    there is no good way for merging the categorical types of parallel
    processes. At least, that was the case when I last tried. That leaves string
    as a type, which would be rather wasteful since the universe of variant
    column values is fairly small. Hence dealing with an every growing list of
    platform names doesn't seem the worst option.
    """
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
    old_names = []
    for line in parts.group("names").splitlines():
        assert line.startswith('    "')
        assert line.endswith('",')
        old_names.append(line[5:-2])

    # Check whether we need to rewrite this module.
    old_names = frozenset(old_names)
    new_names = old_names | frozenset(names)
    if old_names == new_names:
        return False

    # Rewrite the source code
    s = "\n".join(f'    "{n}",' for n in sorted(new_names, key=lambda n: n.casefold()))
    tmp.write_text(f"{prefix}PlatformNames = (\n{s}\n){suffix}", encoding="utf8")
    tmp.replace(file)
    return True


PAGE_PATTERN = re.compile(
    r"""
    <select[ ]name="platform_id\[\]"[ ]id="platform_id"[^>]*>
        \s*
        (
            (?: (?: <option[^>]*>[^<]*</option>) \s* )*
        )
    </select>
    """, re.VERBOSE
)


OPTION_PATTERN = re.compile(r'<option[^>]*>([^<]*)</option>')


class DownloadFailed(Exception):
    pass


def download() -> list[str]:
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

    match = PAGE_PATTERN.search(page)
    assert match is not None

    platforms = OPTION_PATTERN.findall(match.group(1))
    platforms = [CanonicalPlatformNames.get(p, p) for p in platforms]

    for p in platforms:
        print(f'"{p}",')

    return platforms
