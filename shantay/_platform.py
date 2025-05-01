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
import polars as pl


PlatformNames = (
    "Adobe Lightroom",
    "Adobe Photoshop Express",
    "Adobe Stock",
    "AliExpress",
    "Amazon",
    "Amazon Store",
    "App Store",
    "Apple Books (ebooks)",
    "Apple Podcasts Subscriptions",
    "Azar",
    "Badoo",
    "Behance",
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
    "daft.ie",
    "Dailymotion",
    "Deliveroo Ireland",
    "Discord",
    "Doctolib",
    "DoneDeal.ie",
    "EMAG.BG",
    "EMAG.HU",
    "EMAG.RO",
    "eMimino.cz",
    "Facebook",
    "Fashiondays.ro",
    "Flights",
    "Glassdoor",
    "Google Maps",
    "Google Play",
    "Google Shopping",
    "gutefrage.net",
    "Habbo",
    "Használtautó.hu",
    "Hinge",
    "Hotel Hideaway",
    "Hotels",
    "Hírstart",
    "Idealo",
    "imobiliare.ro",
    "Imovirtual",
    "ingatlan.com",
    "Instagram",
    "Joom",
    "Kaggle",
    "Kleinanzeigen",
    "leboncoin",
    "Ligaportal",
    "LinkedIn",
    "Meetic",
    "Microsoft Teams",
    "mobile.de",
    "nebenan.de",
    "OKCupid",
    "OLX",
    "OTTO",
    "Pinterest",
    "Plenty of Fish",
    "Pornhub",
    "Pub.dev",
    "Quora",
    "Rajče",
    "Rakuten",
    "Reddit",
    "Roblox",
    "Shein",
    "Snapchat",
    "SoundCloud",
    "Standvirtual",
    "Stepstone",
    "Stripchat",
    "Studydrive",
    "TAZZ",
    "Telia Yhteisö",
    "Temu",
    "Tenor",
    "The League",
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
    "Viator",
    "Vimeo",
    "Vinted",
    "VSCO",
    "Wallapop",
    "Waze",
    "WhatsApp",
    "willhaben",
    "X",
    "YouTube",
    "Zalando",
    "Živě.cz",
)


CanonicalPlatformNames = MappingProxyType({
    "Adobe Photoshop Lightroom": "Adobe Lightroom",
    "Discord Netherlands B.V.": "Discord",
    "Meetic SAS": "Meetic",
    "Microsoft Teams personal": "Microsoft Teams",
    "OTTO Market": "OTTO",
    "Quora Ireland Limited": "Quora",
    'SIA "JOOM"': "Joom",
    "Vinted UAB": "Vinted",
    "WhatsApp Channels": "WhatsApp",
    "willhaben internet service GmbH & Co KG": "willhaben",
    "www.gutefrage.net": "gutefrage.net"
})


class MissingPlatformError(Exception):
    pass


_KNOWN_PLATFORM_NAMES = frozenset(PlatformNames)
_logger = logging.getLogger(__spec__.parent)


# release is a string to avoid dependency on .model module
def check_new_platform_names(release: str, batch: int, frame: pl.DataFrame) -> None:
    """Check for previously unknown platform names."""
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
        (?P<names>[^)]*)
    [)]
    (?P<suffix>.*)
    $
    """,
    re.VERBOSE | re.DOTALL
)


def update_new_platform_names(names: Sequence[str]) -> None:
    """
    Update the source code of this module with the given platform names. To
    minimize the possibility of conflicting writes, this function should not be
    called from a worker process. However, concurrent tool runs can still result
    in conflicting writes. That is acceptable for three reasons:

     1. The update to the file itself is an atomic file replace operation. Since
        that implies whole file updates, it also ensures that the source code is
        always well-formed (barring bugs in this function).
     2. This function only adds names to the list of platform names. Hence any
        order and combination of serializable updates for the same names will
        always converge on the same final result.
     3. While concurrent updates may make names appear and disappear again, the
        restarted run of shantay will likely fail upon the disappeared name
        again. On the off-chance that it disappears after use, the concatenation
        of summary statistics will fail eventually.

    If concatenation fails, human intervention becomes necessary. It entails
    manually adding new platform names to this module. In other words, in the
    unlikely worst-case, users have to wait for a new tool release. However,
    without this module updating itself, waiting for a new tool release is the
    only option. In other words, the use of self-modifying code is justified.
    """
    # Shantay probably imported this module a while ago.
    # Let's get a more recent version.
    file = Path(__file__)
    tmp = file.with_suffix(".tmp.py")
    source_code = file.read_text(encoding="utf8")

    # Break the module into its parts.
    parts = _MODULE_PARTS.match(source_code)
    assert parts is not None
    prefix = parts.group("prefix")
    suffix = parts.group("suffix")

    # Rebuild the list of unique platform names and format as source code.
    all_names = list(eval(f"(\n{parts.group('names')})"))
    all_names.extend(names)
    unique_names = frozenset(all_names)
    new_names = "\n".join(
        f'    "{n.replace('\\', '\\\\').replace('"', '\\"')}",'
        for n in sorted(unique_names, key=lambda n: n.casefold())
    )

    # Atomically update this module.
    tmp.write_text(f"{prefix}PlatformNames = (\n{new_names}\n){suffix}", encoding="utf8")
    tmp.replace(file)
