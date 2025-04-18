# ===========================
#  This module automatically
#      maintains itself.
#
#     PLEASE DO NOT EDIT!
# ===========================

from collections.abc import Sequence
from types import MappingProxyType
import polars as pl


PlatformNames = (
    "Adobe Lightroom",
    "AliExpress",
    "Amazon",
    "Amazon Store",
    "App Store",
    "Badoo",
    "bolha.com",
    "Booking.com",
    "Bumble",
    "Campfire",
    "Canva",
    "Chrome Web Store",
    "Dailymotion",
    "Discord",
    "DoneDeal.ie",
    "Facebook",
    "Google Maps",
    "Google Play",
    "Google Shopping",
    "gutefrage.net",
    "Habbo",
    "Hinge",
    "Hotel Hideaway",
    "Idealo",
    "Instagram",
    "Joom",
    "Kleinanzeigen",
    "leboncoin",
    "LinkedIn",
    "Meetic",
    "Microsoft Teams",
    "OTTO",
    "Pinterest",
    "Pornhub",
    "Quora",
    "Rajče",
    "Reddit",
    "Roblox",
    "Snapchat",
    "SoundCloud",
    "Stripchat",
    "Temu",
    "Threads",
    "TikTok",
    "Tinder",
    "Twitch",
    "Vimeo",
    "Vinted",
    "VSCO",
    "Wallapop",
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


def do_update(names: Sequence[str]) -> None:
    with open(__file__, mode="r", encoding="utf8") as file:
        source_code = file.read()

    # Beware that patterns include the leading and trailing newlines!
    header, assign_open, _ = source_code.partition("\nPlatformNames = (\n")
    _, assign_close, footer = source_code.partition("\n)")

    names_too = "\n".join(
        f'    "{n.replace('\\', '\\\\').replace('"', '\\"')}",'
        for n in sorted(names, key=lambda n: n.lower())
    )
    with open(__file__, mode="w", encoding="utf8") as file:
        file.write(f"{header}{assign_open}{names_too}{assign_close}{footer}")


_KNOWN_PLATFORM_NAMES = frozenset(PlatformNames)


def detect_new_platform_names(label: str, frame: pl.DataFrame) -> None:
    used_names = frame.select(
        pl.col("platform_name").unique()
    ).get_column("platform_name")

    unknown_names = []
    for name in used_names:
        if name not in _KNOWN_PLATFORM_NAMES:
            unknown_names.append(name)

    if len(unknown_names) == 0:
        return

    all_names = list(PlatformNames)
    all_names.extend(unknown_names)
    do_update(all_names)

    raise MissingPlatformError(f"""

>> Please rerun shantay with the same command line arguments! <<

The transparency data for {label} includes
the following platform(s) for the very first time:
{"\n".join(f"    * {n}" for n in unknown_names)}

Since shantay includes platform names in the `variant` column of
the summary statistics, the corresponding enumeration type must
include all platforms. It takes three steps to make that happen:

 1. *Automatic*: Add platform name(s) to the list of platforms.
    Conveniently, Shantay already did that.
 2. *Manual*: Please restart shantay to pick up the modified list.
    Use the same command line arguments as for the failed run.
 3. *Automatic*: Upgrade existing data frames to the new schema.
    Shantay does so, when processing the failing batch again.

""")
