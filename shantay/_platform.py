# ===========================
#  This module automatically
#      maintains itself.
#
#     PLEASE DO NOT EDIT!
# ===========================

from collections.abc import Sequence
import polars as pl

PlatformNames = (
    "Adobe Lightroom",
    "AliExpress",
    "Amazon",
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
    "Stripchat",
    "Temu",
    "Threads",
    "TikTok",
    "Tinder",
    "VSCO",
    "Vinted",
    "Wallapop",
    "WhatsApp",
    "willhaben",
    "X",
    "YouTube",
    "Zalando",
)


PLATFORM_NAMES = frozenset(PlatformNames)


class MissingPlatformError(Exception):
    pass


def update(names: Sequence[str]) -> None:
    with open(__file__, mode="r", encoding="utf8") as file:
        source_code = file.read()

    # Beware that patterns include the leading and trailing newlines!
    header, assign_open, _ = source_code.partition("\nPlatformNames = (\n")
    _, assign_close, footer = source_code.partition("\n)")

    names_too = "\n".join(f'    "{n}",' for n in sorted(names))
    with open(__file__, mode="w", encoding="utf8") as file:
        file.write(f"{header}{assign_open}{names_too}{assign_close}{footer}")


def check_platform_names(label: str, frame: pl.DataFrame) -> None:
    used_names = frame.select(
        pl.col("platform_name").unique()
    ).get_column("platform_name")

    unknown_names = []
    for name in used_names:
        if name not in PLATFORM_NAMES:
            unknown_names.append(name)

    if len(unknown_names) == 0:
        return

    all_names = list(PlatformNames)
    all_names.extend(unknown_names)
    update(all_names)

    raise MissingPlatformError(f"""\
>> Please rerun shantay with the same command line arguments! <<

The transparency data for {label} includes
the following platforms for the first time:
{"\n".join("    * {n}" for n in unknown_names)}

Shantay includes platform names in the schema for its summary
statistics, which must be updated to include the above platforms.
In fact, shantay has already updated its internal list of
known platform names and needs to be restarted.

Please run shantay again with the exact same command line
arguments as the last run. Shantay will migrate the summary
statistics to the new schema and then resume processing daily
database releases, redoing only the work for the release that
included the new platforms.
""")
