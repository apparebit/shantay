"""
Shantay's color palette for charting categorical data.

It is based on [Observable's 2024 color
palette](https://observablehq.com/blog/crafting-data-colors) with changes to the
purple and brown as well as two more colors. Overally, colors are fairly
saturated and bright, hence facilitating charts that "pop". However, that can be
a bit much at times, so manual curation still matters.
"""

BLUE = "#4269d0"
ORANGE = "#efb118"
RED = "#ff725c"
CYAN = "#6cc5b0"
GREEN = "#3ca951"
PINK = "#ff8ab7"
PURPLE = "#a365ef"
LIGHT_BLUE = "#97bbf5"
BROWN = "#a57356"
GRAY = "#9498a0"

MAGENTA = "#b955a6"
OLIVE = "#b1b747"

RED1 = "#b22817"
RED2 = "#d54b38"
RED3 = "#f86c56"
ORANGE3 = "#bb8310"
ORANGE3 = "#dba23f"
ORANGE3 = "#fdc262"
YELLOW1 = "#d9bd40"
YELLOW2 = "#fade65"
GREEN1 = "#0f782c"
GREEN2 = "#39964a"
GREEN3 = "#5ab668"
GREEN4 = "#7bd686"
GREEN5 = "#9bf8a6"
CYAN1 = "#019585"
CYAN2 = "#3db4a3"
CYAN3 = "#62d4c2"
CYAN4 = "#85f6e3"
BLUE1 = "#3160b2"
BLUE2 = "#4d7ed3"
BLUE3 = "#6b9df5"
PURPLE1 = "#9b5296"
PURPLE2 = "#bc6fb5"
PURPLE3 = "#dd8ed5"
PURPLE4 = "#ffaef7"


RAINBOW = [
    "#ed8bab",
    "#f09270",
    "#d7a448",
    "#a4b95a",
    "#5dc693",
    "#1ac5cd",
    "#5bb8f4",
    "#9fa5fb",
    "#d193df",

    "#ff8386",
    "#e1a103",
    "#60ca46",
    "#0cc5cd",
    "#7daeff",
    "#da8aec",

    "#de4452",
    "#ffba24",
    "#50bb34",
    "#45d6de",
    "#4c81d8",
    "#e383f9",
]


PALETTE = [
    BLUE,
    ORANGE,
    RED,
    CYAN,
    GREEN,
    PINK,
    PURPLE,
    LIGHT_BLUE,
    BROWN,
    GRAY,
]


if __name__ == "__main__":
    from pathlib import Path

    path = Path.cwd() / "palette.txt"
    tmp = path.with_suffix(".tmp.txt")

    with open(tmp, mode="w", encoding="utf8") as file:
        for color in PALETTE:
            file.write(color)
            file.write("\n")

    tmp.replace(path)
