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
    MAGENTA,
    OLIVE,
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
