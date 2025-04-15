"""
Shantay's color palette for charting categorical data.

It is the same as [Observable's 2024 color
palette](https://observablehq.com/blog/crafting-data-colors) with two more
colors (dark purple and yellow green) added. The palette features fairly
saturated and bright colors, hence facilitating charts that "pop." However, that
can be a bit much at times, so manual curation still matters.
"""
BLUE = "#4269d0"
ORANGE = "#efb118"
RED = "#ff725c"
CYAN = "#6cc5b0"
GREEN = "#3ca951"
PINK = "#ff8ab7"
PURPLE = "#a463f2"
LIGHT_BLUE = "#97bbf5"
BROWN = "#9c6b4e"
GRAY = "#9498a0"

DARK_PURPLE = "#a03d8e"
YELLOW_GREEN = "#bad44a"

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
    YELLOW_GREEN,
    DARK_PURPLE,
]

KEYWORD_PALETTE = [
    LIGHT_BLUE, BLUE, PURPLE, RED, ORANGE, GREEN, PINK, CYAN, BROWN, GRAY
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
