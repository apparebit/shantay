# In my experience, all color palettes are problematic in some way, with colors
# being too bright, too dull, too similar, and so on. Observable's color palette
# is no exception. It tends towards bright, clean hues that are fairly
# saturated, which makes for charts that "pop." But by the same token, its
# colors can be overwhelming, too aggressive. As a result, manual curation
# matters. Nonetheless, in my subjective judgement, the palette is clearly
# preferable over the Brewer palettes, which tend towards duller, darker, or
# lighter extremes.
#
# I did make one change, adding more colors, because they were needed for
# illustrating the use of keywords.
#
# https://observablehq.com/blog/crafting-data-colors

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
