import enum

class Palette(enum.StrEnum):
    """
    Shantay's categorical color palette.

    This palette is based on and largely the same as [Observable's 2024 color
    palette](https://observablehq.com/blog/crafting-data-colors). This selection
    tends towards bright and saturated colors, which make charts "pop". That
    same quality can be a bit much at times, so manual curation still matters.
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

    @classmethod
    def cycle(cls, index: int) -> str:
        """Cycle through the colors of the palette, ignoring gray."""
        return _COLORS[index % len(_COLORS)]


_COLORS = [c for c in Palette.__members__.values() if c is not Palette.GRAY]


if __name__ == "__main__":
    from pathlib import Path

    path = Path.cwd() / "palette.txt"
    tmp = path.with_suffix(".tmp.txt")

    with open(tmp, mode="w", encoding="utf8") as file:
        for color in Palette.__members__.values():
            file.write(color)
            file.write("\n")

    tmp.replace(path)
