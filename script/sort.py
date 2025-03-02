#!/usr/bin/env python3

from pathlib import Path
import sys
import traceback

WIDTH = len("2024-03-14 00:00:27") + 2

def sort(argv: list[str]) -> None:
    if len(argv) == 0:
        print("invoke as: `sort <csv-file>+`")
        return

    for str_path in argv:
        path = Path(str_path)
        with path.open(mode="r", encoding="utf8") as file:
            lines = file.readlines()

        header = lines[0]
        body = sorted(lines[1:], key=lambda l: l[-WIDTH:-2])

        path = path.with_suffix(f".sorted{path.suffix}")
        with path.open(mode="w", encoding="utf8") as file:
            file.write(header)
            file.write("".join(body))

if __name__ == "__main__":
    try:
        sort(sys.argv[1:])
        sys.exit(0)
    except Exception as x:
        print("".join(traceback.format_exception(x)))
        sys.exit(1)
