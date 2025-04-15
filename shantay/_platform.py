# ===========================
#  This module automatically
#      maintains itself.
#
#     PLEASE DO NOT EDIT!
# ===========================

from collections.abc import Sequence

PlatformNamesToo = (
)

def update(names: Sequence[str]) -> None:
    with open(__file__, mode="r", encoding="utf8") as file:
        source_code = file.read()

    header, assign_open, _ = source_code.partition("\nPlatformNamesToo = (\n")
    _, assign_close, footer = source_code.partition("\n)")

    # Leading and trailing newlines are Only inject newlines between name lines, since leading and trailing ne
    names_too = "\n".join(f'    "{n}",' for n in names)
    with open(__file__, mode="w", encoding="utf8") as file:
        file.write(f"{header}{assign_open}{names_too}{assign_close}{footer}")
