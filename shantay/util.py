from collections.abc import Iterable
import functools
import inspect
from textwrap import dedent
from typing import Callable


def annotate_error[**P, R](
    filename_arg: None | str = None
) -> Callable[[Callable[P, R]], Callable[P, R]]:
    """
    Annotate errors with missing information.

    Notably, if the error is an OSError without filename attribute, this wrapper
    determines the value of the named argument and updates the error's filename
    attribute with the stringified value of that argument.

    This decorator is motivated by shutil.copyfileobj() not setting the filename
    attribute upon OS error number 28, no space left on device, even though the
    file path is critical for determining the impacted device. Hence the wrapper
    updates the error's filename attribute with the stringified value of the
    named argument. That is, unless the filename is already set, in which case
    the wrapper does nothing.
    """
    def wrapper(fn: Callable[P, R]) -> Callable[P, R]:
        # No argument, nothing to annotate with
        if filename_arg is None:
            return fn

        sig = inspect.signature(fn)

        @functools.wraps(fn)
        def inner(*args: P.args, **kwargs: P.kwargs) -> R:
            try:
                return fn(*args, **kwargs)
            except OSError as x:
                if x.filename is None:
                    assert filename_arg is not None
                    value = sig.bind(*args, **kwargs).arguments[filename_arg]
                    x.filename = str(value)
                raise x
        return inner
    return wrapper


def scale(value: float) -> tuple[float, str]:
    """Scale the value to three digits before the decimal and a unit prefix."""
    if value < 0.001:
        return value * 1_000_000, "micro"
    elif value < 1:
        return value * 1_000, "milli"
    elif value < 1_000:
        return value, ""
    elif value < 1_000_000:
        return value / 1_000, "kilo"
    elif value < 1_000_000_000:
        return value / 1_000_000, "mega"
    else:
        return value / 1_000_000_000, "giga"


def to_markdown_table(rows: list[list[object]], column_names: list[str], title: str) -> str:
    columns = [[it for it in column] for column in zip(*rows)]
    if len(columns) == 0:
        raise ValueError("no data columns to format")
    if len(columns) != len(column_names):
        raise ValueError(f"{len(columns)} columns but {len(column_names)} column names")

    types = [_get_type(column) for column in columns]
    columns = [
        [fmt(it) for it in column]
        for fmt, column in zip((_get_format(tp) for tp in types), columns)
    ]
    widths = [
        max(len(name) + 6, *(len(it) + 2 for it in column))
        for name, column in zip(column_names, columns)
    ]

    def format_row(data: Iterable[str]) -> str:
        items = (
            (f"{it:<{w}}" if tp is str else f"{it:>{w}}")
            for it, w, tp in zip(data, widths, types)
        )
        return f'| {" | ".join(items)} |'

    def format_div() -> str:
        items = []
        for width, tp in zip(widths, types):
            before = ":" if tp is str else ""
            dashes = "-" * (width - 3)
            after = "" if tp is str else ":"
            items.append(f" {before}{dashes}{after} ")
        return f'| {" | ".join(items)} |'

    return dedent(f"""\
        __{title}__

        {format_row(column_names)}
        {format_div()}
        {"\n".join(format_row(row) for row in zip(*columns))}
    """)


def _get_type(column: list[object]) -> type[int] | type[float] | type[str]:
    tp = None
    for cell in column:
        if cell is None:
            continue

        ct = type(cell)
        if tp is None and ct in (int, float):
            tp = ct
        elif tp is ct:
            pass
        elif tp is int and ct is float or tp is float and ct is int:
            tp = float
        else:
            tp = str
            break

    assert tp is not None
    return tp


def _get_format(tp: type) -> Callable[[object], str]:
    if tp is int:
        return lambda c: f"{c:,}"
    elif tp is float:
        return lambda c: f"{c:.1f}"
    else:
        return lambda c: f"{c}"
