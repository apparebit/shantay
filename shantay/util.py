from collections.abc import Iterable, Sequence
import functools
import inspect
import math
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


def minify(value: int) -> str:
    """Format the value with three digits and optionally one letter."""
    limit = 1_000
    round = 2

    while limit <= value and limit <= 1_000_000_000_000:
        limit *= 10
        round += 1

    factor = 1_000 ** (round // 3)
    prec = 2 - (round % 3)
    letter = " KMGT"[round // 3].strip()

    return f"{value / factor:,.{prec}f}{letter}"


def scale(value: float) -> tuple[float, str]:
    """Scale the value to three digits before the decimal and a unit prefix."""
    if value < 0:
        sign = -1
        value *= -1
    else:
        sign = 1

    if value < 0.001:
        return sign * value * 1_000_000, "micro"
    elif value < 1:
        return sign * value * 1_000, "milli"
    elif value < 1_000:
        return sign * value, ""
    elif value < 1_000_000:
        return sign * value / 1_000, "kilo"
    elif value < 1_000_000_000:
        return sign * value / 1_000_000, "mega"
    else:
        return sign * value / 1_000_000_000, "giga"


def scale_time(value: float) -> tuple[float, str]:
    """
    Turn the duration in seconds into minutes, hours, or days if it is at least
    one minute, hour, or day long, respectively.
    """
    if value < 0:
        sign = -1
        value *= -1
    else:
        sign = 1

    if value < 60:
        return sign * value, "sec"
    elif value < 60 * 60:
        return sign * value / 60, "min"
    elif value < 24 * 60 * 60:
        return sign * value / (60 * 60), "hour"
    else:
        return sign * value / (24* 60 * 60), "day"


def upper_limit(n: int, *, leading: int = 2, minimum: int = 100) -> int:
    """
    Ensure that the given, non-negative integer is at least the positive
    `minimum` and has at most the positive `leading` non-zero digits. If that's
    not the case, round up the maximum of the integer `n` and `minimum` to the
    next integer with at most `leading` non-zero digits.

    For example:

    >>> upper_limit(123, leading=1)
    200
    >>> upper_limit(123, leading=2)
    130
    >>> upper_limit(123, leading=3)
    123
    >>> upper_limit(123, leading=1, minimum=999)
    1000

    This function comes in handy for setting the upper limit of the y-axis of a
    chart.
    """
    if n < 0:
        raise ValueError(f'number {n} is negative')
    if leading <= 0:
        raise ValueError(f'number of leading digits {leading} is not positive')
    if minimum <= 0:
        raise ValueError(f'minimum value {minimum} is not positive')

    n = max(n, minimum)
    width = math.ceil(math.log10(n + 1))
    if width <= leading:
        return n

    factor = 10 ** (width - leading)
    return math.ceil(n / factor) * factor


def to_markdown_table(
    *rows: Sequence[object],
    columns: Sequence[str],
    title: None | str = None,
    left_alignments: None | Sequence[bool] = None,
) -> str:
    """
    Format the rows of values as a Markdown table with the given column
    headings. The generated Markdown is nicely formatted so that column
    boundaries are aligned across rows.

    By default, columns with integers and floats are right-aligned and all other
    columns are left-aligned. `None` values are ignored for determining
    alignment.
    """
    column_data = [[it for it in column] for column in zip(*rows)]
    if len(column_data) == 0:
        raise ValueError("no data columns to format")
    if len(column_data) != len(columns):
        raise ValueError(f"{len(column_data)} columns but {len(columns)} column names")
    if left_alignments is not None and len(left_alignments) != len(columns):
        raise ValueError(
            f"{len(columns)} columns but {len(left_alignments)} left alignment values"
        )

    types = [_get_type(column) for column in column_data]
    column_data = [
        [fmt(it) for it in column]
        for fmt, column in zip((_get_format(tp) for tp in types), column_data)
    ]
    widths = [
        max(len(name) + 2, *(l + 2 for it in column if (l := len(it)) < 50))
        for name, column in zip(columns, column_data)
    ]
    if left_alignments is None:
        left_alignments = [tp is str for tp in types]

    def format_row(data: Iterable[str]) -> str:
        items = (
            (f"{it:<{w-2}}" if al else f"{it:>{w}}")
            for it, w, al in zip(data, widths, left_alignments)
        )
        return f'| {" | ".join(items)} |'

    def format_div() -> str:
        items = []
        for width, is_left_aligned in zip(widths, left_alignments):
            before = ":" if is_left_aligned else ""
            dashes = "-" * (width - 3)
            after = "" if is_left_aligned else ":"
            items.append(f"{before}{dashes}{after}")
        return f'| {" | ".join(items)} |'

    return "\n".join([
        *(() if title is None else (f"### {title}", "")),
        format_row(columns),
        format_div(),
        *(format_row(row) for row in zip(*column_data)),
    ])

def _get_type(column: Sequence[object]) -> type[int] | type[float] | type[str]:
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
        return lambda c: "" if c is None else f"{c:,}"
    elif tp is float:
        return lambda c: "" if c is None else f"{c:.1f}"
    else:
        return lambda c: "" if c is None else f"{c}"
