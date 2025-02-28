import great_tables as gt
from IPython.display import HTML
import polars as pl


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


def to_html(df: pl.DataFrame) -> HTML:
    df = df.select(
        pl.all().exclude(
            "incompatible_content_explanation",
            "decision_facts",
            "platform_uid",
        )
    )

    return HTML(
        df.style
        .tab_style(
            style=gt.style.text(weight="bold"),
            locations=gt.loc.column_labels(),
        )
        .cols_align(align="left")
        .tab_options(
            table_font_size="15px",
            table_font_names=gt.system_fonts("humanist"),
        )
        .as_raw_html()
    )

# def to_md_table(df: pl.DataFrame) -> Markdown:
#     df = df.cast(str)
#     widths = df.with_columns(
#         pl.all().str.len_chars().max()
#     ).row(0)
#     colno = len(widths)

#     def row(data: Sequence) -> str:
#         assert len(data) == colno
#         return f"|{'|'.join([f'{data[i] or "":<{widths[i]}}' for i in range(colno)])}|\n"

#     def divider() -> str:
#         return f"|{'|'.join(['-' * widths[i] for i in range(colno)])}|\n"

#     return Markdown(
#         row(df.columns)
#         + divider()
#         + "".join(row(df.row(i)) for i in range(df.height))
#     )
