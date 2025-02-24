import great_tables as gt
from IPython.display import HTML
import polars as pl


def to_html(df: pl.DataFrame) -> str:
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
