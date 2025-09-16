#!.venv/bin/python

import argparse

import polars as pl


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "field",
        help="the text field to query for"
    )
    options = parser.parse_args()

    data = pl.read_parquet("dsa-db-staging/db.parquet")
    result = data.filter(
        pl.col("column").eq(options.field).and_(
            pl.col("count").gt(0)
        )
    ).group_by(
        pl.col("column", "variant", "text"),
    ).agg(
        pl.col("count").sum(),
    ).sort(
        pl.col("count"),
        descending=True,
    )

    pl.Config.set_tbl_rows(100)
    print(result)


if __name__ == "__main__":
    main()
