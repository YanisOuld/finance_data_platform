import polars as pl


def add_return(df: pl.DataFrame, column: str, group_by: str = "symbol") -> pl.DataFrame:
    """Daily simple return, computed per-instrument so multi-ticker batches
    never compute a return across two different symbols' price series.
    """
    column_name = f"{column}_returns"

    df = df.sort([group_by, "ts"])
    df = df.with_columns(((pl.col(column) / pl.col(column).shift(1).over(group_by)) - 1).alias(column_name))

    return df
