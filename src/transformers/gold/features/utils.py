import polars as pl

def requires_column(df: pl.DataFrame, column: str):
	if column not in df.columns:
		raise ValueError(f"The column: {column} is not in the dataset")


def add_log_column(df: pl.DataFrame, column: str):
	column_name = f"log_{column}"
	requires_column(df, column)
	df = df.with_columns(
		pl.col(column).log().alias(column_name)
	)
	return df



