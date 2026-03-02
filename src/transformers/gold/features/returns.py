# TODO Fetch the data in silver
# TODO Add returns features
# TODO Add Moving Average features
# TODO Update the models prices_1d
# TODO Create a supabase link for the data_platform 
# TODO Learn about airflow automatisation and workflows 


import polars as pl


def add_return(df: pl.DataFrame, column):
	column_name = f"{column}_returns"

	df = df.with_columns(
		((pl.col(column) / pl.col(column).shift(1)) - 1 ).alias(column_name)
	)

	return df

