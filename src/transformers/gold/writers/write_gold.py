'''
But de cette section pour l'instant:

Prendre le parquet de s3
- Lire la données et s'assurer de son typage
- Ajouter des features efficacement
  - features de returns
  - features de moving average
- Le date est la primary key
- Upsert postgres avec les nouvelles datas ! 

Update les tableaux qui permet de keep track le dernier fetch
- faire un plan pour la gestion CRON de l'application



'''
import polars as pl

from src.data.crud.priced_1d import upsert_prices_1d
from src.core.database import SessionLocal


def write_gold_price1D(df: pl.DataFrame):
	with SessionLocal() as session:
		rows = df.to_dicts()
		upsert_prices_1d(session, rows)
		session.commit()
