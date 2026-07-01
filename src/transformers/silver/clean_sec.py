from __future__ import annotations

import polars as pl

_SCHEMA = {
    "ticker": pl.Utf8,
    "concept": pl.Utf8,
    "unit": pl.Utf8,
    "period_start": pl.Date,
    "period_end": pl.Date,
    "fy": pl.Int64,
    "fp": pl.Utf8,
    "form": pl.Utf8,
    "val": pl.Float64,
    "accn": pl.Utf8,
    "filed": pl.Date,
}

# Primary key of the gold fundamentals table: one observation per concept/unit
# per reported period (fp/form distinguish quarterly vs annual filings of the
# same concept; fy is informational only and not part of the key since XBRL
# occasionally omits it). XBRL can carry multiple accn (filings) reporting the
# same combo (e.g. a restatement) -- clean_bronze_sec() keeps the most
# recently filed one for each.
_DEDUP_KEY = ["ticker", "concept", "unit", "period_end", "fp", "form"]


def normalize_sec(raw: dict) -> list[dict]:
    """Normalize a SEC EDGAR companyfacts bronze envelope into flat rows.

    raw["payload"] is the raw XBRL companyfacts response, nested
    taxonomy -> concept -> units -> unit -> [observations]:
      {"facts": {"us-gaap": {"Revenues": {"units": {"USD": [
          {"end": "2023-12-31", "start": "2023-01-01", "val": 123,
           "accn": "...", "fy": 2023, "fp": "FY", "form": "10-K",
           "filed": "2024-02-01"}, ...
      ]}}}}}
    raw["meta"]["partitions"]["symbol"] carries the ticker (set by
    ingest_edgar_financial_to_bronze), since the SEC payload itself has no
    ticker, only a CIK.
    """
    meta = raw.get("meta") or {}
    ticker = (meta.get("partitions") or {}).get("symbol")
    if not ticker:
        raise ValueError("bronze envelope is missing meta.partitions.symbol")

    payload = raw.get("payload") or {}
    taxonomies = payload.get("facts") or {}

    out: list[dict] = []
    for taxonomy, concepts in taxonomies.items():
        for concept_name, concept_body in (concepts or {}).items():
            units = (concept_body or {}).get("units") or {}
            for unit, observations in units.items():
                for obs in observations or []:
                    period_end = obs.get("end")
                    val = obs.get("val")
                    if not period_end or val is None:
                        continue
                    out.append(
                        {
                            "ticker": ticker,
                            "concept": f"{taxonomy}:{concept_name}",
                            "unit": unit,
                            "period_start": obs.get("start"),
                            "period_end": period_end,
                            "fy": obs.get("fy"),
                            "fp": obs.get("fp"),
                            "form": obs.get("form"),
                            "val": float(val),
                            "accn": obs.get("accn"),
                            "filed": obs.get("filed"),
                        }
                    )

    return out


def clean_bronze_sec(rows: list[dict]) -> pl.DataFrame:
    if not rows:
        return pl.DataFrame(schema=_SCHEMA)

    df = pl.DataFrame(
        rows,
        schema_overrides={
            "fy": pl.Int64,
            "period_start": pl.Utf8,
            "period_end": pl.Utf8,
            "filed": pl.Utf8,
            "fp": pl.Utf8,
            "form": pl.Utf8,
        },
    )
    df = df.with_columns(
        pl.col("period_start").str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
        pl.col("period_end").str.strptime(pl.Date, format="%Y-%m-%d"),
        pl.col("filed").str.strptime(pl.Date, format="%Y-%m-%d", strict=False),
        # fp/form are part of the table's primary key, so they can't be null.
        pl.col("fp").fill_null("UNK"),
        pl.col("form").fill_null("UNK"),
    )

    # keep the most recently filed observation for each (concept, unit,
    # period, fy/fp/form) combo -- later filings (restatements) win.
    df = df.sort("filed").unique(subset=_DEDUP_KEY, keep="last")
    df = df.sort(["ticker", "concept", "period_end"])

    return df
