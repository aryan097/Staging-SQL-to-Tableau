# sql_connect.py
# Inline SQL aggregation -> pivot-ready rows; no stored procedures.
# Requires: pandas, pyodbc

import json
import logging
from pathlib import Path
from typing import Optional

import pandas as pd
import pyodbc

LOGGER = logging.getLogger(__name__)

def _load_sql_cfg(cfg_path: str | Path = "sql.json") -> dict:
    with open(cfg_path, "r") as f:
        return json.load(f)

def get_sql_connection(cfg_path: str | Path = "sql.json") -> pyodbc.Connection:
    cfg = _load_sql_cfg(cfg_path)
    driver = cfg.get("DRIVER", "{ODBC Driver 17 for SQL Server}")
    conn_str = (
        f"DRIVER={driver};"
        f"SERVER={cfg['SQL_SERVER']};"
        f"DATABASE={cfg['SQL_DATABASE']};"
        f"UID={cfg['SQL_UID']};"
        f"PWD={cfg['SQL_PWD']};"
        "Encrypt=Yes;TrustServerCertificate=Yes;"
    )
    LOGGER.info("Connecting to SQL Server %s / DB %s", cfg["SQL_SERVER"], cfg["SQL_DATABASE"])
    return pyodbc.connect(conn_str)

def fetch_pivot_ready_inline(
    cfg_path: str | Path = "sql.json",
    product_label: str = "Product Not Appropriate",
    start_date: Optional[str] = None,   # 'YYYY-MM-DD' or None
    end_date: Optional[str] = None,     # 'YYYY-MM-DD' or None
    with_grand_total: bool = False,
) -> pd.DataFrame:
    """
    Returns pivot-ready rows with columns:
      [EventEndingWeek, Valid_SumOfVolume, Valid_%OfVolume, Total_SumOfVolume, Total_%]
    If with_grand_total=True, also includes:
      [RowLabel] with a final 'Grand Total' row.
    """
    weekly_sql = """
    WITH Base AS (
        SELECT
            EventEndingWeek = CONVERT(date, SnapDate),
            ValidVol = CASE WHEN segment4 = N'Valid'
                            THEN TRY_CONVERT(decimal(18,4), Volume) ELSE 0 END,
            TotalVol = TRY_CONVERT(decimal(18,4), Volume)
        FROM dbo.Autocomplete
        WHERE segment3 = ?
          AND (? IS NULL OR CONVERT(date, SnapDate) >= ?)
          AND (? IS NULL OR CONVERT(date, SnapDate) <= ?)
    )
    SELECT
        EventEndingWeek,
        Valid_SumOfVolume = SUM(ValidVol),
        Valid_%OfVolume   = CAST(SUM(ValidVol) / NULLIF(SUM(TotalVol),0) AS decimal(18,6)),
        Total_SumOfVolume = SUM(TotalVol),
        Total_%           = CAST(1.0 AS decimal(18,6))
    FROM Base
    GROUP BY EventEndingWeek
    ORDER BY EventEndingWeek;
    """

    rollup_sql = """
    WITH Base AS (
        SELECT
            EventEndingWeek = CONVERT(date, SnapDate),
            ValidVol = CASE WHEN segment4 = N'Valid'
                            THEN TRY_CONVERT(decimal(18,4), Volume) ELSE 0 END,
            TotalVol = TRY_CONVERT(decimal(18,4), Volume)
        FROM dbo.Autocomplete
        WHERE segment3 = ?
          AND (? IS NULL OR CONVERT(date, SnapDate) >= ?)
          AND (? IS NULL OR CONVERT(date, SnapDate) <= ?)
    )
    SELECT
        RowLabel = CASE WHEN GROUPING(EventEndingWeek) = 1 THEN N'Grand Total' ELSE N'' END,
        EventEndingWeek,
        Valid_SumOfVolume = SUM(ValidVol),
        Valid_%OfVolume   = CAST(SUM(ValidVol) / NULLIF(SUM(TotalVol),0) AS decimal(18,6)),
        Total_SumOfVolume = SUM(TotalVol),
        Total_%           = CAST(1.0 AS decimal(18,6))
    FROM Base
    GROUP BY ROLLUP(EventEndingWeek)
    HAVING GROUPING(EventEndingWeek) = 1 OR SUM(TotalVol) > 0
    ORDER BY
        CASE WHEN GROUPING(EventEndingWeek) = 1 THEN 1 ELSE 0 END,
        EventEndingWeek;
    """

    sql = rollup_sql if with_grand_total else weekly_sql
    params = [product_label, start_date, start_date, end_date, end_date]

    LOGGER.info(
        "Running inline SQL (grand_total=%s) for product_label='%s', start=%s, end=%s",
        with_grand_total, product_label, start_date, end_date
    )

    with get_sql_connection(cfg_path) as conn:
        df = pd.read_sql(sql, conn, params=params)

    # Normalize date type for downstream/hyper
    if "EventEndingWeek" in df.columns:
        df["EventEndingWeek"] = pd.to_datetime(df["EventEndingWeek"]).dt.date

    LOGGER.info("Rows fetched: %d", len(df))
    # Print a preview to terminal (comment out later if not needed)
    print("\n--- Pivot Preview (first 10 rows) ---")
    print(df.head(10).to_string(index=False))
    print("-------------------------------------\n")

    return df