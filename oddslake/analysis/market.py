"""Piyasa örtük olasılıkları: bir bürodaki 1X2 oranlarından ham ve normalize olmayan olasılıklar.

Oran geçerliliği: ondalık oran sonlu ve > 1.0 olmalıdır. 0.0, negatif, 1.0 ve NaN /
sonsuz değerler NULL sayılır (NaN, DuckDB'de her sayıdan büyük sayılır; bu yüzden
isfinite kontrolü ayrıca yapılır). Olasılık p = 1 / oran; overround = toplam p.

Yalnızca seçilen büronun üç kolonu (col_id IN (...)) parquet'ten okunur; eşleme
column_catalog / bookmakers / outcomes / phases üzerinden yapılır.
"""

from __future__ import annotations

import duckdb
import pyarrow as pa


def implied_table(
    con: duckdb.DuckDBPyConnection,
    bookmaker_code: str,
    period: str = "FT",
    phase: str = "Acilis",
) -> pa.Table:
    """Her maç için 1X2 ham oranlar, örtük olasılıklar ve overround.

    Sütunlar: match_id INT32, odd_home, odd_draw, odd_away, p_home, p_draw, p_away,
    overround (hepsi DOUBLE). Oran yoksa veya geçersizse ilgili hücre NULL'dır;
    overround üç olasılıktan biri NULL ise NULL'dır. Satırlar match_id sıralıdır ve
    `matches` tablosundaki her maç için bir satır vardır.
    Bilinmeyen büro, faz veya dönem için ValueError fırlatılır.
    """
    keys = {"home": f"{period}_1", "draw": f"{period}_X", "away": f"{period}_2"}
    rows = con.execute(
        """SELECT o.outcome_key, c.col_id
           FROM column_catalog c
           JOIN bookmakers b ON b.bookmaker_id = c.bookmaker_id
           JOIN phases p ON p.phase_id = c.phase_id
           JOIN outcomes o ON o.outcome_id = c.outcome_id
           WHERE b.code = ? AND p.code = ? AND o.line IS NULL
             AND o.outcome_key IN (?, ?, ?)""",
        [bookmaker_code, phase, keys["home"], keys["draw"], keys["away"]],
    ).fetchall()
    col_of = {key: int(cid) for key, cid in rows}
    missing = [key for key in keys.values() if key not in col_of]
    if missing:
        raise ValueError(
            f"kolon bulunamadı: büro={bookmaker_code!r}, faz={phase!r}, sonuçlar={missing}"
        )
    c_home, c_draw, c_away = col_of[keys["home"]], col_of[keys["draw"]], col_of[keys["away"]]

    def _valid_odd(col_id: int) -> str:
        # max(): her (maç, kolon) çifti tek satır olduğundan sonuç değişmez;
        # NULL satırlar aggregate tarafından yok sayılır.
        return (
            f"max(CASE WHEN od.col_id = {col_id} AND isfinite(od.odd) AND od.odd > 1.0 "
            f"THEN CAST(od.odd AS DOUBLE) END)"
        )

    query = f"""
WITH picked AS (
    SELECT od.match_id,
           {_valid_odd(c_home)} AS odd_home,
           {_valid_odd(c_draw)} AS odd_draw,
           {_valid_odd(c_away)} AS odd_away
    FROM odds od
    WHERE od.col_id IN ({c_home}, {c_draw}, {c_away})
    GROUP BY od.match_id
),
joined AS (
    SELECT m.match_id, p.odd_home, p.odd_draw, p.odd_away
    FROM matches m
    LEFT JOIN picked p ON p.match_id = m.match_id
)
SELECT CAST(match_id AS INTEGER) AS match_id,
       odd_home, odd_draw, odd_away,
       1.0 / odd_home AS p_home,
       1.0 / odd_draw AS p_draw,
       1.0 / odd_away AS p_away,
       1.0 / odd_home + 1.0 / odd_draw + 1.0 / odd_away AS overround
FROM joined
ORDER BY match_id
"""
    return con.execute(query).to_arrow_table()
