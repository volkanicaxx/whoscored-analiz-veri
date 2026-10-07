"""Takım formu: son N oynanmış maçta puan, gol ve galibiyet oranları.

Her maç için iki takım satırı üretilir (ev sahibi 'H', deplasman 'A'); oynanmamış
maçlar da satır olarak yer alır, çünkü fikstürün form özelliği de gerekir. Pencere
metrikleri yalnızca takımın KESİN ÖNCEKİ, oynanmış maçlarından hesaplanır:

  * sıralama (kickoff_utc, match_id); kendi maçı ve sonraki maçlar dahil edilmez,
  * oynanmamış maçlar (ft_home IS NULL) geçmişe girmez,
  * 0-0 maçlar girer (0 gol, beraberlik, gol yememe),
  * pencere sezon sınırında sıfırlanmaz; takım geçmişi sezonlar arasında taşınır.

Hesap tek bir DuckDB sorgusudur ve doğrudan Parquet'e COPY edilir; sonuç Python'a
tam yüklenmez. Pencere için önek toplamları (prefix sums) kullanılır: takımın c
oynanmış maçı varsa, son n maçın toplamı = önek[c] - önek[max(c - n, 0)]. Bu sayede
pencere fonksiyonu hiçbir zaman oynanmamış satırları saymaz.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Sequence

import duckdb


def _validate_windows(windows: Sequence[int]) -> tuple[int, ...]:
    ws = tuple(int(w) for w in windows)
    if not ws:
        raise ValueError("en az bir pencere gerekli")
    if any(w < 1 for w in ws):
        raise ValueError(f"pencere 1 veya daha büyük olmalı: {ws}")
    if len(set(ws)) != len(ws):
        raise ValueError(f"pencereler benzersiz olmalı: {ws}")
    return ws


def _form_sql(windows: tuple[int, ...]) -> str:
    """Form sorgusunu üretir. Pencere değerleri doğrulanmış tam sayılardır."""
    # Takım başına her maç satırı için: c = o takımın kendi maçından ÖNCE oynadığı
    # oynanmış maç sayısı. count(gf) boş çerçevede 0 döner, NULL üretmez.
    lookup_joins = []
    raw_cols = []
    out_cols = []
    for n in windows:
        a = f"lo{n}"
        lookup_joins.append(
            f"JOIN pref AS {a} ON {a}.team_id = b.team_id AND {a}.k = greatest(b.c - {n}, 0)"
        )
        raw_cols.append(
            f"b.c - {a}.k AS games_{n}, "
            f"hi.cpts - {a}.cpts AS pts_{n}, "
            f"hi.cgf - {a}.cgf AS gf_{n}, "
            f"hi.cga - {a}.cga AS ga_{n}, "
            f"hi.ccs - {a}.ccs AS cs_{n}, "
            f"hi.cwin - {a}.cwin AS win_{n}"
        )
        out_cols.append(
            f"games_{n}, "
            f"CAST(pts_{n} AS DOUBLE) / NULLIF(games_{n}, 0) AS ppg_{n}, "
            f"CAST(gf_{n} AS DOUBLE) / NULLIF(games_{n}, 0) AS gf_avg_{n}, "
            f"CAST(ga_{n} AS DOUBLE) / NULLIF(games_{n}, 0) AS ga_avg_{n}, "
            f"CAST(cs_{n} AS DOUBLE) / NULLIF(games_{n}, 0) AS cs_rate_{n}, "
            f"CAST(win_{n} AS DOUBLE) / NULLIF(games_{n}, 0) AS win_rate_{n}"
        )
    return f"""
WITH tm AS (
    SELECT m.match_id, m.home_team_id AS team_id, 'H' AS side, m.kickoff_utc,
           m.league_id, m.season,
           CAST(m.ft_home AS BIGINT) AS gf, CAST(m.ft_away AS BIGINT) AS ga
    FROM matches m
    UNION ALL
    SELECT m.match_id, m.away_team_id, 'A', m.kickoff_utc,
           m.league_id, m.season,
           CAST(m.ft_away AS BIGINT), CAST(m.ft_home AS BIGINT)
    FROM matches m
),
seq AS (
    SELECT tm.*,
           count(gf) OVER (
               PARTITION BY team_id ORDER BY kickoff_utc, match_id
               ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
           ) AS c
    FROM tm
),
played AS (
    SELECT team_id, kickoff_utc, match_id, gf, ga,
           CAST(CASE WHEN gf > ga THEN 3 WHEN gf = ga THEN 1 ELSE 0 END AS BIGINT) AS pts,
           CAST(CASE WHEN gf > ga THEN 1 ELSE 0 END AS BIGINT) AS win,
           CAST(CASE WHEN ga = 0 THEN 1 ELSE 0 END AS BIGINT) AS cs
    FROM seq
    WHERE gf IS NOT NULL
),
pref AS (
    SELECT team_id, k, cgf, cga, cpts, ccs, cwin
    FROM (
        SELECT team_id,
               row_number() OVER w AS k,
               sum(gf)  OVER w AS cgf,
               sum(ga)  OVER w AS cga,
               sum(pts) OVER w AS cpts,
               sum(cs)  OVER w AS ccs,
               sum(win) OVER w AS cwin
        FROM played
        WINDOW w AS (PARTITION BY team_id ORDER BY kickoff_utc, match_id ROWS UNBOUNDED PRECEDING)
    )
    UNION ALL
    SELECT team_id, CAST(0 AS BIGINT), CAST(0 AS HUGEINT), CAST(0 AS HUGEINT),
           CAST(0 AS HUGEINT), CAST(0 AS HUGEINT), CAST(0 AS HUGEINT)
    FROM (SELECT DISTINCT team_id FROM tm)
),
base AS (
    SELECT match_id, team_id, side, kickoff_utc, league_id, season, c
    FROM seq
),
raw AS (
    SELECT b.match_id, b.team_id, b.side, b.kickoff_utc, b.league_id, b.season,
           {", ".join(raw_cols)}
    FROM base b
    JOIN pref AS hi ON hi.team_id = b.team_id AND hi.k = b.c
    {" ".join(lookup_joins)}
)
SELECT match_id, team_id, side, kickoff_utc, league_id, season,
       {", ".join(out_cols)}
FROM raw
ORDER BY kickoff_utc, match_id, side
"""


def compute_form(con: duckdb.DuckDBPyConnection, out_path: Path, windows: tuple[int, ...] = (5, 10)) -> int:
    """Her maç x takım için pencere form metriklerini Parquet'e yazar.

    Dönüş: yazılan satır sayısı (her maç için 2 satır; beklenen toplam = 2 x matches).
    Sütunlar: match_id, team_id, side, kickoff_utc, league_id, season; her n için
    games_n, ppg_n, gf_avg_n, ga_avg_n, cs_rate_n, win_rate_n.
    """
    ws = _validate_windows(windows)
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    expected = int(con.execute("SELECT count(*) FROM matches").fetchone()[0]) * 2

    tmp = out_path.with_name(out_path.name + ".partial")
    tmp.unlink(missing_ok=True)
    query = _form_sql(ws)
    tmp_sql = "'" + tmp.as_posix().replace("'", "''") + "'"
    try:
        written = int(con.execute(f"COPY ({query}) TO {tmp_sql} (FORMAT parquet)").fetchone()[0])
        if written != expected:
            raise RuntimeError(f"form satır sayısı beklenenden farklı: {written} != {expected}")
        os.replace(tmp, out_path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return written
