"""Elo derecesi: maçları kickoff sırasıyla akıtarak her maçın ÖNCEKİ derecelerini yazar.

Maçlar DuckDB'den Arrow kayıt parçaları (batch) halinde okunur; durum yalnızca
takım -> derece sözlüğüdür (birkaç bin girdi). Her parçanın sonucu hemen Parquet'e
yazılır, tüm sonuç RAM'de tutulmaz.

Kurallar:
  * İlk görülen takımın derecesi `base`'dir.
  * E_ev = 1 / (1 + 10^((R_dep - (R_ev + ev_avantajı)) / 400))
  * S_ev = 1.0 (ev kazanır), 0.5 (beraberlik, 0-0 dahil), 0.0 (deplasman kazanır)
  * R_ev += k * (S - E_ev);  R_dep += k * ((1 - S) - (1 - E_ev))
  * Oynanmamış maç (ft NULL): ön değerler yazılır, derece güncellenmez.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Callable

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

ELO_SCHEMA = pa.schema(
    [
        ("match_id", pa.int32()),
        ("home_elo_pre", pa.float64()),
        ("away_elo_pre", pa.float64()),
    ]
)


def compute_elo(
    con: duckdb.DuckDBPyConnection,
    out_path: Path,
    batch_rows: int = 50_000,
    k: float = 20.0,
    home_advantage: float = 65.0,
    base: float = 1500.0,
    cancel: Callable[[], bool] | None = None,
    progress: Callable[[int], None] | None = None,
) -> int:
    """Elo ön değerlerini Parquet'e yazar; dönüş: yazılan satır sayısı.

    Çıktı sütunları: match_id INT32, home_elo_pre DOUBLE, away_elo_pre DOUBLE.
    Satır sırası (kickoff_utc, match_id) ile aynıdır.
    `cancel` her parça başında çağrılır; True dönerse InterruptedError fırlatılır
    ve yarım kalan dosya silinir. `progress` her parçadan sonra işlenen toplam
    satır sayısıyla çağrılır.
    """
    if batch_rows < 1:
        raise ValueError("batch_rows en az 1 olmalı")
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(out_path.name + ".partial")
    tmp.unlink(missing_ok=True)

    ratings: dict[int, float] = {}
    written = 0
    reader = con.execute(
        """SELECT match_id, home_team_id, away_team_id, ft_home, ft_away
           FROM matches
           ORDER BY kickoff_utc, match_id"""
    ).to_arrow_reader(batch_rows)
    try:
        with pq.ParquetWriter(tmp, ELO_SCHEMA) as writer:
            for batch in reader:
                if cancel is not None and cancel():
                    raise InterruptedError("Elo hesabı iptal edildi")
                n = batch.num_rows
                mids = batch.column("match_id").to_pylist()
                homes = batch.column("home_team_id").to_pylist()
                aways = batch.column("away_team_id").to_pylist()
                fts_h = batch.column("ft_home").to_pylist()
                fts_a = batch.column("ft_away").to_pylist()
                pre_h = [0.0] * n
                pre_a = [0.0] * n
                for i in range(n):
                    h = homes[i]
                    a = aways[i]
                    r_h = ratings.setdefault(h, base)
                    r_a = ratings.setdefault(a, base)
                    pre_h[i] = r_h
                    pre_a[i] = r_a
                    gh = fts_h[i]
                    ga = fts_a[i]
                    # 0 geçerli bir skordur; yalnızca None (skor yok) atlanır.
                    if gh is None or ga is None:
                        continue
                    e_home = 1.0 / (1.0 + 10.0 ** ((r_a - (r_h + home_advantage)) / 400.0))
                    if gh > ga:
                        s = 1.0
                    elif gh == ga:
                        s = 0.5
                    else:
                        s = 0.0
                    ratings[h] = r_h + k * (s - e_home)
                    ratings[a] = r_a + k * ((1.0 - s) - (1.0 - e_home))
                table = pa.table(
                    {
                        "match_id": pa.array(mids, pa.int32()),
                        "home_elo_pre": pa.array(pre_h, pa.float64()),
                        "away_elo_pre": pa.array(pre_a, pa.float64()),
                    },
                    schema=ELO_SCHEMA,
                )
                writer.write_table(table)
                written += n
                if progress is not None:
                    progress(written)
        os.replace(tmp, out_path)
    except BaseException:
        tmp.unlink(missing_ok=True)
        raise
    return written
