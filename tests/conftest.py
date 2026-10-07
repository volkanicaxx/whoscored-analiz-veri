"""Testler için ortak küçük sentetik lake kurucusu.

Gerçek 800 GB verisi yerine, bütün kolon/satır/skor kenar durumlarını (0-0 maç,
oynanmamış maç, eksik oran, farklı sezon ve lig) içeren birkaç maçlık bir lake
üretir. Parquet dosyaları doğrudan pyarrow ile yazılır; ingest hattından bağımsızdır.
"""

from __future__ import annotations

import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from oddslake.catalog import create_catalog, init_lake  # noqa: E402
from oddslake.config import LakePaths  # noqa: E402

# (match_id, season, league_id, kickoff, home_id, away_id, ft_h, ft_a, ht_h, ht_a)
SYNTH_MATCHES = (
    (100, 2019, 1, "2019-08-01 15:00:00", 1, 2, 0, 0, 0, 0),       # 0-0: skor VAR, sıfır gol
    (101, 2019, 2, "2019-08-02 15:00:00", 3, 4, 2, 1, 1, 0),
    (102, 2020, 1, "2020-08-02 15:00:00", 2, 1, None, None, None, None),  # oynanmadı
    (103, 2020, 1, "2020-08-09 15:00:00", 1, 5, 1, 3, 0, 2),
    (104, 2020, 2, "2020-08-09 17:00:00", 4, 3, 0, 0, 0, 0),       # 0-0 ikinci lig
)


def build_small_lake(root: Path) -> LakePaths:
    paths = LakePaths(root)
    init_lake(paths)
    con = create_catalog(paths)
    con.execute("INSERT INTO leagues VALUES (1, 'TR', 'Süper Lig'), (2, 'EN', 'Premier League')")
    con.execute(
        "INSERT INTO teams VALUES (1, 'Galatasaray'), (2, 'Fenerbahçe'), (3, 'Arsenal'), "
        "(4, 'Chelsea'), (5, 'Beşiktaş')"
    )
    for mid, season, league, kick, h, a, fh, fa, hh, ha in SYNTH_MATCHES:
        con.execute(
            "INSERT INTO matches VALUES (?, ?, ?, CAST(? AS TIMESTAMP), ?, ?, ?, ?, ?, ?)",
            [mid, season, league, kick, h, a, fh, fa, hh, ha],
        )

    col_of = dict(con.execute("SELECT column_name, col_id FROM column_catalog").fetchall())
    rows: dict[tuple[int, int], list[tuple[int, int, float]]] = {}
    for idx, (mid, season, league, *_rest) in enumerate(SYNTH_MATCHES):
        base_h, base_d, base_a = 2.00 + 0.05 * idx, 3.40, 3.60 - 0.05 * idx
        for book in ("bet365", "bwin"):
            for phase, shift in (("Acilis", 0.0), ("Kapanis", -0.10)):
                for sel, odd in (("1", base_h), ("X", base_d), ("2", base_a)):
                    name = f"{book}_FT_{sel}_{phase}"
                    if name not in col_of:
                        continue
                    rows.setdefault((season, league), []).append(
                        (mid, col_of[name], round(odd + shift, 2))
                    )
        # bet365 2.5 gol alt/üst: sadece bir maçta eksik bırakılır (boş hücre testi)
        if mid != 103:
            for sel, odd in (("O", 1.95), ("U", 1.85)):
                name = f"bet365_FT_2.5_{sel}_Acilis"
                if name in col_of:
                    rows.setdefault((season, league), []).append((mid, col_of[name], odd))

    for (season, league), items in rows.items():
        out = paths.odds_partition_file(season, league)
        out.parent.mkdir(parents=True, exist_ok=True)
        tbl = pa.table(
            {
                "match_id": pa.array([r[0] for r in items], pa.int32()),
                "col_id": pa.array([r[1] for r in items], pa.int32()),
                "odd": pa.array([r[2] for r in items], pa.float32()),
            }
        ).sort_by([("col_id", "ascending"), ("match_id", "ascending")])
        pq.write_table(tbl, out, row_group_size=32)
        con.execute(
            "INSERT INTO lake_partitions VALUES (?, ?, ?, ?, ?, ?, now())",
            [
                season,
                league,
                out.relative_to(paths.root).as_posix(),
                tbl.num_rows,
                int(tbl["col_id"][0].as_py()),
                int(tbl["col_id"][-1].as_py()),
            ],
        )

    con.execute(
        "INSERT INTO column_stats SELECT col_id, count(*) FROM ("
        " SELECT col_id FROM (SELECT * FROM read_parquet(?, hive_partitioning = true)))"
        " GROUP BY col_id",
        [paths.odds_dir.as_posix() + "/season=*/league_id=*/data.parquet"],
    )
    con.close()
    return paths


@pytest.fixture
def lake(tmp_path: Path) -> LakePaths:
    return build_small_lake(tmp_path / "lake")
