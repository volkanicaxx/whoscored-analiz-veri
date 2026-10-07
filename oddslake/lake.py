"""Okuma tarafı: DuckDB oturumu, satır indeksi, kolon seti, karo ve kolon şeridi sorguları.

Her süreç kendi bellek-içi DuckDB örneğini açar ve catalog.duckdb'yi
READ_ONLY olarak bağlar. Birden çok süreç (arayüz + analiz işçisi) aynı anda
salt-okunur bağlanabilir. Oran verisi Parquet'ten okunur; sorgular her zaman
  1) sadece gereken (sezon, lig) dosyalarını (lake_partitions kaydından),
  2) sadece match_id, col_id, odd kolonlarını (kolon tabanlı projeksiyon),
  3) col_id BETWEEN min AND max + col_id IN (...) filtresiyle sadece ilgili
     satır gruplarını (row-group min/max istatistikleri)
okur.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Iterable, Sequence

import duckdb
import numpy as np

from .config import LakePaths, MemoryBudget, default_threads


def _sql_str(s: str) -> str:
    return "'" + s.replace("'", "''") + "'"


def _int_list(values: Iterable[int]) -> str:
    return ", ".join(str(int(v)) for v in values)


def connect(paths: LakePaths, memory_mb: int, threads: int | None = None) -> duckdb.DuckDBPyConnection:
    """Bellek sınırlı, diske taşabilen (spill) bellek-içi DuckDB + salt-okunur katalog."""
    paths.temp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(
        ":memory:",
        config={
            "memory_limit": f"{memory_mb}MB",
            "threads": threads or default_threads(),
            "temp_directory": str(paths.temp_dir),
            "preserve_insertion_order": False,
        },
    )
    con.execute(f"ATTACH {_sql_str(str(paths.catalog_db))} AS cat (READ_ONLY)")
    for t in ("bookmakers", "markets", "outcomes", "phases", "column_catalog",
              "leagues", "teams", "matches", "lake_partitions", "column_stats"):
        con.execute(f"CREATE VIEW {t} AS SELECT * FROM cat.main.{t}")
    odds_glob = paths.odds_dir.as_posix() + "/season=*/league_id=*/data.parquet"
    has_files = con.execute("SELECT count(*) FROM lake_partitions").fetchone()[0] > 0
    if has_files:
        con.execute(
            f"""CREATE VIEW odds AS SELECT match_id, col_id, odd, season, league_id
                FROM read_parquet({_sql_str(odds_glob)}, hive_partitioning = true,
                                  hive_types = {{'season': SMALLINT, 'league_id': INTEGER}})"""
        )
    else:
        con.execute(
            "CREATE VIEW odds AS SELECT NULL::INTEGER AS match_id, NULL::INTEGER AS col_id, "
            "NULL::FLOAT AS odd, NULL::SMALLINT AS season, NULL::INTEGER AS league_id WHERE false"
        )
    return con


@dataclass
class RowFilter:
    league_ids: Sequence[int] | None = None
    season_from: int | None = None
    season_to: int | None = None
    team_name_like: str | None = None   # '%galatasaray%' gibi; ev veya deplasman

    def where_sql(self) -> str:
        parts = ["true"]
        if self.league_ids:
            parts.append(f"m.league_id IN ({_int_list(self.league_ids)})")
        if self.season_from is not None:
            parts.append(f"m.season >= {int(self.season_from)}")
        if self.season_to is not None:
            parts.append(f"m.season <= {int(self.season_to)}")
        if self.team_name_like:
            pat = _sql_str(self.team_name_like)
            parts.append(f"(th.name ILIKE {pat} OR ta.name ILIKE {pat})")
        return " AND ".join(parts)


@dataclass
class RowIndex:
    """Ekrandaki satır sırası. 1 milyon maç için ~10 MB (3 numpy dizisi)."""
    match_id: np.ndarray   # int32
    season: np.ndarray     # int16
    league_id: np.ndarray  # int32

    def __len__(self) -> int:
        return int(self.match_id.shape[0])


@dataclass
class ColumnFilter:
    name_pattern: str | None = None      # '*_FT_1_Acilis' ya da 'bet365_*' ('*' joker)
    bookmaker_codes: Sequence[str] | None = None
    hide_empty: bool = False

    def where_sql(self) -> str:
        parts = ["true"]
        if self.name_pattern:
            like = self.name_pattern.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_").replace("*", "%")
            parts.append(f"c.column_name LIKE {_sql_str(like)} ESCAPE '\\'")
        if self.bookmaker_codes:
            parts.append(f"b.code IN ({', '.join(_sql_str(x) for x in self.bookmaker_codes)})")
        if self.hide_empty:
            parts.append("EXISTS (SELECT 1 FROM column_stats s WHERE s.col_id = c.col_id AND s.non_null > 0)")
        return " AND ".join(parts)


@dataclass
class ColumnSet:
    col_id: np.ndarray          # int32, ekran sırasında
    names: list[str] = field(default_factory=list)

    def __len__(self) -> int:
        return int(self.col_id.shape[0])


class LakeSession:
    """Tek DuckDB örneği; her iş parçacığı `cursor()` ile kendi bağlantısını alır."""

    def __init__(self, paths: LakePaths, memory_mb: int | None = None, threads: int | None = None):
        self.paths = paths
        self.con = connect(paths, memory_mb or MemoryBudget().ui_duckdb_mb, threads)
        self._partitions: dict[tuple[int, int], str] = {
            (int(s), int(l)): str((paths.root / p).as_posix())
            for s, l, p in self.con.execute("SELECT season, league_id, path FROM lake_partitions").fetchall()
        }
        self._lock = threading.Lock()

    def cursor(self) -> duckdb.DuckDBPyConnection:
        with self._lock:
            return self.con.cursor()

    def close(self) -> None:
        self.con.close()

    # ---- satır ve kolon setleri -------------------------------------------------

    def row_index(self, flt: RowFilter, cur: duckdb.DuckDBPyConnection | None = None) -> RowIndex:
        cur = cur or self.con
        res = cur.execute(
            f"""SELECT m.match_id, m.season, m.league_id
                FROM matches m
                JOIN teams th ON th.team_id = m.home_team_id
                JOIN teams ta ON ta.team_id = m.away_team_id
                WHERE {flt.where_sql()}
                ORDER BY m.kickoff_utc, m.match_id"""
        ).fetchnumpy()
        return RowIndex(
            match_id=np.asarray(res["match_id"], dtype=np.int32),
            season=np.asarray(res["season"], dtype=np.int16),
            league_id=np.asarray(res["league_id"], dtype=np.int32),
        )

    def column_set(self, flt: ColumnFilter, cur: duckdb.DuckDBPyConnection | None = None) -> ColumnSet:
        cur = cur or self.con
        rows = cur.execute(
            f"""SELECT c.col_id, c.column_name
                FROM column_catalog c JOIN bookmakers b USING (bookmaker_id)
                WHERE {flt.where_sql()}
                ORDER BY c.display_order"""
        ).fetchall()
        return ColumnSet(np.fromiter((r[0] for r in rows), dtype=np.int32, count=len(rows)), [r[1] for r in rows])

    def col_ids_for_names(self, names: Sequence[str], cur: duckdb.DuckDBPyConnection | None = None) -> dict[str, int]:
        cur = cur or self.con
        rows = cur.execute(
            f"SELECT column_name, col_id FROM column_catalog WHERE column_name IN ({', '.join(_sql_str(n) for n in names)})"
        ).fetchall()
        return dict(rows)

    # ---- dosya budama ------------------------------------------------------------

    def partition_files(self, seasons: np.ndarray, leagues: np.ndarray) -> list[str]:
        """Verilen satırların düştüğü (sezon, lig) dosyaları. Olmayan bölüm atlanır."""
        if seasons.size == 0:
            return []
        pairs = np.unique(np.stack([seasons.astype(np.int64), leagues.astype(np.int64)], axis=1), axis=0)
        out = []
        for s, l in pairs:
            f = self._partitions.get((int(s), int(l)))
            if f is not None:
                out.append(f)
        return out

    # ---- veri sorguları ----------------------------------------------------------

    def fetch_odds_block(
        self,
        match_ids: np.ndarray,
        col_ids: np.ndarray,
        files: list[str],
        cur: duckdb.DuckDBPyConnection | None = None,
    ) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
        """(match_id, col_id, odd) üçlüleri; sadece istenen maç x kolon kesişimi."""
        if not files or match_ids.size == 0 or col_ids.size == 0:
            e = np.empty(0, dtype=np.int32)
            return e, e, np.empty(0, dtype=np.float32)
        cur = cur or self.con
        file_list = "[" + ", ".join(_sql_str(f) for f in files) + "]"
        res = cur.execute(
            f"""SELECT match_id, col_id, odd
                FROM read_parquet({file_list})
                WHERE col_id BETWEEN {int(col_ids.min())} AND {int(col_ids.max())}
                  AND col_id IN ({_int_list(np.unique(col_ids))})
                  AND match_id BETWEEN {int(match_ids.min())} AND {int(match_ids.max())}
                  AND match_id IN ({_int_list(np.unique(match_ids))})"""
        ).fetchnumpy()
        return (
            np.asarray(res["match_id"], dtype=np.int32),
            np.asarray(res["col_id"], dtype=np.int32),
            np.asarray(res["odd"], dtype=np.float32),
        )

    def fetch_match_info(self, match_ids: np.ndarray, cur: duckdb.DuckDBPyConnection | None = None) -> dict[int, tuple]:
        """Dondurulmuş sol kolonlar: tarih, lig, ev, deplasman, skor."""
        if match_ids.size == 0:
            return {}
        cur = cur or self.con
        rows = cur.execute(
            f"""SELECT m.match_id, strftime(m.kickoff_utc, '%Y-%m-%d %H:%M'), l.name, th.name, ta.name,
                       CASE WHEN m.ft_home IS NULL THEN '' ELSE m.ft_home::VARCHAR || '-' || m.ft_away::VARCHAR END
                FROM matches m
                JOIN leagues l ON l.league_id = m.league_id
                JOIN teams th ON th.team_id = m.home_team_id
                JOIN teams ta ON ta.team_id = m.away_team_id
                WHERE m.match_id IN ({_int_list(np.unique(match_ids))})"""
        ).fetchall()
        return {r[0]: r[1:] for r in rows}

    def column_strip(self, col_id: int, rows: RowIndex, cur: duckdb.DuckDBPyConnection | None = None) -> np.ndarray:
        """Tek bir mantıksal kolonu (örn. bet365_FT_1_Acilis) satır indeksine hizalı float32 dizi olarak döner.

        Diskten sadece ilgili dosyaların match_id/col_id/odd kolonlarının, col_id
        aralığını içeren satır grupları okunur. Eksik hücre = NaN.
        """
        out = np.full(len(rows), np.nan, dtype=np.float32)
        files = self.partition_files(rows.season, rows.league_id)
        if not files:
            return out
        cur = cur or self.con
        file_list = "[" + ", ".join(_sql_str(f) for f in files) + "]"
        res = cur.execute(
            f"SELECT match_id, odd FROM read_parquet({file_list}) WHERE col_id = {int(col_id)}"
        ).fetchnumpy()
        mids = np.asarray(res["match_id"], dtype=np.int32)
        if mids.size == 0:
            return out
        order = np.argsort(rows.match_id, kind="stable")
        sorted_ids = rows.match_id[order]
        pos = np.searchsorted(sorted_ids, mids)
        pos_clip = np.minimum(pos, sorted_ids.size - 1)
        hit = sorted_ids[pos_clip] == mids
        out[order[pos_clip[hit]]] = np.asarray(res["odd"], dtype=np.float32)[hit]
        return out
