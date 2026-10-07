"""Yol düzeni ve bellek bütçesi.

Bütün süreçler (arayüz + tek ağır analiz işçisi) toplamda 4 GB altında kalacak
şekilde bütçelenir. DuckDB'nin `memory_limit` ayarı her DuckDB örneği için
ayrıdır; bu yüzden arayüz ve işçi süreci için ayrı paylar verilir ve aynı anda
en fazla bir ağır işçi süreci çalışır (bkz. jobs.JobManager).
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class MemoryBudget:
    # Arayüz süreci + işçi süreci için kesin üst sınır (MemoryGuard bunu izler).
    hard_limit_mb: int = 3584
    # Arayüz sürecindeki DuckDB örneği (karo sorguları, satır indeksi, kolon seti).
    ui_duckdb_mb: int = 768
    # Analiz işçi sürecindeki DuckDB örneği (form / Elo / piyasa farkı).
    worker_duckdb_mb: int = 1536
    # Karo önbelleği için en fazla karo sayısı (bir karo = 64x32 float32 = 8 KB).
    max_tiles: int = 512
    # Elo gibi satır satır akan işlerde tek seferde RAM'e alınan satır sayısı.
    stream_batch_rows: int = 50_000


@dataclass(frozen=True)
class LakePaths:
    root: Path

    @property
    def catalog_db(self) -> Path:
        return self.root / "catalog.duckdb"

    @property
    def odds_dir(self) -> Path:
        return self.root / "odds"

    @property
    def staging_dir(self) -> Path:
        return self.root / "staging"

    @property
    def rejects_dir(self) -> Path:
        return self.root / "rejects"

    @property
    def derived_dir(self) -> Path:
        return self.root / "derived"

    @property
    def temp_dir(self) -> Path:
        return self.root / "tmp"

    def odds_partition_file(self, season: int, league_id: int) -> Path:
        return self.odds_dir / f"season={season}" / f"league_id={league_id}" / "data.parquet"

    def ensure(self) -> None:
        for p in (self.root, self.odds_dir, self.staging_dir, self.rejects_dir, self.derived_dir, self.temp_dir):
            p.mkdir(parents=True, exist_ok=True)


def default_threads() -> int:
    # Arayüzü besleyen çekirdeği boş bırak.
    return max(1, (os.cpu_count() or 2) - 1)
