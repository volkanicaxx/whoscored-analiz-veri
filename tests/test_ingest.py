"""oddslake.ingest testleri: CSV sözleşmesi, red sebepleri, idempotentlik ve bellek.

Testler kendi girdi CSV'lerini tmp_path altına yazar; `lake` fixture'ı (conftest) kullanılmaz.
Hızlı testler: `python -m pytest tests/test_ingest.py -q -m 'not slow'`.
Büyük test (300k satır, RSS ölçümü): `python -m pytest tests/test_ingest.py -q -m slow -s`.
"""

from __future__ import annotations

import csv
import hashlib
import shutil
import sys
import threading
import time
from decimal import Decimal
from pathlib import Path

import duckdb
import numpy as np
import psutil
import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from oddslake.catalog import DEFAULT_BOOKMAKERS, init_lake, outcome_key  # noqa: E402
from oddslake.config import LakePaths  # noqa: E402
from oddslake.ingest import (  # noqa: E402
    REQUIRED_COLUMNS,
    IngestReport,
    ingest_files,
    main as ingest_main,
    sql_outcome_key,
)
from oddslake.lake import LakeSession, RowFilter  # noqa: E402

HEADER = list(REQUIRED_COLUMNS)

BASE_ROW: dict[str, str] = {
    "season": "2019",
    "league_id": "1",
    "league_name": "Süper Lig",
    "league_country": "TR",
    "kickoff_utc": "2019-08-01 15:00:00",
    "match_id": "100",
    "home_team_id": "1",
    "home_team": "Galatasaray",
    "away_team_id": "2",
    "away_team": "Fenerbahçe",
    "ft_home": "0",
    "ft_away": "0",
    "ht_home": "",
    "ht_away": "",
    "bookmaker_code": "bet365",
    "market_code": "1X2",
    "period": "FT",
    "line": "",
    "selection": "1",
    "phase": "Acilis",
    "odd": "2.50",
}


def obs(**overrides: str) -> dict[str, str]:
    row = dict(BASE_ROW)
    row.update(overrides)
    return row


def write_csv(path: Path, rows: list[dict[str, str]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(HEADER)
        for row in rows:
            writer.writerow([row[c] for c in HEADER])
    return path


def _q(path: Path) -> str:
    return "'" + path.as_posix().replace("'", "''") + "'"


def catalog_query(paths: LakePaths, sql: str) -> list[tuple]:
    con = duckdb.connect(str(paths.catalog_db), read_only=True)
    try:
        return con.execute(sql).fetchall()
    finally:
        con.close()


def parquet_query(path: Path, sql: str) -> list[tuple]:
    con = duckdb.connect(":memory:")
    try:
        return con.execute(sql.replace("{f}", _q(path))).fetchall()
    finally:
        con.close()


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


@pytest.fixture(scope="module")
def template_catalog(tmp_path_factory) -> Path:
    """Katalog (100.696 kolon) modül başına bir kez kurulur; testler kopyasını kullanır."""
    template = LakePaths(tmp_path_factory.mktemp("template") / "lake")
    init_lake(template)
    return template.catalog_db


@pytest.fixture
def lake_paths(tmp_path: Path, template_catalog: Path) -> LakePaths:
    paths = LakePaths(tmp_path / "lake")
    paths.ensure()
    shutil.copy2(template_catalog, paths.catalog_db)
    return paths


# ---------------------------------------------------------------------------
# Hızlı testler
# ---------------------------------------------------------------------------

def test_zero_score_is_stored_and_unplayed_match_stays_null(lake_paths, tmp_path):
    src = write_csv(
        tmp_path / "in" / "2019_scores.csv",
        [
            obs(match_id="100", ft_home="0", ft_away="0", ht_home="0", ht_away="0"),
            obs(match_id="102", home_team_id="2", away_team_id="1", ft_home="", ft_away="",
                ht_home="", ht_away="", selection="2", odd="3.40"),
        ],
    )
    report = ingest_files(lake_paths, [src], memory_mb=256)

    assert report.files_failed == {}
    assert (report.rows_in, report.rows_written, report.rows_rejected) == (2, 2, 0)
    rows = catalog_query(
        lake_paths,
        "SELECT match_id, ft_home, ft_away, ht_home, ht_away FROM matches ORDER BY match_id",
    )
    assert rows == [(100, 0, 0, 0, 0), (102, None, None, None, None)]
    assert rows[0][1] is not None and rows[0][2] is not None


@pytest.mark.parametrize("bad_odd", ["1.0", "0", "0.0", "-2.5", "", "abc", "NaN", "inf", "1,5"])
def test_invalid_odd_is_rejected(lake_paths, tmp_path, bad_odd):
    src = write_csv(tmp_path / "in" / "2019_odds.csv", [obs(odd=bad_odd), obs(selection="X", odd="1.01")])
    report = ingest_files(lake_paths, [src], memory_mb=256)

    assert report.rejects_by_reason == {"bad_odd": 1}
    assert (report.rows_in, report.rows_written, report.rows_rejected) == (2, 1, 1)


def test_market_code_mismatch_is_unknown_outcome(lake_paths, tmp_path):
    # FT_1 anahtarı 1X2 marketine aittir; market_code DC ile gelirse sonuç bulunamaz.
    src = write_csv(tmp_path / "in" / "2019_mismatch.csv", [obs(market_code="DC")])
    report = ingest_files(lake_paths, [src], memory_mb=256)

    assert report.rejects_by_reason == {"unknown_outcome": 1}
    assert report.rows_written == 0


@pytest.mark.parametrize(
    "kickoff, accepted",
    [
        ("2019-08-01 15:00:00", True),
        ("2019-08-01T15:00:00Z", True),
        ("2019-08-01 15:00:00+00:00", True),
        ("2019-08-01 15:00:00+00", True),
        ("2019-08-01 18:00:00+03", False),
        ("2019-08-01 18:00:00+03:00", False),
        ("2019-08-01 15:00:00-05:00", False),
    ],
)
def test_kickoff_offset_must_be_utc(lake_paths, tmp_path, kickoff, accepted):
    src = write_csv(tmp_path / "in" / "2019_tz.csv", [obs(kickoff_utc=kickoff)])
    report = ingest_files(lake_paths, [src], memory_mb=256)

    if accepted:
        assert report.rejects_by_reason == {}
        assert report.rows_written == 1
    else:
        assert report.rejects_by_reason == {"bad_match": 1}
        assert report.rows_written == 0


def test_one_x_two_with_empty_line_is_null_and_accepted(lake_paths, tmp_path):
    src = write_csv(tmp_path / "in" / "2019_1x2.csv", [obs(line="", odd="1.01")])
    report = ingest_files(lake_paths, [src], memory_mb=256)

    assert report.rejects_by_reason == {}
    assert report.rows_written == 1
    lines = catalog_query(lake_paths, "SELECT count(*) FROM outcomes WHERE line IS NULL AND outcome_key = 'FT_1'")
    assert lines == [(1,)]


@pytest.mark.parametrize(
    "period, line, selection",
    [
        ("FT", "2.5", "U"),
        ("FT", "2.50", "U"),
        ("FT", "-0.25", "AH1"),
        ("FT", "3", "O"),
        ("FT", "3.0", "O"),
        ("FT", "0.5", "O"),
        ("HT", "-1.5", "AH2"),
        ("FT", "0", "O"),
        ("FT", "-0.0", "O"),
        ("FT", "10", "O"),
        ("FT", None, "1"),
    ],
)
def test_sql_outcome_key_matches_python(period, line, selection):
    con = duckdb.connect(":memory:")
    try:
        line_sql = "NULL" if line is None else f"'{line}'"
        sql_key = con.execute(
            f"SELECT {sql_outcome_key(repr(period), line_sql, repr(selection))}"
        ).fetchone()[0]
    finally:
        con.close()
    py_line = None if line is None else Decimal(line)
    assert sql_key == outcome_key(period, py_line, selection)


def test_sql_outcome_key_matches_python_for_every_catalog_outcome(lake_paths):
    rows = catalog_query(
        lake_paths,
        "SELECT m.period, o.line, o.selection, o.outcome_key "
        "FROM outcomes o JOIN markets m ON m.market_id = o.market_id",
    )
    assert len(rows) > 300
    values = ", ".join(
        f"({_sql_lit(period)}, {_sql_lit(None if line is None else str(line))}, {_sql_lit(sel)}, {_sql_lit(key)})"
        for period, line, sel, key in rows
    )
    con = duckdb.connect(":memory:")
    try:
        mismatches = con.execute(
            f"SELECT count(*) FROM (VALUES {values}) t(period, line, sel, stored) "
            f"WHERE ({sql_outcome_key('period', 'line', 'sel')}) IS DISTINCT FROM stored"
        ).fetchone()[0]
    finally:
        con.close()
    assert mismatches == 0


def _sql_lit(value: str | None) -> str:
    return "NULL" if value is None else "'" + value.replace("'", "''") + "'"


def test_duplicate_is_rejected_and_rerun_is_idempotent(lake_paths, tmp_path):
    src = write_csv(
        tmp_path / "in" / "2019_dup.csv",
        [obs(), obs(odd="2.60"), obs(selection="X", odd="3.10")],
    )
    first = ingest_files(lake_paths, [src], memory_mb=256)

    assert (first.rows_in, first.rows_written, first.rows_rejected) == (3, 2, 1)
    assert first.rejects_by_reason == {"duplicate": 1}
    part = lake_paths.odds_partition_file(2019, 1)
    digest = sha256(part)
    rejects = lake_paths.rejects_dir / "2019_dup.parquet"
    assert parquet_query(rejects, "SELECT kaynak_satir_no, sebep FROM read_parquet({f})") == [(2, "duplicate")]
    kept = parquet_query(part, "SELECT match_id, col_id, odd FROM read_parquet({f}) ORDER BY col_id")
    assert kept[0] == (100, 0, pytest.approx(2.5, abs=1e-6))

    second = ingest_files(lake_paths, [src], memory_mb=256)

    assert second == first
    assert sha256(part) == digest
    assert catalog_query(lake_paths, "SELECT row_count FROM lake_partitions") == [(2,)]
    assert parquet_query(rejects, "SELECT count(*) FROM read_parquet({f})") == [(1,)]


def test_unknown_bookmaker_and_bad_phase_are_counted_by_reason(lake_paths, tmp_path):
    src = write_csv(
        tmp_path / "in" / "2019_reasons.csv",
        [
            obs(),
            obs(bookmaker_code="nosuch", selection="X", odd="3.00"),
            obs(bookmaker_code="nosuch2", selection="2", odd="4.00"),
            obs(phase="Orta", selection="2", odd="4.10"),
        ],
    )
    report = ingest_files(lake_paths, [src], memory_mb=256)

    assert report.rejects_by_reason == {"unknown_bookmaker": 2, "bad_phase": 1}
    assert (report.rows_in, report.rows_written, report.rows_rejected) == (4, 1, 3)


def test_broken_files_fail_alone_and_good_file_is_processed(lake_paths, tmp_path):
    good = write_csv(tmp_path / "in" / "2019_good.csv", [obs()])

    bad_header = tmp_path / "in" / "2019_badheader.csv"
    bad_header.write_text(",".join(c for c in HEADER if c != "odd") + "\n", encoding="utf-8")

    malformed = tmp_path / "in" / "2019_malformed.csv"
    lines = [",".join(HEADER), ",".join(BASE_ROW[c] for c in HEADER), "2019,1,short-row"]
    malformed.write_text("\n".join(lines) + "\n", encoding="utf-8")

    report = ingest_files(lake_paths, [bad_header, malformed, good], memory_mb=256)

    assert set(report.files_failed) == {str(bad_header), str(malformed)}
    assert report.files_failed[str(bad_header)].startswith("ValueError")
    assert report.files_ok == 1
    assert (report.rows_in, report.rows_written, report.rows_rejected) == (1, 1, 0)
    assert not (lake_paths.rejects_dir / "2019_badheader.parquet").exists()
    assert not (lake_paths.rejects_dir / "2019_malformed.parquet").exists()


def test_lake_session_reads_back_input_values(lake_paths, tmp_path):
    rng = np.random.default_rng(7)
    rows: list[dict[str, str]] = []
    expected: dict[tuple[int, str], float] = {}
    layout = [
        (100, "2019", "1", "2019-08-01 15:00:00", "1", "2"),
        (103, "2020", "1", "2020-08-09 15:00:00", "1", "5"),
        (104, "2020", "2", "2020-08-09 17:00:00", "4", "3"),
    ]
    for match_id, season, league, kickoff, home, away in layout:
        for book in ("bet365", "bwin"):
            for phase in ("Acilis", "Kapanis"):
                for sel in ("1", "X", "2"):
                    odd = round(float(rng.uniform(1.2, 9.0)), 2)
                    rows.append(obs(
                        season=season, league_id=league, kickoff_utc=kickoff, match_id=str(match_id),
                        home_team_id=home, away_team_id=away, bookmaker_code=book, phase=phase,
                        selection=sel, odd=f"{odd:.2f}", ft_home="", ft_away="", ht_home="", ht_away="",
                        league_name=f"Lig {league}", home_team=f"T{home}", away_team=f"T{away}",
                    ))
                    expected[(match_id, f"{book}_FT_{sel}_{phase}")] = odd
    src = write_csv(tmp_path / "in" / "2019_2020_mix.csv", rows)
    report = ingest_files(lake_paths, [src], memory_mb=256)
    assert report.rows_written == len(rows)

    session = LakeSession(lake_paths, memory_mb=256, threads=1)
    try:
        index = session.row_index(RowFilter())
        names = sorted({name for _, name in expected})
        col_ids = session.col_ids_for_names(names)
        for name in names:
            strip = session.column_strip(col_ids[name], index)
            for i, match_id in enumerate(index.match_id.tolist()):
                key = (match_id, name)
                if key in expected:
                    assert abs(float(strip[i]) - expected[key]) < 1e-6, (match_id, name)
                else:
                    assert np.isnan(strip[i]), (match_id, name)
    finally:
        session.close()


def test_match_conflict_keeps_first_meta_and_rejects_others(lake_paths, tmp_path):
    src = write_csv(
        tmp_path / "in" / "2019_conflict.csv",
        [
            obs(match_id="100", ft_home="0", ft_away="0"),
            obs(match_id="100", selection="X", odd="3.10", ft_home="1", ft_away="0"),
        ],
    )
    report = ingest_files(lake_paths, [src], memory_mb=256)

    assert report.rejects_by_reason == {"match_conflict": 1}
    assert (report.rows_written, report.rows_rejected) == (1, 1)
    assert catalog_query(lake_paths, "SELECT ft_home, ft_away FROM matches WHERE match_id = 100") == [(0, 0)]


def test_first_file_wins_for_cross_file_duplicate(lake_paths, tmp_path):
    fa = write_csv(tmp_path / "in" / "2019_a.csv", [obs(odd="2.10")])
    fb = write_csv(tmp_path / "in" / "2019_b.csv", [obs(odd="9.90")])

    report = ingest_files(lake_paths, [fb, fa], memory_mb=256)

    assert report.rejects_by_reason == {"duplicate": 1}
    odd = parquet_query(lake_paths.odds_partition_file(2019, 1), "SELECT odd FROM read_parquet({f})")
    assert len(odd) == 1 and abs(odd[0][0] - 2.10) < 1e-6
    assert parquet_query(lake_paths.rejects_dir / "2019_b.parquet", "SELECT kaynak_satir_no, sebep FROM read_parquet({f})") == [(1, "duplicate")]


def test_partitions_accumulate_across_separate_runs(lake_paths, tmp_path):
    f1 = write_csv(tmp_path / "in" / "2019_bet365.csv", [obs(), obs(selection="X", odd="3.10")])
    f2 = write_csv(tmp_path / "in" / "2019_bwin.csv", [obs(bookmaker_code="bwin", odd="2.20")])

    ingest_files(lake_paths, [f1], memory_mb=256)
    second = ingest_files(lake_paths, [f2], memory_mb=256)

    assert second.partitions_written == [(2019, 1)]
    assert catalog_query(lake_paths, "SELECT row_count FROM lake_partitions") == [(3,)]
    assert parquet_query(lake_paths.odds_partition_file(2019, 1), "SELECT count(*) FROM read_parquet({f})") == [(3,)]


def test_column_stats_equal_full_recount_after_incremental_runs(lake_paths, tmp_path):
    f1 = write_csv(tmp_path / "in" / "2019_one.csv", [obs(), obs(selection="X", odd="3.10")])
    f2 = write_csv(tmp_path / "in" / "2019_two.csv", [obs(bookmaker_code="bwin"), obs(bookmaker_code="bwin", selection="2", odd="4.5")])
    ingest_files(lake_paths, [f1], memory_mb=256)
    ingest_files(lake_paths, [f2], memory_mb=256)

    stored = dict(catalog_query(lake_paths, "SELECT col_id, non_null FROM column_stats"))
    files = [str(p) for p in lake_paths.odds_dir.glob("season=*/league_id=*/data.parquet")]
    con = duckdb.connect(":memory:")
    try:
        recount = dict(con.execute(
            f"SELECT col_id, count(*) FROM read_parquet({files!r}) GROUP BY col_id").fetchall())
    finally:
        con.close()
    assert stored == recount


def test_cli_initialises_missing_catalog_and_ingests(tmp_path, capsys):
    src = write_csv(tmp_path / "in" / "2019_cli.csv", [obs()])
    code = ingest_main(["--lake", str(tmp_path / "lake_cli"), "--input", str(src), "--memory-mb", "256"])

    assert code == 0
    out = capsys.readouterr().out
    assert '"rows_written": 1' in out
    assert (tmp_path / "lake_cli" / "catalog.duckdb").exists()


# ---------------------------------------------------------------------------
# Büyük test: 300k satır, bellek tepe değeri
# ---------------------------------------------------------------------------

class _RssSampler(threading.Thread):
    """Süreç RSS değerini kısa aralıklarla örnekleyip tepe değerini tutar."""

    def __init__(self, interval: float = 0.01) -> None:
        super().__init__(daemon=True)
        self._proc = psutil.Process()
        self._interval = interval
        self._stop_event = threading.Event()
        self.peak = self._proc.memory_info().rss

    def run(self) -> None:
        while not self._stop_event.is_set():
            self.peak = max(self.peak, self._proc.memory_info().rss)
            self._stop_event.wait(self._interval)

    def finish(self) -> int:
        self._stop_event.set()
        self.join()
        self.peak = max(self.peak, self._proc.memory_info().rss)
        return self.peak


@pytest.mark.slow
def test_large_csv_streams_within_memory_budget(tmp_path):
    paths = LakePaths(tmp_path / "lake")
    init_lake(paths)
    bookmakers = DEFAULT_BOOKMAKERS[:50]
    n_matches = 1000
    bad_rows = {1000, 150_000, 299_999}   # 1'den başlayan veri satırı numaraları; odd = 0 olacak

    src = tmp_path / "in" / "2019_big.csv"
    src.parent.mkdir(parents=True, exist_ok=True)
    data_no = 0
    with open(src, "w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow(HEADER)
        for m in range(n_matches):
            match_id = 500_000 + m
            home, away = 1 + m % 20, 21 + m % 20
            kickoff = f"2019-08-{1 + m % 28:02d} 15:00:00"
            for book in bookmakers:
                for phase in ("Acilis", "Kapanis"):
                    for sel in ("1", "X", "2"):
                        data_no += 1
                        odd = "0" if data_no in bad_rows else f"{1.5 + ((m * 7 + data_no) % 300) / 100:.2f}"
                        writer.writerow([
                            "2019", "1", "Süper Lig", "TR", kickoff, str(match_id), str(home), f"T{home}",
                            str(away), f"T{away}", str(m % 4), str(m % 3), "", "", book, "1X2", "FT", "",
                            sel, phase, odd,
                        ])
    total_rows = data_no
    assert total_rows == 300_000

    baseline = psutil.Process().memory_info().rss
    sampler = _RssSampler()
    sampler.start()
    started = time.perf_counter()
    try:
        report: IngestReport = ingest_files(paths, [src], memory_mb=512)
    finally:
        peak = sampler.finish()
    elapsed = time.perf_counter() - started

    peak_mb = peak / (1 << 20)
    print(f"\npeak_rss_mb={peak_mb:.1f} baseline_rss_mb={baseline / (1 << 20):.1f} "
          f"rows={total_rows} ingest_seconds={elapsed:.2f}")
    assert report.files_failed == {}
    assert (report.rows_in, report.rows_written, report.rows_rejected) == (total_rows, total_rows - 3, 3)
    assert report.rejects_by_reason == {"bad_odd": 3}
    rejected = parquet_query(
        paths.rejects_dir / "2019_big.parquet",
        "SELECT kaynak_satir_no, sebep FROM read_parquet({f}) ORDER BY kaynak_satir_no",
    )
    assert rejected == [(1000, "bad_odd"), (150_000, "bad_odd"), (299_999, "bad_odd")]
    assert catalog_query(paths, "SELECT row_count FROM lake_partitions") == [(total_rows - 3,)]
    assert peak_mb < 1024, f"ingest tepe RSS {peak_mb:.1f} MB bütçeyi aştı"
