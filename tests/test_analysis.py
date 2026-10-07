"""oddslake.analysis testleri: takım formu, Elo, piyasa örtük olasılıkları, iptal ve bellek.

Hızlı testler conftest.py'deki `lake` fixture'ını (5 maçlık sentetik lake) kullanır.
Büyük bellek testi `@pytest.mark.slow` ile işaretlidir:
    python -m pytest tests/test_analysis.py -q -m 'not slow'
"""

from __future__ import annotations

import threading
import time
from datetime import datetime, timedelta
from pathlib import Path

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from oddslake.analysis import compute_elo, compute_form, implied_table
from oddslake.catalog import create_catalog
from oddslake.config import LakePaths
from oddslake.lake import LakeSession


@pytest.fixture
def cur(lake: LakePaths):
    session = LakeSession(lake, memory_mb=512, threads=2)
    try:
        yield session.cursor()
    finally:
        session.close()


def _form_rows(path: Path) -> dict[tuple[int, str], dict]:
    return {(r["match_id"], r["side"]): r for r in pq.read_table(path).to_pylist()}


def _assert_window(row: dict, n: int, games: int, ppg, gf, ga, cs, win) -> None:
    assert row[f"games_{n}"] == games
    expected = {"ppg": ppg, "gf_avg": gf, "ga_avg": ga, "cs_rate": cs, "win_rate": win}
    for name, exp in expected.items():
        got = row[f"{name}_{n}"]
        if exp is None:
            assert got is None, f"{name}_{n} NULL olmalı, {got!r} geldi"
        else:
            assert got == pytest.approx(exp, abs=1e-12), f"{name}_{n}"


# ---------------------------------------------------------------- form

def test_form_zero_zero_draw_and_unplayed_match_excluded(cur, tmp_path):
    out = tmp_path / "form.parquet"
    assert compute_form(cur, out) == 10  # 5 maç x 2 takım, oynanmamış 102 dahil

    rows = _form_rows(out)
    assert set(rows) == {
        (100, "H"), (100, "A"), (101, "H"), (101, "A"), (102, "H"),
        (102, "A"), (103, "H"), (103, "A"), (104, "H"), (104, "A"),
    }

    # İlk maçlarda geçmiş yok: games 0, bütün oranlar NULL (0 değil).
    _assert_window(rows[(100, "H")], 5, 0, None, None, None, None, None)
    _assert_window(rows[(100, "A")], 10, 0, None, None, None, None, None)

    # 102 (oynanmamış, team 1 deplasman): tek geçmiş maç 100 (0-0 beraberlik).
    # 0-0: 1 puan, 0 gol attı, 0 gol yedi, gol yemedi (cs=1), galibiyet yok.
    r = rows[(102, "A")]
    assert r["team_id"] == 1
    _assert_window(r, 5, 1, ppg=1.0, gf=0.0, ga=0.0, cs=1.0, win=0.0)
    _assert_window(r, 10, 1, ppg=1.0, gf=0.0, ga=0.0, cs=1.0, win=0.0)

    # 103 (team 1 ev): 102 oynanmadığı için geçmişe girmez, kendi 1-3 maçı da girmez.
    # Geçmiş yalnızca 100; bu yüzden games 1 ve gf_avg 0.0 (kendi 1 gol sayılsaydı 0.5 olurdu).
    r = rows[(103, "H")]
    assert r["team_id"] == 1
    _assert_window(r, 5, 1, ppg=1.0, gf=0.0, ga=0.0, cs=1.0, win=0.0)

    # 102 ev sahibi team 2: 0-0 maçı deplasman olarak oynamış, puanı 1.
    r = rows[(102, "H")]
    assert r["team_id"] == 2
    _assert_window(r, 5, 1, ppg=1.0, gf=0.0, ga=0.0, cs=1.0, win=0.0)

    # 104 team 3 (deplasman): 101'de 2-1 kazandı (galibiyet, 3 puan, gol yedi 1).
    r = rows[(104, "A")]
    assert r["team_id"] == 3
    _assert_window(r, 5, 1, ppg=3.0, gf=2.0, ga=1.0, cs=0.0, win=1.0)

    # 104 team 4 (ev): 101'de 1-2 kaybetti (0 puan, gol attı 1, gol yedi 2).
    r = rows[(104, "H")]
    assert r["team_id"] == 4
    _assert_window(r, 5, 1, ppg=0.0, gf=1.0, ga=2.0, cs=0.0, win=0.0)


def test_form_window_truncation_side_and_no_leak(lake: LakePaths, tmp_path):
    # Takım 6 (Alfa, ev) ve 7 (Beta, deplasman): 2021 sezonunda 6 oynanmış + 1 oynanmamış maç.
    # Sonuçlar: 201 1-0, 202 2-0, 203 0-0, 204 3-1, 205 0-2, 206 1-1, 207 oynanmadı.
    con = create_catalog(lake)
    try:
        con.execute("INSERT INTO teams VALUES (6, 'Alfa'), (7, 'Beta')")
        scenario = [
            (201, "2021-01-01 15:00:00", 1, 0),
            (202, "2021-01-08 15:00:00", 2, 0),
            (203, "2021-01-15 15:00:00", 0, 0),
            (204, "2021-01-22 15:00:00", 3, 1),
            (205, "2021-01-29 15:00:00", 0, 2),
            (206, "2021-02-05 15:00:00", 1, 1),
            (207, "2021-02-12 15:00:00", None, None),
        ]
        for mid, kick, fh, fa in scenario:
            con.execute(
                "INSERT INTO matches VALUES (?, 2021, 1, CAST(? AS TIMESTAMP), 6, 7, ?, ?, NULL, NULL)",
                [mid, kick, fh, fa],
            )
    finally:
        con.close()

    session = LakeSession(lake, memory_mb=512, threads=2)
    try:
        out = tmp_path / "form_scn.parquet"
        assert compute_form(session.cursor(), out, windows=(5, 10)) == 24  # 12 maç x 2 takım
    finally:
        session.close()

    rows = _form_rows(out)
    # 207, team 6 (ev): son 5 maç = 202..206.
    #   gol attı: 2+0+3+0+1 = 6 -> 1.2; gol yedi: 0+0+1+2+1 = 4 -> 0.8
    #   puan: 3+1+3+0+1 = 8 -> 1.6; gol yemedi (202, 203): 2/5 = 0.4; galibiyet (202, 204): 0.4
    r = rows[(207, "H")]
    assert r["team_id"] == 6
    _assert_window(r, 5, 5, ppg=8 / 5, gf=6 / 5, ga=4 / 5, cs=2 / 5, win=2 / 5)
    # Pencere 10: bütün 6 oynanmış maç (201 dahil). Gol attı 7 -> 7/6; yedi 4 -> 4/6;
    # puan 11 -> 11/6; gol yemedi (201, 202, 203) 3/6; galibiyet (201, 202, 204) 3/6.
    _assert_window(r, 10, 6, ppg=11 / 6, gf=7 / 6, ga=4 / 6, cs=3 / 6, win=3 / 6)

    # 207, team 7 (deplasman): son 5 maçta (202..206) puan 0+1+0+3+1 = 5 -> 1.0;
    # gol attı 0+0+1+2+1 = 4 -> 0.8; gol yedi 2+0+3+0+1 = 6 -> 1.2; gol yemedi (203, 205) 0.4;
    # galibiyet (205) 0.2.
    r = rows[(207, "A")]
    assert r["team_id"] == 7
    _assert_window(r, 5, 5, ppg=1.0, gf=0.8, ga=1.2, cs=0.4, win=0.2)

    # 206, team 6 (ev): geçmiş 201..205 (tam 5 maç, pencere 5'e sığar).
    #   sonuçlar: 201 W(1-0), 202 W(2-0), 203 D(0-0), 204 W(3-1), 205 L(0-2)
    #   puan 3+3+1+3+0 = 10 -> 2.0; gol attı 1+2+0+3+0 = 6 -> 1.2; gol yedi 0+0+0+1+2 = 3 -> 0.6
    #   gol yemedi (201, 202, 203) 3/5 = 0.6; galibiyet (201, 202, 204) 3/5 = 0.6
    r = rows[(206, "H")]
    _assert_window(r, 5, 5, ppg=10 / 5, gf=6 / 5, ga=3 / 5, cs=3 / 5, win=3 / 5)


def _naive_form(matches: list[tuple], windows: tuple[int, ...]) -> dict[tuple[int, str], dict]:
    """Referans uygulama: her takım için kendi maçından önceki oynanmış maçları Python'da seçer."""
    team_rows: dict[int, list[tuple]] = {}
    for mid, kick, home, away, fh, fa in matches:
        team_rows.setdefault(home, []).append((kick, mid, "H", fh, fa))
        team_rows.setdefault(away, []).append((kick, mid, "A", fa, fh))
    out: dict[tuple[int, str], dict] = {}
    for team, lst in team_rows.items():
        lst.sort(key=lambda x: (x[0], x[1]))
        for idx, (_kick, mid, side, gf, ga) in enumerate(lst):
            prior = [x for x in lst[:idx] if x[3] is not None]
            metrics: dict = {"team_id": team}
            for n in windows:
                win = prior[-n:]
                g = len(win)
                metrics[f"games_{n}"] = g
                if g == 0:
                    for name in ("ppg", "gf_avg", "ga_avg", "cs_rate", "win_rate"):
                        metrics[f"{name}_{n}"] = None
                    continue
                metrics[f"ppg_{n}"] = sum(3 if x[3] > x[4] else (1 if x[3] == x[4] else 0) for x in win) / g
                metrics[f"gf_avg_{n}"] = sum(x[3] for x in win) / g
                metrics[f"ga_avg_{n}"] = sum(x[4] for x in win) / g
                metrics[f"cs_rate_{n}"] = sum(1 for x in win if x[4] == 0) / g
                metrics[f"win_rate_{n}"] = sum(1 for x in win if x[3] > x[4]) / g
            out[(mid, side)] = metrics
    return out


def test_form_matches_naive_reference_on_random_history(lake: LakePaths, tmp_path):
    import random

    rng = random.Random(20240601)
    base = datetime(2019, 8, 1, 15, 0)
    con = create_catalog(lake)
    try:
        # 12 takım; conftest takımlarıyla (1..5) çakışmaz.
        con.execute("INSERT INTO teams SELECT t, 'T' || t FROM range(100, 112) AS r(t)")
        records = []
        for i in range(200):
            home, away = rng.sample(range(100, 112), 2)
            kick = base + timedelta(days=rng.randrange(0, 1200), hours=rng.choice((12, 15, 17)))
            season = kick.year if kick.month >= 7 else kick.year - 1
            if rng.random() < 0.1:
                fh, fa = None, None  # oynanmamış maç
            else:
                fh, fa = rng.randrange(0, 4), rng.randrange(0, 4)  # 0-0 dahil
            records.append((1000 + i, season, 1 + i % 2, kick, home, away, fh, fa))
        con.executemany(
            "INSERT INTO matches VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, NULL)",
            records,
        )
        all_matches = con.execute(
            "SELECT match_id, kickoff_utc, home_team_id, away_team_id, ft_home, ft_away FROM matches"
        ).fetchall()
    finally:
        con.close()

    session = LakeSession(lake, memory_mb=512, threads=2)
    try:
        out = tmp_path / "form_random.parquet"
        windows = (3, 7)
        n_written = compute_form(session.cursor(), out, windows=windows)
    finally:
        session.close()

    expected = _naive_form(
        [(mid, kick, home, away, fh, fa) for mid, kick, home, away, fh, fa in all_matches],
        windows,
    )
    got = _form_rows(out)
    assert n_written == 2 * len(all_matches) == len(got)
    assert set(got) == set(expected)
    for key, exp in expected.items():
        row = got[key]
        assert row["team_id"] == exp["team_id"], key
        for n in windows:
            _assert_window(
                row,
                n,
                exp[f"games_{n}"],
                exp[f"ppg_{n}"],
                exp[f"gf_avg_{n}"],
                exp[f"ga_avg_{n}"],
                exp[f"cs_rate_{n}"],
                exp[f"win_rate_{n}"],
            )


def test_form_rejects_invalid_windows(cur, tmp_path):
    with pytest.raises(ValueError):
        compute_form(cur, tmp_path / "f.parquet", windows=())
    with pytest.raises(ValueError):
        compute_form(cur, tmp_path / "f.parquet", windows=(0, 5))
    with pytest.raises(ValueError):
        compute_form(cur, tmp_path / "f.parquet", windows=(5, 5))


# ---------------------------------------------------------------- elo

def _elo_rows(path: Path) -> dict[int, dict]:
    return {r["match_id"]: r for r in pq.read_table(path).to_pylist()}


def test_elo_pre_values_follow_hand_computed_updates(cur, tmp_path):
    out = tmp_path / "elo.parquet"
    # batch_rows=2: 5 maç üç parçaya bölünür; durum parçalar arasında korunmalı.
    assert compute_elo(cur, out, batch_rows=2) == 5
    rows = _elo_rows(out)

    # Elle hesap (k=20, ev avantajı 65, taban 1500):
    #   E0 = 1 / (1 + 10^((1500 - (1500 + 65)) / 400)) = 1 / (1 + 10^(-0.1625))
    e0 = 1.0 / (1.0 + 10.0 ** (-65.0 / 400.0))
    # 100 (ev 1, dep 2, 0-0, S = 0.5): R1 = 1500 + 20*(0.5 - E0); R2 = 1500 + 20*((0.5) - (1 - E0))
    r1_after_100 = 1500.0 + 20.0 * (0.5 - e0)
    r2_after_100 = 1500.0 + 20.0 * ((1 - 0.5) - (1 - e0))
    # 101 (ev 3, dep 4, 2-1, S = 1.0): R3 = 1500 + 20*(1 - E0); R4 = 1500 + 20*(0 - (1 - E0))
    r3_after_101 = 1500.0 + 20.0 * (1.0 - e0)
    r4_after_101 = 1500.0 + 20.0 * ((1 - 1.0) - (1 - e0))

    # İlk görülen takımlar 1500 ile başlar.
    assert rows[100]["home_elo_pre"] == 1500.0
    assert rows[100]["away_elo_pre"] == 1500.0
    assert rows[101]["home_elo_pre"] == 1500.0
    assert rows[101]["away_elo_pre"] == 1500.0

    # 0-0 beraberlik: iki takım 1500'den eşit ve zıt yönde değişir, toplam 3000 kalır.
    assert r1_after_100 + r2_after_100 == pytest.approx(3000.0, abs=1e-9)

    # 102 oynanmamış: ön değerler mevcut dereceler (ev = team 2, dep = team 1), güncelleme yok.
    assert rows[102]["home_elo_pre"] == pytest.approx(r2_after_100, abs=1e-9)
    assert rows[102]["away_elo_pre"] == pytest.approx(r1_after_100, abs=1e-9)

    # 103: team 1 hâlâ r1_after_100 (102 onu değiştirmedi); team 5 ilk kez 1500.
    assert rows[103]["home_elo_pre"] == pytest.approx(r1_after_100, abs=1e-9)
    assert rows[103]["away_elo_pre"] == 1500.0

    # 104: ev team 4 (101'de kaybetti), dep team 3 (101'de kazandı).
    assert rows[104]["home_elo_pre"] == pytest.approx(r4_after_101, abs=1e-9)
    assert rows[104]["away_elo_pre"] == pytest.approx(r3_after_101, abs=1e-9)

    # Parça boyutu sonucu değiştirmemeli.
    out_big = tmp_path / "elo_big.parquet"
    compute_elo(cur, out_big, batch_rows=50_000)
    assert pq.read_table(out_big).equals(pq.read_table(out))


def test_elo_parquet_schema(cur, tmp_path):
    out = tmp_path / "elo.parquet"
    compute_elo(cur, out)
    schema = pq.read_schema(out)
    assert schema.field("match_id").type == pa.int32()
    assert schema.field("home_elo_pre").type == pa.float64()
    assert schema.field("away_elo_pre").type == pa.float64()


def test_elo_cancel_raises_and_removes_partial_file(cur, tmp_path):
    out = tmp_path / "elo_cancel.parquet"
    with pytest.raises(InterruptedError):
        compute_elo(cur, out, cancel=lambda: True)
    assert not out.exists()
    assert not out.with_name(out.name + ".partial").exists()

    # İlk parçadan sonra iptal: yine InterruptedError, yarım dosya kalmaz.
    calls = {"n": 0}

    def cancel_after_first_batch() -> bool:
        calls["n"] += 1
        return calls["n"] > 1

    with pytest.raises(InterruptedError):
        compute_elo(cur, out, batch_rows=2, cancel=cancel_after_first_batch)
    assert not out.exists()


def test_elo_progress_reports_processed_rows(cur, tmp_path):
    seen: list[int] = []
    out = tmp_path / "elo_progress.parquet"
    assert compute_elo(cur, out, batch_rows=2, progress=seen.append) == 5
    assert seen, "progress en az bir kez çağrılmalı"
    assert seen[-1] == 5
    assert seen == sorted(seen)


# ---------------------------------------------------------------- market

_BET365_ACILIS_1X2 = {
    # match_id: (ham ev, ham beraberlik, ham deplasman) -- conftest'teki değerler
    100: (2.00, 3.40, 3.60),
    101: (2.05, 3.40, 3.55),
    102: (2.10, 3.40, 3.50),
    103: (2.15, 3.40, 3.45),
    104: (2.20, 3.40, 3.40),
}


def test_implied_bet365_acilis_probabilities_and_overround(cur):
    t = implied_table(cur, "bet365")
    assert t.column_names == [
        "match_id", "odd_home", "odd_draw", "odd_away",
        "p_home", "p_draw", "p_away", "overround",
    ]
    assert t.column("match_id").to_pylist() == [100, 101, 102, 103, 104]
    got = {r["match_id"]: r for r in t.to_pylist()}

    # float32 oran depolama hassasiyeti için göreli tolerans (~1e-6).
    # Maç 100: p_home = 1/2.00 = 0.5; overround = 1/2.00 + 1/3.40 + 1/3.60 = 1.0718954...
    r = got[100]
    assert r["odd_home"] == pytest.approx(2.00, rel=1e-6)
    assert r["p_home"] == pytest.approx(1 / 2.00, rel=1e-6)
    assert r["p_draw"] == pytest.approx(1 / 3.40, rel=1e-6)
    assert r["p_away"] == pytest.approx(1 / 3.60, rel=1e-6)
    assert r["overround"] == pytest.approx(1 / 2.00 + 1 / 3.40 + 1 / 3.60, rel=1e-6)

    # Maç 101: p_home = 1/2.05; overround = 1/2.05 + 1/3.40 + 1/3.55.
    r = got[101]
    assert r["p_home"] == pytest.approx(1 / 2.05, rel=1e-6)
    assert r["overround"] == pytest.approx(1 / 2.05 + 1 / 3.40 + 1 / 3.55, rel=1e-6)

    # Her maç için ham oranlar fikstürdeki değerlerle eşleşir.
    for mid, (h, d, a) in _BET365_ACILIS_1X2.items():
        assert got[mid]["odd_home"] == pytest.approx(h, rel=1e-6)
        assert got[mid]["odd_draw"] == pytest.approx(d, rel=1e-6)
        assert got[mid]["odd_away"] == pytest.approx(a, rel=1e-6)


def test_implied_kapanis_phase(cur):
    t = implied_table(cur, "bet365", phase="Kapanis")
    r = {x["match_id"]: x for x in t.to_pylist()}[100]
    # Kapanis = Açılış - 0.10: 1.90 / 3.30 / 3.50
    assert r["odd_home"] == pytest.approx(1.90, rel=1e-6)
    assert r["p_home"] == pytest.approx(1 / 1.90, rel=1e-6)
    assert r["overround"] == pytest.approx(1 / 1.90 + 1 / 3.30 + 1 / 3.50, rel=1e-6)


def test_implied_missing_odds_are_null_not_dropped(cur):
    # conftest yalnızca FT 1X2 oranı üretir; HT için satır yok demektir: her maç
    # satırı korunur, bütün oran ve olasılık hücreleri NULL olur.
    t = implied_table(cur, "bet365", period="HT")
    assert t.column("match_id").to_pylist() == [100, 101, 102, 103, 104]
    for r in t.to_pylist():
        for key in ("odd_home", "odd_draw", "odd_away", "p_home", "p_draw", "p_away", "overround"):
            assert r[key] is None, (r["match_id"], key)


def test_implied_unknown_bookmaker_raises(cur):
    with pytest.raises(ValueError):
        implied_table(cur, "yokburo")
    with pytest.raises(ValueError):
        implied_table(cur, "bet365", phase="Olmayan")


def test_implied_invalid_odds_become_null(lake: LakePaths):
    # Maç 100 (2019, lig 1) için bet365 Açılış 1X2 oranlarını geçersiz değerlerle değiştir:
    #   ev = 1.0 (> 1.0 değil), beraberlik = NaN, deplasman = 0.0.
    con = create_catalog(lake)
    col_of = dict(con.execute(
        "SELECT column_name, col_id FROM column_catalog WHERE column_name IN "
        "('bet365_FT_1_Acilis', 'bet365_FT_X_Acilis', 'bet365_FT_2_Acilis')"
    ).fetchall())
    con.close()
    replacements = {
        (100, col_of["bet365_FT_1_Acilis"]): 1.0,
        (100, col_of["bet365_FT_X_Acilis"]): float("nan"),
        (100, col_of["bet365_FT_2_Acilis"]): 0.0,
    }
    part = lake.odds_partition_file(2019, 1)
    tbl = pq.read_table(part)
    mids = tbl["match_id"].to_pylist()
    cols = tbl["col_id"].to_pylist()
    odds = tbl["odd"].to_pylist()
    new_odds = [replacements.get((m, c), o) for m, c, o in zip(mids, cols, odds)]
    patched = pa.table(
        {
            "match_id": tbl["match_id"],
            "col_id": tbl["col_id"],
            "odd": pa.array(new_odds, pa.float32()),
        }
    )
    pq.write_table(patched, part, row_group_size=32)

    session = LakeSession(lake, memory_mb=512, threads=2)
    try:
        t = implied_table(session.cursor(), "bet365")
    finally:
        session.close()
    got = {r["match_id"]: r for r in t.to_pylist()}
    for key in ("odd_home", "odd_draw", "odd_away", "p_home", "p_draw", "p_away", "overround"):
        assert got[100][key] is None, key
    # Diğer maçlar etkilenmez.
    assert got[101]["odd_home"] == pytest.approx(2.05, rel=1e-6)
    assert got[101]["overround"] is not None


# ---------------------------------------------------------------- büyük bellek testi

def _vm_hwm_kb() -> int | None:
    try:
        with open("/proc/self/status", encoding="ascii") as fh:
            for line in fh:
                if line.startswith("VmHWM:"):
                    return int(line.split()[1])
    except OSError:
        return None
    return None


@pytest.mark.slow
def test_compute_elo_two_million_matches_within_4gb(tmp_path):
    """2M sentetik maç + 2M oran satırı; compute_elo tepe RSS'i 4 GB altında kalmalı."""
    import psutil

    budget_bytes = 4 * 1024**3
    con = duckdb.connect(
        ":memory:",
        config={"memory_limit": "1GB", "threads": 2, "temp_directory": str(tmp_path / "tmp")},
    )
    n = 2_000_000
    con.execute(
        f"""CREATE TABLE matches AS
            SELECT CAST(i + 1 AS INTEGER) AS match_id,
                   CAST(2000 + i % 25 AS SMALLINT) AS season,
                   CAST(i % 3 + 1 AS INTEGER) AS league_id,
                   TIMESTAMP '2000-01-01 00:00:00' + to_minutes(i) AS kickoff_utc,
                   CAST(i % 20000 + 1 AS INTEGER) AS home_team_id,
                   CAST((i % 20000 + 1 + (i * 7) % 19998) % 20000 + 1 AS INTEGER) AS away_team_id,
                   CASE WHEN i % 97 = 0 THEN NULL ELSE CAST(i % 5 AS SMALLINT) END AS ft_home,
                   CASE WHEN i % 97 = 0 THEN NULL ELSE CAST((i * 3) % 4 AS SMALLINT) END AS ft_away
            FROM range({n}) t(i)"""
    )
    con.execute(
        f"""CREATE TABLE odds_syn AS
            SELECT CAST(i + 1 AS INTEGER) AS match_id,
                   CAST(0 AS INTEGER) AS col_id,
                   CAST(1.5 + (i % 100) / 50.0 AS FLOAT) AS odd
            FROM range({n}) t(i)"""
    )
    assert con.execute("SELECT count(*) FROM matches").fetchone()[0] == n

    proc = psutil.Process()
    peak = {"rss": proc.memory_info().rss}
    stop = threading.Event()

    def sample_rss() -> None:
        while not stop.is_set():
            rss = proc.memory_info().rss
            if rss > peak["rss"]:
                peak["rss"] = rss
            stop.wait(0.01)

    sampler = threading.Thread(target=sample_rss, daemon=True)
    sampler.start()
    out = tmp_path / "elo_2m.parquet"
    started = time.perf_counter()
    try:
        written = compute_elo(con, out, batch_rows=50_000)
    finally:
        stop.set()
        sampler.join()
    elapsed = time.perf_counter() - started
    con.close()

    hwm_kb = _vm_hwm_kb()
    hwm_bytes = hwm_kb * 1024 if hwm_kb is not None else 0
    peak_rss = max(peak["rss"], hwm_bytes)
    print(
        f"\nELO_2M rows={written} elapsed_s={elapsed:.1f} "
        f"peak_rss_sampled_mb={peak['rss'] / 1024**2:.0f} "
        f"vmhwm_mb={hwm_bytes / 1024**2:.0f} budget_mb={budget_bytes / 1024**2:.0f}"
    )
    assert written == n
    assert pq.ParquetFile(out).metadata.num_rows == n
    assert peak_rss < budget_bytes
