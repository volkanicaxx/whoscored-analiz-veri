"""oddslake.ui testleri: karo önbelleği, sanal ızgara, ağır iş çalıştırıcısı ve ana pencere.

Hızlı testler: python -m pytest tests/test_ui.py -q
Arayüz testleri offscreen Qt platformunda çalışır; ekran gerekmez.
Bekleme yardımcıları olay döngüsünü çalıştırır ve her beklemeyi 10 sn ile sınırlar.
"""

from __future__ import annotations

import os

os.environ.setdefault("QT_QPA_PLATFORM", "offscreen")

import statistics  # noqa: E402
import sys  # noqa: E402
import time  # noqa: E402

import numpy as np  # noqa: E402
import pytest  # noqa: E402
from PySide6.QtCore import QEventLoop, QThreadPool, QTimer, Qt  # noqa: E402
from PySide6.QtWidgets import QApplication, QListWidget, QTableView  # noqa: E402

from oddslake.config import LakePaths, MemoryBudget  # noqa: E402
from oddslake.lake import ColumnFilter, LakeSession, RowFilter  # noqa: E402
from oddslake.ui.app import MainWindow  # noqa: E402
from oddslake.ui.__main__ import main as ui_main  # noqa: E402
from oddslake.ui.grid_model import INFO_COUNT, INFO_HEADERS, TILE_COLS, GridModel, TileCache  # noqa: E402
from oddslake.ui.jobs import HeavyJobRunner  # noqa: E402

WAIT_MS = 10_000


# ---------------------------------------------------------------- yardımcılar


def _pump(ms: int) -> None:
    """Olay döngüsünü verilen süre kadar çalıştırır (işçi sinyallerinin işlenmesi için)."""
    loop = QEventLoop()
    timer = QTimer()
    timer.setSingleShot(True)
    timer.timeout.connect(loop.quit)
    timer.start(ms)
    loop.exec()
    timer.stop()


def _wait_until(predicate, timeout_ms: int = WAIT_MS) -> bool:
    """Koşul sağlanana kadar olay döngüsünü çalıştırır. Zaman aşımında False döner."""
    if predicate():
        return True
    loop = QEventLoop()
    poll = QTimer()
    poll.setInterval(10)
    deadline = QTimer()
    deadline.setSingleShot(True)
    result = {"ok": False}

    def check() -> None:
        if predicate():
            result["ok"] = True
            loop.quit()

    poll.timeout.connect(check)
    deadline.timeout.connect(loop.quit)
    poll.start()
    deadline.start(timeout_ms)
    loop.exec()
    poll.stop()
    deadline.stop()
    return result["ok"] or predicate()


def _collect(runner: HeavyJobRunner) -> dict[str, list]:
    """Sinyalleri basit listelerde toplar (QSignalSpy yerine)."""
    events: dict[str, list] = {"progress": [], "finished": [], "failed": [], "memory": []}
    runner.progress.connect(lambda v: events["progress"].append(v))
    runner.finished.connect(lambda v: events["finished"].append(v))
    runner.failed.connect(lambda v: events["failed"].append(v))
    runner.memory_warning.connect(lambda v: events["memory"].append(v))
    return events


def _spin(cancel, progress):
    """İptal görene kadar döner; iptal görünce InterruptedError fırlatır."""
    progress(1)
    for _ in range(2000):
        if cancel():
            raise InterruptedError()
        time.sleep(0.005)
    return "zaman aşımı"


def _raise_value_error(cancel, progress):
    raise ValueError("kötü girdi")


@pytest.fixture(scope="module")
def qapp():
    app = QApplication.instance()
    if app is None:
        app = QApplication([sys.argv[0]])
    return app


@pytest.fixture
def session(lake: LakePaths):
    s = LakeSession(lake, memory_mb=256, threads=2)
    yield s
    s.close()


@pytest.fixture
def pool(qapp):
    p = QThreadPool()
    p.setMaxThreadCount(2)
    yield p
    p.waitForDone(WAIT_MS)


# ---------------------------------------------------------------- TileCache


def test_tile_cache_never_exceeds_max_tiles():
    cache = TileCache(3)
    for i in range(10):
        cache.put(i, f"tile{i}")
    assert len(cache) == 3
    assert cache.get(0) is None
    assert cache.get(6) is None
    assert cache.get(9) == "tile9"


def test_tile_cache_get_refreshes_recency():
    cache = TileCache(3)
    for key in ("a", "b", "c"):
        cache.put(key, key.upper())
    assert cache.get("a") == "A"  # a en yeni olur; en eski artık b
    cache.put("d", "D")
    assert cache.get("b") is None
    assert cache.get("a") == "A"
    assert cache.get("c") == "C"
    assert cache.get("d") == "D"


def test_tile_cache_rejects_zero_capacity():
    with pytest.raises(ValueError):
        TileCache(0)


# ---------------------------------------------------------------- GridModel


def test_model_counts_follow_filters(session, pool):
    model = GridModel(session, pool, MemoryBudget())
    assert _wait_until(lambda: model.rowCount() == 5)
    full_columns = session.column_set(ColumnFilter()).col_id.size
    assert model.columnCount() == INFO_COUNT + full_columns

    model.set_filters(RowFilter(), ColumnFilter(name_pattern="bet365_FT_1_Acilis"))
    assert _wait_until(lambda: model.columnCount() == INFO_COUNT + 1)
    assert model.rowCount() == 5
    assert model.headerData(INFO_COUNT, Qt.Orientation.Horizontal) == "bet365_FT_1_Acilis"
    assert model.headerData(5, Qt.Orientation.Horizontal) == "match_id"
    assert model.headerData(0, Qt.Orientation.Horizontal) == INFO_HEADERS[0]


def test_odds_tile_loads_in_background(session, pool):
    model = GridModel(session, pool, MemoryBudget())
    model.set_filters(RowFilter(), ColumnFilter(name_pattern="bet365_FT_1_Acilis"))
    assert _wait_until(lambda: model.columnCount() == INFO_COUNT + 1)

    index = model.index(0, INFO_COUNT)
    assert model.data(index) == "…"
    assert _wait_until(lambda: model.data(index) == "2.00")
    assert model.data(index, Qt.ItemDataRole.UserRole) == pytest.approx(2.0, abs=1e-6)


def test_uncached_tile_lookup_stays_under_one_millisecond(session, pool):
    model = GridModel(session, pool, MemoryBudget())
    assert _wait_until(lambda: model.rowCount() == 5)
    durations = []
    for tile_col in range(12):
        index = model.index(0, INFO_COUNT + tile_col * TILE_COLS)
        start = time.perf_counter()
        value = model.data(index)
        durations.append(time.perf_counter() - start)
        assert value == "…"
    assert statistics.median(durations) < 0.001


def test_info_columns_show_score_and_blank_for_unplayed(session, pool):
    model = GridModel(session, pool, MemoryBudget())
    assert _wait_until(lambda: model.rowCount() == 5)
    # Satır sırası (kickoff_utc, match_id): 100, 101, 102, 103, 104.
    assert model.data(model.index(0, 5), Qt.ItemDataRole.UserRole) == 100
    assert model.data(model.index(2, 5), Qt.ItemDataRole.UserRole) == 102

    assert _wait_until(lambda: model.data(model.index(0, 4)) == "0-0")
    assert _wait_until(lambda: model.data(model.index(2, 4)) == "")
    assert model.data(model.index(0, 1)) == "Süper Lig"
    assert model.data(model.index(0, 2)) == "Galatasaray"
    assert model.data(model.index(0, 3)) == "Fenerbahçe"


def test_bookmaker_filter_changes_column_count(session, pool):
    model = GridModel(session, pool, MemoryBudget())
    assert _wait_until(lambda: model.rowCount() == 5)
    full_columns = session.column_set(ColumnFilter()).col_id.size
    bet365_columns = session.column_set(ColumnFilter(bookmaker_codes=["bet365"])).col_id.size
    assert 0 < bet365_columns < full_columns

    model.set_filters(RowFilter(), ColumnFilter(bookmaker_codes=["bet365"]))
    assert _wait_until(lambda: model.columnCount() == INFO_COUNT + bet365_columns)


def test_latest_filter_wins_over_stale_results(session, pool):
    model = GridModel(session, pool, MemoryBudget())
    assert _wait_until(lambda: model.rowCount() == 5)

    model.set_filters(RowFilter(), ColumnFilter(name_pattern="bet365_FT_1_Acilis"))
    model.set_filters(RowFilter(), ColumnFilter(name_pattern="bwin_FT_2_Kapanis"))
    assert _wait_until(
        lambda: model.columnCount() == INFO_COUNT + 1
        and model.headerData(INFO_COUNT, Qt.Orientation.Horizontal) == "bwin_FT_2_Kapanis"
    )
    _pump(300)  # Eski isteğin gecikmiş sonucu gelirse uygulanmamalı.
    assert model.columnCount() == INFO_COUNT + 1
    assert model.headerData(INFO_COUNT, Qt.Orientation.Horizontal) == "bwin_FT_2_Kapanis"


def test_tile_values_match_column_strip(session, pool):
    model = GridModel(session, pool, MemoryBudget())
    col_filter = ColumnFilter(bookmaker_codes=["bwin"])
    model.set_filters(RowFilter(), col_filter)
    assert _wait_until(lambda: model.rowCount() == 5)
    colset = session.column_set(col_filter)
    rows = session.row_index(RowFilter())
    width = min(20, len(colset))
    assert _wait_until(
        lambda: all(
            model.data(model.index(r, c)) != "…"
            for r in range(5)
            for c in range(INFO_COUNT, INFO_COUNT + width)
        )
    )
    for offset in range(width):
        expected = session.column_strip(int(colset.col_id[offset]), rows)
        for r in range(5):
            value = model.data(model.index(r, INFO_COUNT + offset), Qt.ItemDataRole.UserRole)
            if np.isnan(expected[r]):
                assert value is None
            else:
                assert value == pytest.approx(float(expected[r]), rel=1e-6)


# ---------------------------------------------------------------- HeavyJobRunner


def test_second_submit_is_rejected_and_cancel_reports_iptal(qapp):
    runner = HeavyJobRunner(MemoryBudget())
    events = _collect(runner)
    assert runner.submit("uzun", _spin) is True
    assert runner.submit("ikinci", lambda cancel, progress: 1) is False
    runner.cancel()
    assert _wait_until(lambda: events["failed"])
    assert events["failed"] == ["iptal"]
    assert events["finished"] == []
    assert runner.wait_idle(WAIT_MS)
    assert not runner.is_busy


def test_result_progress_and_exception_are_delivered(qapp):
    runner = HeavyJobRunner(MemoryBudget())
    events = _collect(runner)

    def finishing(cancel, progress):
        progress(7)
        return 42

    assert runner.submit("sonuc", finishing) is True
    assert _wait_until(lambda: events["finished"])
    assert events["finished"] == [42]
    assert events["progress"] == [7]
    assert runner.wait_idle(WAIT_MS)

    assert runner.submit("hata", _raise_value_error) is True
    assert _wait_until(lambda: events["failed"])
    assert events["failed"] == ["ValueError: kötü girdi"]
    assert runner.wait_idle(WAIT_MS)


def test_memory_guard_aborts_job_over_hard_limit(qapp):
    runner = HeavyJobRunner(MemoryBudget(hard_limit_mb=1))
    events = _collect(runner)
    assert runner.submit("bellek", _spin) is True
    assert _wait_until(lambda: events["failed"])
    assert events["failed"][0].startswith("bellek siniri asildi: ")
    assert events["memory"] and events["memory"][0] > 1
    assert runner.wait_idle(WAIT_MS)
    # Bekçi tek bir sonlandırma sinyali yayar; işçinin dönüşü ikinci sinyal üretmez.
    assert len(events["failed"]) == 1
    assert events["finished"] == []


# ---------------------------------------------------------------- ana pencere ve CLI


def test_main_window_renders_lake(session, tmp_path, qapp):
    window = MainWindow(session, MemoryBudget())
    try:
        window.resize(1280, 800)
        window.show()
        view = window.findChild(QTableView)
        assert view is not None
        model = view.model()
        assert _wait_until(lambda: model.rowCount() == 5)

        counts = sorted(widget.count() for widget in window.findChildren(QListWidget))
        league_count = session.con.execute("SELECT count(*) FROM leagues").fetchone()[0]
        bookmaker_count = session.con.execute("SELECT count(*) FROM bookmakers").fetchone()[0]
        assert league_count in counts
        assert bookmaker_count in counts

        _pump(200)
        pixmap = window.grab()
        out = tmp_path / "main_window.png"
        assert pixmap.save(str(out), "PNG")
        assert out.stat().st_size > 0
        assert pixmap.width() > 0 and pixmap.height() > 0
    finally:
        window.close()


def test_main_rejects_missing_lake(tmp_path, capsys):
    with pytest.raises(SystemExit) as info:
        ui_main(["--lake", str(tmp_path / "yok")])
    assert info.value.code == 2
    assert "catalog.duckdb" in capsys.readouterr().err
