"""Ana pencere: filtre paneli, ağır analiz düğmeleri, sanal ızgara ve durum çubuğu.

Filtre paneli lig, sezon, büro, kolon deseni ve boş kolon seçeneğini toplar. "Uygula"
RowFilter ve ColumnFilter üretip GridModel.set_filters çağırır. Analiz düğmeleri
HeavyJobRunner üzerinden tek slotta çalışır; analiz modülleri ilgili eylem anında
yüklenir, böylece arayüz analiz katmanı olmadan da açılabilir.
"""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path
from typing import Any

import psutil
from PySide6.QtCore import QThreadPool, QTimer, Qt
from PySide6.QtGui import QCloseEvent
from PySide6.QtWidgets import (
    QAbstractItemView,
    QCheckBox,
    QFormLayout,
    QGroupBox,
    QHBoxLayout,
    QHeaderView,
    QLabel,
    QLineEdit,
    QListWidget,
    QListWidgetItem,
    QMainWindow,
    QProgressBar,
    QPushButton,
    QSpinBox,
    QSplitter,
    QTableView,
    QVBoxLayout,
    QWidget,
)

from ..config import MemoryBudget, default_threads
from ..lake import ColumnFilter, LakeSession, RowFilter
from .grid_model import INFO_COUNT, GridModel
from .jobs import CANCELLED_MESSAGE, HeavyJobRunner

_MB = 1024 * 1024
# Bilgi kolonlarının sabit genişlikleri: Tarih, Lig, Ev, Deplasman, Skor, match_id.
_INFO_WIDTHS = (150, 110, 170, 170, 60, 80)
_SEASON_MAX = 2100
# Sezon kutusunda minimum değer "Tümü" anlamına gelir; sınır uygulanmaz.
_ANY_SEASON = 0
_STATUS_MS = 15000

JobCall = Callable[[Any, Callable[[], bool], Callable[[int], None]], object]


class MainWindow(QMainWindow):
    """oddslake ana penceresi. session ve budget dışarıdan verilir; sahiplik çağırana aittir."""

    def __init__(self, session: LakeSession, budget: MemoryBudget):
        super().__init__()
        self.setWindowTitle("oddslake")
        self.resize(1400, 860)
        self._session = session
        self._budget = budget
        self._paths = session.paths
        self._process = psutil.Process()
        self._job_name: str | None = None
        self._match_total = 0

        self._pool = QThreadPool()
        self._pool.setMaxThreadCount(max(1, default_threads()))
        self._model = GridModel(session, self._pool, budget)
        self._runner = HeavyJobRunner(budget, self)

        self._build_ui()
        self._load_reference_lists()
        self._connect_signals()
        self._set_busy(False)

        self._timer = QTimer(self)
        self._timer.setInterval(1000)
        self._timer.timeout.connect(self._refresh_resources)
        self._timer.start()
        self._refresh_status()
        self._refresh_resources()

    # ---- arayüz kurulumu -----------------------------------------------------------

    def _build_ui(self) -> None:
        splitter = QSplitter(Qt.Orientation.Horizontal)
        splitter.addWidget(self._build_filter_panel())
        splitter.addWidget(self._build_grid_area())
        splitter.setStretchFactor(1, 1)
        splitter.setSizes([300, 1100])
        self.setCentralWidget(splitter)

        self._rows_label = QLabel()
        self._cols_label = QLabel()
        self._cache_label = QLabel()
        self._rss_label = QLabel()
        status = self.statusBar()
        for label in (self._rows_label, self._cols_label, self._cache_label, self._rss_label):
            status.addPermanentWidget(label)

    def _build_filter_panel(self) -> QWidget:
        panel = QWidget()
        layout = QVBoxLayout(panel)

        layout.addWidget(QLabel("Lig"))
        self._league_list = self._multi_select_list()
        layout.addWidget(self._league_list)

        season_box = QGroupBox("Sezon (başlangıç yılı)")
        season_form = QFormLayout(season_box)
        self._season_from = self._season_spin()
        self._season_to = self._season_spin()
        season_form.addRow("Başlangıç", self._season_from)
        season_form.addRow("Bitiş", self._season_to)
        layout.addWidget(season_box)

        layout.addWidget(QLabel("Büro"))
        self._bookmaker_list = self._multi_select_list()
        layout.addWidget(self._bookmaker_list)

        column_box = QGroupBox("Kolonlar")
        column_layout = QVBoxLayout(column_box)
        self._pattern_edit = QLineEdit()
        self._pattern_edit.setPlaceholderText("örn. bet365_FT_1_*   (* joker)")
        self._hide_empty = QCheckBox("boş kolonları gizle")
        column_layout.addWidget(self._pattern_edit)
        column_layout.addWidget(self._hide_empty)
        layout.addWidget(column_box)

        self._apply_button = QPushButton("Uygula")
        layout.addWidget(self._apply_button)
        layout.addStretch(1)
        return panel

    def _build_grid_area(self) -> QWidget:
        area = QWidget()
        layout = QVBoxLayout(area)

        bar = QHBoxLayout()
        self._elo_button = QPushButton("Elo hesapla")
        self._form_button = QPushButton("Form hesapla")
        self._progress = QProgressBar()
        self._progress.setRange(0, 1)
        self._progress.setValue(0)
        self._cancel_button = QPushButton("İptal")
        bar.addWidget(self._elo_button)
        bar.addWidget(self._form_button)
        bar.addWidget(self._progress, 1)
        bar.addWidget(self._cancel_button)
        layout.addLayout(bar)

        self._view = QTableView()
        self._view.setModel(self._model)
        # QTableView'de setUniformRowHeights yoktur (QTreeView API'si). Eşdeğeri: satır
        # yüksekliği içeriğe göre yeniden hesaplanmaz; her satır sabit boyutta kalır.
        self._view.verticalHeader().setSectionResizeMode(QHeaderView.ResizeMode.Fixed)
        self._view.verticalHeader().setDefaultSectionSize(20)
        self._view.setAlternatingRowColors(True)
        self._view.setHorizontalScrollMode(QAbstractItemView.ScrollMode.ScrollPerItem)
        self._view.setVerticalScrollMode(QAbstractItemView.ScrollMode.ScrollPerItem)
        layout.addWidget(self._view, 1)
        return area

    @staticmethod
    def _multi_select_list() -> QListWidget:
        widget = QListWidget()
        widget.setSelectionMode(QAbstractItemView.SelectionMode.MultiSelection)
        return widget

    @staticmethod
    def _season_spin() -> QSpinBox:
        spin = QSpinBox()
        spin.setRange(_ANY_SEASON, _SEASON_MAX)
        spin.setSpecialValueText("Tümü")
        spin.setValue(_ANY_SEASON)
        return spin

    def _load_reference_lists(self) -> None:
        """Lig ve büro listelerini ve maç sayısını bir kez doldurur (ana iş parçacığında, küçük tablolar)."""
        cur = self._session.cursor()
        try:
            leagues = cur.execute(
                "SELECT league_id, name, country FROM leagues ORDER BY name, league_id"
            ).fetchall()
            bookmakers = cur.execute(
                "SELECT code, name FROM bookmakers ORDER BY sort_order, code"
            ).fetchall()
            self._match_total = int(cur.execute("SELECT count(*) FROM matches").fetchone()[0])
        finally:
            cur.close()
        for league_id, name, country in leagues:
            label = name if country is None else f"{name} ({country})"
            item = QListWidgetItem(label)
            item.setData(Qt.ItemDataRole.UserRole, int(league_id))
            self._league_list.addItem(item)
        for code, name in bookmakers:
            item = QListWidgetItem(name)
            item.setData(Qt.ItemDataRole.UserRole, str(code))
            self._bookmaker_list.addItem(item)

    def _connect_signals(self) -> None:
        self._apply_button.clicked.connect(lambda *_: self._apply_filters())
        self._pattern_edit.returnPressed.connect(self._apply_filters)
        self._elo_button.clicked.connect(lambda *_: self._start_elo())
        self._form_button.clicked.connect(lambda *_: self._start_form())
        self._cancel_button.clicked.connect(lambda *_: self._runner.cancel())
        self._runner.progress.connect(self._on_progress)
        self._runner.finished.connect(self._on_job_finished)
        self._runner.failed.connect(self._on_job_failed)
        self._runner.memory_warning.connect(self._on_memory_warning)
        self._model.load_failed.connect(self._on_load_failed)
        self._model.modelReset.connect(self._refresh_status)
        self._model.modelReset.connect(self._apply_column_widths)

    # ---- filtreler ------------------------------------------------------------------

    @staticmethod
    def _season_bound(spin: QSpinBox) -> int | None:
        value = spin.value()
        return None if value == spin.minimum() else value

    @staticmethod
    def _selected_data(widget: QListWidget) -> list:
        return [item.data(Qt.ItemDataRole.UserRole) for item in widget.selectedItems()]

    def _apply_filters(self) -> None:
        row_filter = RowFilter(
            league_ids=self._selected_data(self._league_list) or None,
            season_from=self._season_bound(self._season_from),
            season_to=self._season_bound(self._season_to),
        )
        pattern = self._pattern_edit.text().strip()
        col_filter = ColumnFilter(
            name_pattern=pattern or None,
            bookmaker_codes=self._selected_data(self._bookmaker_list) or None,
            hide_empty=self._hide_empty.isChecked(),
        )
        self._model.set_filters(row_filter, col_filter)
        self.statusBar().showMessage("Filtre uygulanıyor…", 3000)

    # ---- analiz eylemleri -------------------------------------------------------------

    def _derived_path(self, name: str) -> Path:
        self._paths.derived_dir.mkdir(parents=True, exist_ok=True)
        return self._paths.derived_dir / name

    def _start_elo(self) -> None:
        try:
            import oddslake.analysis.elo as elo
        except ImportError as exc:
            self.statusBar().showMessage(f"Elo modülü yüklenemedi: {exc}", _STATUS_MS)
            return
        out_path = self._derived_path("elo.parquet")
        batch_rows = self._budget.stream_batch_rows
        self._submit(
            "Elo",
            lambda cur, cancel, progress: elo.compute_elo(
                cur, out_path, batch_rows=batch_rows, cancel=cancel, progress=progress
            ),
        )

    def _start_form(self) -> None:
        try:
            import oddslake.analysis.form as form
        except ImportError as exc:
            self.statusBar().showMessage(f"Form modülü yüklenemedi: {exc}", _STATUS_MS)
            return
        out_path = self._derived_path("form.parquet")
        self._submit("Form", lambda cur, cancel, progress: form.compute_form(cur, out_path))

    def _submit(self, name: str, call: JobCall) -> None:
        """Analiz çağrısını işçi slotuna koyar. İşçi kendi DuckDB bağlantısını açar ve kapatır."""
        session = self._session

        def job(cancel: Callable[[], bool], progress: Callable[[int], None]) -> object:
            if cancel():
                raise InterruptedError()
            cur = session.cursor()
            try:
                return call(cur, cancel, progress)
            finally:
                cur.close()

        if not self._runner.submit(name, job):
            self.statusBar().showMessage("Başka bir ağır iş çalışıyor", 5000)
            return
        self._job_name = name
        self._progress.setRange(0, 0)
        self._set_busy(True)
        self.statusBar().showMessage(f"{name} başladı")

    def _set_busy(self, busy: bool) -> None:
        self._elo_button.setEnabled(not busy)
        self._form_button.setEnabled(not busy)
        self._cancel_button.setEnabled(busy)

    def _reset_progress(self) -> None:
        self._progress.setRange(0, 1)
        self._progress.setValue(0)

    def _on_progress(self, value: int) -> None:
        # Elo ilerlemeyi işlenen satır sayısı olarak bildirir; toplam maç sayısına göre ölçeklenir.
        if self._progress.maximum() == 0 and self._match_total > 0:
            self._progress.setRange(0, self._match_total)
        if self._progress.maximum() > 0:
            self._progress.setValue(min(value, self._progress.maximum()))

    def _on_job_finished(self, result: object) -> None:
        name = self._job_name or "İş"
        self._job_name = None
        self._reset_progress()
        self._set_busy(self._runner.is_busy)
        self.statusBar().showMessage(f"{name} tamamlandı: {result}", _STATUS_MS)

    def _on_job_failed(self, message: str) -> None:
        name = self._job_name or "İş"
        self._job_name = None
        self._reset_progress()
        self._set_busy(self._runner.is_busy)
        text = "iptal edildi" if message == CANCELLED_MESSAGE else f"başarısız: {message}"
        self.statusBar().showMessage(f"{name} {text}", _STATUS_MS)

    def _on_memory_warning(self, rss_mb: int) -> None:
        self.statusBar().showMessage(
            f"Bellek sınırı aşıldı: RSS {rss_mb} MB (sınır {self._budget.hard_limit_mb} MB)",
            _STATUS_MS,
        )

    def _on_load_failed(self, message: str) -> None:
        self.statusBar().showMessage(message, _STATUS_MS)

    # ---- ızgara ve durum çubuğu -------------------------------------------------------

    def _apply_column_widths(self) -> None:
        header = self._view.horizontalHeader()
        for col, width in enumerate(_INFO_WIDTHS[:INFO_COUNT]):
            if col < self._model.columnCount():
                header.setSectionResizeMode(col, QHeaderView.ResizeMode.Fixed)
                header.resizeSection(col, width)

    def _refresh_status(self) -> None:
        self._rows_label.setText(f"Satır: {self._model.rowCount()}")
        self._cols_label.setText(f"Kolon: {self._model.columnCount()}")

    def _refresh_resources(self) -> None:
        rss_mb = self._process.memory_info().rss // _MB
        cache = self._model.tile_cache
        self._rss_label.setText(f"RSS {rss_mb} MB / sınır {self._budget.hard_limit_mb} MB")
        self._cache_label.setText(f"Karo önbelleği {len(cache)}/{cache.max_tiles}")
        # İş, bellek koruyucusu tarafından kesilmiş ama işçisi henüz dönmemişse de slot dolu sayılır.
        self._set_busy(self._runner.is_busy)

    def closeEvent(self, event: QCloseEvent) -> None:
        self._timer.stop()
        self._runner.cancel()
        self._runner.wait_idle(10_000)
        self._pool.waitForDone(5_000)
        super().closeEvent(event)
