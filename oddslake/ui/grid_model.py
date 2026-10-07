"""Sanal ızgara modeli: satır ve kolon indeksleri, karo önbelleği, arka plan yüklemesi.

Ekranda 100.000'den fazla oran kolonu ve milyonlarca maç olabilir. Model hücre
verisini tutmaz; bellekte yalnızca iki kısa dizi kalır (RowIndex ve ColumnSet).
Hücre değerleri 32 satır x 16 kolonluk karolar halinde LRU önbelleğinde durur.
Görünen bir karo yüklü değilse data() "…" döner, karo QThreadPool işçisinde
DuckDB'den okunur ve bitince dataChanged yayılır.

Kurallar:
  * data(), headerData(), rowCount(), columnCount() ve set_filters() yalnızca ana
    iş parçacığından çağrılır.
  * Ana iş parçacığında DuckDB sorgusu çalışmaz; sorgular QRunnable içinde yapılır.
  * İşçiler sonuçlarını model durumuna doğrudan yazmaz; _Bridge sinyalleriyle ana
    iş parçacığına bildirir. Önbellek yazımı da ana iş parçacığında yapılır.
  * Her filtre isteği bir generation numarası alır. Eski generation'a ait filtre
    ve karo sonuçları model durumuna yazılmadan atılır.
"""

from __future__ import annotations

import itertools
import math
import threading
from collections import OrderedDict
from collections.abc import Hashable

import numpy as np
from PySide6.QtCore import (
    QAbstractTableModel,
    QModelIndex,
    QObject,
    QRunnable,
    QThreadPool,
    Qt,
    Signal,
)

from ..config import MemoryBudget
from ..lake import ColumnFilter, ColumnSet, LakeSession, RowFilter, RowIndex

INFO_HEADERS: tuple[str, ...] = ("Tarih", "Lig", "Ev", "Deplasman", "Skor", "match_id")
INFO_COUNT = len(INFO_HEADERS)
MATCH_ID_COL = INFO_COUNT - 1
TILE_ROWS = 32
TILE_COLS = 16
# Karo anahtarında kolon karosu yerine kullanılan değer: bilgi (Tarih..Skor) karosu.
INFO_TILE = -1
PLACEHOLDER = "…"
ERROR_MARK = "!"

_DISPLAY = int(Qt.ItemDataRole.DisplayRole)
_USER = int(Qt.ItemDataRole.UserRole)
_NO_INFO: tuple[str, ...] = ("", "", "", "", "")
# Filtre sonuçları, bekleyen karo işlerinden önce çalışsın diye daha yüksek öncelik.
_INDEX_PRIORITY = 1

# (generation, satır karosu, kolon karosu veya INFO_TILE)
TileKey = tuple[int, int, int]


def is_valid_odd(values: np.ndarray) -> np.ndarray:
    """Ondalık oran geçerlilik maskesi: sonlu ve 1.0'dan büyük olmalı.

    0.0, negatif değerler ve NaN geçersizdir.
    """
    return np.isfinite(values) & (values > 1.0)


def _empty_row_index() -> RowIndex:
    return RowIndex(
        match_id=np.empty(0, dtype=np.int32),
        season=np.empty(0, dtype=np.int16),
        league_id=np.empty(0, dtype=np.int32),
    )


class TileCache:
    """Karolar için iş parçacığı güvenli LRU önbelleği.

    get() kullanılan karoyu en yeni konuma taşır. max_tiles aşılırsa en eski karo atılır.
    """

    def __init__(self, max_tiles: int):
        if max_tiles < 1:
            raise ValueError("max_tiles en az 1 olmalı")
        self._max_tiles = int(max_tiles)
        self._data: OrderedDict[Hashable, object] = OrderedDict()
        self._lock = threading.Lock()

    @property
    def max_tiles(self) -> int:
        return self._max_tiles

    def get(self, key: Hashable) -> object | None:
        with self._lock:
            if key not in self._data:
                return None
            self._data.move_to_end(key)
            return self._data[key]

    def put(self, key: Hashable, value: object) -> None:
        with self._lock:
            self._data[key] = value
            self._data.move_to_end(key)
            while len(self._data) > self._max_tiles:
                self._data.popitem(last=False)

    def clear(self) -> None:
        with self._lock:
            self._data.clear()

    def __contains__(self, key: object) -> bool:
        with self._lock:
            return key in self._data

    def __len__(self) -> int:
        with self._lock:
            return len(self._data)


def _scatter_odds(
    block: np.ndarray,
    row_ids: np.ndarray,
    col_ids: np.ndarray,
    mids: np.ndarray,
    cids: np.ndarray,
    odds: np.ndarray,
) -> None:
    """Uzun formattaki (match_id, col_id, odd) üçlülerini karo bloğuna yerleştirir.

    row_ids ve col_ids karonun satır ve kolon kimlikleridir (sırasız olabilir).
    Karo dışına düşen üçlüler atlanır; geçersiz oranlar yazılmaz ve NaN kalır.
    """
    if mids.size == 0:
        return
    valid = is_valid_odd(odds)
    mids, cids, odds = mids[valid], cids[valid], odds[valid]
    if mids.size == 0:
        return
    row_order = np.argsort(row_ids, kind="stable")
    row_sorted = row_ids[row_order]
    row_pos = np.minimum(np.searchsorted(row_sorted, mids), row_sorted.size - 1)
    row_hit = row_sorted[row_pos] == mids
    col_order = np.argsort(col_ids, kind="stable")
    col_sorted = col_ids[col_order]
    col_pos = np.minimum(np.searchsorted(col_sorted, cids), col_sorted.size - 1)
    col_hit = col_sorted[col_pos] == cids
    keep = row_hit & col_hit
    block[row_order[row_pos[keep]], col_order[col_pos[keep]]] = odds[keep]


def load_info_rows(
    session: LakeSession, cur, rows: RowIndex, start: int, stop: int
) -> list[tuple[str, ...]]:
    """Satır aralığı için (Tarih, Lig, Ev, Deplasman, Skor) demetlerini döner.

    Skor boşsa (oynanmamış maç) "" döner; 0-0 maç "0-0" olarak gelir.
    """
    mids = rows.match_id[start:stop]
    found = session.fetch_match_info(mids, cur)
    return [found.get(int(mid), _NO_INFO) for mid in mids]


def load_odds_block(
    session: LakeSession,
    cur,
    rows: RowIndex,
    cols: ColumnSet,
    row_start: int,
    row_stop: int,
    col_start: int,
    col_stop: int,
) -> np.ndarray:
    """Satır x kolon karosunu float32 olarak yükler. Eksik ve geçersiz hücreler NaN'dır."""
    block = np.full((row_stop - row_start, col_stop - col_start), np.nan, dtype=np.float32)
    mids = rows.match_id[row_start:row_stop]
    cids = cols.col_id[col_start:col_stop]
    files = session.partition_files(rows.season[row_start:row_stop], rows.league_id[row_start:row_stop])
    if not files:
        return block
    got_mids, got_cids, got_odds = session.fetch_odds_block(mids, cids, files, cur)
    _scatter_odds(block, mids, cids, got_mids, got_cids, got_odds)
    return block


class _Bridge(QObject):
    """İşçi iş parçacıklarından ana iş parçacığına sonuç taşıyan sinyal köprüsü.

    Her yük (payload) ilk öğesi olarak sahip modelin kimliğini taşır; model yalnızca
    kendi sonuçlarını işler. Köprü süreç boyunca tek bir nesnedir (bkz. _shared_bridge).
    """

    tile_done = Signal(object)     # (sahip, (anahtarlar), {anahtar: değer})
    tile_failed = Signal(object)   # (sahip, (anahtarlar), mesaj)
    index_done = Signal(object)    # (sahip, generation, RowIndex, ColumnSet)
    index_failed = Signal(object)  # (sahip, generation, mesaj)


_BRIDGE: _Bridge | None = None
_OWNER_IDS = itertools.count(1)


def _shared_bridge() -> _Bridge:
    """Süreç boyunca yaşayan tek köprü; ilk çağrı ana iş parçacığında yapılmalıdır.

    İşçi görevleri köprüye referans tutar. Köprü bir QObject olduğu için son referans
    bir işçi iş parçacığında bırakılırsa nesne orada yok edilirdi. Bu yüzden köprü hiç
    yok edilmez; modeller kendi kimlikleriyle yüklerini ayırır.
    """
    global _BRIDGE
    if _BRIDGE is None:
        _BRIDGE = _Bridge()
    return _BRIDGE


class _TileTask(QRunnable):
    """Bir karo (ve gerekiyorsa aynı satırların bilgi karosu) için DuckDB okuması."""

    def __init__(
        self,
        bridge: _Bridge,
        owner: int,
        session: LakeSession,
        covered: tuple[TileKey, ...],
        rows: RowIndex,
        cols: ColumnSet,
    ):
        super().__init__()
        self.setAutoDelete(True)
        self._bridge = bridge
        self._owner = owner
        self._session = session
        self._covered = covered
        self._rows = rows
        self._cols = cols

    def run(self) -> None:
        cur = None
        try:
            cur = self._session.cursor()
            values: dict[TileKey, object] = {}
            for key in self._covered:
                _, tile_row, tile_col = key
                row_start = tile_row * TILE_ROWS
                row_stop = min(row_start + TILE_ROWS, len(self._rows))
                if tile_col == INFO_TILE:
                    values[key] = load_info_rows(self._session, cur, self._rows, row_start, row_stop)
                else:
                    col_start = tile_col * TILE_COLS
                    col_stop = min(col_start + TILE_COLS, len(self._cols))
                    values[key] = load_odds_block(
                        self._session, cur, self._rows, self._cols,
                        row_start, row_stop, col_start, col_stop,
                    )
            self._bridge.tile_done.emit((self._owner, self._covered, values))
        except Exception as exc:
            self._bridge.tile_failed.emit((self._owner, self._covered, f"{type(exc).__name__}: {exc}"))
        finally:
            if cur is not None:
                cur.close()


class _IndexTask(QRunnable):
    """Satır ve kolon filtrelerini DuckDB'de uygulayıp RowIndex / ColumnSet üretir."""

    def __init__(
        self,
        bridge: _Bridge,
        owner: int,
        session: LakeSession,
        generation: int,
        row_filter: RowFilter,
        col_filter: ColumnFilter,
    ):
        super().__init__()
        self.setAutoDelete(True)
        self._bridge = bridge
        self._owner = owner
        self._session = session
        self._generation = generation
        self._row_filter = row_filter
        self._col_filter = col_filter

    def run(self) -> None:
        cur = None
        try:
            cur = self._session.cursor()
            rows = self._session.row_index(self._row_filter, cur)
            cols = self._session.column_set(self._col_filter, cur)
            self._bridge.index_done.emit((self._owner, self._generation, rows, cols))
        except Exception as exc:
            self._bridge.index_failed.emit((self._owner, self._generation, f"{type(exc).__name__}: {exc}"))
        finally:
            if cur is not None:
                cur.close()


class GridModel(QAbstractTableModel):
    """Sanal ızgara: önce bilgi kolonları (Tarih..match_id), sonra oran kolonları.

    Satır sayısı RowIndex uzunluğudur; kolon sayısı 6 + ColumnSet uzunluğudur.
    load_failed(str) bir karo veya filtre yüklenemediğinde yayılır.
    """

    load_failed = Signal(str)

    def __init__(self, session: LakeSession, pool: QThreadPool, budget: MemoryBudget):
        super().__init__()
        self._session = session
        self._pool = pool
        self._tiles = TileCache(budget.max_tiles)
        self._owner = next(_OWNER_IDS)
        self._bridge = _shared_bridge()
        self._bridge.tile_done.connect(self._on_tile_done)
        self._bridge.tile_failed.connect(self._on_tile_failed)
        self._bridge.index_done.connect(self._on_index_done)
        self._bridge.index_failed.connect(self._on_index_failed)
        self._lock = threading.Lock()
        # Kuyruğa alınmış ama henüz bitmemiş karolar: anahtar -> görev referansı.
        self._pending: dict[TileKey, QRunnable] = {}
        self._index_tasks: dict[int, QRunnable] = {}
        self._errors: dict[TileKey, str] = {}
        self._requested_gen = 0
        self._applied_gen = 0
        self._row_index = _empty_row_index()
        self._colset = ColumnSet(np.empty(0, dtype=np.int32), [])
        self.set_filters(RowFilter(), ColumnFilter())

    @property
    def tile_cache(self) -> TileCache:
        return self._tiles

    # ---- filtre ---------------------------------------------------------------

    def set_filters(self, row_filter: RowFilter, col_filter: ColumnFilter) -> None:
        """Satır ve kolon filtrelerini arka planda uygular.

        Her çağrı yeni bir generation açar. Sonuç yalnızca hâlâ en son generation ise
        model durumuna yazılır; önceki sonuçlar sessizce atılır.
        """
        with self._lock:
            self._requested_gen += 1
            generation = self._requested_gen
        task = _IndexTask(self._bridge, self._owner, self._session, generation, row_filter, col_filter)
        self._index_tasks[generation] = task
        self._pool.start(task, _INDEX_PRIORITY)

    def _on_index_done(self, payload: tuple) -> None:
        owner, generation, row_index, colset = payload
        if owner != self._owner:
            return
        self._index_tasks.pop(generation, None)
        if generation != self._requested_gen:
            return  # Daha yeni bir filtre istendi; bu sonuç eski.
        self.beginResetModel()
        self._row_index = row_index
        self._colset = colset
        self._applied_gen = generation
        self._tiles.clear()
        self._errors.clear()
        self.endResetModel()

    def _on_index_failed(self, payload: tuple) -> None:
        owner, generation, message = payload
        if owner != self._owner:
            return
        self._index_tasks.pop(generation, None)
        if generation == self._requested_gen:
            self.load_failed.emit(f"filtre uygulanamadı: {message}")

    # ---- karo yükleme ----------------------------------------------------------

    def _schedule(self, key: TileKey) -> None:
        """Karoyu arka plan işçisine kuyruğa alır. Aynı karo için tek iş bekler."""
        generation, tile_row, tile_col = key
        info_key = (generation, tile_row, INFO_TILE)
        with self._lock:
            if key in self._pending:
                return
            covered = [key]
            # Kolon karosu yüklenirken aynı satırların bilgi karosu da alınır (tek iş).
            if tile_col != INFO_TILE and info_key not in self._tiles and info_key not in self._pending:
                covered.append(info_key)
            task = _TileTask(
                self._bridge, self._owner, self._session, tuple(covered), self._row_index, self._colset
            )
            for covered_key in covered:
                self._pending[covered_key] = task
        self._pool.start(task)

    def _on_tile_done(self, payload: tuple) -> None:
        owner, covered, values = payload
        if owner != self._owner:
            return
        with self._lock:
            for key in covered:
                self._pending.pop(key, None)
        if covered[0][0] != self._applied_gen:
            return  # Karo eski bir filtreye ait; atılır.
        for key, value in values.items():
            self._tiles.put(key, value)
            self._notify_range(key)

    def _on_tile_failed(self, payload: tuple) -> None:
        owner, covered, message = payload
        if owner != self._owner:
            return
        with self._lock:
            for key in covered:
                self._pending.pop(key, None)
        if covered[0][0] != self._applied_gen:
            return
        for key in covered:
            self._errors[key] = message
            self._notify_range(key)
        self.load_failed.emit(f"karo yüklenemedi: {message}")

    def _unavailable(self, key: TileKey, role: int):
        """Karo yüklü değilken hücre değerini döner ve yüklemeyi kuyruğa alır."""
        if key in self._errors:
            return ERROR_MARK if role == _DISPLAY else None
        self._schedule(key)
        return PLACEHOLDER if role == _DISPLAY else None

    def _notify_range(self, key: TileKey) -> None:
        """Karonun kapsadığı hücre aralığı için dataChanged yayar."""
        _, tile_row, tile_col = key
        row_start = tile_row * TILE_ROWS
        row_stop = min(row_start + TILE_ROWS, len(self._row_index)) - 1
        if tile_col == INFO_TILE:
            col_start, col_stop = 0, INFO_COUNT - 1
        else:
            col_start = INFO_COUNT + tile_col * TILE_COLS
            col_stop = min(col_start + TILE_COLS, self.columnCount()) - 1
        self.dataChanged.emit(
            self.index(row_start, col_start),
            self.index(row_stop, col_stop),
            [_DISPLAY, _USER],
        )

    # ---- QAbstractTableModel arayüzü --------------------------------------------

    def rowCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else len(self._row_index)

    def columnCount(self, parent: QModelIndex = QModelIndex()) -> int:
        return 0 if parent.isValid() else INFO_COUNT + len(self._colset)

    def headerData(self, section: int, orientation: Qt.Orientation, role: int = _DISPLAY):
        if role != _DISPLAY:
            return None
        if orientation == Qt.Orientation.Vertical:
            return str(section + 1)
        if section < INFO_COUNT:
            return INFO_HEADERS[section]
        offset = section - INFO_COUNT
        return self._colset.names[offset] if offset < len(self._colset.names) else None

    def data(self, index: QModelIndex, role: int = _DISPLAY):
        if not index.isValid() or role not in (_DISPLAY, _USER):
            return None
        row = index.row()
        col = index.column()
        if row >= len(self._row_index) or col >= self.columnCount():
            return None
        if col == MATCH_ID_COL:
            match_id = int(self._row_index.match_id[row])
            return str(match_id) if role == _DISPLAY else match_id

        generation = self._applied_gen
        tile_row, row_in_tile = divmod(row, TILE_ROWS)
        if col < INFO_COUNT:
            info_key = (generation, tile_row, INFO_TILE)
            info = self._tiles.get(info_key)
            if info is None:
                return self._unavailable(info_key, role)
            return info[row_in_tile][col]

        tile_col, col_in_tile = divmod(col - INFO_COUNT, TILE_COLS)
        key = (generation, tile_row, tile_col)
        block = self._tiles.get(key)
        if block is None:
            return self._unavailable(key, role)
        value = float(block[row_in_tile, col_in_tile])
        if math.isnan(value):
            return "" if role == _DISPLAY else None
        return f"{value:.2f}" if role == _DISPLAY else value
