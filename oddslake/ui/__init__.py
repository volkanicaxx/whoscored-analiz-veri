"""Masaüstü arayüzü: sanal ızgara modeli, ağır iş çalıştırıcısı ve ana pencere.

Qt çekirdeği (grid_model, jobs) yalnızca QtCore kullanır ve burada dışa aktarılır.
Ana pencere oddslake.ui.app modülündedir ve PySide6.QtWidgets gerektirir; başlatmak için
`python -m oddslake.ui --lake DIR` kullanılır.
"""

from .grid_model import INFO_HEADERS, TILE_COLS, TILE_ROWS, GridModel, TileCache
from .jobs import HeavyJobRunner

__all__ = [
    "GridModel",
    "HeavyJobRunner",
    "INFO_HEADERS",
    "TILE_COLS",
    "TILE_ROWS",
    "TileCache",
]
