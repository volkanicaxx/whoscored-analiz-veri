"""Komut satırı girişi: python -m oddslake.ui --lake DIR [--memory-mb N].

Lake klasöründe catalog.duckdb yoksa açıklayıcı bir hata yazılır ve çıkış kodu 2 olur.
Lake doğrulaması QApplication oluşturulmadan yapılır.
"""

from __future__ import annotations

import argparse
import sys
from collections.abc import Sequence
from pathlib import Path

from PySide6.QtWidgets import QApplication

from ..config import LakePaths, MemoryBudget
from ..lake import LakeSession
from .app import MainWindow


def build_parser() -> argparse.ArgumentParser:
    budget = MemoryBudget()
    parser = argparse.ArgumentParser(
        prog="python -m oddslake.ui",
        description="oddslake masaüstü arayüzü: oran arşivini sanal ızgarada gösterir.",
    )
    parser.add_argument(
        "--lake",
        required=True,
        type=Path,
        metavar="DIR",
        help="lake kök klasörü (içinde catalog.duckdb bulunmalı)",
    )
    parser.add_argument(
        "--memory-mb",
        type=int,
        default=budget.ui_duckdb_mb,
        metavar="MB",
        help=f"arayüz DuckDB bellek sınırı (varsayılan {budget.ui_duckdb_mb} MB)",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    paths = LakePaths(args.lake)
    if not paths.catalog_db.is_file():
        parser.error(f"lake bulunamadı: {paths.catalog_db} yok (önce lake'i init ya da ingest ile oluşturun)")
    if args.memory_mb <= 0:
        parser.error("--memory-mb pozitif bir tam sayı olmalı")

    budget = MemoryBudget(ui_duckdb_mb=args.memory_mb)
    app = QApplication.instance()
    if app is None:
        app = QApplication([sys.argv[0]])
    app.setApplicationName("oddslake")
    session = LakeSession(paths, memory_mb=args.memory_mb)
    try:
        window = MainWindow(session, budget)
        window.show()
        return app.exec()
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
