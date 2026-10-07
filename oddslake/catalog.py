"""catalog.duckdb oluşturma, boyut tablolarını doldurma ve kolon sözlüğü.

Mantıksal kolon = (büro, sonuç, faz) üçlüsü. Fiziksel olarak hiçbir yerde
100.000 kolonlu bir tablo yoktur; oranlar uzun formatta (match_id, col_id, odd)
saklanır ve ekrandaki geniş görünüm sadece görünen pencere için kurulur.

col_id = outcome_id * 4096 + phase_id * 512 + bookmaker_id
  * bookmaker_id < 512, phase_id < 8, outcome_id < 524288  => col_id < 2^31 (INTEGER)
  * Aynı sonuç + faz için bütün bürolar ardışık col_id alır; Parquet dosyaları
    col_id'ye göre sıralı yazıldığından ekranda yan yana duran 164 büro kolonu
    diskte de aynı satır grubunda (row group) durur.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Iterable, Sequence

import duckdb

from .config import LakePaths

SCHEMA_SQL = (Path(__file__).with_name("schema.sql")).read_text(encoding="utf-8")

COL_ID_OUTCOME_MUL = 4096
COL_ID_PHASE_MUL = 512

PHASES: tuple[tuple[int, str], ...] = ((0, "Acilis"), (1, "Kapanis"))


@dataclass(frozen=True)
class MarketDef:
    code: str
    period: str
    selections: tuple[str, ...]
    lines: tuple[Decimal, ...] | None = None


def _steps(start: str, stop: str, step: str) -> tuple[Decimal, ...]:
    a, b, s = Decimal(start), Decimal(stop), Decimal(step)
    out = []
    while a <= b:
        out.append(a)
        a += s
    return tuple(out)


_CS_FT = tuple(f"CS{h}-{a}" for h in range(5) for a in range(5))
_CS_HT = tuple(f"CS{h}-{a}" for h in range(3) for a in range(3))
_HTFT = tuple(f"HTFT_{h}/{f}" for h in ("1", "X", "2") for f in ("1", "X", "2"))

# Varsayılan market seti (ekran sırası). 307 sonuç x 164 büro x 2 faz = 100.696 kolon.
DEFAULT_MARKETS: tuple[MarketDef, ...] = (
    MarketDef("1X2", "FT", ("1", "X", "2")),
    MarketDef("DC", "FT", ("1X", "12", "X2")),
    MarketDef("DNB", "FT", ("DNB1", "DNB2")),
    MarketDef("BTTS", "FT", ("BTTS_Y", "BTTS_N")),
    MarketDef("OU", "FT", ("O", "U"), _steps("0.5", "6.5", "0.25")),
    MarketDef("AH", "FT", ("AH1", "AH2"), _steps("-3", "3", "0.25")),
    MarketDef("CS", "FT", _CS_FT),
    MarketDef("HTFT", "FT", _HTFT),
    MarketDef("OE", "FT", ("ODD", "EVEN")),
    MarketDef("TTH", "FT", ("HOME_O", "HOME_U"), _steps("0.5", "3.5", "0.5")),
    MarketDef("TTA", "FT", ("AWAY_O", "AWAY_U"), _steps("0.5", "3.5", "0.5")),
    MarketDef("1X2", "HT", ("1", "X", "2")),
    MarketDef("DC", "HT", ("1X", "12", "X2")),
    MarketDef("DNB", "HT", ("DNB1", "DNB2")),
    MarketDef("BTTS", "HT", ("BTTS_Y", "BTTS_N")),
    MarketDef("OU", "HT", ("O", "U"), _steps("0.5", "3.5", "0.25")),
    MarketDef("AH", "HT", ("AH1", "AH2"), _steps("-1.5", "1.5", "0.25")),
    MarketDef("CS", "HT", _CS_HT),
    MarketDef("1X2", "2H", ("1", "X", "2")),
    MarketDef("DC", "2H", ("1X", "12", "X2")),
    MarketDef("DNB", "2H", ("DNB1", "DNB2")),
    MarketDef("BTTS", "2H", ("BTTS_Y", "BTTS_N")),
    MarketDef("OU", "2H", ("O", "U"), _steps("0.5", "3.5", "0.25")),
    MarketDef("AH", "2H", ("AH1", "AH2"), _steps("-1.5", "1.5", "0.25")),
)

# Sentetik/demonstrasyon büro listesi. Gerçek liste `load_bookmakers_csv` ile verilir.
_NAMED_BOOKMAKERS = (
    "bet365", "bwin", "pinnacle", "williamhill", "unibet",
    "betfair", "1xbet", "marathonbet", "betway", "interwetten",
)
DEFAULT_BOOKMAKERS: tuple[str, ...] = _NAMED_BOOKMAKERS + tuple(
    f"bk{i:03d}" for i in range(len(_NAMED_BOOKMAKERS) + 1, 165)
)


def format_line(line: Decimal | float | None) -> str | None:
    """2.50 -> '2.5', -1.00 -> '-1', 0.25 -> '0.25', 0 -> '0'."""
    if line is None:
        return None
    d = Decimal(str(line)).normalize()
    if d == 0:
        return "0"
    s = format(d, "f")
    return s


def outcome_key(period: str, line: Decimal | float | None, selection: str) -> str:
    fl = format_line(line)
    return f"{period}_{selection}" if fl is None else f"{period}_{fl}_{selection}"


def col_id_for(outcome_id: int, phase_id: int, bookmaker_id: int) -> int:
    return outcome_id * COL_ID_OUTCOME_MUL + phase_id * COL_ID_PHASE_MUL + bookmaker_id


def create_catalog(paths: LakePaths) -> duckdb.DuckDBPyConnection:
    """catalog.duckdb'yi (yoksa) oluşturur, şemayı uygular ve yazılabilir bağlantı döner."""
    paths.ensure()
    con = duckdb.connect(str(paths.catalog_db))
    con.execute(SCHEMA_SQL)
    return con


def upsert_phases(con: duckdb.DuckDBPyConnection, phases: Sequence[tuple[int, str]] = PHASES) -> None:
    for i, (phase_id, code) in enumerate(phases):
        con.execute(
            "INSERT INTO phases VALUES (?, ?, ?) ON CONFLICT (phase_id) DO UPDATE SET code = excluded.code",
            [phase_id, code, i],
        )


def upsert_bookmakers(con: duckdb.DuckDBPyConnection, codes_names: Iterable[tuple[str, str]]) -> None:
    """Var olan kodun id'si korunur; yeni kod bir sonraki boş id'yi alır (0..511)."""
    existing = dict(con.execute("SELECT code, bookmaker_id FROM bookmakers").fetchall())
    next_id = (max(existing.values()) + 1) if existing else 0
    next_sort = con.execute("SELECT coalesce(max(sort_order) + 1, 0) FROM bookmakers").fetchone()[0]
    for code, name in codes_names:
        if code in existing:
            con.execute("UPDATE bookmakers SET name = ? WHERE code = ?", [name, code])
            continue
        if next_id > 511:
            raise ValueError("bookmaker_id 511'i aşıyor; col_id kodlaması 512 büroya kadar destekler")
        con.execute("INSERT INTO bookmakers VALUES (?, ?, ?, ?)", [next_id, code, name, next_sort])
        existing[code] = next_id
        next_id += 1
        next_sort += 1


def load_bookmakers_csv(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    """CSV kolonları: code,name (satır sırası = ekran sırası)."""
    with open(path, newline="", encoding="utf-8") as fh:
        rows = [(r["code"].strip(), (r.get("name") or r["code"]).strip()) for r in csv.DictReader(fh)]
    upsert_bookmakers(con, rows)


def upsert_markets(con: duckdb.DuckDBPyConnection, markets: Sequence[MarketDef] = DEFAULT_MARKETS) -> None:
    """Market ve sonuçları ekler. Var olan outcome_key'in id'si değişmez."""
    m_existing = {(c, p): mid for mid, c, p in con.execute("SELECT market_id, code, period FROM markets").fetchall()}
    o_existing = dict(con.execute("SELECT outcome_key, outcome_id FROM outcomes").fetchall())
    next_mid = (max(m_existing.values()) + 1) if m_existing else 0
    next_oid = (max(o_existing.values()) + 1) if o_existing else 0
    m_sort = con.execute("SELECT coalesce(max(sort_order) + 1, 0) FROM markets").fetchone()[0]
    for md in markets:
        mid = m_existing.get((md.code, md.period))
        if mid is None:
            mid = next_mid
            next_mid += 1
            con.execute("INSERT INTO markets VALUES (?, ?, ?, ?)", [mid, md.code, md.period, m_sort])
            m_existing[(md.code, md.period)] = mid
            m_sort += 1
        o_sort = con.execute("SELECT coalesce(max(sort_order) + 1, 0) FROM outcomes WHERE market_id = ?", [mid]).fetchone()[0]
        for line in md.lines if md.lines is not None else (None,):
            for sel in md.selections:
                key = outcome_key(md.period, line, sel)
                if key in o_existing:
                    continue
                if next_oid > 524287:
                    raise ValueError("outcome_id 524287'yi aşıyor")
                con.execute(
                    "INSERT INTO outcomes VALUES (?, ?, ?, ?, ?, ?)",
                    [next_oid, mid, line, sel, key, o_sort],
                )
                o_existing[key] = next_oid
                next_oid += 1
                o_sort += 1


def rebuild_column_catalog(con: duckdb.DuckDBPyConnection) -> int:
    """Eksik (büro, sonuç, faz) kombinasyonlarını ekler, ekran sırasını yeniden hesaplar.

    Mevcut col_id'ler asla değişmez (deterministik formül), bu yüzden Parquet
    dosyalarının yeniden yazılması gerekmez. Toplam kolon sayısını döner.
    """
    con.execute(
        f"""
        INSERT INTO column_catalog
        SELECT o.outcome_id * {COL_ID_OUTCOME_MUL} + p.phase_id * {COL_ID_PHASE_MUL} + b.bookmaker_id,
               b.bookmaker_id, o.outcome_id, p.phase_id,
               b.code || '_' || o.outcome_key || '_' || p.code,
               0
        FROM bookmakers b CROSS JOIN outcomes o CROSS JOIN phases p
        WHERE NOT EXISTS (
            SELECT 1 FROM column_catalog c
            WHERE c.col_id = o.outcome_id * {COL_ID_OUTCOME_MUL} + p.phase_id * {COL_ID_PHASE_MUL} + b.bookmaker_id
        )
        """
    )
    con.execute(
        """
        UPDATE column_catalog SET display_order = r.rn
        FROM (
            SELECT c.col_id,
                   (row_number() OVER (ORDER BY m.sort_order, o.sort_order, p.sort_order, b.sort_order, c.col_id) - 1)::INTEGER AS rn
            FROM column_catalog c
            JOIN bookmakers b USING (bookmaker_id)
            JOIN outcomes   o USING (outcome_id)
            JOIN markets    m ON m.market_id = o.market_id
            JOIN phases     p USING (phase_id)
        ) r
        WHERE column_catalog.col_id = r.col_id
        """
    )
    return con.execute("SELECT count(*) FROM column_catalog").fetchone()[0]


def init_lake(
    paths: LakePaths,
    bookmakers_csv: Path | None = None,
    markets: Sequence[MarketDef] = DEFAULT_MARKETS,
) -> int:
    """Lake klasörünü ve kataloğu hazırlar. Kolon sayısını döner."""
    con = create_catalog(paths)
    try:
        con.execute("BEGIN")
        upsert_phases(con)
        if bookmakers_csv is not None:
            load_bookmakers_csv(con, bookmakers_csv)
        else:
            upsert_bookmakers(con, ((c, c) for c in DEFAULT_BOOKMAKERS))
        upsert_markets(con, markets)
        n = rebuild_column_catalog(con)
        con.execute("COMMIT")
        return n
    except Exception:
        con.execute("ROLLBACK")
        raise
    finally:
        con.close()
