"""CSV oran gözlemlerini DuckDB + Parquet lake'ine aktarır (ingest).

Katalog (`catalog.duckdb`) ve kolon kodlaması `catalog.py` ile tanımlıdır; bu modül onu
doldurur. Kaynak verinin biçimi projede önceden tanımlı değildi. Aşağıdaki CSV sözleşmesi
burada tanımlanır; kaynak veri bu sözleşmeye dönüştürülerek verilmelidir.

Giriş sözleşmesi
----------------
* UTF-8 (BOM kabul edilir), ayraç `,`, alan kılıfı `"`. İlk satır zorunlu başlıktır ve
  kolonlar tam olarak şu sırayla bulunmalıdır:
  season, league_id, league_name, league_country, kickoff_utc, match_id, home_team_id,
  home_team, away_team_id, away_team, ft_home, ft_away, ht_home, ht_away, bookmaker_code,
  market_code, period, line, selection, phase, odd
* Bir satır = bir oran gözlemi. Aynı dosyada birden çok (sezon, lig) bulunabilir; sezon ve
  lig her satırdan okunur.
* ft_* / ht_*: boş -> NULL (oynanmadı veya bilinmiyor); negatif olmayan tam sayı -> değer
  (0 geçerlidir). Bir çiftin (ev / deplasman) yalnızca biri boşsa satır reddedilir.
* line: boş -> NULL (1X2 gibi çizgisiz marketler); aksi halde ondalık metin ("2.5",
  "-0.25", "3"). Biçim `catalog.format_line` ile birebir aynıdır.
* outcome_key = period + "_" + format_line(line) + "_" + selection; line NULL ise
  period + "_" + selection. Anahtar `outcomes` tablosunda bulunmalı ve market_code / period
  ile tutarlı olmalıdır.
* phase: Acilis veya Kapanis. bookmaker_code `bookmakers` tablosunda bulunmalıdır.
* odd: sonlu ve > 1.0 olmalıdır. Boş, sayı olmayan, NaN, sonsuz ve <= 1.0 değerler reddedilir.
* kickoff_utc: UTC zamanı. Ofset yazılmışsa yalnızca sıfır ofset ('Z', '+00', '+00:00') kabul
  edilir; '18:00+03' gibi değerler bad_match ile reddedilir.
* kaynak_satir_no: veri satırının 1'den başlayan sıra numarası (başlık hariç).
* Başlık sözleşmeye uymuyorsa, satırların kolon sayısı tutmuyorsa ya da dosya
  çözümlenemiyorsa DOSYANIN TAMAMI atlanır; hata `files_failed` altında metniyle raporlanır.
  Diğer dosyalar yine işlenir.

Satır red sebepleri (`rejects_by_reason` anahtarları)
-----------------------------------------------------
bad_match         : sezon/lig/maç/takım kimliği, başlangıç zamanı veya takım/lig adı geçersiz
bad_score         : skor alanı tam sayı değil, negatif, ya da bir çiftin yalnız biri dolu
bad_line          : line boş değil ama ondalık sayı değil
bad_odd           : odd boş, sayı değil, sonlu değil veya <= 1.0
unknown_bookmaker : bookmaker_code `bookmakers` tablosunda yok
bad_phase         : phase Acilis / Kapanis değil
unknown_outcome   : outcome_key `outcomes` tablosunda yok (market_code / period uyumsuz dahil)
match_conflict    : aynı match_id için, ilk görülen maçtan farklı sezon, lig, başlangıç
                    zamanı, takım kimliği veya skor içeren satır
duplicate         : aynı (match_id, col_id) çiftinin ilk görülmeyen satırı

Mimari
------
A) Stage: her girdi dosyası ayrı ayrı DuckDB `read_csv` ile okunur (kolonlar VARCHAR, ham
   değer korunur, doğrulama TRY_CAST ile yapılır). Geçerli satırlar
   `staging/season=S/<dosya_adi>.parquet` dosyalarına yazılır; kolonları
   (match_id, league_id, col_id, odd, kaynak_satir_no). Reddedilenler
   `rejects/<dosya_adi>.parquet` dosyasına (kaynak_satir_no, sebep) olarak yazılır.
   Staging yalnızca bu çalıştırmanın girdileri için üretilir ve her çalıştırmada temizlenir.
B) Compaction: her (sezon, lig) için staging okunur. Aynı (match_id, col_id) çiftinin ilk
   satırı tutulur (sıra: dosya adı, sonra kaynak_satir_no); diğerleri `duplicate` olarak
   rejects'e eklenir. Partition'ın mevcut satırları, bu çalıştırmanın (match_id, col_id)
   anahtarlarıyla değiştirilerek birleştirilir. Sonuç `ORDER BY col_id, match_id` ile
   `data.parquet.tmp` dosyasına yazılır. İçerik değiştiyse `os.replace` ile atomik taşınır;
   değişmediyse dosyaya dokunulmaz.
C) Katalog: matches, teams ve leagues upsert edilir (skor alanları girdiyle birebir, NULL
   korunur). lake_partitions'ta etkilenen (sezon, lig) satırları yeniden yazılır. column_stats
   etkilenen col_id'ler için tüm partition'lar üzerinden yeniden sayılır. Adım C tek bir
   transaction'dır.

Maç tutarlılığı: bir match_id için bu çalıştırmada ilk görülen (dosya adı sırası, sonra
kaynak_satir_no) maç bilgisi geçerlidir. Sonraki farklı bilgili satırlar match_conflict ile
reddedilir; böylece bir maçın oranları iki ayrı partition'a dağılmaz. Takım ve lig adları
ilk görülen satırdan alınır.

Birden çok çalıştırma: partition'lar çalıştırmalar arasında birleşir; aynı (sezon, lig)
için dosyaları ayrı ayrı vermek veriyi silmez. Aynı (match_id, col_id) daha sonraki bir
çalıştırmada gelirse yeni değer eskisinin yerine geçer. Önceki bir çalıştırmada gelip
sonrakinde gelmeyen satırlar silinmez. Aynı girdiyle tekrar çalıştırma satır sayılarını ve
partition dosyasının içeriğini değiştirmez.

Sınırlar ve eşzamanlılık
------------------------
* Ingest sırasında aynı süreçte salt-okunur `LakeSession` açık olmamalıdır; katalog bu
  süreçte yazılabilir olarak açılır. Aynı lake üzerinde iki ingest'i eşzamanlı çalıştırmayın.
* Bir maç başka bir (sezon, lig) partition'ına taşınırsa eski partition'daki satırlar
  otomatik silinmez.
* Bellek: her DuckDB bağlantısı `memory_mb` ile sınırlanır; geçici veri `paths.temp_dir`
  altına taşar. Girdinin tamamı Python'a alınmaz.

CLI
---
    python -m oddslake.ingest --lake DIR --input YOL [YOL ...] [--bookmakers CSV] [--memory-mb 1536]

Katalog yoksa (ya da --bookmakers verilmişse) önce `init_lake` çalıştırılır. Herhangi bir
dosya başarısız olursa çıkış kodu 1 olur.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
from collections import Counter
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

import duckdb

from .catalog import create_catalog, init_lake
from .config import LakePaths, default_threads

REQUIRED_COLUMNS: tuple[str, ...] = (
    "season", "league_id", "league_name", "league_country", "kickoff_utc", "match_id",
    "home_team_id", "home_team", "away_team_id", "away_team", "ft_home", "ft_away",
    "ht_home", "ht_away", "bookmaker_code", "market_code", "period", "line", "selection",
    "phase", "odd",
)

ROW_GROUP_SIZE = 122880

REASON_BAD_MATCH = "bad_match"
REASON_BAD_SCORE = "bad_score"
REASON_BAD_LINE = "bad_line"
REASON_BAD_ODD = "bad_odd"
REASON_UNKNOWN_BOOKMAKER = "unknown_bookmaker"
REASON_BAD_PHASE = "bad_phase"
REASON_UNKNOWN_OUTCOME = "unknown_outcome"
REASON_MATCH_CONFLICT = "match_conflict"
REASON_DUPLICATE = "duplicate"


@dataclass
class IngestReport:
    """Bir ingest çalıştırmasının özeti.

    Sayılar yalnızca başarıyla işlenen dosyalara aittir. Her işlenen satır ya yazılır
    (rows_written) ya da reddedilir (rows_rejected); rows_in = rows_written + rows_rejected.
    """

    files_ok: int = 0
    rows_in: int = 0
    rows_written: int = 0
    rows_rejected: int = 0
    partitions_written: list[tuple[int, int]] = field(default_factory=list)
    rejects_by_reason: dict[str, int] = field(default_factory=dict)
    files_failed: dict[str, str] = field(default_factory=dict)


@dataclass
class _StageResult:
    rows_in: int
    rows_kept: int
    reasons: dict[str, int]
    pairs: set[tuple[int, int]]


@dataclass
class _PartitionOut:
    season: int
    league_id: int
    target: Path
    tmp: Path
    row_count: int
    min_col_id: int | None
    max_col_id: int | None
    changed: bool


# ---------------------------------------------------------------------------
# SQL yardımcıları
# ---------------------------------------------------------------------------

def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def sql_outcome_key(period_sql: str, line_sql: str, selection_sql: str) -> str:
    """`catalog.outcome_key` ile birebir aynı sonucu veren SQL ifadesi.

    Argümanlar SQL ifadeleridir (kolon adı veya literal). `line_sql` ham çizgi metnidir;
    NULL ya da boşsa çizgisiz anahtar üretilir. Biçim: "2.50" -> "2.5", "-0.25" -> "-0.25",
    "3.0" -> "3", "0" ve "-0" -> "0".
    """
    line_clean = f"NULLIF(trim({line_sql}), '')"
    dec = f"TRY_CAST({line_clean} AS DECIMAL(18, 6))"
    fmt = f"CASE WHEN ({dec}) = 0 THEN '0' ELSE rtrim(rtrim(CAST({dec} AS VARCHAR), '0'), '.') END"
    return (
        f"CASE WHEN {line_clean} IS NULL THEN {period_sql} || '_' || {selection_sql} "
        f"ELSE {period_sql} || '_' || ({fmt}) || '_' || {selection_sql} END"
    )


# kickoff_utc: DuckDB TRY_CAST sıfır olmayan ofseti sessizce atar (18:00+03 -> 18:00).
# Bu yüzden ofsetli değerler yalnızca sıfır ofsetse ('Z', '+00', '+00:00') kabul edilir.
_KICKOFF_ANY_OFFSET = r"[0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]+)?)?(Z|[+-][0-9]{2}(:?[0-9]{2})?)$"
_KICKOFF_ZERO_OFFSET = r"[0-9]{2}:[0-9]{2}(:[0-9]{2}(\.[0-9]+)?)?(Z|[+-]00(:?00)?)$"


def _int_sql(column: str, sql_type: str) -> str:
    """Yalnızca ondalıksız, işaretsiz tam sayı metnini kabul eder; diğerleri NULL olur."""
    return f"CASE WHEN regexp_full_match({column}, '[0-9]+') THEN TRY_CAST({column} AS {sql_type}) END"


def _csv_source(path: Path) -> str:
    """Bütün kolonları VARCHAR okuyan, ayrıcı ve kolon sayısı sabitlenmiş read_csv ifadesi.

    auto_detect kapalıdır: DuckDB'nin ayraç sezgisi tek bozuk satırda dosyayı yanlış
    ayrıştırabilir. Kolon sayısı tutmayan satır hata verir ve dosya atlanır.
    """
    cols = ", ".join(f"{_sql_str(c)}: 'VARCHAR'" for c in REQUIRED_COLUMNS)
    return (
        f"read_csv({_sql_str(path.as_posix())}, auto_detect = false, header = true, "
        f"delim = ',', quote = '\"', escape = '\"', columns = {{{cols}}})"
    )


def _raw_sql(source: str) -> str:
    return f"""
CREATE OR REPLACE TEMP TABLE f_raw AS
SELECT row_number() OVER () AS src_row,
       NULLIF(trim(season), '')          AS season_t,
       NULLIF(trim(league_id), '')       AS league_t,
       NULLIF(trim(league_name), '')     AS league_name,
       NULLIF(trim(league_country), '')  AS league_country,
       NULLIF(trim(kickoff_utc), '')     AS kickoff_t,
       NULLIF(trim(match_id), '')        AS match_t,
       NULLIF(trim(home_team_id), '')    AS home_id_t,
       NULLIF(trim(home_team), '')       AS home_name,
       NULLIF(trim(away_team_id), '')    AS away_id_t,
       NULLIF(trim(away_team), '')       AS away_name,
       NULLIF(trim(ft_home), '')         AS ft_home_t,
       NULLIF(trim(ft_away), '')         AS ft_away_t,
       NULLIF(trim(ht_home), '')         AS ht_home_t,
       NULLIF(trim(ht_away), '')         AS ht_away_t,
       NULLIF(trim(bookmaker_code), '')  AS bk_t,
       NULLIF(trim(market_code), '')     AS mc_t,
       NULLIF(trim(period), '')          AS period_t,
       NULLIF(trim(line), '')            AS line_t,
       NULLIF(trim(selection), '')       AS sel_t,
       NULLIF(trim(phase), '')           AS phase_t,
       NULLIF(trim(odd), '')             AS odd_t
FROM {source}
"""


def _parse_sql() -> str:
    return f"""
CREATE OR REPLACE TEMP TABLE f_parse AS
SELECT p.*,
       coalesce(p.season IS NOT NULL AND p.league_id IS NOT NULL AND p.match_id IS NOT NULL
                AND p.home_id IS NOT NULL AND p.away_id IS NOT NULL AND p.kickoff IS NOT NULL
                AND p.league_name IS NOT NULL AND p.home_name IS NOT NULL
                AND p.away_name IS NOT NULL, false) AS match_ok,
       coalesce(((p.ft_home_t IS NULL AND p.ft_away_t IS NULL)
                 OR (p.ft_home IS NOT NULL AND p.ft_away IS NOT NULL))
                AND ((p.ht_home_t IS NULL AND p.ht_away_t IS NULL)
                     OR (p.ht_home IS NOT NULL AND p.ht_away IS NOT NULL)), false) AS score_ok,
       coalesce(p.line_t IS NULL OR p.line_dec IS NOT NULL, false) AS line_ok,
       coalesce(p.odd_f IS NOT NULL AND isfinite(p.odd_f) AND p.odd_f > 1.0::FLOAT, false) AS odd_ok
FROM (
    SELECT src_row, league_name, league_country, home_name, away_name,
           ft_home_t, ft_away_t, ht_home_t, ht_away_t, bk_t, mc_t, period_t, line_t, sel_t,
           phase_t,
           {_int_sql('season_t', 'SMALLINT')} AS season,
           {_int_sql('league_t', 'INTEGER')} AS league_id,
           {_int_sql('match_t', 'INTEGER')} AS match_id,
           {_int_sql('home_id_t', 'INTEGER')} AS home_id,
           {_int_sql('away_id_t', 'INTEGER')} AS away_id,
           CASE WHEN regexp_matches(kickoff_t, '{_KICKOFF_ANY_OFFSET}') AND NOT regexp_matches(kickoff_t, '{_KICKOFF_ZERO_OFFSET}')
                THEN NULL ELSE TRY_CAST(kickoff_t AS TIMESTAMP) END AS kickoff,
           {_int_sql('ft_home_t', 'SMALLINT')} AS ft_home,
           {_int_sql('ft_away_t', 'SMALLINT')} AS ft_away,
           {_int_sql('ht_home_t', 'SMALLINT')} AS ht_home,
           {_int_sql('ht_away_t', 'SMALLINT')} AS ht_away,
           TRY_CAST(line_t AS DECIMAL(18, 6)) AS line_dec,
           TRY_CAST(odd_t AS FLOAT) AS odd_f
    FROM f_raw
) p
"""


def _all_sql() -> str:
    key = sql_outcome_key("f.period_t", "f.line_t", "f.sel_t")
    col_id = (
        "CASE WHEN rb.bookmaker_id IS NULL OR rp.phase_id IS NULL OR ro.outcome_id IS NULL THEN NULL "
        "ELSE CAST(CAST(ro.outcome_id AS BIGINT) * 4096 + CAST(rp.phase_id AS BIGINT) * 512 "
        "+ CAST(rb.bookmaker_id AS BIGINT) AS INTEGER) END"
    )
    meta_key = (
        "CAST(f.season AS VARCHAR) || '|' || CAST(f.league_id AS VARCHAR) || '|' "
        "|| CAST(f.kickoff AS VARCHAR) || '|' || CAST(f.home_id AS VARCHAR) || '|' "
        "|| CAST(f.away_id AS VARCHAR) || '|' "
        "|| CASE WHEN f.ft_home IS NULL THEN 'N' ELSE CAST(f.ft_home AS VARCHAR) END || '|' "
        "|| CASE WHEN f.ft_away IS NULL THEN 'N' ELSE CAST(f.ft_away AS VARCHAR) END || '|' "
        "|| CASE WHEN f.ht_home IS NULL THEN 'N' ELSE CAST(f.ht_home AS VARCHAR) END || '|' "
        "|| CASE WHEN f.ht_away IS NULL THEN 'N' ELSE CAST(f.ht_away AS VARCHAR) END"
    )
    return f"""
CREATE OR REPLACE TEMP TABLE f_all AS
SELECT f.src_row, f.season, f.league_id, f.match_id, f.home_id, f.away_id, f.kickoff,
       f.league_name, f.league_country, f.home_name, f.away_name,
       f.ft_home, f.ft_away, f.ht_home, f.ht_away,
       {col_id} AS col_id,
       f.odd_f,
       {meta_key} AS meta_key,
       CASE
           WHEN NOT f.match_ok THEN '{REASON_BAD_MATCH}'
           WHEN NOT f.score_ok THEN '{REASON_BAD_SCORE}'
           WHEN NOT f.line_ok THEN '{REASON_BAD_LINE}'
           WHEN NOT f.odd_ok THEN '{REASON_BAD_ODD}'
           WHEN rb.bookmaker_id IS NULL THEN '{REASON_UNKNOWN_BOOKMAKER}'
           WHEN rp.phase_id IS NULL THEN '{REASON_BAD_PHASE}'
           WHEN ro.outcome_id IS NULL THEN '{REASON_UNKNOWN_OUTCOME}'
           ELSE NULL
       END AS reason0
FROM f_parse f
LEFT JOIN ref_bookmakers rb ON rb.code = f.bk_t
LEFT JOIN ref_phases rp ON rp.code = f.phase_t
LEFT JOIN ref_outcomes ro
       ON ro.outcome_key = {key} AND ro.market_code = f.mc_t AND ro.period = f.period_t
"""


def _res_sql(seq: int) -> list[str]:
    """Geçerli satırlar için maç kazananını belirler; çelişen satırlara match_conflict verir."""
    win = """
CREATE OR REPLACE TEMP TABLE f_win AS
SELECT * FROM f_all WHERE reason0 IS NULL
QUALIFY row_number() OVER (PARTITION BY match_id ORDER BY src_row) = 1
"""
    res = """
CREATE OR REPLACE TEMP TABLE f_res AS
SELECT a.src_row, a.season, a.league_id, a.match_id, a.col_id, a.odd_f,
       CASE
           WHEN a.reason0 IS NOT NULL THEN a.reason0
           WHEN a.meta_key = coalesce(r.meta_key, w.meta_key) THEN NULL
           ELSE '""" + REASON_MATCH_CONFLICT + """'
       END AS reason
FROM f_all a
LEFT JOIN f_win w ON w.match_id = a.match_id
LEFT JOIN run_matches r ON r.match_id = a.match_id
"""
    insert = f"""
INSERT INTO run_matches (match_id, season, league_id, kickoff, home_id, away_id,
                         ft_home, ft_away, ht_home, ht_away, league_name, league_country,
                         home_name, away_name, meta_key, file_seq, src_row)
SELECT w.match_id, w.season, w.league_id, w.kickoff, w.home_id, w.away_id,
       w.ft_home, w.ft_away, w.ht_home, w.ht_away, w.league_name, w.league_country,
       w.home_name, w.away_name, w.meta_key, {int(seq)}, w.src_row
FROM f_win w
WHERE NOT EXISTS (SELECT 1 FROM run_matches r WHERE r.match_id = w.match_id)
"""
    return [win, res, insert]


# ---------------------------------------------------------------------------
# Bağlantılar ve başlangıç
# ---------------------------------------------------------------------------

def _open_work(paths: LakePaths, memory_mb: int, threads: int) -> duckdb.DuckDBPyConnection:
    """Bellek-içi çalışma bağlantısı: staging ve compaction burada yürür (memory_limit sınırlı)."""
    con = duckdb.connect(
        ":memory:",
        config={
            "memory_limit": f"{int(memory_mb)}MB",
            "threads": int(threads),
            "temp_directory": str(paths.temp_dir),
        },
    )
    con.execute(
        "CREATE TEMP TABLE run_matches (match_id INTEGER, season SMALLINT, league_id INTEGER, "
        "kickoff TIMESTAMP, home_id INTEGER, away_id INTEGER, ft_home SMALLINT, ft_away SMALLINT, "
        "ht_home SMALLINT, ht_away SMALLINT, league_name VARCHAR, league_country VARCHAR, "
        "home_name VARCHAR, away_name VARCHAR, meta_key VARCHAR, file_seq INTEGER, src_row BIGINT)"
    )
    con.execute("CREATE TEMP TABLE dup_rows (filename VARCHAR, kaynak_satir_no BIGINT)")
    con.execute("CREATE TEMP TABLE affected (col_id INTEGER)")
    return con


def _load_refs(cat: duckdb.DuckDBPyConnection, work: duckdb.DuckDBPyConnection) -> None:
    """Küçük başvuru tablolarını (büro, faz, sonuç) katalogdan çalışma bağlantısına kopyalar."""
    bookmakers = cat.execute("SELECT code, bookmaker_id FROM bookmakers").fetchall()
    phases = cat.execute("SELECT code, phase_id FROM phases").fetchall()
    outcomes = cat.execute(
        "SELECT o.outcome_key, o.outcome_id, m.code, m.period "
        "FROM outcomes o JOIN markets m ON m.market_id = o.market_id"
    ).fetchall()
    work.execute("CREATE TEMP TABLE ref_bookmakers (code VARCHAR, bookmaker_id INTEGER)")
    work.execute("CREATE TEMP TABLE ref_phases (code VARCHAR, phase_id INTEGER)")
    work.execute(
        "CREATE TEMP TABLE ref_outcomes (outcome_key VARCHAR, outcome_id INTEGER, "
        "market_code VARCHAR, period VARCHAR)"
    )
    if bookmakers:
        work.executemany("INSERT INTO ref_bookmakers VALUES (?, ?)", bookmakers)
    if phases:
        work.executemany("INSERT INTO ref_phases VALUES (?, ?)", phases)
    if outcomes:
        work.executemany("INSERT INTO ref_outcomes VALUES (?, ?, ?, ?)", outcomes)


def _normalize_inputs(input_files: Sequence[Path | str]) -> list[Path]:
    """Girdileri tekilleştirir ve deterministik sıraya koyar (dosya adı + '.parquet')."""
    unique: dict[str, Path] = {}
    for item in input_files:
        path = Path(item)
        unique.setdefault(str(path.resolve()), path)
    ordered = sorted(unique.values(), key=lambda p: f"{p.stem}.parquet")
    counts = Counter(p.stem for p in ordered)
    clashes = sorted(name for name, n in counts.items() if n > 1)
    if clashes:
        raise ValueError(f"aynı dosya adına sahip birden çok girdi var: {clashes}")
    return ordered


def _check_header(path: Path) -> None:
    with open(path, newline="", encoding="utf-8-sig") as fh:
        header = next(csv.reader(fh), None)
    if header is None:
        raise ValueError("dosya boş: başlık satırı yok")
    names = [h.strip() for h in header]
    if names != list(REQUIRED_COLUMNS):
        missing = [c for c in REQUIRED_COLUMNS if c not in names]
        extra = [c for c in names if c not in REQUIRED_COLUMNS]
        detail = f"eksik={missing}, fazla={extra}" if (missing or extra) else "kolon sırası farklı"
        raise ValueError(f"başlık sözleşmeye uymuyor ({detail})")


# ---------------------------------------------------------------------------
# A) Stage
# ---------------------------------------------------------------------------

def _stage_file(work: duckdb.DuckDBPyConnection, paths: LakePaths, src: Path, seq: int) -> _StageResult:
    """Tek bir girdi dosyasını doğrular ve staging / rejects dosyalarını yazar.

    Hata olursa bu dosyanın yazdığı çıktılar silinir ve istisna yukarı iletilir; böylece
    yarım kalmış bir dosya ne staging'e ne de run_matches'e sızar.
    """
    _check_header(src)
    stem = src.stem
    created: list[Path] = []
    try:
        work.execute(_raw_sql(_csv_source(src)))
        work.execute(_parse_sql())
        work.execute(_all_sql())
        win_sql, res_sql, insert_sql = _res_sql(seq)
        work.execute(win_sql)
        work.execute(res_sql)

        rows_in, rows_kept = work.execute(
            "SELECT count(*), count(*) FILTER (WHERE reason IS NULL) FROM f_res"
        ).fetchone()
        reasons = {
            reason: int(n)
            for reason, n in work.execute(
                "SELECT reason, count(*) FROM f_res WHERE reason IS NOT NULL GROUP BY reason"
            ).fetchall()
        }

        rej_path = paths.rejects_dir / f"{stem}.parquet"
        paths.rejects_dir.mkdir(parents=True, exist_ok=True)
        created.append(rej_path)
        work.execute(
            f"COPY (SELECT src_row AS kaynak_satir_no, reason AS sebep FROM f_res "
            f"WHERE reason IS NOT NULL ORDER BY src_row) TO {_sql_str(rej_path.as_posix())} (FORMAT PARQUET)"
        )

        pairs: set[tuple[int, int]] = set()
        seasons = [int(r[0]) for r in work.execute(
            "SELECT DISTINCT season FROM f_res WHERE reason IS NULL ORDER BY season").fetchall()]
        for season in seasons:
            stg_dir = paths.staging_dir / f"season={season}"
            stg_dir.mkdir(parents=True, exist_ok=True)
            stg_path = stg_dir / f"{stem}.parquet"
            created.append(stg_path)
            work.execute(
                f"COPY (SELECT match_id, league_id, col_id, odd_f AS odd, src_row AS kaynak_satir_no "
                f"FROM f_res WHERE reason IS NULL AND season = {season}) "
                f"TO {_sql_str(stg_path.as_posix())} (FORMAT PARQUET)"
            )
        pairs.update(
            (int(s), int(l)) for s, l in work.execute(
                "SELECT DISTINCT season, league_id FROM f_res WHERE reason IS NULL").fetchall()
        )

        work.execute(insert_sql)
        return _StageResult(rows_in=int(rows_in), rows_kept=int(rows_kept), reasons=reasons, pairs=pairs)
    except BaseException:
        for path in created:
            path.unlink(missing_ok=True)
        raise


# ---------------------------------------------------------------------------
# B) Compaction
# ---------------------------------------------------------------------------

def _same_bytes(a: Path, b: Path) -> bool:
    if a.stat().st_size != b.stat().st_size:
        return False
    with open(a, "rb") as fa, open(b, "rb") as fb:
        while True:
            chunk_a = fa.read(1 << 20)
            chunk_b = fb.read(1 << 20)
            if chunk_a != chunk_b:
                return False
            if not chunk_a:
                return True


def _compact_pair(
    work: duckdb.DuckDBPyConnection, paths: LakePaths, season: int, league_id: int
) -> tuple[_PartitionOut, int, int]:
    """Bir (sezon, lig) partition'ını staging'den ve mevcut dosyadan yeniden üretir.

    Döner: (çıktı, yazılan satır sayısı, duplicate sayısı). Çıktı .tmp dosyasındadır; taşıma
    çağıran tarafından yapılır.
    """
    stg_glob = (paths.staging_dir / f"season={season}" / "*.parquet").as_posix()
    work.execute(
        f"""
        CREATE OR REPLACE TEMP TABLE part AS
        SELECT match_id, col_id, odd, kaynak_satir_no, filename,
               row_number() OVER (PARTITION BY match_id, col_id
                                  ORDER BY filename, kaynak_satir_no) AS rn
        FROM read_parquet({_sql_str(stg_glob)}, filename = true)
        WHERE league_id = {int(league_id)}
        """
    )
    kept, dups = work.execute(
        "SELECT count(*) FILTER (WHERE rn = 1), count(*) FILTER (WHERE rn > 1) FROM part"
    ).fetchone()
    work.execute("INSERT INTO dup_rows SELECT filename, kaynak_satir_no FROM part WHERE rn > 1")
    work.execute("INSERT INTO affected SELECT DISTINCT col_id FROM part WHERE rn = 1")

    target = paths.odds_partition_file(season, league_id)
    tmp = target.with_name(target.name + ".tmp")
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp.unlink(missing_ok=True)

    body = "SELECT match_id, col_id, odd FROM part WHERE rn = 1"
    if target.exists():
        body += f"""
        UNION ALL
        SELECT o.match_id, o.col_id, o.odd
        FROM read_parquet({_sql_str(target.as_posix())}) AS o
        WHERE NOT EXISTS (SELECT 1 FROM part k
                          WHERE k.rn = 1 AND k.match_id = o.match_id AND k.col_id = o.col_id)
        """
    work.execute(
        f"COPY (SELECT * FROM ({body}) AS merged ORDER BY col_id, match_id) "
        f"TO {_sql_str(tmp.as_posix())} (FORMAT PARQUET, ROW_GROUP_SIZE {ROW_GROUP_SIZE})"
    )
    row_count, min_col, max_col = work.execute(
        f"SELECT count(*), min(col_id), max(col_id) FROM read_parquet({_sql_str(tmp.as_posix())})"
    ).fetchone()
    changed = not (target.exists() and _same_bytes(tmp, target))
    out = _PartitionOut(
        season=int(season),
        league_id=int(league_id),
        target=target,
        tmp=tmp,
        row_count=int(row_count),
        min_col_id=None if min_col is None else int(min_col),
        max_col_id=None if max_col is None else int(max_col),
        changed=changed,
    )
    if not changed:
        tmp.unlink(missing_ok=True)
    return out, int(kept), int(dups)


def _rewrite_dup_rejects(work: duckdb.DuckDBPyConnection, paths: LakePaths) -> None:
    """duplicate kayıtlarını ilgili dosyanın rejects dosyasına ekler."""
    files = [r[0] for r in work.execute("SELECT DISTINCT filename FROM dup_rows ORDER BY filename").fetchall()]
    for fname in files:
        rej = paths.rejects_dir / f"{Path(fname).stem}.parquet"
        rej_tmp = rej.with_name(rej.name + ".tmp")
        rej_tmp.unlink(missing_ok=True)
        work.execute(
            f"""
            COPY (SELECT kaynak_satir_no, sebep FROM (
                      SELECT kaynak_satir_no, sebep FROM read_parquet({_sql_str(rej.as_posix())})
                      UNION ALL
                      SELECT kaynak_satir_no, '{REASON_DUPLICATE}' FROM dup_rows
                      WHERE filename = {_sql_str(fname)}
                  ) AS merged ORDER BY kaynak_satir_no)
            TO {_sql_str(rej_tmp.as_posix())} (FORMAT PARQUET)
            """
        )
        os.replace(rej_tmp, rej)


# ---------------------------------------------------------------------------
# C) Katalog
# ---------------------------------------------------------------------------

def _publish_catalog(
    cat: duckdb.DuckDBPyConnection,
    work: duckdb.DuckDBPyConnection,
    paths: LakePaths,
    parts: list[_PartitionOut],
) -> None:
    """matches / teams / leagues / lake_partitions / column_stats güncellemesi; tek transaction."""
    matches_tbl = work.execute(
        "SELECT match_id, season, league_id, kickoff AS kickoff_utc, home_id AS home_team_id, "
        "away_id AS away_team_id, ft_home, ft_away, ht_home, ht_away FROM run_matches"
    ).to_arrow_table()
    teams_tbl = work.execute(
        "SELECT team_id, name FROM ("
        " SELECT home_id AS team_id, home_name AS name, file_seq, src_row FROM run_matches"
        " UNION ALL"
        " SELECT away_id, away_name, file_seq, src_row FROM run_matches"
        ") AS t QUALIFY row_number() OVER (PARTITION BY team_id ORDER BY file_seq, src_row) = 1"
    ).to_arrow_table()
    leagues_tbl = work.execute(
        "SELECT league_id, league_country, league_name FROM run_matches "
        "QUALIFY row_number() OVER (PARTITION BY league_id ORDER BY file_seq, src_row) = 1"
    ).to_arrow_table()
    affected_tbl = work.execute("SELECT DISTINCT col_id FROM affected").to_arrow_table()
    now = datetime.now(timezone.utc).replace(tzinfo=None)

    cat.register("v_matches", matches_tbl)
    cat.register("v_teams", teams_tbl)
    cat.register("v_leagues", leagues_tbl)
    cat.register("v_affected", affected_tbl)
    try:
        cat.execute("BEGIN TRANSACTION")
        try:
            cat.execute("INSERT OR REPLACE INTO leagues (league_id, country, name) "
                        "SELECT league_id, league_country, league_name FROM v_leagues")
            cat.execute("INSERT OR REPLACE INTO teams (team_id, name) SELECT team_id, name FROM v_teams")
            cat.execute(
                "INSERT OR REPLACE INTO matches (match_id, season, league_id, kickoff_utc, home_team_id, "
                "away_team_id, ft_home, ft_away, ht_home, ht_away) "
                "SELECT match_id, season, league_id, kickoff_utc, home_team_id, away_team_id, "
                "ft_home, ft_away, ht_home, ht_away FROM v_matches"
            )
            for part in parts:
                rel = part.target.relative_to(paths.root).as_posix()
                cat.execute("DELETE FROM lake_partitions WHERE season = ? AND league_id = ?",
                            [part.season, part.league_id])
                cat.execute(
                    "INSERT INTO lake_partitions VALUES (?, ?, ?, ?, ?, ?, ?)",
                    [part.season, part.league_id, rel, part.row_count,
                     part.min_col_id, part.max_col_id, now],
                )
            files = [paths.root / r[0] for r in cat.execute(
                "SELECT path FROM lake_partitions ORDER BY season, league_id").fetchall()]
            if files:
                file_list = "[" + ", ".join(_sql_str(f.as_posix()) for f in files) + "]"
                stats_tbl = work.execute(
                    f"SELECT col_id, count(*) AS n FROM read_parquet({file_list}) "
                    f"WHERE col_id IN (SELECT col_id FROM affected) GROUP BY col_id"
                ).to_arrow_table()
                cat.register("v_stats", stats_tbl)
                try:
                    cat.execute("DELETE FROM column_stats WHERE col_id IN (SELECT col_id FROM v_affected)")
                    cat.execute("INSERT INTO column_stats SELECT col_id, n FROM v_stats")
                finally:
                    cat.unregister("v_stats")
            cat.execute("COMMIT")
        except BaseException:
            cat.execute("ROLLBACK")
            raise
    finally:
        for name in ("v_matches", "v_teams", "v_leagues", "v_affected"):
            cat.unregister(name)


# ---------------------------------------------------------------------------
# Ana giriş noktası
# ---------------------------------------------------------------------------

def ingest_files(
    paths: LakePaths,
    input_files: Sequence[Path | str],
    memory_mb: int = 1536,
    threads: int | None = None,
) -> IngestReport:
    """CSV girdilerini lake'e aktarır. Ayrıntılar için modül docstring'ine bakın.

    Ön koşul: `init_lake(paths)` ile katalog hazırlanmış olmalıdır. Bu çağrı sırasında aynı
    süreçte salt-okunur LakeSession açık olmamalıdır.
    """
    sources = _normalize_inputs(input_files)
    report = IngestReport()
    if not sources:
        return report
    if not paths.catalog_db.exists():
        raise FileNotFoundError(f"katalog yok: {paths.catalog_db}; önce init_lake çalıştırın")
    if memory_mb <= 0:
        raise ValueError("memory_mb pozitif olmalıdır")
    n_threads = int(threads) if threads else default_threads()

    paths.ensure()
    shutil.rmtree(paths.staging_dir, ignore_errors=True)
    paths.staging_dir.mkdir(parents=True, exist_ok=True)

    cat = create_catalog(paths)
    work: duckdb.DuckDBPyConnection | None = None
    outs: list[_PartitionOut] = []
    try:
        cat.execute(f"SET memory_limit = '{int(memory_mb)}MB'")
        cat.execute(f"SET threads = {n_threads}")
        cat.execute(f"SET temp_directory = {_sql_str(str(paths.temp_dir))}")
        work = _open_work(paths, memory_mb, n_threads)
        _load_refs(cat, work)

        reasons: Counter[str] = Counter()
        rows_in = 0
        stage_kept = 0
        pairs: set[tuple[int, int]] = set()
        for seq, src in enumerate(sources):
            try:
                result = _stage_file(work, paths, src, seq)
            except Exception as exc:  # dosya düzeyinde izolasyon; hata raporlanır
                report.files_failed[str(src)] = f"{type(exc).__name__}: {exc}"
                continue
            report.files_ok += 1
            rows_in += result.rows_in
            stage_kept += result.rows_kept
            reasons.update(result.reasons)
            pairs.update(result.pairs)

        try:
            total_kept = 0
            dup_total = 0
            for season, league_id in sorted(pairs):
                out, kept, dups = _compact_pair(work, paths, season, league_id)
                outs.append(out)
                total_kept += kept
                dup_total += dups
            if dup_total:
                _rewrite_dup_rejects(work, paths)

            # İç tutarlılık: staging'deki her kabul edilen satır bir partition'a düşmeli ve
            # her girdi satırı ya yazılmalı ya da reddedilmiş olmalıdır.
            if total_kept + dup_total != stage_kept:
                raise RuntimeError(
                    f"partition satırları staging ile tutmuyor: {total_kept} + {dup_total} != {stage_kept}"
                )
            report.rows_in = rows_in
            report.rows_written = total_kept
            report.rows_rejected = sum(reasons.values()) + dup_total
            if report.rows_in != report.rows_written + report.rows_rejected:
                raise RuntimeError("satır sayıları tutmuyor: rows_in != rows_written + rows_rejected")
            if dup_total:
                reasons[REASON_DUPLICATE] += dup_total
            report.rejects_by_reason = dict(sorted(reasons.items()))

            for out in outs:
                if out.changed:
                    os.replace(out.tmp, out.target)
        except BaseException:
            for out in outs:
                out.tmp.unlink(missing_ok=True)
            raise

        if outs:
            _publish_catalog(cat, work, paths, outs)
        report.partitions_written = [(o.season, o.league_id) for o in outs]
    finally:
        if work is not None:
            work.close()
        cat.close()
    return report


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="python -m oddslake.ingest",
        description="CSV oran dosyalarını oddslake lake'ine aktarır.",
    )
    parser.add_argument("--lake", required=True, type=Path, help="lake kök dizini")
    parser.add_argument("--input", required=True, nargs="+", type=Path, help="CSV dosyaları")
    parser.add_argument("--bookmakers", type=Path, default=None,
                        help="bookmakers CSV (code,name); verilirse katalog bununla güncellenir")
    parser.add_argument("--memory-mb", type=int, default=1536, help="DuckDB memory_limit (MB)")
    args = parser.parse_args(argv)

    paths = LakePaths(args.lake)
    if args.bookmakers is not None or not paths.catalog_db.exists():
        init_lake(paths, bookmakers_csv=args.bookmakers)
    report = ingest_files(paths, args.input, memory_mb=args.memory_mb)
    print(json.dumps(asdict(report), ensure_ascii=False, indent=2))
    return 1 if report.files_failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
