-- catalog.duckdb: küçük boyut tabloları + kolon sözlüğü + bölüm kaydı.
-- Büyük oran verisi bu dosyada DEĞİL, odds/season=S/league_id=L/data.parquet
-- dosyalarında uzun formatta (match_id, col_id, odd) durur.
--
-- Yabancı anahtar (FOREIGN KEY) kullanılmaz: DuckDB'de FK, ebeveyn satırın
-- silinmesini/güncellenmesini engeller ve INSERT OR REPLACE ile çakışır.
-- Bütünlük ingest.validate_catalog() içindeki anti-join sorgularıyla denetlenir.

CREATE TABLE IF NOT EXISTS bookmakers (
    bookmaker_id SMALLINT PRIMARY KEY CHECK (bookmaker_id BETWEEN 0 AND 511),
    code         VARCHAR  NOT NULL UNIQUE,      -- kolon adında kullanılır: 'bet365'
    name         VARCHAR  NOT NULL,
    sort_order   SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS markets (
    market_id  SMALLINT PRIMARY KEY,
    code       VARCHAR  NOT NULL,               -- '1X2', 'OU', 'AH', 'BTTS', ...
    period     VARCHAR  NOT NULL,               -- 'FT', 'HT', '2H'
    sort_order SMALLINT NOT NULL,
    UNIQUE (code, period)
);

CREATE TABLE IF NOT EXISTS outcomes (
    outcome_id  INTEGER  PRIMARY KEY CHECK (outcome_id BETWEEN 0 AND 524287),
    market_id   SMALLINT NOT NULL,
    line        DECIMAL(5, 2),                  -- 2.50, -0.25; hatsız marketlerde NULL
    selection   VARCHAR  NOT NULL,              -- '1', 'X', '2', 'O', 'U', 'AH1', ...
    outcome_key VARCHAR  NOT NULL UNIQUE,       -- 'FT_1', 'FT_2.5_U' (kolon adının orta kısmı)
    sort_order  SMALLINT NOT NULL
);

CREATE TABLE IF NOT EXISTS phases (
    phase_id   UTINYINT PRIMARY KEY CHECK (phase_id BETWEEN 0 AND 7),
    code       VARCHAR  NOT NULL UNIQUE,        -- 'Acilis', 'Kapanis'
    sort_order UTINYINT NOT NULL
);

-- Mantıksal kolon sözlüğü. 164 büro x 307 sonuç x 2 faz = 100.696 satır.
-- col_id, (sonuç, faz, büro) üçlüsünden DETERMİNİSTİK üretilir; yeni büro ya da
-- market eklemek mevcut col_id'leri değiştirmez, Parquet dosyaları yeniden
-- yazılmaz. Ekran sırası ayrı tutulur (display_order) ve serbestçe yeniden
-- hesaplanır.
CREATE TABLE IF NOT EXISTS column_catalog (
    col_id        INTEGER  PRIMARY KEY,
    bookmaker_id  SMALLINT NOT NULL,
    outcome_id    INTEGER  NOT NULL,
    phase_id      UTINYINT NOT NULL,
    column_name   VARCHAR  NOT NULL UNIQUE,     -- 'bet365_FT_1_Acilis'
    display_order INTEGER  NOT NULL,
    UNIQUE (bookmaker_id, outcome_id, phase_id),
    CHECK (col_id = outcome_id * 4096 + phase_id * 512 + bookmaker_id)
);

CREATE TABLE IF NOT EXISTS leagues (
    league_id INTEGER PRIMARY KEY,
    country   VARCHAR,
    name      VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS teams (
    team_id INTEGER PRIMARY KEY,
    name    VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS matches (
    match_id     INTEGER   PRIMARY KEY,
    season       SMALLINT  NOT NULL,            -- sezon başlangıç yılı: 2019 => 2019/20
    league_id    INTEGER   NOT NULL,
    kickoff_utc  TIMESTAMP NOT NULL,
    home_team_id INTEGER   NOT NULL,
    away_team_id INTEGER   NOT NULL,
    -- NULL = skor yok (oynanmadı/bilinmiyor); 0 = sıfır gol. İkisi asla karıştırılmaz.
    ft_home      SMALLINT,
    ft_away      SMALLINT,
    ht_home      SMALLINT,
    ht_away      SMALLINT,
    CHECK ((ft_home IS NULL) = (ft_away IS NULL)),
    CHECK ((ht_home IS NULL) = (ht_away IS NULL))
);

-- Diskte yazılmış her (sezon, lig) Parquet dosyasının kaydı. Arayüz karo
-- sorgularında glob yerine bu tablodan kesin dosya listesi kurar.
CREATE TABLE IF NOT EXISTS lake_partitions (
    season     SMALLINT  NOT NULL,
    league_id  INTEGER   NOT NULL,
    path       VARCHAR   NOT NULL,              -- lake köküne göre göreli yol
    row_count  BIGINT    NOT NULL,
    min_col_id INTEGER,
    max_col_id INTEGER,
    written_at TIMESTAMP NOT NULL,
    PRIMARY KEY (season, league_id)
);

-- Her kolonda kaç dolu hücre var: "boş kolonları gizle" filtresi için.
CREATE TABLE IF NOT EXISTS column_stats (
    col_id   INTEGER PRIMARY KEY,
    non_null BIGINT  NOT NULL
);
