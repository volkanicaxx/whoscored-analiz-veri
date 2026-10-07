# oddslake — mimari

Hedef: 2009–2026 futbol oran arşivi (164 büro, 100.000+ mantıksal kolon) standart bir
masaüstü bilgisayarda kilitlenmeden sorgulanabilsin. Bu belge kararları, gerekçelerini ve
ölçümleri listeler. Ölçülmemiş her şey açıkça "ÖLÇÜLMEDİ" diye işaretlidir.

## 1. Temel karar: geniş tablo değil, uzun format

Her (büro, sonuç, faz) üçlüsü bir **mantıksal kolon**dur. Bunları fiziksel olarak 100.000
kolonlu tek bir tabloda tutmak denendi ve reddedildi:

| Ölçüm (bu makine, 4 CPU / 16 GB) | Sonuç |
|---|---|
| DuckDB, 100.000 FLOAT kolonlu tablo: `CREATE TABLE` | 1,3 s, RSS 217 MB |
| Aynı tabloya 1.000 satır `INSERT` | 27,3 s, RSS 1.068 MB, dosya 316 MB |
| Parquet, 100.000 kolon, 1.000 satır (%1 dolu) | dosya 77 MB, **yalnızca footer 46 MB** |
| Parquet tek kolon okuma (100.000 kolonlu dosya) | 1,55 s |

Geniş format her satır için 100.000 kolon meta verisi taşır; seyrek oran verisinde (çoğu
hücre boş) bu hem diski hem footer okumasını şişirir. Bu yüzden veri **uzun formatta**
saklanır:

```
odds/season=S/league_id=L/data.parquet   ->   (match_id INT32, col_id INT32, odd FLOAT)
```

- `col_id = outcome_id*4096 + phase_id*512 + bookmaker_id` deterministiktir. Yeni büro
  eklemek mevcut `col_id`'leri değiştirmez, Parquet dosyaları yeniden yazılmaz.
- Dosyalar `ORDER BY col_id, match_id` ile yazılır. Ekranda yan yana duran 164 büro kolonu
  diskte aynı satır grubunda durur; `col_id BETWEEN min AND max` filtresi satır grubu
  istatistikleriyle (row-group pruning) okumayı budar.
- Tek bir kolon (örn. `bet365_FT_1_Acilis`) istendiğinde yalnızca `col_id = X` satırları
  okunur; diğer kolonlar diske dokunulmadan atlanır. Bu, "sadece o kolon şeridini RAM'e
  çek" gereksinimidir.

## 2. Bölümleme (partitioning)

- Birincil bölüm: **sezon × lig**. Yol: `odds/season=S/league_id=L/data.parquet`.
- Sezon ve lig, `read_parquet(..., hive_partitioning = true)` ile sorguda kolon olarak gelir;
  dosya budama bu değerlerle yapılır.
- Her bölümün `min_col_id`, `max_col_id`, `row_count` değerleri `lake_partitions` tablosunda
  tutulur. Arayüz glob taramak yerine bu tablodan kesin dosya listesi kurar.
- Bölüm sayısı: **BİLİNMİYOR** (lig sayısı bu projede tanımlı değil). Her bölüm bir dosya
  olduğundan, bölüm sayısı tipik olarak birkaç bin dosya sınırının altında tutulmalıdır.
- Ham boyut tahmini: **ÖLÇÜLMEDİ**. 800 GB kaynak verinin uzun formata dönüşüm oranı, gerçek
  veriyle ingest yapılmadan bilinemez.

## 3. Şema (catalog.duckdb)

Büyük oran verisi bu dosyada **değildir**. Katalog yalnızca boyut ve meta tablolardır:

| Tablo | Amaç |
|---|---|
| `bookmakers` | 164 büro; `code` kolon adında kullanılır (`bet365`) |
| `markets`, `outcomes` | market × çizgi × seçim; `outcome_key` = `FT_2.5_U` |
| `phases` | `Acilis`, `Kapanis` |
| `column_catalog` | mantıksal kolonlar, 100.696 satır; `display_order` ekran sırası |
| `leagues`, `teams`, `matches` | maç bilgisi; skor alanlarında **NULL = skor yok, 0 = sıfır gol** |
| `lake_partitions` | yazılmış her bölümün kaydı |
| `column_stats` | kolon başına dolu hücre sayısı ("boş kolonları gizle") |

Yabancı anahtar kullanılmaz: DuckDB'de ebeveyn satır güncellemesi çocuk tabloları
engelleyebilir ve `INSERT OR REPLACE` ile çakışır. Bütünlük ingest sonrası anti-join
sorgularıyla denetlenir.

## 4. Bellek bütçesi (4 GB sınırı)

| Bileşen | Sınır | Kaynak |
|---|---|---|
| Arayüz süreci DuckDB | 768 MB | `MemoryBudget.ui_duckdb_mb` |
| Analiz işçisi DuckDB | 1.536 MB | `MemoryBudget.worker_duckdb_mb` |
| Karo önbelleği | en fazla 512 karo × ~2 KB ≈ 1 MB | `MemoryBudget.max_tiles` |
| Satır indeksi | 1 milyon maç × 10 byte ≈ 10 MB | `RowIndex` |
| Qt ve Python yükü | ölçülmedi | — |
| **Toplam üst sınır** | **3.584 MB** (`hard_limit_mb`) | RSS koruyucusu |

DuckDB `memory_limit` aşıldığında geçici veriyi `temp_directory` altına diske döker
(spill). Yani sorgu yavaşlar ama çökmez. Arayüz RSS değerini her saniye ölçer; sınır
aşılırsa ağır işi iptal eder.

## 5. Sanal ızgara (Virtual Grid)

- `QTableView` yalnızca **görünen hücrelerin** `data()` metodunu çağırır. Model 100.000+
  kolonu ve milyonlarca satırı bellekte tutmaz; yalnızca iki kısa `int32` dizisi (satır ve
  kolon kimlikleri) tutulur.
- Hücre değerleri **karolar** (32 satır × 16 kolon) halinde önbelleğe alınır. Karo, LRU
  önbellekte en fazla `max_tiles` tane tutulur; eskiler atılır.
- Karo yüklü değilse `data()` `"…"` döner, arka plan işçisi (QThreadPool) karoyu DuckDB'den
  yükler ve bitince `dataChanged` yayar. **Ana iş parçacığında DuckDB sorgusu çalışmaz.**
- Filtre değişikliği "generation" sayacını artırır; eski işin sonucu yeni tabloya
  yazılmaz.
- Ağır analizler (Elo, form, piyasa) tek bir iş slotunda çalışır; iptal, DuckDB'nin
  `interrupt()` ve bir bayrak ile yapılır.

## 6. Analizler

| Analiz | Yöntem | Bellek |
|---|---|---|
| Form L5/L10 | DuckDB pencere fonksiyonları, `ROWS BETWEEN n PRECEDING AND 1 PRECEDING` | DuckDB sınırı içinde, diske taşabilir |
| Elo | maçlar `to_arrow_reader(batch)` ile akar; durum takım başına bir sayı | sadece takım sözlüğü + batch |
| Piyasa farkı / overround | yalnızca bir büronun kolonları (`col_id IN (...)`) | bir büronun satırları |

Sıfır ve boş ayrımı: 0-0 maç form ve Elo'da **dahil** edilir (0 gol, beraberlik). Oynanmamış
maç (`ft_home IS NULL`) hiçbir hesaba girmez. Kodda `if not x` ya da `COALESCE(x, 0)`
kalıbıyla skor dışlanmaz.

## 7. Veri akışı

```
CSV (kaynak sözleşmesi, bkz. oddslake/ingest.py)
  │  ingest.py  (A) doğrula, col_id'ye eşle, staging/ yaz; reddedilenler rejects/
  │             (B) (sezon, lig) başına ORDER BY col_id, match_id ile compaction; .tmp + os.replace
  │             (C) matches / teams / leagues upsert; lake_partitions; column_stats
  ▼
lake/  catalog.duckdb  +  odds/season=S/league_id=L/data.parquet
  │
  ├── lake.py      LakeSession: READ_ONLY ATTACH, satır indeksi, kolon seti, karo sorguları
  ├── analysis/    form.py, elo.py, market.py  (cursor üzerinden, akış)
  └── ui/          GridModel (karo önbelleği) ← QThreadPool işçileri
                   HeavyJobRunner (tek iş slotu, RSS koruyucusu)
```

## 8. Kaynak sözleşmesi (girdi)

Projede kaynak verinin formatı tanımlı değildi. `ingest.py` tek bir CSV sözleşmesi tanımlar
(modül docstring'inde tam kolon listesi). Gerçek kaynak bu sözleşmeye dönüştürülmelidir.
Sözleşmeye uymayan satır **sessizce atılmaz**; `rejects/` altına sebebiyle yazılır.

## 9. Bilinen sınırlar

- Oran değerleri `FLOAT` (float32) saklanır. `2.05` gibi değerler okunurken
  `2.049999952` olarak gelir. Görüntü iki ondalığa yuvarlanır; eşitlik karşılaştırması
  yapılmamalıdır.
- Kolon adında `*` joker karakteri desteklenir; `bet365_FT_1_*` deseni 1X2 ev sahibi
  kolonlarıyla birlikte `FT_1_O`, `FT_1_U` (1.0 gol çizgisi) kolonlarını da getirir. Tam
  ad aramak için `ColumnFilter` yerine doğrudan `col_ids_for_names` kullanılır.
- Üretimde 800 GB ölçeğinde ingest süresi ve disk kullanımı **ÖLÇÜLMEDİ**.
