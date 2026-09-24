# whoscored-analiz-veri

Tarayıcıda **manuel olarak** gezdiğiniz maç sayfalarından kaydettiğiniz dosyaları
yerelde işleyen bir analiz hattı. Betik hiçbir ağ isteği yapmaz.

## 1. Veriyi tarayıcıdan kaydetme

Maç sayfasını (Match Centre) normal şekilde açın, sonra şunlardan birini yapın:

- **HAR:** DevTools (F12) > Network > sayfayı yenileyin > sağ tık > *Save all as HAR with content*
- **HTML:** Sayfada `Ctrl+S` > *Web sayfası, yalnızca HTML*
- **JSON:** Network sekmesindeki ilgili yanıtı kopyalayıp `.json` olarak kaydedin

Dosyaları `captures/` klasörüne koyun (git'e eklenmez).

### Alternatif: tarayıcı otomasyonu (`fetch_matches.py`)

Maç sayfalarını gerçek bir Chromium penceresinde sırayla açar. Sayfa yüklenirken
gelen ağ yanıtlarını (HTML belgesi, XHR/fetch JSON'ları) dinler; veri orada yoksa
sayfadaki `matchCentreData` nesnesini bellekten okur ve `captures/<matchId>.json`
olarak kaydeder. Zaman aşımı gibi hatalarda maç atlanır ve `captures/failed_ids.txt`
dosyasına yazılır; `--ids-file captures/failed_ids.txt` ile yeniden denenebilir.

```bash
pip install -r requirements.txt && playwright install chromium
python fetch_matches.py --ids 1900001 1900002
python fetch_matches.py --ids-file ids.txt --max 30
python fetch_matches.py --fixture-url "https://www.whoscored.com/..."   # sayfadaki maç bağlantılarını toplar
```

Kapatılamayan sınırlar: robots.txt'ye uyulur (okunamazsa çalışma durur), sayfalar
arasında 5-10 sn bekleme, çalıştırma başına en fazla `--max` (50) maç, zaten kaydedilmiş
maçlar yeniden açılmaz, engelleme/CAPTCHA sayfasında çalışma durur ve tarayıcı kimliği
değiştirilmez. Sitenin kullanım koşullarına uymak kullanıcının sorumluluğundadır.

## 2. İşleme

```bash
pip install -r requirements.txt
python process_captures.py captures/ --out data/matches --start 2026-05-01 --end 2026-09-30
# yalnızca belirli maçlar:
python process_captures.py captures/ --ids 1900001 1900002
```

Her maç için şunlar üretilir:

| Dosya | İçerik |
|---|---|
| `<matchId>.json` | Ham MatchCentre verisi (events, qualifiers, x/y) |
| `<matchId>.events.csv` | Düzleştirilmiş event tablosu |
| `<matchId>.shots.csv` | Şutlar, mesafe/açı ve basit `xg_baseline` |

## 3. Analiz

```python
from matchcentre import load_match, events_frame, pass_network, shots_frame

df = events_frame(load_match("data/matches/1900001.json"))
nodes, edges = pass_network(df, team_id=df["team_id"].iloc[0])   # ilk değişikliğe kadar
shots = shots_frame(df)
```

Notlar:
- Koordinatlar Opta sistemindedir (0-100, hücum soldan sağa).
- WhoScored verisinde xG yoktur; `xg_baseline` mesafe/açıya dayalı kaba bir tahmindir.
  Ciddi analiz için kendi modelinizi (ör. StatsBomb açık verisiyle) eğitin.
- Pas alıcısı, aynı takımın bir sonraki event'ini yapan oyuncu olarak kabul edilir.
