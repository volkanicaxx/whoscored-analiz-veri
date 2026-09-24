# whoscored-analiz-veri

Tarayıcıda **manuel olarak** gezdiğiniz maç sayfalarından kaydettiğiniz dosyaları
yerelde işleyen bir analiz hattı. Betik hiçbir ağ isteği yapmaz.

## 1. Veriyi tarayıcıdan kaydetme

Maç sayfasını (Match Centre) normal şekilde açın, sonra şunlardan birini yapın:

- **HAR:** DevTools (F12) > Network > sayfayı yenileyin > sağ tık > *Save all as HAR with content*
- **HTML:** Sayfada `Ctrl+S` > *Web sayfası, yalnızca HTML*
- **JSON:** Network sekmesindeki ilgili yanıtı kopyalayıp `.json` olarak kaydedin

Dosyaları `captures/` klasörüne koyun (git'e eklenmez).

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
