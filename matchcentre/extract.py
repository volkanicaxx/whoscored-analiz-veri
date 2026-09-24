"""Tarayıcıdan manuel kaydedilen dosyalardan MatchCentre verisini çıkarır.

Desteklenen girdiler:
  * .har   - DevTools > Network > "Save all as HAR with content"
  * .html  - Maç sayfasında "Farklı kaydet" (Ctrl+S) ile kaydedilen sayfa
  * .json  - DevTools'tan kopyalanıp kaydedilmiş ham JSON yanıtı

Bu modül hiçbir ağ isteği yapmaz; yalnızca yerel diskteki dosyaları okur.
"""

from __future__ import annotations

import json
import re
from datetime import date, datetime
from pathlib import Path
from typing import Iterator

# Maç sayfasında veri `require.config.params["args"] = {...}` bloğunda,
# `matchCentreData: {...},` satırı olarak gömülü gelir.
_MATCH_CENTRE_RE = re.compile(r"matchCentreData\s*:\s*(\{.*?\})\s*,\s*\n", re.DOTALL)
_MATCH_ID_RE = re.compile(r"matchId\s*:\s*(\d+)")


def _from_html_text(text: str) -> dict | None:
    m = _MATCH_CENTRE_RE.search(text)
    if not m:
        return None
    data = json.loads(m.group(1))
    if "matchId" not in data:
        id_match = _MATCH_ID_RE.search(text)
        if id_match:
            data["matchId"] = int(id_match.group(1))
    return data


def _looks_like_match_centre(obj: object) -> bool:
    return isinstance(obj, dict) and "events" in obj and ("home" in obj or "playerIdNameDictionary" in obj)


def _from_json_obj(obj: object) -> dict | None:
    if _looks_like_match_centre(obj):
        return obj
    if isinstance(obj, dict):
        for key in ("matchCentreData", "data"):
            if _looks_like_match_centre(obj.get(key)):
                inner = dict(obj[key])
                inner.setdefault("matchId", obj.get("matchId"))
                return inner
    return None


def _from_har(path: Path) -> Iterator[dict]:
    har = json.loads(path.read_text(encoding="utf-8"))
    for entry in har.get("log", {}).get("entries", []):
        content = entry.get("response", {}).get("content", {})
        text = content.get("text")
        if not text:
            continue
        if content.get("encoding") == "base64":
            import base64

            text = base64.b64decode(text).decode("utf-8", errors="replace")
        mime = content.get("mimeType", "")
        data = None
        if "json" in mime:
            try:
                data = _from_json_obj(json.loads(text))
            except json.JSONDecodeError:
                pass
        elif "html" in mime:
            data = _from_html_text(text)
        if data:
            yield data


def extract_file(path: str | Path) -> list[dict]:
    """Bir dosyadaki tüm MatchCentre bloklarını döndürür."""
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix == ".har":
        return list(_from_har(path))
    text = path.read_text(encoding="utf-8", errors="replace")
    if suffix == ".json":
        data = _from_json_obj(json.loads(text))
    else:
        data = _from_html_text(text)
    return [data] if data else []


def match_date(data: dict) -> date | None:
    raw = data.get("startDate") or data.get("startTime")
    if not raw:
        return None
    try:
        return datetime.fromisoformat(str(raw).replace("Z", "")).date()
    except ValueError:
        return None


def save_matches(
    inputs: list[Path],
    out_dir: Path,
    match_ids: set[int] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> list[Path]:
    """Girdilerdeki maçları filtreleyip `out_dir/<matchId>.json` olarak kaydeder."""
    out_dir.mkdir(parents=True, exist_ok=True)
    written: list[Path] = []
    for path in inputs:
        for data in extract_file(path):
            mid = data.get("matchId")
            if mid is None:
                print(f"[atlandı] {path.name}: matchId bulunamadı")
                continue
            mid = int(mid)
            if match_ids and mid not in match_ids:
                continue
            d = match_date(data)
            if d and ((start and d < start) or (end and d > end)):
                continue
            target = out_dir / f"{mid}.json"
            target.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            written.append(target)
            print(f"[kaydedildi] {mid} ({d}) <- {path.name}: {len(data.get('events', []))} event")
    return written
