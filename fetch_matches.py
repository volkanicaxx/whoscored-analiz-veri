"""Maç sayfalarını gerçek bir tarayıcıda açıp MatchCentre verisini `captures/` altına kaydeder.

Veri iki yoldan alınır: önce sayfanın yüklenirken aldığı ağ yanıtları dinlenir
(HTML belgesi ve JSON yanıtları); orada bulunamazsa sayfadaki `matchCentreData`
nesnesi bellekten okunur.

Örnek:
    python fetch_matches.py --ids 1900001 1900002
    python fetch_matches.py --ids-file ids.txt --max 30
    python fetch_matches.py --fixture-url "https://www.whoscored.com/Regions/.../Fixtures"

Ardından:
    python process_captures.py captures/ --start 2026-05-01 --end 2026-09-30

Sınırlar (bilerek konmuştur, kapatma bayrağı yoktur):
  * robots.txt bir yolu yasaklıyorsa o sayfa açılmaz; robots.txt okunamazsa çalışma durur.
  * Sayfa istekleri arasında 5-10 sn rastgele bekleme vardır.
  * Tek çalıştırmada en fazla `--max` maç (varsayılan 50) açılır.
  * `captures/` içinde zaten olan maçlar yeniden açılmaz.
  * Engelleme ya da doğrulama (CAPTCHA) sayfası görülürse çalışma durur; aşılmaya çalışılmaz.
    Zaman aşımı gibi sıradan hatalarda ise maç atlanır, ID'si `failed_ids.txt`
    dosyasına yazılır ve bir sonraki maça geçilir.
  * Tarayıcı kimliği (User-Agent, parmak izi) değiştirilmez.
"""

from __future__ import annotations

import argparse
import json
import random
import re
import sys
import time
from pathlib import Path
from urllib.parse import urljoin, urlparse
from urllib.robotparser import RobotFileParser

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import Page, Response, sync_playwright

from matchcentre.extract import _from_html_text, _from_json_obj

BASE_URL = "https://www.whoscored.com"
MATCH_URL = BASE_URL + "/Matches/{match_id}/Live"
MIN_DELAY, MAX_DELAY = 5.0, 10.0

# Veri, sayfada `require.config.params["args"].matchCentreData` altında durur;
# bazı sürümlerde global `matchCentreData` olarak da bulunabilir.
_EXTRACT_JS = """
() => {
  const args = window.require && window.require.config && window.require.config.params
    ? window.require.config.params.args : null;
  if (args && args.matchCentreData) return JSON.stringify(args.matchCentreData);
  if (typeof matchCentreData !== 'undefined' && matchCentreData) return JSON.stringify(matchCentreData);
  return null;
}
"""

_BLOCK_MARKERS = ("incapsula", "access denied", "captcha", "are you a robot", "request unsuccessful")


class Blocked(RuntimeError):
    """Site erişimi reddetti veya doğrulama istedi; çalışma durdurulur."""


def _load_robots(page: Page, base_url: str) -> RobotFileParser:
    robots_url = urljoin(base_url, "/robots.txt")
    resp = page.goto(robots_url, wait_until="domcontentloaded")
    if resp is None or not resp.ok:
        raise Blocked(f"robots.txt okunamadı ({resp.status if resp else 'yanıt yok'}); güvenli tarafta kalıp duruyorum.")
    parser = RobotFileParser(robots_url)
    parser.parse(resp.text().splitlines())
    return parser


def _check_blocked(page: Page) -> None:
    text = (page.title() + " " + page.content()[:5000]).lower()
    if any(marker in text for marker in _BLOCK_MARKERS):
        raise Blocked(f"Engelleme/doğrulama sayfası algılandı: {page.url}")


def _polite_pause() -> None:
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))


def _data_from_response(response: Response) -> dict | None:
    """Bir ağ yanıtında MatchCentre verisi varsa onu döndürür."""
    content_type = response.headers.get("content-type", "")
    if response.request.resource_type not in ("document", "xhr", "fetch") or not response.ok:
        return None
    try:
        text = response.text()
    except PlaywrightError:  # yönlendirme ya da gövdesi alınamayan yanıt
        return None
    if "json" in content_type:
        try:
            return _from_json_obj(json.loads(text))
        except json.JSONDecodeError:
            return None
    if "html" in content_type and "matchCentreData" in text:
        return _from_html_text(text)
    return None


def fetch_match(page: Page, match_id: int, out_dir: Path) -> Path | None:
    """Tek bir maç sayfasını açar ve veriyi `out_dir/<match_id>.json` olarak kaydeder."""
    captured: list[dict] = []

    def on_response(response: Response) -> None:
        data = _data_from_response(response)
        if data:
            captured.append(data)

    page.on("response", on_response)
    try:
        resp = page.goto(MATCH_URL.format(match_id=match_id), wait_until="domcontentloaded")
        if resp is not None and resp.status in (401, 403, 429):
            raise Blocked(f"HTTP {resp.status} ({match_id}); çalışma durduruldu.")
        _check_blocked(page)
        if not captured:
            page.wait_for_load_state("networkidle", timeout=15_000)
    finally:
        page.remove_listener("response", on_response)

    if captured:
        data, source = captured[-1], "ağ yanıtı"
    else:
        raw = page.evaluate(_EXTRACT_JS)
        if not raw:
            print(f"[veri yok] {match_id}: ağ yanıtlarında ve sayfada matchCentreData bulunamadı")
            return None
        data, source = json.loads(raw), "sayfa belleği"

    target = out_dir / f"{match_id}.json"
    # process_captures.py bu sarmalayıcıyı tanır ve matchId'yi buradan alır.
    payload = {"matchId": match_id, "matchCentreData": data}
    target.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    print(f"[kaydedildi] {match_id}: {len(data.get('events', []))} event ({source})")
    return target


def ids_from_fixture_page(page: Page, url: str) -> list[int]:
    """Fikstür sayfasındaki `/Matches/<id>/` bağlantılarından maç ID'lerini toplar."""
    page.goto(url, wait_until="networkidle")
    _check_blocked(page)
    hrefs = page.eval_on_selector_all("a[href*='/Matches/']", "els => els.map(e => e.getAttribute('href'))")
    ids = {int(m.group(1)) for h in hrefs if h and (m := re.search(r"/Matches/(\d+)/", h))}
    return sorted(ids)


def run(match_ids: list[int], fixture_urls: list[str], out_dir: Path, max_matches: int, headless: bool) -> int:
    out_dir.mkdir(parents=True, exist_ok=True)
    saved = 0
    failed: list[int] = []
    with sync_playwright() as pw:
        browser = pw.chromium.launch(headless=headless)
        page = browser.new_page()
        try:
            robots = _load_robots(page, BASE_URL)
            user_agent = page.evaluate("navigator.userAgent")

            ids = list(match_ids)
            for url in fixture_urls:
                if not robots.can_fetch(user_agent, url):
                    print(f"[robots.txt] {url} yasaklı, atlandı")
                    continue
                _polite_pause()
                found = ids_from_fixture_page(page, url)
                print(f"[fikstür] {url}: {len(found)} maç")
                ids += found

            ids = [i for i in dict.fromkeys(ids) if not (out_dir / f"{i}.json").exists()]
            if len(ids) > max_matches:
                print(f"{len(ids)} maçtan ilk {max_matches} tanesi işlenecek (--max)")
                ids = ids[:max_matches]

            for n, match_id in enumerate(ids, 1):
                url = MATCH_URL.format(match_id=match_id)
                if not robots.can_fetch(user_agent, url):
                    print(f"[robots.txt] {url} yasaklı, atlandı")
                    continue
                _polite_pause()
                print(f"({n}/{len(ids)}) ", end="")
                try:
                    if fetch_match(page, match_id, out_dir):
                        saved += 1
                    else:
                        failed.append(match_id)
                except Blocked:
                    raise
                except (PlaywrightError, json.JSONDecodeError) as exc:
                    print(f"[hata] {match_id}: {str(exc).splitlines()[0]}")
                    failed.append(match_id)
        except Blocked as exc:
            print(f"\n[DURDU] {exc}", file=sys.stderr)
        finally:
            browser.close()
    if failed:
        (out_dir / "failed_ids.txt").write_text("\n".join(map(str, failed)) + "\n")
        print(f"\n{len(failed)} maç alınamadı; yeniden denemek için: --ids-file {out_dir / 'failed_ids.txt'}")
    print(f"Toplam {saved} maç kaydedildi -> {out_dir}")
    return saved


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--ids", nargs="*", type=int, default=[], help="Maç ID'leri")
    p.add_argument("--ids-file", type=Path, help="Her satırda bir maç ID'si olan dosya")
    p.add_argument("--fixture-url", nargs="*", default=[], help="Maç ID'lerinin toplanacağı fikstür sayfaları")
    p.add_argument("--out", type=Path, default=Path("captures"))
    p.add_argument("--max", type=int, default=50, help="Tek çalıştırmada açılacak en fazla maç sayısı")
    p.add_argument("--headless", action="store_true", help="Tarayıcı penceresini göstermeden çalıştır")
    args = p.parse_args()

    ids = list(args.ids)
    if args.ids_file:
        ids += [int(line) for line in args.ids_file.read_text().split() if line.strip().isdigit()]
    for url in args.fixture_url:
        if not urlparse(url).netloc.endswith("whoscored.com"):
            p.error(f"Fikstür adresi {BASE_URL} altında olmalı: {url}")
    if not ids and not args.fixture_url:
        p.error("--ids, --ids-file veya --fixture-url vermelisiniz")

    run(ids, args.fixture_url, args.out, args.max, args.headless)


if __name__ == "__main__":
    main()
