"""Tarayıcıdan manuel kaydedilen HAR/HTML/JSON dosyalarını işler.

Örnek:
    python process_captures.py captures/ --out data/matches \
        --start 2026-05-01 --end 2026-09-30 --ids 1900001 1900002

Betik ağ isteği yapmaz; `captures/` klasöründeki dosyaları okur, MatchCentre
verisini ayıklar, `data/matches/<matchId>.json` olarak kaydeder ve her maç için
events/şut CSV'lerini yazar.
"""

from __future__ import annotations

import argparse
from datetime import date
from pathlib import Path

from matchcentre import events_frame, load_match, save_matches, shots_frame


def main() -> None:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("inputs", nargs="+", type=Path, help="Dosya ya da klasör (.har/.html/.json)")
    p.add_argument("--out", type=Path, default=Path("data/matches"))
    p.add_argument("--ids", nargs="*", type=int, help="Yalnızca bu maç ID'lerini tut")
    p.add_argument("--start", type=date.fromisoformat, default=date(2026, 5, 1))
    p.add_argument("--end", type=date.fromisoformat, default=None)
    args = p.parse_args()

    files: list[Path] = []
    for item in args.inputs:
        if item.is_dir():
            files += sorted(f for f in item.rglob("*") if f.suffix.lower() in {".har", ".html", ".htm", ".json"})
        else:
            files.append(item)

    written = save_matches(files, args.out, set(args.ids or []), args.start, args.end)

    for path in written:
        df = events_frame(load_match(path))
        df.drop(columns=["qualifiers"]).to_csv(path.with_suffix(".events.csv"), index=False)
        shots = shots_frame(df)
        shots.drop(columns=["qualifiers"]).to_csv(path.with_suffix(".shots.csv"), index=False)

    print(f"\nToplam {len(written)} maç kaydedildi -> {args.out}")


if __name__ == "__main__":
    main()
