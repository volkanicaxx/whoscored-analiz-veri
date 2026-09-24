"""Kaydedilmiş MatchCentre JSON'larını analiz tablolarına dönüştürür.

Koordinatlar Opta sistemindedir: x ve y 0-100 aralığında, hücum her zaman
soldan sağa (x=100 rakip kale çizgisi).
"""

from __future__ import annotations

import json
import math
from pathlib import Path

import pandas as pd

# Opta 0-100 koordinatlarını metreye çevirmek için standart saha ölçüsü.
PITCH_LENGTH = 105.0
PITCH_WIDTH = 68.0
GOAL_WIDTH = 7.32


def load_match(path: str | Path) -> dict:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def events_frame(data: dict) -> pd.DataFrame:
    """Event listesini düz bir DataFrame'e çevirir (qualifier'lar dahil)."""
    players = {int(k): v for k, v in data.get("playerIdNameDictionary", {}).items()}
    teams = {}
    for side in ("home", "away"):
        team = data.get(side) or {}
        if "teamId" in team:
            teams[team["teamId"]] = team.get("name", side)

    rows = []
    for ev in data.get("events", []):
        quals = {
            q["type"]["displayName"]: q.get("value", True)
            for q in ev.get("qualifiers", [])
            if "type" in q
        }
        rows.append(
            {
                "id": ev.get("id"),
                "event_id": ev.get("eventId"),
                "minute": ev.get("minute"),
                "second": ev.get("second"),
                "period": (ev.get("period") or {}).get("displayName"),
                "team_id": ev.get("teamId"),
                "team": teams.get(ev.get("teamId")),
                "player_id": ev.get("playerId"),
                "player": players.get(ev.get("playerId")),
                "type": (ev.get("type") or {}).get("displayName"),
                "outcome": (ev.get("outcomeType") or {}).get("displayName"),
                "x": ev.get("x"),
                "y": ev.get("y"),
                "end_x": ev.get("endX"),
                "end_y": ev.get("endY"),
                "is_shot": bool(ev.get("isShot")),
                "is_goal": bool(ev.get("isGoal")),
                "qualifiers": quals,
            }
        )
    return pd.DataFrame(rows)


def pass_network(df: pd.DataFrame, team_id: int, min_passes: int = 3):
    """Bir takımın pas ağını döndürür: (düğümler, kenarlar).

    Pasın alıcısı, aynı takımın bir sonraki event'ini yapan oyuncu kabul edilir.
    Varsayılan olarak ilk oyuncu değişikliğine kadar olan paslar kullanılır.
    """
    team_df = df[df["team_id"] == team_id].reset_index(drop=True)
    subs = team_df.index[team_df["type"] == "SubstitutionOff"]
    if len(subs):
        team_df = team_df.loc[: subs[0] - 1]

    team_df = team_df.assign(receiver=team_df["player"].shift(-1))
    passes = team_df[(team_df["type"] == "Pass") & (team_df["outcome"] == "Successful")]
    passes = passes.dropna(subset=["player", "receiver"])
    passes = passes[passes["player"] != passes["receiver"]]

    nodes = (
        passes.groupby("player")
        .agg(x=("x", "mean"), y=("y", "mean"), passes=("id", "count"))
        .reset_index()
    )
    edges = passes.groupby(["player", "receiver"]).size().reset_index(name="count")
    edges = edges[edges["count"] >= min_passes]
    edges = edges.merge(nodes[["player", "x", "y"]], on="player").merge(
        nodes[["player", "x", "y"]].rename(columns={"player": "receiver"}),
        on="receiver",
        suffixes=("", "_end"),
    )
    return nodes, edges


def _shot_geometry(x: float, y: float) -> tuple[float, float]:
    """Şut mesafesi (m) ve kaleyi görme açısı (radyan)."""
    dx = (100 - x) * PITCH_LENGTH / 100
    dy = (y - 50) * PITCH_WIDTH / 100
    distance = math.hypot(dx, dy)
    half = GOAL_WIDTH / 2
    angle = math.atan2(GOAL_WIDTH * dx, dx**2 + dy**2 - half**2)
    if angle < 0:
        angle += math.pi
    return distance, angle


def shots_frame(df: pd.DataFrame) -> pd.DataFrame:
    """Şutları mesafe/açı özellikleri ve basit bir xG tahminiyle döndürür.

    WhoScored verisinde xG alanı yoktur. `xg_baseline`, yalnızca mesafe ve
    açıya dayalı kaba bir lojistik tahmindir; kendi modelinizi eğitene kadar
    karşılaştırma amaçlı kullanın. Penaltılar sabit 0.76 alır.
    """
    shots = df[df["is_shot"]].copy()
    if shots.empty:
        return shots.assign(distance_m=[], angle_rad=[], xg_baseline=[])
    geom = shots.apply(lambda r: _shot_geometry(r["x"], r["y"]), axis=1)
    shots["distance_m"] = [g[0] for g in geom]
    shots["angle_rad"] = [g[1] for g in geom]
    logit = -1.1 - 0.11 * shots["distance_m"] + 1.3 * shots["angle_rad"]
    shots["xg_baseline"] = 1 / (1 + (-logit).map(math.exp))
    is_pen = shots["qualifiers"].map(lambda q: "Penalty" in q)
    shots.loc[is_pen, "xg_baseline"] = 0.76
    return shots
