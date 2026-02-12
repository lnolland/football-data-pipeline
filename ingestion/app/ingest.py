import os
import time
import requests
from dateutil import parser as dtparser

API_BASE = "https://api.football-data.org/v4"

def _headers():
    return {"X-Auth-Token": os.environ["FOOTBALL_DATA_TOKEN"]}

def fetch_teams(competition_code: str) -> list[dict]:
    url = f"{API_BASE}/competitions/{competition_code}/teams"
    r = requests.get(url, headers=_headers(), timeout=30)
    r.raise_for_status()
    return r.json().get("teams", [])

def fetch_matches(competition_code: str, date_from: str | None, date_to: str | None) -> list[dict]:
    params = {}
    if date_from:
        params["dateFrom"] = date_from  # YYYY-MM-DD
    if date_to:
        params["dateTo"] = date_to      # YYYY-MM-DD

    url = f"{API_BASE}/competitions/{competition_code}/matches"
    r = requests.get(url, headers=_headers(), params=params, timeout=30)
    r.raise_for_status()
    return r.json().get("matches", [])

def parse_ts(s: str | None):
    if not s:
        return None
    return dtparser.isoparse(s)

def upsert_teams(conn, teams: list[dict]) -> int:
    sql = """
    INSERT INTO teams (id, name, short_name, tla, crest)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE SET
      name = EXCLUDED.name,
      short_name = EXCLUDED.short_name,
      tla = EXCLUDED.tla,
      crest = EXCLUDED.crest;
    """
    rows = 0
    with conn.cursor() as cur:
        for t in teams:
            cur.execute(sql, (
                t["id"],
                t.get("name"),
                t.get("shortName"),
                t.get("tla"),
                t.get("crest"),
            ))
            rows += 1
    return rows

def upsert_matches(conn, matches: list[dict]) -> int:
    sql = """
    INSERT INTO matches (
      id, utc_date, status, matchday, stage,
      home_team_id, away_team_id,
      home_score, away_score, last_updated
    )
    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
    ON CONFLICT (id) DO UPDATE SET
      utc_date = EXCLUDED.utc_date,
      status = EXCLUDED.status,
      matchday = EXCLUDED.matchday,
      stage = EXCLUDED.stage,
      home_team_id = EXCLUDED.home_team_id,
      away_team_id = EXCLUDED.away_team_id,
      home_score = EXCLUDED.home_score,
      away_score = EXCLUDED.away_score,
      last_updated = EXCLUDED.last_updated;
    """
    rows = 0
    with conn.cursor() as cur:
        for m in matches:
            score = m.get("score", {}).get("fullTime", {}) or {}
            cur.execute(sql, (
                m["id"],
                parse_ts(m.get("utcDate")),
                m.get("status"),
                m.get("matchday"),
                m.get("stage"),
                m.get("homeTeam", {}).get("id"),
                m.get("awayTeam", {}).get("id"),
                score.get("home"),
                score.get("away"),
                parse_ts(m.get("lastUpdated")),
            ))
            rows += 1
    return rows

def run_ingestion(conn, competition_code: str, date_from: str | None, date_to: str | None) -> dict:
    t0 = time.time()

    teams = fetch_teams(competition_code)
    team_rows = upsert_teams(conn, teams)

    matches = fetch_matches(competition_code, date_from, date_to)
    match_rows = upsert_matches(conn, matches)

    dt = time.time() - t0
    return {
        "competition": competition_code,
        "teams_upserted": team_rows,
        "matches_upserted": match_rows,
        "seconds": round(dt, 3),
        "date_from": date_from,
        "date_to": date_to,
    }
