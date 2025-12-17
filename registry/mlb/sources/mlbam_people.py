import json
import os
import random
import time

import requests


CACHE_DIR = "cache/mlbam_people"

SPORT_ID_RANGES = [
    range(1, 1 + 1),
    range(11, 14 + 1),
    range(16, 16 + 1),
    range(22, 22 + 1),
]
SPORT_IDS = set().union(*SPORT_ID_RANGES)

SOURCE_URL_TEMPLATE = "https://statsapi.mlb.com/api/v1/sports/{sport_id}/players"

TEAMS_URL_TEMPLATE = "https://statsapi.mlb.com/api/v1/teams?sportId={sport_id}"


def load_teams(sport_id, refresh=False):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f"teams_{sport_id}.json")

    if not refresh and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    url = TEAMS_URL_TEMPLATE.format(sport_id=sport_id)
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return data


def map_teams_by_id(teams_data):
    return {t["id"]: t for t in teams_data["teams"]}


def load_sport(sport_id, refresh=False):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f"{sport_id}.json")

    if not refresh and os.path.exists(path):
        print(f"[mlbam_people] Using cached data for {sport_id}")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    print(f"[mlbam_people] Downloading players for {sport_id}")
    url = SOURCE_URL_TEMPLATE.format(sport_id=sport_id)
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return data


def load(refresh=False):
    all_players = {}

    parent_teams = load_teams(sport_id=1, refresh=refresh)
    parent_teams_by_id = map_teams_by_id(parent_teams)

    for sport_id in SPORT_IDS:
        sport_data = load_sport(sport_id=sport_id, refresh=refresh)
        teams_data = load_teams(sport_id=sport_id, refresh=refresh)
        teams_by_id = map_teams_by_id(teams_data)

        for person in sport_data.get("people", []):
            player_id = person["id"]
            team_id = person.get("currentTeam", {}).get("id", None)
            parent_team_id = None
            if team_id:
                team = teams_by_id[team_id]
                person["team"] = team
                parent_team_id = team.get("parentOrgId", None)
                if parent_team_id:
                    person["parentTeam"] = parent_teams_by_id[parent_team_id]
            if player_id:
                all_players[str(player_id)] = person

        if refresh:
            time.sleep(random.uniform(0.75, 1.5))

    return all_players
