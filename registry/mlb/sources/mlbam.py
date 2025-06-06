import json
import os
import random
import time

import requests


CACHE_DIR = "cache/mlb"

TEAM_ID_RANGES = [range(108, 121 + 1), range(133, 147 + 1), range(158, 159)]
TEAM_IDS = set().union(*TEAM_ID_RANGES)

SOURCE_URL_TEMPLATE = (
    "https://statsapi.mlb.com/api/v1/teams/{team_id}/roster/"
    "40Man?hydrate=person,currentTeam,team"
)

TEAMS_URL = "https://statsapi.mlb.com/api/v1/teams?sportId=1"


def load_teams(refresh=False):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, "teams.json")

    if not refresh and os.path.exists(path):
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    response = requests.get(TEAMS_URL)
    response.raise_for_status()
    data = response.json()

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return data


def map_teams_by_id(teams_data):
    return {t["id"]: t for t in teams_data["teams"]}


def load_team(team_id, refresh=False):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f"{team_id}.json")

    if not refresh and os.path.exists(path):
        print(f"[mlb] Using cached roster for {team_id}")
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)

    print(f"[mlb] Downloading roster for {team_id}")
    url = SOURCE_URL_TEMPLATE.format(team_id=team_id)
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    return data


def load(refresh=False):
    all_players = {}

    teams_by_id = map_teams_by_id(load_teams(refresh=refresh))

    for team_id in TEAM_IDS:
        team_data = load_team(team_id, refresh=refresh)
        team_info = teams_by_id.get(team_id, {})

        for player in team_data.get("roster", []):
            player["team"] = team_info
            player_id = player.get("person", {}).get("id", None)
            if player_id:
                all_players[str(player_id)] = player

        if refresh:
            time.sleep(random.uniform(0.75, 1.5))

    return all_players
