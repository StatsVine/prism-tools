import csv
import os
from pathlib import Path

import requests


CACHE_DIR = "cache/nflverse"

SOURCE_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/players/players.csv"
)


def load(refresh=False):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = Path(os.path.join(CACHE_DIR, "players.csv"))

    if not path.exists() or refresh:
        print("[nflverse] Downloading players csv")
        response = requests.get(SOURCE_URL)
        response.raise_for_status()
        with open(path, "wb") as f:
            f.write(response.content)
    else:
        print("[nflverse] Using cached file for players csv")

    people = {}
    # Load from disk
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row["gsis_id"].strip()
            if key:
                people[key] = row
    return people
