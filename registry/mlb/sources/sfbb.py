import csv
import os
from pathlib import Path

import requests


CACHE_DIR = "cache/sfbb"

SHEET_ID = "1JgczhD5VDQ1EiXqVG-blttZcVwbZd5_Ne_mefUGwJnk"
GID = 0
SOURCE_URL = (
    f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
    f"/pub?gid={GID}&single=true&output=csv"
)


def load(refresh=False):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = Path(os.path.join(CACHE_DIR, "players.csv"))

    if not path.exists() or refresh:
        print("[sfbb] Downloading players csv")
        response = requests.get(SOURCE_URL)
        response.raise_for_status()
        with open(path, "wb") as f:
            f.write(response.content)
    else:
        print("[sfbb] Using cached file for players csv")

    people = {}
    # Load from disk
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row["MLBID"].strip()
            if key:
                people[key] = row
    return people
