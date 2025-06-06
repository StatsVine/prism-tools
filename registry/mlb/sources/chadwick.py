import csv
import os
import random
import time
from pathlib import Path

import requests


CACHE_DIR = "cache/chadwick"

SOURCE_URL_TEMPLATE = (
    "https://raw.githubusercontent.com/chadwickbureau/register/"
    "master/data/people-{suffix}.csv"
)
HEX_SUFFIXES = [f"{i:x}" for i in range(16)]  # ['0', '1', ..., 'f']


def load_file(suffix, refresh=False):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = Path(os.path.join(CACHE_DIR, f"people-{suffix}.csv"))

    if not path.exists() or refresh:
        print(f"[chadwick] Downloading people-{suffix}.csv")
        url = SOURCE_URL_TEMPLATE.format(suffix=suffix)
        response = requests.get(url)
        response.raise_for_status()
        with open(path, "wb") as f:
            f.write(response.content)
    else:
        print(f"[chadwick] Using cached file for people-{suffix}")

    people = {}
    # Load from disk
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row["key_mlbam"].strip()
            if key:
                people[key] = row
    return people


def load(refresh=False):
    all_players = {}

    for suffix in HEX_SUFFIXES:
        player_data = load_file(suffix, refresh=refresh)

        for id, player in player_data.items():
            all_players[str(id)] = player

        if refresh:
            time.sleep(random.uniform(0.5, 1.5))

    return all_players
