import json
import os
from pathlib import Path

import requests


CACHE_DIR = "cache/sleeper"

# The Sleeper players endpoint is a ~19MB download the docs ask you to pull at
# most once a day, so read our own git-scraped snapshot instead. The JSON
# mirror rather than either CSV view: it is the only copy that carries the full
# 12k-player population *and* the per-player detail, and it keeps `metadata` as
# a real nested object, which the registry's dotted paths traverse natively.
SOURCE_URL = (
    "https://raw.githubusercontent.com/statsvine/data-snapshots/main/"
    "data/sleeper/players-nfl.json"
)


def prune(record):
    """
    Strip null and empty values out of a Sleeper record.

    `players_registry.get_nested` walks a dotted path with `data.get(key, {})`,
    so a null on any segment but the last raises AttributeError -- and Sleeper
    stores `metadata: null` for 2,662 of its players, which is exactly the path
    `sleeper.metadata.rookie_year` needs. Dropping the empties costs nothing
    (get_nested already returns None for a falsy leaf) and keeps the parsed map
    small enough not to care about.
    """
    pruned = {}
    for key, value in record.items():
        if isinstance(value, dict):
            value = prune(value)
        if value is None or value == "" or value == [] or value == {}:
            continue
        pruned[key] = value
    return pruned


def load(refresh=False):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = Path(os.path.join(CACHE_DIR, "players-nfl.json"))

    if not path.exists() or refresh:
        print("[sleeper] Downloading players json")
        response = requests.get(SOURCE_URL)
        response.raise_for_status()
        with open(path, "wb") as f:
            f.write(response.content)
    else:
        print("[sleeper] Using cached file for players json")

    with open(path, encoding="utf-8") as f:
        players = json.load(f)

    # Sleeper carries team defenses in the same map, keyed by team abbreviation
    # ("ARI") rather than a numeric id. They are not players, so drop them.
    return {key: prune(player) for key, player in players.items() if key.isdigit()}
