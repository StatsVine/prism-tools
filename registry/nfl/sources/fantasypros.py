import json
import os
import re
from pathlib import Path

import requests


CACHE_DIR = "cache/fantasypros"

# Our own git-scraped snapshot rather than the FantasyPros API directly, the
# same arrangement as sleeper.py and tools/crosswalk/nfl/validate_fantasypros_ids.py.
# 8534 NFL players, one flat JSON object each, no pagination and no auth.
SOURCE_URL = (
    "https://raw.githubusercontent.com/StatsVine/data-snapshots/main/"
    "data/fantasypros/players-nfl.json"
)

# FantasyPros does not publish a slug field. It publishes `filename`, the full
# player-page URL, and the slug is its last path segment minus the extension:
#   https://www.fantasypros.com/nfl/players/cam-skattebo.php -> cam-skattebo
# Derived here rather than in the schema's `preprocess` because it is a fact
# about FantasyPros' field naming, not a presentation choice -- the same reason
# validate_fantasypros_ids.py derives it in its fetch layer. Verified over all
# 8534 records: every one yields a slug and all 508 of ours match the
# fantasypros_slug the crosswalk already carries.
SLUG_RE = re.compile(r"([^/]+)\.php$")


def normalize(record):
    """Stringify scalars and add the derived `slug`.

    Everything keeps its upstream FantasyPros name so the schema's dotted paths
    read as FantasyPros (`fantasypros.player_name`, not a renamed field), the
    same convention nflverse.py and sleeper.py follow.

    Two reasons to stringify. `player_id` is an int in the JSON but a string in
    the crosswalk, and `build_intermediate` joins on raw equality, so an int key
    would match nothing. `draft_class` is likewise an int, and the registry
    writes every other NFL source's fields out as strings.

    Empty values are deliberately left in place rather than pruned. FantasyPros
    records are flat, so sleeper.py's null-traversal problem cannot arise here,
    and the schema's Jinja `preprocess` templates need every key to exist --
    a missing one renders as Undefined and raises on attribute access, where an
    empty string does not. `get_nested` already turns a falsy leaf into None, so
    the blanks never reach the output.
    """
    normalized = {
        key: (
            value
            if isinstance(value, list)
            else str(value if value is not None else "")
        )
        for key, value in record.items()
    }
    match = SLUG_RE.search(normalized.get("filename", ""))
    normalized["slug"] = match.group(1) if match else ""
    return normalized


def load(refresh=False):
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = Path(os.path.join(CACHE_DIR, "players-nfl.json"))

    if not path.exists() or refresh:
        print("[fantasypros] Downloading players json")
        response = requests.get(SOURCE_URL)
        response.raise_for_status()
        with open(path, "wb") as f:
            f.write(response.content)
    else:
        print("[fantasypros] Using cached file for players json")

    with open(path, encoding="utf-8") as f:
        players = json.load(f)

    # FantasyPros carries the 32 team defenses in the same list, as position_id
    # "DST" with a /nfl/teams/ page rather than /nfl/players/. They are not
    # players and their player_ids sit in the same numeric range as real ones,
    # so drop them before they can be joined to anything.
    return {
        record["player_id"]: record
        for record in (normalize(p) for p in players)
        if record["position_id"] != "DST"
    }
