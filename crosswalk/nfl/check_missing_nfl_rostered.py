import argparse
import csv
import io
import re
import sys
from datetime import datetime

import requests
import yaml


# nflverse's player master. Deliberately NOT the `rosters` release: at the 2026
# cutdowns that file still held the 90-man (ACT=2839, median 91/team, week 1
# only), while this one's `status` column had already flipped to the 53-man.
NFLVERSE_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/players/players.csv"
)

# Filtered on `position`, NOT `position_group`: K lives in position_group SPEC
# alongside P and LS, and position_group RB swallows FB.
DEFAULT_POSITIONS = ["QB", "RB", "WR", "TE", "K", "FB"]

# nflverse `status` is the NFL transaction code. ACT is the active roster --
# 1695 rows against 53*32=1696 the day after the 2026 cutdowns. Widen it with
# --statuses (RES, PUP, RSR, RSN, SUS, EXE).
#
# There is no practice-squad code in this column: PS players read as CUT. The
# parallel `ngs_status` column briefly carried them as DEV, but that vocabulary
# is NOT stable -- between 2026-08-31 and 2026-09-01 it was rewritten wholesale
# (DEV and its 696 blanks vanished, W04 appeared) and the "Practice Squad"
# descriptions went with it. --ngs-statuses is kept as the escape hatch for
# whatever that column carries next; check what is actually in it first.
DEFAULT_STATUSES = ["ACT"]

# Both nflverse and DynastyProcess substitute an Elias (ESB) id into the
# gsis_id column when a player has no GSIS id, so pattern-check before using it
# as a join key.
GSIS_RE = re.compile(r"^00-\d{7}$")

# Our columns nflverse also publishes, in join priority order. gsis leads: it
# is 100% populated on current-season rows and is the id both sides agree on.
MATCH_KEYS = {
    # PRISM key : nflverse column
    "gsis_id": "gsis_id",
    "pfr_id": "pfr_id",
    "espn_id": "espn_id",
}

# Our NFL rows carry no suffix and no middle name -- "Joe Milton III" is
# stored as Milton/Joe. prism_id hashes on the name, so strip before emitting.
SUFFIX_RE = re.compile(r"\s+(Jr\.?|Sr\.?|II|III|IV|V)$", re.IGNORECASE)


def clean(value: str) -> str:
    return (value or "").strip()


def download_nflverse_data(url: str = NFLVERSE_URL):
    response = requests.get(url)
    # Force UTF-8 encoding, requests defaults to ISO-8859-1 if no charset given
    response.encoding = "utf-8"
    if not response.ok:
        print(f"Failed to fetch CSV. Status code: {response.status_code}")
        response.raise_for_status()

    reader = csv.DictReader(io.StringIO(response.text))
    return list(reader)


def split_name(row: dict) -> tuple[str, str]:
    """
    nflverse `first_name` is the legal name (Andrew Dalton, Casey Keenum);
    every one of our rows uses the common name instead, so prefer that.
    """
    first = clean(row.get("common_first_name")) or clean(row.get("first_name"))
    last = clean(row.get("last_name"))
    return SUFFIX_RE.sub("", last).strip(), SUFFIX_RE.sub("", first).strip()


def load_known_ids(csv_path: str) -> dict:
    known = {key: set() for key in MATCH_KEYS}
    with open(csv_path, newline="") as f:
        for row in csv.DictReader(f):
            for key in MATCH_KEYS:
                value = clean(row.get(key))
                if value:
                    known[key].add(value)
    return known


def load_ignores(ignores_file: str) -> dict:
    if not ignores_file:
        return {}
    with open(ignores_file) as f:
        return yaml.safe_load(f) or {}


def resolve_season(rows: list[dict], season: str) -> str:
    """Latest season in the feed, so the check does not go blind in January."""
    if season:
        return season
    seasons = {clean(r.get("last_season")) for r in rows}
    return max(s for s in seasons if s.isdigit())


def select_candidates(rows, season, positions, statuses, ngs_statuses) -> list[dict]:
    positions = {p.upper() for p in positions}
    statuses = {s.upper() for s in statuses}
    ngs_statuses = {s.upper() for s in ngs_statuses}
    out = []
    for row in rows:
        if clean(row.get("last_season")) != season:
            continue
        if clean(row.get("position")).upper() not in positions:
            continue
        if clean(row.get("status")).upper() in statuses:
            out.append(row)
        elif ngs_statuses and clean(row.get("ngs_status")).upper() in ngs_statuses:
            out.append(row)
    return out


def find_missing(candidates, known, ignores, quiet=False) -> list[dict]:
    missing = []
    for row in candidates:
        gsis = clean(row.get("gsis_id"))
        if gsis and not GSIS_RE.match(gsis):
            # An Elias id in the gsis column -- unusable as a join key.
            if not quiet:
                print(f"Skipping non-GSIS id {gsis} for {row.get('display_name')}")
            gsis = ""

        found = False
        for our_key, their_key in MATCH_KEYS.items():
            value = gsis if our_key == "gsis_id" else clean(row.get(their_key))
            if value and value in known[our_key]:
                found = True
                break
        if found:
            continue

        if gsis and gsis in ignores:
            if not quiet:
                print(f"Ignoring {row.get('display_name')} ({gsis})")
            continue

        last_name, first_name = split_name(row)
        missing.append(
            {
                "gsis_id": gsis,
                "last_name": last_name,
                "first_name": first_name,
                "birth_date": clean(row.get("birth_date")),
                "position": clean(row.get("position")),
                "team": clean(row.get("latest_team")),
                "espn_id": clean(row.get("espn_id")),
                "pfr_id": clean(row.get("pfr_id")),
            }
        )
    missing.sort(key=lambda m: (m["position"], m["last_name"], m["first_name"]))
    return missing


def write_issues_txt(issues: list[dict], outfile_path: str = "issues.txt") -> None:
    """
    Writes a markdown-formatted issues file listing rostered NFL players that
    are absent from the crosswalk.
    """
    now = datetime.utcnow().isoformat()
    header = [
        "## 🔍 Missing players from NFL rosters",
        "",
        f"_Generated {now} UTC_",
        "",
        "The following rostered players are missing from the PRISM Crosswalk",
        "",
        "| GSIS ID | Name | Pos | Team |",
        "|---------|------|-----|------|",
    ]

    table_rows = []
    csv_rows = []
    for issue in issues:
        table_rows.append(
            f"| {issue['gsis_id']} | {issue['last_name']}, {issue['first_name']} "
            f"| {issue['position']} | {issue['team']} |"
        )
        csv_rows.append(
            f"{issue['gsis_id']},{issue['last_name']},{issue['first_name']},"
            f"{issue['birth_date']},{issue['espn_id']},{issue['pfr_id']}"
        )

    csv_section = [
        "",
        "### Suggested Edits",
        "",
        "You may want to add the following players to the sheet:",
        "",
        "```csv",
        "gsis_id,last_name,first_name,birth_date,espn_id,pfr_id",
        *csv_rows,
        "```",
        "",
        "Please verify manually before updating.",
        "",
        "---",
        "",
        "✅ Auto-generated by `check_missing_nfl_rostered.py`",
    ]

    with open(outfile_path, "w") as f:
        f.write("\n".join(header + table_rows + csv_section))


def main(args):
    rows = download_nflverse_data(args.url)
    season = resolve_season(rows, args.season)
    candidates = select_candidates(
        rows,
        season,
        args.positions.split(","),
        args.statuses.split(","),
        args.ngs_statuses.split(",") if args.ngs_statuses else [],
    )
    known = load_known_ids(args.csv_path)
    ignores = load_ignores(args.ignores_file)
    missing = find_missing(candidates, known, ignores, args.quiet)

    if not args.quiet:
        print(f"Season {season}: {len(candidates)} rostered candidates")

    if missing:
        print(f"⚠️ Missing {len(missing)} rostered players")
        for m in missing:
            print(
                f"  {m['position']:3} {m['last_name']}, {m['first_name']} "
                f"({m['gsis_id']}) {m['team']}"
            )
        if args.issues_file:
            write_issues_txt(missing, args.issues_file)
        else:
            sys.exit(1)
    else:
        print("✅ No missing players from NFL rosters")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check NFL rosters for players missing from PRISM."
    )
    parser.add_argument("csv_path", help="Path to player CSV file")
    parser.add_argument(
        "--positions",
        default=",".join(DEFAULT_POSITIONS),
        help="Comma-separated nflverse `position` values to check "
        f"(default: {','.join(DEFAULT_POSITIONS)})",
    )
    parser.add_argument(
        "--statuses",
        default=",".join(DEFAULT_STATUSES),
        help="Comma-separated nflverse `status` values to treat as rostered "
        f"(default: {','.join(DEFAULT_STATUSES)}; widen with RES,PUP,RSR,SUS,EXE)",
    )
    parser.add_argument(
        "--ngs-statuses",
        default="",
        help="Comma-separated `ngs_status` values to additionally include, "
        "unioned with --statuses. NGS uses its own vocabulary and changes it "
        "without notice -- inspect the column before relying on a value",
    )
    parser.add_argument(
        "--season",
        help="Season to check (default: latest `last_season` in the feed)",
    )
    parser.add_argument("--quiet", action="store_true", help="Essential output only")
    parser.add_argument(
        "--url", default=NFLVERSE_URL, help="Override the nflverse players.csv URL"
    )
    parser.add_argument(
        "--issues-file", help="Create an issues file (for creating a GitHub Issue)"
    )
    parser.add_argument(
        "--ignores-file",
        help="Path to a yaml file keyed by gsis_id listing players to skip",
    )
    args = parser.parse_args()

    main(args)
