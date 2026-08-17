"""
Check our wikidata_id values against Wikidata itself.

Wikidata is the one source that spans leagues, so this script is driven by a
per-league config rather than being forked per sport: add an entry to LEAGUES
and the rest of the flow is unchanged.

For each configured join property we ask the Wikidata Query Service which item
carries our id (e.g. "which item has Pro-Football-Reference id MahoPa00"), then
compare the item's QID to the wikidata_id we already store.
"""

import argparse
import csv
import io
import sys
from datetime import datetime

import requests
import yaml


WDQS_URL = "https://query.wikidata.org/sparql"

# WDQS blocks the default python-requests agent, so identify ourselves.
USER_AGENT = "prism-crosswalk-validator/0.1 (https://github.com/statsvine/prism-tools)"

# Our column holding the QID. Same name in every league.
WIKIDATA_FIELD = "wikidata_id"


def wikidata_to_pfr(value: str) -> str:
    """
    P3561 stores the Pro-Football-Reference path, not the bare id: MahoPa00 is
    held as "M/MahoPa00". The directory is the uppercased first letter of the
    id, including for the lowercase kicker/punter ids (gouldrob01 -> G/...).
    """
    return value.rsplit("/", 1)[-1]


# league : { wikidata property : (our field, from_wikidata) }
# from_wikidata converts an upstream value into our format, or None if the two
# already agree. Properties are tried in order, so list the highest-coverage
# join first.
LEAGUES = {
    "nfl": {
        "P3561": ("pfr_id", wikidata_to_pfr),
        "P3686": ("espn_id", None),
    },
    "mlb": {
        # Untested against a live CSV -- data/mlb/players.csv has no
        # wikidata_id column yet, so every matched row reports as missing.
        "P3541": ("mlbam_id", None),
    },
}


def query_wdqs(query: str) -> list[dict]:
    """Ask WDQS for CSV rather than JSON -- it parses straight into dicts."""
    response = requests.post(
        WDQS_URL,
        data={"query": query},
        headers={"Accept": "text/csv", "User-Agent": USER_AGENT},
        timeout=120,
    )
    if not response.ok:
        print(f"WDQS query failed. Status code: {response.status_code}")
        response.raise_for_status()
    return list(csv.DictReader(io.StringIO(response.text)))


def fetch_property_maps(properties: dict) -> dict:
    """
    Pull every item carrying any of our join properties, in a single query.

    Wikidata answers this for a whole league in about a second, so there is no
    reason to ask id-by-id: the cost is fixed no matter how many rows we hold.
    Returns { property : { our-format id : QID } }.
    """
    props = " ".join(f"wdt:{prop}" for prop in properties)
    query = (
        f"SELECT ?item ?prop ?value WHERE {{ "
        f"VALUES ?prop {{ {props} }} "
        f"?item ?prop ?value . }}"
    )

    maps = {prop: {} for prop in properties}
    for row in query_wdqs(query):
        prop = row["prop"].rsplit("/", 1)[-1]
        if prop not in maps:
            continue
        _, from_wikidata = properties[prop]
        value = row["value"]
        # Re-key by our own id so callers never deal with Wikidata formatting.
        key = from_wikidata(value) if from_wikidata else value
        maps[prop][key] = row["item"].rsplit("/", 1)[-1]
    return maps


def write_issues_txt(issues: list[dict], outfile_path: str = "issues.txt") -> None:
    """
    Writes a markdown-formatted issues.txt file based on ID discrepancies.

    Each issue should be a dict with:
    - prism_id
    - last_name
    - first_name
    - matched_on
    - wikidata_value
    - prism_value
    """
    now = datetime.utcnow().isoformat()
    header = [
        "## 📃 Wikidata differences found",
        "",
        f"_Generated {now} UTC_",
        "",
        "The following Wikidata mismatches found.",
        "",
        "| Prism ID | Name         | Matched On | Wikidata Value | PRISM Value |",
        "|----------|--------------|------------|----------------|-------------|",
    ]

    table_rows = []
    for issue in issues:
        table_rows.append(
            f"| {issue['prism_id']} | {issue['last_name']}, {issue['first_name']} "
            f"| {issue['matched_on']} | {issue['wikidata_value']} "
            f"| {issue.get('prism_value', '')} |"
        )

    with open(outfile_path, "w") as f:
        f.write("\n".join(header + table_rows))


def build_lookups(rows: list[dict], properties: dict) -> dict:
    """Fetch the Wikidata side, then report how much of ours it covers."""
    lookups = fetch_property_maps(properties)
    for prop, (our_field, _) in properties.items():
        ours = {row[our_field] for row in rows if row.get(our_field)}
        hits = sum(1 for value in ours if value in lookups[prop])
        print(f"{prop} ({our_field}): matched {hits} of {len(ours)} ids")
    return lookups


def validate_csv(
    csv_path: str,
    league: str = "nfl",
    start: int = 1,
    quiet: bool = False,
    issues_file: str = None,
    ignores_file: str = None,
):
    issues = []
    matches = 0

    properties = LEAGUES[league]

    ignores = {}
    if ignores_file:
        with open(ignores_file, "r") as f:
            ignores = yaml.safe_load(f) or {}

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    if WIKIDATA_FIELD not in fieldnames:
        print(
            f"Note: '{WIKIDATA_FIELD}' is not a column in {csv_path}, "
            f"so every matched row will report as missing."
        )

    lookups = build_lookups(rows, properties)

    for idx, row in enumerate(rows, start=1):
        if idx < start:
            continue
        prism_id = row.get("prism_id", None)

        wikidata_val = None
        matched_on = None
        for prop, (our_field, _) in properties.items():
            our_id = row.get(our_field, None)
            if our_id:
                wikidata_val = lookups[prop].get(our_id, None)
            if wikidata_val:
                matched_on = our_field
                break
        if not wikidata_val:
            # not in Wikidata, skip
            continue
        matches += 1

        is_ignore_key = False
        if ignores.get(prism_id, None):
            if (
                WIKIDATA_FIELD in ignores[prism_id]
                or ignores[prism_id] == WIKIDATA_FIELD
            ):
                is_ignore_key = True
        our_val = row.get(WIKIDATA_FIELD, None)

        if our_val == wikidata_val:
            continue

        if not our_val:
            # present in Wikidata, not in PRISM
            print(
                f"Row {idx}, {prism_id}: Missing {WIKIDATA_FIELD}, "
                f"Wikidata has {wikidata_val} (matched on {matched_on}). "
                f"Ignoring: {is_ignore_key}"
            )
        else:
            # Mismatch
            print(
                f"Row {idx}, {prism_id}: "
                f"Diff {WIKIDATA_FIELD}. Wikidata: {wikidata_val}, Prism: {our_val} "
                f"(matched on {matched_on}). Ignoring: {is_ignore_key}"
            )
        if not is_ignore_key:
            issues.append(
                {
                    "prism_id": prism_id,
                    "last_name": row["last_name"],
                    "first_name": row["first_name"],
                    "matched_on": matched_on,
                    "wikidata_value": wikidata_val,
                    "prism_value": our_val,
                }
            )

    skipped = len(rows) - matches
    if issues:
        if not quiet:
            print(
                f"{len(issues)} differences found. "
                f"Matched: {matches}, skipped {skipped}"
            )
        if issues_file:
            write_issues_txt(issues, issues_file)
        else:
            sys.exit(1)
    else:
        print(f"No mismatches found. Matched: {matches}, skipped {skipped}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Detect mismatches between Prism IDs and Wikidata"
    )
    parser.add_argument("csv_path", help="Path to player CSV file")
    parser.add_argument(
        "--league",
        default="nfl",
        choices=sorted(LEAGUES),
        help="Which league's join properties to use",
    )
    parser.add_argument(
        "--start", type=int, default=1, help="Row number to start at (1-based)"
    )
    parser.add_argument("--quiet", action="store_true", help="Essential output only")
    parser.add_argument(
        "--issues-file", help="Create an issues file (for creating a GitHub Issue)"
    )
    parser.add_argument(
        "--ignores-file",
        type=str,
        required=False,
        help="Path to a YAML file containing a dictionary of player IDs mapped to "
        "lists of keys that should be ignored/skipped",
    )
    args = parser.parse_args()
    validate_csv(
        args.csv_path,
        args.league,
        args.start,
        args.quiet,
        args.issues_file,
        args.ignores_file,
    )
