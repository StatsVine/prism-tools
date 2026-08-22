import argparse
import csv
import io
import sys
from collections import defaultdict
from datetime import datetime

import requests
import yaml


# The Sleeper players endpoint is a 14MB download the docs ask you to pull at
# most once a day, so hit our own git-scraped snapshot instead. The `-ids` view
# keeps every player Sleeper has ever carried -- retired ones included -- which
# is what a crosswalk needs to look up.
SLEEPER_URL = (
    "https://raw.githubusercontent.com/statsvine/data-snapshots/main/"
    "csv/sleeper/players-nfl-ids.csv"
)

# TODO externalize these in a yaml config?
# Sleeper is first-hand for sleeper_id and publishes four more of our sources.
# gsis and espn overlap validate_nflverse_ids.py, sportradar and yahoo overlap
# validate_dynastyprocess_ids.py -- kept deliberately, so a disagreement between
# two independent sources shows up as two issues rather than none. Trim this
# dict if the duplicate reporting stops being worth it.
MAPPINGS = {
    # Sleeper column : PRISM key
    "_key": "sleeper_id",
    "espn_id": "espn_id",
    "gsis_id": "gsis_id",
    "sportradar_id": "sportradar_id",
    "yahoo_id": "yahoo_id",
}

# Keys we match players on, in priority order. sleeper_id comes last on purpose:
# joining on it would make the sleeper_id comparison a tautology, and checking
# that id is the main reason to consult Sleeper directly. sportradar_id leads --
# it is a uuid, so a wrong one cannot quietly collide with another player, and
# Sleeper populates it far more widely than gsis.
MATCH_KEYS = {
    # PRISM key : Sleeper column
    "sportradar_id": "sportradar_id",
    "gsis_id": "gsis_id",
    "espn_id": "espn_id",
    "yahoo_id": "yahoo_id",
    "sleeper_id": "_key",
}


def clean(value: str) -> str:
    return (value or "").strip()


def download_sleeper_data(url: str = SLEEPER_URL):
    response = requests.get(url)
    # Force UTF-8 encoding, requests defaults to ISO-8859-1 if no charset is included
    response.encoding = "utf-8"
    if not response.ok:
        print(f"Failed to fetch CSV. Status code: {response.status_code}")
        response.raise_for_status()

    reader = csv.DictReader(io.StringIO(response.text))
    # Sleeper carries team defenses in the same map, keyed by team abbreviation
    # ("ARI") rather than a numeric id. They are not players, so drop them.
    return [row for row in reader if clean(row.get("_key", "")).isdigit()]


def build_index(sleeper_data: list[dict], key: str) -> dict:
    """
    Index Sleeper rows by one join key.

    A handful of ids are reused across rows, so drop any ambiguous key rather
    than picking a winner -- the caller falls through to the next key instead.
    """
    rows_by_value = defaultdict(list)
    for row in sleeper_data:
        value = clean(row.get(key, ""))
        if not value:
            continue
        rows_by_value[value].append(row)
    return {value: rows[0] for value, rows in rows_by_value.items() if len(rows) == 1}


def write_issues_txt(issues: list[dict], outfile_path: str = "issues.txt") -> None:
    """
    Writes a markdown-formatted issues.txt file based on ID discrepancies.

    Each issue should be a dict with:
    - prism_id
    - last_name
    - first_name
    - prism_key
    - sleeper_value
    - prism_value
    """
    now = datetime.utcnow().isoformat()
    header = [
        "## 📃 Sleeper differences found",
        "",
        f"_Generated {now} UTC_",
        "",
        "The following Sleeper mismatches found.",
        "",
        "| Prism ID | Name         | Key       | Sleeper Value | PRISM Value |",
        "|----------|--------------|-----------|---------------|-------------|",
    ]

    table_rows = []
    for issue in issues:
        table_rows.append(
            f"| {issue['prism_id']} | {issue['last_name']}, {issue['first_name']} "
            f"| {issue['prism_key']} | {issue['sleeper_value']} "
            f"| {issue.get('prism_value', '')} |"
        )

    with open(outfile_path, "w") as f:
        f.write("\n".join(header + table_rows))


def validate_csv(
    csv_path: str,
    start: int = 1,
    quiet: bool = False,
    issues_file: str = None,
    ignores_file: str = None,
):
    issues = []
    matches = 0
    rows = 0

    ignores = {}
    if ignores_file:
        with open(ignores_file, "r") as f:
            ignores = yaml.safe_load(f) or {}

    sleeper_data = download_sleeper_data()
    sleeper_by_key = {
        our_key: build_index(sleeper_data, sleeper_key)
        for our_key, sleeper_key in MATCH_KEYS.items()
    }

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            rows += 1
            if idx < start:
                continue
            prism_id = row.get("prism_id", None)

            found = None
            for key in MATCH_KEYS:
                our_id = row.get(key, None)
                if our_id:
                    found = sleeper_by_key[key].get(our_id, None)
                if found:
                    break
            if not found:
                # not in Sleeper, skip
                continue
            else:
                matches += 1

            # Check our ID mappings against Sleeper's items
            for sleeper_key, our_key in MAPPINGS.items():
                is_ignore_key = False
                if ignores.get(prism_id, None):
                    if our_key in ignores[prism_id] or ignores[prism_id] == our_key:
                        is_ignore_key = True
                sleeper_val = clean(found.get(sleeper_key, ""))
                our_val = row.get(our_key, None)

                if not our_val and sleeper_val:
                    # present in Sleeper, not in PRISM
                    print(
                        f"Row {idx}, {prism_id}: Missing {our_key}, "
                        f"Sleeper has {sleeper_val}. Ignoring: {is_ignore_key}"
                    )
                    if not is_ignore_key:
                        issues.append(
                            {
                                "prism_id": prism_id,
                                "last_name": row["last_name"],
                                "first_name": row["first_name"],
                                "prism_key": our_key,
                                "sleeper_value": sleeper_val,
                                "prism_value": our_val,
                            }
                        )
                elif sleeper_val and sleeper_val != our_val:
                    # Mismatch
                    print(
                        f"Row {idx}, {prism_id}: "
                        f"Diff {our_key}. Sleeper: {sleeper_val}, Prism: {our_val}. "
                        f"Ignoring: {is_ignore_key}"
                    )
                    if not is_ignore_key:
                        issues.append(
                            {
                                "prism_id": prism_id,
                                "last_name": row["last_name"],
                                "first_name": row["first_name"],
                                "prism_key": our_key,
                                "sleeper_value": sleeper_val,
                                "prism_value": our_val,
                            }
                        )

    if issues:
        if not quiet:
            print(
                f"{len(issues)} differences found. "
                f"Matched: {matches}, skipped {rows - matches}"
            )
        if issues_file:
            write_issues_txt(issues, issues_file)
        else:
            sys.exit(1)
    else:
        print(f"No mismatches found. Matched: {matches}, skipped {rows - matches}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Detect mismatches between Prism IDs and Sleeper"
    )
    parser.add_argument("csv_path", help="Path to player CSV file")
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
        args.csv_path, args.start, args.quiet, args.issues_file, args.ignores_file
    )
