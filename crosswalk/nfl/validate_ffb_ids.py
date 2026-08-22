import argparse
import csv
import io
import sys
from collections import defaultdict
from datetime import datetime

import requests
import yaml


FFB_IDS_URL = (
    "https://raw.githubusercontent.com/mayscopeland/ffb_ids/main/player_ids.csv"
)

# TODO externalize these in a yaml config?
# Every column ffb_ids and PRISM both carry. nffc_id is the reason to be here --
# the only upstream map for that column we have found -- but the rest are worth
# comparing too, since an id that is right in three places and wrong in a fourth
# is exactly what a crosswalk should notice.
#
# Two caveats on reading the output. This file is refreshed by hand roughly once
# a year, so it lags rather than disagrees: expect blanks on recent arrivals, not
# conflicts. And it is not an independent opinion on sleeper_id -- its own README
# says those ids come from Sleeper -- so a sleeper_id finding here should be
# checked against validate_sleeper_ids.py, which reads Sleeper first-hand.
MAPPINGS = {
    # ffb_ids key : PRISM key
    "espn_id": "espn_id",
    "nffc_id": "nffc_id",
    "sleeper_id": "sleeper_id",
    "yahoo_id": "yahoo_id",
}

# Keys we match players on, in priority order. sleeper_id leads because ffb_ids
# is keyed on it -- every row has one and none repeat -- where its yahoo and espn
# columns are only half populated. The trade is that the sleeper_id comparison
# above is then a tautology for any row that joined on it, and only bites on rows
# matched by yahoo or espn instead. Joining on the sparse columns first to avoid
# that would cost real matches, which is the worse deal.
MATCH_KEYS = {
    # PRISM key : ffb_ids column
    "sleeper_id": "sleeper_id",
    "yahoo_id": "yahoo_id",
    "espn_id": "espn_id",
}


def clean(value: str) -> str:
    return (value or "").strip()


def download_ffb_data(url: str = FFB_IDS_URL):
    response = requests.get(url)
    # Force UTF-8 encoding, requests defaults to ISO-8859-1 if no charset is included
    response.encoding = "utf-8"
    if not response.ok:
        print(f"Failed to fetch CSV. Status code: {response.status_code}")
        response.raise_for_status()

    reader = csv.DictReader(io.StringIO(response.text))
    return list(reader)


def build_index(ffb_data: list[dict], key: str) -> dict:
    """
    Index ffb_ids rows by one join key.

    A handful of ids are reused across rows, so drop any ambiguous key rather
    than picking a winner -- the caller falls through to the next key instead.
    """
    rows_by_value = defaultdict(list)
    for row in ffb_data:
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
    - ffb_value
    - prism_value
    """
    now = datetime.utcnow().isoformat()
    header = [
        "## 📃 ffb_ids differences found",
        "",
        f"_Generated {now} UTC_",
        "",
        "The following ffb_ids mismatches found.",
        "",
        "| Prism ID | Name         | Key       | ffb_ids Value | PRISM Value |",
        "|----------|--------------|-----------|---------------|-------------|",
    ]

    table_rows = []
    for issue in issues:
        table_rows.append(
            f"| {issue['prism_id']} | {issue['last_name']}, {issue['first_name']} "
            f"| {issue['prism_key']} | {issue['ffb_value']} "
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

    ffb_data = download_ffb_data()
    ffb_by_key = {
        our_key: build_index(ffb_data, ffb_key)
        for our_key, ffb_key in MATCH_KEYS.items()
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
                    found = ffb_by_key[key].get(our_id, None)
                if found:
                    break
            if not found:
                # ffb_ids covers draft-relevant players only -- no individual
                # defensive players, per its README -- so an absent row is
                # expected rather than a finding. Skip.
                continue
            else:
                matches += 1

            # Check our ID mappings against ffb_ids's items
            for ffb_key, our_key in MAPPINGS.items():
                is_ignore_key = False
                if ignores.get(prism_id, None):
                    if our_key in ignores[prism_id] or ignores[prism_id] == our_key:
                        is_ignore_key = True
                ffb_val = clean(found.get(ffb_key, ""))
                our_val = row.get(our_key, None)

                if not our_val and ffb_val:
                    # present in ffb_ids, not in PRISM
                    print(
                        f"Row {idx}, {prism_id}: Missing {our_key}, "
                        f"ffb_ids has {ffb_val}. Ignoring: {is_ignore_key}"
                    )
                    if not is_ignore_key:
                        issues.append(
                            {
                                "prism_id": prism_id,
                                "last_name": row["last_name"],
                                "first_name": row["first_name"],
                                "prism_key": our_key,
                                "ffb_value": ffb_val,
                                "prism_value": our_val,
                            }
                        )
                elif ffb_val and ffb_val != our_val:
                    # Mismatch
                    print(
                        f"Row {idx}, {prism_id}: "
                        f"Diff {our_key}. ffb_ids: {ffb_val}, Prism: {our_val}. "
                        f"Ignoring: {is_ignore_key}"
                    )
                    if not is_ignore_key:
                        issues.append(
                            {
                                "prism_id": prism_id,
                                "last_name": row["last_name"],
                                "first_name": row["first_name"],
                                "prism_key": our_key,
                                "ffb_value": ffb_val,
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
        description="Detect mismatches between Prism IDs and ffb_ids"
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
