import argparse
import csv
import io
import sys
from datetime import datetime

import requests
import yaml


NFLVERSE_URL = (
    "https://github.com/nflverse/nflverse-data/releases/download/players/players.csv"
)

# TODO externalize these in a yaml config?
# nflverse only publishes these three of our source ids. sleeper, sportradar and
# yahoo live in the DynastyProcess player id map, nffc and wikidata in neither.
MAPPINGS = {
    # nflverse key : PRISM key
    "gsis_id": "gsis_id",
    "pfr_id": "pfr_id",
    "espn_id": "espn_id",
}

# Keys we match players on, in priority order. Rookies frequently have no gsis_id
# until late summer, so fall back to the other ids before giving up.
MATCH_KEYS = ["gsis_id", "pfr_id", "espn_id"]


def download_nflverse_data(url: str = NFLVERSE_URL):
    response = requests.get(url)
    # Force UTF-8 encoding, requests defaults to ISO-8859-1 if no charset is included
    response.encoding = "utf-8"
    if not response.ok:
        print(f"Failed to fetch CSV. Status code: {response.status_code}")
        response.raise_for_status()

    reader = csv.DictReader(io.StringIO(response.text))
    return list(reader)


def write_issues_txt(issues: list[dict], outfile_path: str = "issues.txt") -> None:
    """
    Writes a markdown-formatted issues.txt file based on ID discrepancies.

    Each issue should be a dict with:
    - prism_id
    - last_name
    - first_name
    - prism_key
    - nflverse_value
    - prism_value
    """
    now = datetime.utcnow().isoformat()
    header = [
        "## 📃 nflverse differences found",
        "",
        f"_Generated {now} UTC_",
        "",
        "The following nflverse mismatches found.",
        "",
        "| Prism ID | Name         | Key       | nflverse Value | PRISM Value |",
        "|----------|--------------|-----------|----------------|-------------|",
    ]

    table_rows = []
    for issue in issues:
        table_rows.append(
            f"| {issue['prism_id']} | {issue['last_name']}, {issue['first_name']} "
            f"| {issue['prism_key']} | {issue['nflverse_value']} "
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

    nflverse_data = download_nflverse_data()
    nflverse_by_key = {
        key: {r[key]: r for r in nflverse_data if len(r.get(key, "")) > 0}
        for key in MATCH_KEYS
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
                    found = nflverse_by_key[key].get(our_id, None)
                if found:
                    break
            if not found:
                # not in nflverse, skip
                continue
            else:
                matches += 1

            # Check our ID mappings against nflverse's items
            for nflverse_key, our_key in MAPPINGS.items():
                is_ignore_key = False
                if ignores.get(prism_id, None):
                    if our_key in ignores[prism_id] or ignores[prism_id] == our_key:
                        is_ignore_key = True
                nflverse_val = found.get(nflverse_key, None)
                our_val = row.get(our_key, None)

                if not our_val and nflverse_val:
                    # present in nflverse, not in PRISM
                    print(
                        f"Row {idx}, {prism_id}: Missing {our_key}, "
                        f"nflverse has {nflverse_val}. Ignoring: {is_ignore_key}"
                    )
                    if not is_ignore_key:
                        issues.append(
                            {
                                "prism_id": prism_id,
                                "last_name": row["last_name"],
                                "first_name": row["first_name"],
                                "prism_key": our_key,
                                "nflverse_value": nflverse_val,
                                "prism_value": our_val,
                            }
                        )
                elif nflverse_val and nflverse_val != our_val:
                    # Mismatch
                    print(
                        f"Row {idx}, {prism_id}: "
                        f"Diff {our_key}. nflverse: {nflverse_val}, Prism: {our_val}. "
                        f"Ignoring: {is_ignore_key}"
                    )
                    if not is_ignore_key:
                        issues.append(
                            {
                                "prism_id": prism_id,
                                "last_name": row["last_name"],
                                "first_name": row["first_name"],
                                "prism_key": our_key,
                                "nflverse_value": nflverse_val,
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
        description="Detect mismatches between Prism IDs and nflverse"
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
