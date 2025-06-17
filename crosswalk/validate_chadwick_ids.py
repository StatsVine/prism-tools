import argparse
import csv
import os
import sys
from datetime import datetime
from pathlib import Path

import requests
import yaml


CACHE_DIR = "cache/chadwick"

SOURCE_URL_TEMPLATE = (
    "https://raw.githubusercontent.com/chadwickbureau/register/"
    "master/data/people-{suffix}.csv"
)
HEX_SUFFIXES = [f"{i:x}" for i in range(16)]  # ['0', '1', ..., 'f']

# TODO externalize these in a yaml config?
MAPPINGS = {
    # Chadwick key : PRISM key
    "key_fangraphs": "fangraphs_id",
    "key_mlbam": "mlbam_id",
    "key_bbref": "bbref_id",
}


def load_id_map(yaml_path):
    with open(yaml_path, "r") as f:
        return yaml.safe_load(f)


def download_chadwick_data(suffix, refresh=False):
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
        # print(f"[chadwick] Using cached file for people-{suffix}")
        pass

    people = {}
    # Load from disk
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            key = row["key_mlbam"].strip()
            if key:
                people[key] = row
    return people


def write_issues_txt(issues: list[dict], outfile_path: str = "issues.txt") -> None:
    """
    Writes a markdown-formatted issues.txt file based on ID discrepancies.

    Each issue should be a dict with:
    - prism_id
    - last_name
    - first_name
    - prism_key
    - chadwick_value
    - prism_value
    """
    now = datetime.utcnow().isoformat()
    header = [
        "## 📋 Chadwick differences found",
        "",
        f"_Generated {now} UTC_",
        "",
        "The following Chadwick mismatches found.",
        "",
        "| Prism ID | Name         | Key       | Chadwick Value | PRISM Value |",
        "|----------|--------------|-----------|----------------|-------------|",
    ]

    table_rows = []
    for issue in issues:
        table_rows.append(
            f"| {issue['prism_id']} | {issue['last_name']}, {issue['first_name']} "
            f"| {issue['prism_key']} | {issue['chadwick_value']} "
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

    ignores = load_id_map(ignores_file) if ignores_file else {}

    for suffix in HEX_SUFFIXES:
        chadwick_by_mlbam = download_chadwick_data(suffix)

        with open(csv_path, newline="", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for idx, row in enumerate(reader, start=1):
                rows += 1
                if idx < start:
                    continue
                mlbam_id = row.get("mlbam_id", None)
                prism_id = row.get("prism_id", None)

                found = chadwick_by_mlbam.get(mlbam_id, None)
                if not found:
                    # not in chadwick, skip
                    continue
                else:
                    matches += 1

                # Check our ID mappings against chadwick's items
                for chadwick_key, our_key in MAPPINGS.items():
                    ignore_key = False
                    if ignores.get(prism_id, None):
                        if our_key in ignores[prism_id] or ignores[prism_id] == our_key:
                            print(f"Row {idx}, {prism_id}: Ignoring {our_key}")
                            ignore_key = True
                    chadwick_val = found.get(chadwick_key, None)
                    our_val = row.get(our_key, None)

                    if our_key == "fangraphs_id" and our_val != chadwick_val:
                        # HACK special handling of fangraphs_id differences
                        if not our_val.startswith("sa") and chadwick_val.startswith(
                            "sa"
                        ):
                            # assume our non-sa value is correct if chadwick has a sa prefix
                            continue
                    if not our_val and chadwick_val:
                        # present in Chadwick, not in PRISM
                        print(
                            f"Row {idx}, {prism_id}: Missing {our_key}, "
                            f"Chadwick has {chadwick_val}. "
                            f"ignoring: {ignore_key}"
                        )
                        if not ignore_key:
                            issues.append(
                                {
                                    "prism_id": prism_id,
                                    "last_name": row["last_name"],
                                    "first_name": row["first_name"],
                                    "prism_key": our_key,
                                    "chadwick_value": chadwick_val,
                                    "prism_value": our_val,
                                }
                            )
                    elif chadwick_val and chadwick_val != our_val:
                        # Mismatch
                        print(
                            f"Row {idx}, {prism_id}: "
                            f"Diff {our_key}, Chadwick:{chadwick_val}, Prism:{our_val}. "
                            f"ignoring: {ignore_key}"
                        )
                        if not ignore_key:
                            issues.append(
                                {
                                    "prism_id": prism_id,
                                    "last_name": row["last_name"],
                                    "first_name": row["first_name"],
                                    "prism_key": our_key,
                                    "chadwick_value": chadwick_val,
                                    "prism_value": our_val,
                                }
                            )
    rows = rows // len(HEX_SUFFIXES)
    if issues:
        if not quiet:
            print(
                f"{len(issues)} differences found. Matched {matches}, skipped {rows - matches}"
            )
        if issues_file:
            write_issues_txt(issues, issues_file)
            pass
        else:
            sys.exit(1)
    else:
        print(f"No mismatches found. Matched {matches}, skipped {rows - matches}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Detect mismatches between Prism IDs and Chadwick"
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
        help="Path to a YAML file containing a dictionary of player IDs mapped "
        "to lists of keys that should be ignored/skipped",
    )
    args = parser.parse_args()
    validate_csv(
        args.csv_path, args.start, args.quiet, args.issues_file, args.ignores_file
    )
