import argparse
import csv
import io
import sys
from datetime import datetime

import requests
import yaml


# TODO externalize these in a yaml config?
MAPPINGS = {
    # SFBB key : PRISM key
    "IDPLAYER": "sfbb_id",
    "IDFANGRAPHS": "fangraphs_id",
    "MLBID": "mlbam_id",
    "BREFID": "bbref_id",
    "NFBCID": "nfbc_id",
    "YAHOOID": "yahoo_id",
}


def sfbb_url(
    sheet_id: str = "1JgczhD5VDQ1EiXqVG-blttZcVwbZd5_Ne_mefUGwJnk", gid: str = 0
) -> str:
    url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    )
    return url


def download_sfbb_data():
    url = sfbb_url()
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
    - sfbb_value
    - prism_value
    """
    now = datetime.utcnow().isoformat()
    header = [
        "## 📃 SFBB differences found",
        "",
        f"_Generated {now} UTC_",
        "",
        "The following SFBB mismatches found.",
        "",
        "| Prism ID | Name         | Key       | SFBB Value | PRISM Value |",
        "|----------|--------------|-----------|------------|-------------|",
    ]

    table_rows = []
    for issue in issues:
        table_rows.append(
            f"| {issue['prism_id']} | {issue['last_name']}, {issue['first_name']} "
            f"| {issue['prism_key']} | {issue['sfbb_value']} "
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
            ignores = yaml.safe_load(f)

    sfbb_data = download_sfbb_data()
    sfbb_by_sfbb_id = {r["IDPLAYER"]: r for r in sfbb_data}
    sfbb_by_mlb_id = {r["MLBID"]: r for r in sfbb_data}

    with open(csv_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for idx, row in enumerate(reader, start=1):
            rows += 1
            if idx < start:
                continue
            sfbb_id = row.get("sfbb_id", None)
            mlbam_id = row.get("mlbam_id", None)
            prism_id = row.get("prism_id", None)

            found = sfbb_by_sfbb_id.get(sfbb_id, None) or sfbb_by_mlb_id.get(
                mlbam_id, None
            )
            if not found:
                # not in SFBB, skip
                continue
            else:
                matches += 1

            # Check our ID mappings against SFBB's items
            for sfbb_key, our_key in MAPPINGS.items():
                is_ignore_key = False
                if ignores.get(prism_id, None):
                    if our_key in ignores[prism_id] or ignores[prism_id] == our_key:
                        is_ignore_key = True
                sfbb_val = found.get(sfbb_key, None)
                our_val = row.get(our_key, None)

                if our_key == "fangraphs_id" and our_val != sfbb_val:
                    # HACK special handling of fangraphs_id differences
                    if not our_val.startswith("sa") and sfbb_val.startswith("sa"):
                        # assume our value is correct if sfbb has a sa prefix still and we don't
                        continue
                if our_key == "bbref_id" and our_val != sfbb_val:
                    # HACK ignore our missing bbref id, presumably it's a minor league player
                    if our_val == "" and sfbb_val:
                        continue
                if not our_val and sfbb_val:
                    # present in SFBB, not in PRISM
                    print(
                        f"Row {idx}, {prism_id}: Missing {our_key}, SFBB has {sfbb_val}. "
                        f"Ignoring: {is_ignore_key}"
                    )
                    if not is_ignore_key:
                        issues.append(
                            {
                                "prism_id": prism_id,
                                "last_name": row["last_name"],
                                "first_name": row["first_name"],
                                "prism_key": our_key,
                                "sfbb_value": sfbb_val,
                                "prism_value": our_val,
                            }
                        )
                elif sfbb_val and sfbb_val != our_val:
                    # Mismatch
                    print(
                        f"Row {idx}, {prism_id}: "
                        f"Diff {our_key}. SFBB: {sfbb_val}, Prism: {our_val}. "
                        f"Ignoring: {is_ignore_key}"
                    )
                    if not is_ignore_key:
                        issues.append(
                            {
                                "prism_id": prism_id,
                                "last_name": row["last_name"],
                                "first_name": row["first_name"],
                                "prism_key": our_key,
                                "sfbb_value": sfbb_val,
                                "prism_value": our_val,
                            }
                        )

    if issues:
        if not quiet:
            print(
                f"{len(issues)} differences found. Matched: {matches}, skipped {rows - matches}"
            )
        if issues_file:
            write_issues_txt(issues, issues_file)
        else:
            sys.exit(1)
    else:
        print(f"No mismatches found. Matched: {matches}, skipped {rows - matches}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Detect mismatches between Prism IDs and SFBB"
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
