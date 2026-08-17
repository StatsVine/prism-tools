"""
Check that prism_id values are globally unique across every league CSV.

validate_players.py runs once per league file, so its uniqueness check only
covers a single CSV. This script loads them all at once and fails if any
prism_id appears more than once, whether within one file or across leagues.
"""

import argparse
import csv
import sys
from collections import defaultdict
from pathlib import Path


DEFAULT_DATA_DIR = "data"
DEFAULT_PATTERN = "*/players.csv"


def discover_csvs(data_dir: Path, pattern: str) -> list[Path]:
    return sorted(data_dir.glob(pattern))


def describe_row(row: dict) -> str:
    name = row.get("name", "").strip()
    if name:
        return name
    last = row.get("last_name", "").strip()
    first = row.get("first_name", "").strip()
    return ", ".join(p for p in (last, first) if p) or "<unnamed>"


def load_ids(csv_path: Path) -> tuple[dict[str, list[str]], int]:
    """Map each prism_id to a list of 'path:row (name)' locations."""
    locations = defaultdict(list)
    count = 0
    with csv_path.open(newline="") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader, start=2):  # start=2 to account for header
            count += 1
            prism_id = row.get("prism_id", "").strip()
            if not prism_id:
                continue
            locations[prism_id].append(f"{csv_path}:{i} ({describe_row(row)})")
    return locations, count


def check_global_ids(csv_paths: list[Path]) -> list[str]:
    errors = []
    all_locations = defaultdict(list)

    for csv_path in csv_paths:
        locations, count = load_ids(csv_path)
        print(f"Loaded {count} rows from {csv_path}")
        for prism_id, where in locations.items():
            all_locations[prism_id].extend(where)

    for prism_id, where in sorted(all_locations.items()):
        if len(where) > 1:
            errors.append(
                f"Duplicate prism_id '{prism_id}' in {len(where)} rows:\n  "
                + "\n  ".join(where)
            )

    print(f"Checked {len(all_locations)} distinct prism_ids")
    return errors


def main(args):
    if args.csv:
        csv_paths = [Path(p) for p in args.csv]
    else:
        csv_paths = discover_csvs(Path(args.data_dir), args.pattern)

    missing = [p for p in csv_paths if not p.is_file()]
    if missing:
        print("❌ No such file(s): " + ", ".join(str(p) for p in missing))
        sys.exit(2)

    if not csv_paths:
        print(f"❌ No CSVs matched '{args.pattern}' under '{args.data_dir}'")
        sys.exit(2)

    errors = check_global_ids(csv_paths)

    if errors:
        print(f"\nValidation failed with {len(errors)} errors:\n")
        for e in errors:
            print(e)
        sys.exit(1)
    else:
        print("Validation successful ✅")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Check prism_id global uniqueness across all league CSVs"
    )
    parser.add_argument(
        "--csv",
        metavar="PATH",
        nargs="+",
        help="Player CSVs to check (default: discover under --data-dir)",
    )
    parser.add_argument(
        "--data-dir", default=DEFAULT_DATA_DIR, help="Root directory of league data"
    )
    parser.add_argument(
        "--pattern",
        default=DEFAULT_PATTERN,
        help="Glob for player CSVs relative to --data-dir",
    )
    args = parser.parse_args()

    main(args)
