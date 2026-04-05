import argparse
import csv
from io import StringIO

import requests
import yaml


def load_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_fields(core_schema_path, source_schema_path):
    # Load schemas
    core_schema = load_yaml(core_schema_path)["fields"]
    source_schema = []
    if source_schema_path:
        source_schema = load_yaml(source_schema_path)["players"]

    return list(core_schema.keys()) + [s["id_field"] for s in source_schema]


def download_csv(sheet_id: str, gid: str):
    url = (
        f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv&gid={gid}"
    )
    response = requests.get(url)
    # Force UTF-8 encoding, requests defaults to ISO-8859-1 if no charset is included (as per RFC 2616)
    response.encoding = "utf-8"
    if not response.ok:
        print(f"Failed to fetch CSV. Status code: {response.status_code}")
        response.raise_for_status()
    return response.text


def write_csv(csv_text: str, output_file: str):
    with open(
        output_file,
        "w",
        encoding="utf-8",
    ) as f:
        f.write(csv_text)


def filter_fields(csv_text: str, schema_fields: list, include_unknown: bool) -> str:
    input_io = StringIO(csv_text)
    output_io = StringIO()

    reader = csv.DictReader(input_io)
    fields = reader.fieldnames if include_unknown else schema_fields

    writer = csv.DictWriter(output_io, fieldnames=fields)
    writer.writeheader()

    for row in reader:
        filtered_row = {field: row.get(field, "") for field in fields}
        writer.writerow(filtered_row)

    return output_io.getvalue()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Download a public Google Sheet as CSV"
    )
    parser.add_argument("sheet_id", help="Google Sheet ID")
    parser.add_argument("--gid", default="0", help="Sheet GID (tab id), default is 0")
    parser.add_argument("--core-schema", default="schema/players.yaml")
    parser.add_argument("--source-schema", required=False)
    parser.add_argument(
        "--include-unknown", action="store_false", help="include untracked columns"
    )
    parser.add_argument(
        "--out",
        default="data/mlb/players.csv",
        help="Output filename (default: dump.csv)",
    )

    args = parser.parse_args()
    fields = load_fields(args.core_schema, args.source_schema)
    downloaded = download_csv(args.sheet_id, args.gid)
    filtered = filter_fields(downloaded, fields, not args.include_unknown)
    write_csv(filtered, args.out)
