import argparse
import csv
import gzip
import json
import os
import shutil
from pathlib import Path

import yaml


def load_yaml(path):
    with open(path, "r") as f:
        return yaml.safe_load(f)


def load_fields(core_schema_path, source_schema_path):
    core_schema = load_yaml(core_schema_path)["fields"]
    source_schema = load_yaml(source_schema_path)["players"]

    return list(core_schema.keys()) + [
        s["id_field"] for s in source_schema if s.get("active", True)
    ]


def gzip_compress(input_file):
    with open(input_file, "rb") as f_in:
        with open(Path(input_file.parent) / (input_file.name + ".gz"), "wb") as f_out:
            with gzip.GzipFile(fileobj=f_out, mode="wb", mtime=0) as gz:
                shutil.copyfileobj(f_in, gz)


def load_csv(filepath):
    with open(filepath, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return list(reader)


def write_csv(data, filename, fields):
    with open(filename, "w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fields, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(data)
    return filename


def write_json(data, filepath, minified=False):
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=None if minified else 2, ensure_ascii=False)
    return filepath


def write_ndjson(data, filepath):
    with open(filepath, "w", encoding="utf-8") as f:
        for row in data:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")
    return filepath


# def write_parquet(data, filename):
#    df = pd.DataFrame(data)
#    df.to_parquet(filename, engine='pyarrow', compression='snappy')


def write_id_mappings(data, output_dir, id_fields):
    os.makedirs(output_dir, exist_ok=True)
    for id_field in id_fields:
        mapping = {}
        for row in data:
            id_value = row.get(id_field)
            if id_value:
                mapping[id_value] = row
        if not mapping:
            continue
        gzip_compress(
            write_json(
                mapping, Path(output_dir) / f"players.{id_field}.json", minified=False
            )
        )
        gzip_compress(
            write_json(
                mapping,
                Path(output_dir) / f"players.{id_field}.min.json",
                minified=True,
            )
        )


def write_all(data, output_dir, fields):
    write_csv(data, output_dir / "players.csv", fields)
    gzip_compress(output_dir / "players.csv")

    write_json(data, output_dir / "players.json", minified=False)
    gzip_compress(output_dir / "players.json")

    write_json(data, output_dir / "players.min.json", minified=True)
    gzip_compress(output_dir / "players.min.json")

    write_ndjson(data, output_dir / "players.ndjson")
    gzip_compress(output_dir / "players.ndjson")

    # write_parquet(data, output_dir / "players.parquet")

    # Detect all *_id columns
    id_fields = [key for key in data[0] if key and key.endswith("_id")]
    write_id_mappings(data, output_dir / "by_id", id_fields)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("csv", help="Path to validated players.csv")
    parser.add_argument("output_dir", help="Where to write the output files")
    parser.add_argument("--core-schema", default="schema/players.yaml")
    parser.add_argument("--source-schema", default="schema/leagues/mlb/sources.yaml")
    args = parser.parse_args()

    fields = load_fields(args.core_schema, args.source_schema)

    os.makedirs(args.output_dir, exist_ok=True)
    full_dir = (Path(args.output_dir) / "players") / "full"
    os.makedirs(full_dir, exist_ok=True)
    ids_dir = (Path(args.output_dir) / "players") / "ids"
    os.makedirs(ids_dir, exist_ok=True)

    data = load_csv(args.csv)
    # Skip non-active fields
    data = [{k: d[k] or None for k in fields if k in d} for d in data]

    # Full Output formats
    write_all(data, full_dir, fields)

    # Strip non-id cols
    for d in data:
        for key in list(d.keys()):
            if key and not key.endswith("_id"):
                del d[key]
    write_all(data, ids_dir, fields)


if __name__ == "__main__":
    main()
