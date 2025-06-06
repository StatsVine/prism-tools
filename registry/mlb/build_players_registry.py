import argparse
import csv
import importlib
import json
from collections import defaultdict
from pathlib import Path

import yaml
from jinja2 import Template


def load_yaml(registry_path):
    with open(registry_path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def load_json(crosswalk_path):
    with open(crosswalk_path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_source_module(name):
    module_path = f"sources.{name}"
    return importlib.import_module(module_path)


def preprocess_source(data, transformations):
    for transform in transformations:
        field = transform["field"]
        template = transform["template"]
        data[field] = Template(template).render(**data).strip()
    return data


def build_intermediate(crosswalk, registry):
    intermediates = defaultdict(dict)
    for player in crosswalk:
        intermediates[player["prism_id"]]["crosswalk"] = player
    for source_name, source_conf in registry["sources"].items():
        pivot_field = source_conf["crosswalk_key"]
        module = load_source_module(source_name)
        print(f"Loading data from source: {source_name}. Pivot field: {pivot_field}")
        data = module.load()  # Assumes each loader defines `load() -> dict`
        for player in intermediates.values():
            field_val = player.get("crosswalk", {}).get(pivot_field, None)
            player_data = data.get(field_val, {})
            if player_data:
                player_data = preprocess_source(
                    player_data, source_conf.get("preprocess", [])
                )
            player[source_name] = player_data
    return intermediates


def get_nested(data, path):
    for key in path.split("."):
        data = data.get(key, {})
    return data if data else None


def transform_field(intermediate, sources):
    if isinstance(sources, str):
        return get_nested(intermediate, sources)
    for source in sources:
        value = get_nested(intermediate, source)
        if value:
            return value
    return value


def transform_record(intermediate, mapping):
    return {m["dest"]: transform_field(intermediate, m["src"]) for m in mapping}


def transform_records(intermediates, registry):
    outputs = []
    for intermediate in intermediates.values():
        output = dict(intermediate["crosswalk"])
        output.update(transform_record(intermediate, registry["mappings"]))
        outputs.append(output)
    return outputs


def parse_fieldsets(registry):
    input_fieldsets = registry["fieldsets"]
    fieldsets = {}

    # Get names + fields
    for name, fieldset in input_fieldsets.items():
        fieldsets[name] = list(fieldset.get("fields", []))

    # Do fieldsets
    for name, fieldset in input_fieldsets.items():
        for src_fieldset in fieldset.get("fieldsets", []):
            fieldsets[name].extend(fieldsets[src_fieldset])

    # Get unique fields, preserving order
    return {f: list(dict.fromkeys(v)) for f, v in fieldsets.items()}


def parse_product_fields(product, fieldsets):
    fields = []

    if not product:
        return fields

    # Load fieldset fields
    for fieldset in product.get("fieldsets", []):
        fields.extend(fieldsets[fieldset])

    # Append fields
    fields.extend(product.get("fields", []))

    # Get unique fields preserving order
    return list(dict.fromkeys(fields))


def nest_fields(flat_row):
    nested = {}
    for k, v in flat_row.items():
        parts = k.split(".")
        current = nested
        for part in parts[:-1]:
            current = current.setdefault(part, {})
        current[parts[-1]] = v
    return nested


def filter_and_nest_row(row, fields):
    return nest_fields({f: row.get(f, None) for f in fields})


def filter_and_nest_rows(rows, fields):
    return [filter_and_nest_row(r, fields) for r in rows]


def write_outputs(name, output_dir, transformed, fields):
    output_dir.mkdir(parents=True, exist_ok=True)

    # CSV
    with open(output_dir / f"{name}.csv", "w", newline="") as f:
        writer = csv.DictWriter(
            f, extrasaction="ignore", fieldnames=[f.replace(".", "_") for f in fields]
        )
        writer.writeheader()
        for row in transformed:
            filtered = {f.replace(".", "_"): row.get(f, "") for f in fields}
            writer.writerow(filtered)

    # Filter data + convert dot notation to nested objects
    filtered_data = filter_and_nest_rows(transformed, fields)

    # Pretty JSON
    with open(output_dir / f"{name}.json", "w") as f:
        json.dump(filtered_data, f, indent=2, ensure_ascii=False)

    # Minified JSON
    with open(output_dir / f"{name}.min.json", "w") as f:
        json.dump(filtered_data, f, separators=(",", ":"), ensure_ascii=False)

    # NDJSON
    with open(output_dir / f"{name}.ndjson", "w") as f:
        for row in filtered_data:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")


def write_pivot(product_name, pivot_name, pivot_dir, pivot_field, data, output_fields):
    pivot_on = pivot_field["field"]
    # TODO don't see needing below 2 levels of nesting but...
    # making pivots recursive would be cleaner
    pivot_subfield = pivot_field.get("subfield", None)
    pivot_field_name = pivot_field["name"]
    is_array = pivot_field.get("is_array", True)
    null_key = pivot_field.get("null_key", None)

    if pivot_subfield:
        outputs = defaultdict(lambda: defaultdict(list if is_array else None))
    else:
        outputs = defaultdict(list if is_array else None)
    for row in data:
        row_key = row.get(pivot_on, None)
        row_key = row_key if row_key else null_key
        if not row_key:
            continue

        if pivot_subfield:
            parent = outputs[row_key]
            row_key = row.get(pivot_subfield, None)
            row_key = row_key if row_key else null_key
        else:
            parent = outputs

        row_data = filter_and_nest_row(row, output_fields)
        if is_array:
            parent[row_key].append(row_data)
        else:
            # TODO consider warning if we're overwriting an existing key
            parent[row_key] = row_data

    sorted_output = dict(sorted((str(k), v) for k, v in outputs.items()))
    with open(pivot_dir / f"{pivot_field_name}.json", "w") as f:
        json.dump(sorted_output, f, indent=2, ensure_ascii=False)


def resolve_pivot_spec(pivot_spec, shared_pivots):
    if isinstance(pivot_spec, str):
        return shared_pivots[pivot_spec]
    elif isinstance(pivot_spec, dict):
        return pivot_spec
    else:
        raise ValueError(f"Unsupported pivot format: {pivot_spec}")


def write_pivots(name, output_dir, transformed, fields, product, shared_pivots):
    for p in product.get("pivots", []):
        pivot = resolve_pivot_spec(p, shared_pivots)
        pivot_name = pivot["name"]
        pivot_fields = pivot["fields"]
        pivot_dir = output_dir / pivot_name

        for pivot_field in pivot_fields:
            pivot_dir.mkdir(parents=True, exist_ok=True)
            write_pivot(name, pivot_name, pivot_dir, pivot_field, transformed, fields)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("crosswalk_path", help="Path to crosswalk players.json")
    parser.add_argument("output_dir", help="Where to put the output files")
    parser.add_argument(
        "--registry-file",
        default="schema/leagues/mlb/players.yaml",
        help="Registry file",
    )
    parser.add_argument(
        "--dump-intermediate",
        action="store_true",
        help="Dump intermediate data to exports/intermediate.json for debugging",
    )
    return parser.parse_args()


def main():
    args = parse_args()
    output_dir = Path(args.output_dir)
    output_dir.mkdir(exist_ok=True, parents=True)

    crosswalk = load_json(Path(args.crosswalk_path))
    registry = load_yaml(Path(args.registry_file))
    intermediate = build_intermediate(crosswalk, registry)
    if args.dump_intermediate:
        with (output_dir / "intermediate.json").open("w", encoding="utf-8") as f:
            json.dump(intermediate, f, indent=2, ensure_ascii=False)
        print("Intermediate data written to exports/intermediate.json")

    transformed = transform_records(intermediate, registry)
    fieldsets_all = parse_fieldsets(registry)

    shared_pivots = registry.get("pivots", {})
    for name, product in registry.get("products", {}).items():
        fields = parse_product_fields(product, fieldsets_all)
        product_dir = output_dir / name
        product_dir.mkdir(exist_ok=True, parents=True)

        if fields:
            write_outputs(name, product_dir, transformed, fields)
            write_pivots(name, product_dir, transformed, fields, product, shared_pivots)

    print(f"Build complete. Outputs written to {output_dir}/")


if __name__ == "__main__":
    main()
