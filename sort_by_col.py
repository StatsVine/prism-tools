import argparse
import csv


def sort_csv_by_prism_id(
    input_path,
    field: str,
    output_path=None,
):
    with open(input_path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        header = reader.fieldnames

    rows.sort(key=lambda row: row[field].strip().lower())

    out_path = output_path or input_path
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Sorted CSV written to {out_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sort CSV by specified field.")
    parser.add_argument("input", help="Path to the input CSV file.")
    parser.add_argument("--field", default="prism_id", help="Specify sort field")
    parser.add_argument("--output", help="Optional path for sorted output file.")
    args = parser.parse_args()

    sort_csv_by_prism_id(args.input, args.field, args.output)
