import csv
import hashlib
from base64 import b32encode


# Our Base32 uses a custom alphabet: we sanitize standard base32
STATSVINE_B32_ALPHABET = "0123456789bcdefghjklmnpqrstvwxyz"
STANDARD_B32_ALPHABET = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567"


def statsvine_base32(data: bytes) -> str:
    encoded = b32encode(data).decode("utf-8").lower()
    trans = str.maketrans(STANDARD_B32_ALPHABET.lower(), STATSVINE_B32_ALPHABET)
    sanitized = encoded.translate(trans)
    return sanitized.rstrip("=")


def generate_id(prefix: str, row: dict, fields: list[str], digest_bytes: int) -> str:
    values = (
        [
            prefix,
        ]
        if prefix
        else []
    )
    values += [row.get(f, "").strip().lower() for f in fields]
    hash_input = "|".join(values)
    digest = hashlib.blake2b(
        hash_input.encode("utf-8"), digest_size=digest_bytes
    ).digest()
    print(f"Hashed {hash_input} to {statsvine_base32(digest)}")
    return statsvine_base32(digest)


def process_csv(
    file_path: str,
    id_field="prism_id",
    id_fields=["player_name", "birth_year"],
    digest_bytes: int = 6,
    prefix: str = None,
    force: bool = False,
):
    updated_rows = []
    changed = False

    with open(file_path, "r", newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        if id_field not in fieldnames:
            fieldnames = [id_field] + fieldnames
        for row in reader:
            if not row.get(id_field) or force:
                row[id_field] = generate_id(prefix, row, id_fields, digest_bytes)
                changed = True
            updated_rows.append(row)

    if changed:
        with open(file_path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(updated_rows)
        print(f"Updated file: {file_path}")
    else:
        print("No missing IDs found. No changes made.")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Generate missing IDs.")
    parser.add_argument(
        "--csv", default="data/mlb/players.csv", help="Path to player CSV"
    )
    parser.add_argument(
        "--id-field", default="prism_id", help="Field name for generated ID"
    )
    parser.add_argument(
        "--id-fields",
        nargs="+",
        default=[
            "last_name",
            "first_name",
            "middle_name",
            "birth_year",
            "birth_month",
            "birth_day",
        ],
        help="Fields to hash",
    )
    parser.add_argument(
        "--digest-bytes", default=6, help="ID size (in bytes)", type=int
    )
    parser.add_argument(
        "--prefix",
        default=None,
        required=False,
        help="Prefix to add to fields pre-hash",
    )
    parser.add_argument(
        "--force",
        action=argparse.BooleanOptionalAction,
        help="Force regenerate all ids",
    )
    args = parser.parse_args()

    process_csv(
        args.csv,
        id_field=args.id_field,
        id_fields=args.id_fields,
        digest_bytes=args.digest_bytes,
        prefix=args.prefix,
        force=args.force,
    )
