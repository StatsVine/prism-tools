import csv
import yaml
import re
import sys
from collections import defaultdict
from pathlib import Path

def load_yaml(path):
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def validate_field(value, rules, field_name, row_num):
    errors = []

    if 'active' in rules and not rules.get('active'):
        return errors

    # Check if required value present
    if rules.get('required') and not value:
        errors.append(f"Row {row_num}: '{field_name}' is required")
    
    # Check whitespace
    if len(value) != len(value.strip()):
        errors.append(f"Row {row_num}: '{field_name}' contains leading/trailing whitespace.")

    # Validate pattern
    if 'pattern' in rules and value:
        if not re.fullmatch(rules['pattern'], value):
            errors.append(f"Row {row_num}: '{field_name}' value '{value}' does not match pattern")

    # Check enum value valid
    if 'enum' in rules and value not in rules['enum']:
        errors.append(f"Row {row_num}: '{field_name}' value '{value}' not in allowed values")

    return errors    

def check_duplicate_ids(id_field_name: str, id_val: any, seen_ids: set, line_idx: int):
    errors = []

    if not id_val:
        return errors

    if id_val in seen_ids:
        errors.append(f"Row {line_idx}: Duplicate value '{id_val}' for column '{id_field_name}'")
    else:
        seen_ids.add(id_val)
    return errors

def validate_csv(csv_path, core_schema_path, source_schema_path, fail_fast=False):
    errors = []

    # Load schemas
    core_schema = load_yaml(core_schema_path)['fields']
    source_schema = load_yaml(source_schema_path)['players']

    seen_ids = defaultdict(set)

    with open(csv_path, newline='') as f:
        reader = csv.DictReader(f)
        prev_id = None
        for i, row in enumerate(reader, start=2):  # start=2 to account for header
            # Verify sorting
            if prev_id and row.get('prism_id') < prev_id:
                errors.append(f"Row {i}: Not sorted, ID '{row.get('prism_id')}' comes after '{prev_id}'")
                if fail_fast and errors:
                    print('\n'.join(errors))
                    sys.exit(1)
            for field, rules in core_schema.items():
                value = row.get(field, '')
                errors.extend(validate_field(value, rules, field, i))
                if rules.get('unique', False):
                    # Metadata fields assumed to not be unique
                    errors.extend(check_duplicate_ids(field, value, seen_ids[field], i))
                if fail_fast and errors:
                    print('\n'.join(errors))
                    sys.exit(1)
            for s in source_schema:
                field = s['id_field']
                rules = s
                value = row.get(field, '')
                errors.extend(validate_field(value, rules, field, i))
                if rules.get('unique', True):
                    # IDs are assumed to be unique
                    errors.extend(check_duplicate_ids(field, value, seen_ids[field], i))
                if fail_fast and errors:
                    print('\n'.join(errors))
                    sys.exit(1)
            prev_id = row['prism_id']

    if errors:
        print(f"\nValidation failed with {len(errors)} errors:\n")
        for e in errors:
            print(e)
        sys.exit(1)
    else:
        print("Validation successful ✅")

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description="Validate player CSV against schema")
    parser.add_argument('--csv', default='data/mlb/players.csv')
    parser.add_argument('--core-schema', default='schema/players.yaml')
    parser.add_argument('--source-schema', default='schema/leagues/mlb/sources.yaml')
    parser.add_argument('--fail-fast', action='store_true', help='Stop on first error')
    args = parser.parse_args()

    validate_csv(args.csv, args.core_schema, args.source_schema, fail_fast=args.fail_fast)