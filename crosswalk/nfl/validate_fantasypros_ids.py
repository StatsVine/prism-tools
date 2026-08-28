"""Compare our NFL ids against FantasyPros.

FantasyPros is the canonical source for two of our columns -- fantasypros_id
(its API key) and fantasypros_slug (its web key) -- so a blank or disagreeing
value in either is ours to fix, not to suppress. The three columns it shares
with us -- sportradar_id (published as `sportsdata_player_id`), espn_id and
yahoo_id -- are corroboration rather than authority.

Players are joined on sportradar_id where possible, which keeps the
fantasypros_id, slug, espn and yahoo comparisons independent of the join. See
the note on MATCH_KEYS for when that does not hold.
"""

import argparse
import csv
import re
import sys
from collections import defaultdict
from datetime import datetime

import requests
import yaml


# ---------------------------------------------------------------------------
# Fetch layer.
#
# This is the only part of the script that knows FantasyPros' field naming.
# Everything below works on the normalised dicts fetch_fantasypros_data returns.
# ---------------------------------------------------------------------------

# Our own git-scraped snapshot rather than the FantasyPros API directly, the
# same arrangement as validate_sleeper_ids.py. 8534 NFL players, one flat JSON
# object each, no pagination and no auth.
FANTASYPROS_URL = (
    "https://raw.githubusercontent.com/StatsVine/data-snapshots/main/"
    "data/fantasypros/players-nfl.json"
)

# FantasyPros does not publish a slug field. It publishes `filename`, the full
# player-page URL, and the slug is its last path segment minus the extension:
#   https://www.fantasypros.com/nfl/players/cam-skattebo.php -> cam-skattebo
# Verified over all 8534 records: every one yields a slug, all are unique, and
# all satisfy the fantasypros_slug pattern in the NFL source schema.
SLUG_RE = re.compile(r"([^/]+)\.php$")


def fetch_fantasypros_data(url: str = FANTASYPROS_URL) -> list[dict]:
    """Pull every NFL player from the snapshot and normalise the records.

    Returns a list of flat dicts keyed by our own column names, so the matching
    logic never has to know FantasyPros' field naming.
    """
    response = requests.get(url)
    if not response.ok:
        print(f"Failed to fetch FantasyPros data. Status: {response.status_code}")
        response.raise_for_status()

    return [normalise_player(p) for p in response.json()]


def extract_slug(filename: str) -> str:
    match = SLUG_RE.search(clean(filename))
    return match.group(1) if match else ""


def normalise_player(player: dict) -> dict:
    """Flatten one FantasyPros record into our column names.

    Only the ids we carry are pulled across. FantasyPros also publishes cbs_id,
    mfl_id and nfl_id, which we deliberately do not track -- and note its
    `nfl_id` is an NFL.com *player* id, a different namespace from the `nfl_id`
    on teams. Do not assume the two are comparable.

    Two field names are worth knowing:

      * `player_id` is an int in the JSON, not a string. clean() stringifies it.
      * `sportsdata_player_id` is the Sportradar uuid -- the same value we hold
        in sportradar_id, under a different name. It is 89.3% populated, the
        best-covered id here, and the primary join key. Do not mistake it for an
        id from a distinct "sportsdata" provider.

    Coverage is uneven, so an absent value means FantasyPros has no opinion and
    the caller skips rather than reporting: yahoo 66.1%, espn 44.0%.
    """
    return {
        "fantasypros_id": clean(player.get("player_id")),
        "fantasypros_slug": extract_slug(player.get("filename")),
        "sportradar_id": clean(player.get("sportsdata_player_id")),
        "espn_id": clean(player.get("espn_id")),
        "yahoo_id": clean(player.get("yahoo_id")),
        # Kept only for the issue table, never compared.
        "name": clean(player.get("player_name")),
    }


# ---------------------------------------------------------------------------
# Matching and comparison.
# ---------------------------------------------------------------------------

# TODO externalize these in a yaml config?
# FantasyPros is first-hand for fantasypros_id and fantasypros_slug -- those are
# the reason to consult it and a disagreement there is an error. sportradar_id,
# espn_id and yahoo_id overlap validate_sleeper_ids.py and
# validate_dynastyprocess_ids.py, kept deliberately so a disagreement between
# independent sources surfaces as several issues rather than none.
MAPPINGS = {
    # FantasyPros key : PRISM key
    "fantasypros_id": "fantasypros_id",
    "fantasypros_slug": "fantasypros_slug",
    "sportradar_id": "sportradar_id",
    "espn_id": "espn_id",
    "yahoo_id": "yahoo_id",
}

# Keys we match players on, in priority order.
#
# sportradar_id leads and does the real work: FantasyPros publishes it as
# `sportsdata_player_id` at 89.3% coverage, it is a uuid so a wrong one cannot
# quietly collide with another player, and it matched 389 of our 390 NFL rows.
# Its presence is what makes this checker worth running -- joining on it leaves
# fantasypros_id, the slug, espn and yahoo as genuinely independent comparisons,
# where a checker limited to espn/yahoo would be comparing the same columns it
# joined on.
#
# espn and yahoo follow as fallbacks despite thin upstream coverage (44.0% and
# 66.1%), for the rows sportradar misses. fantasypros_id comes last on purpose,
# the same way sleeper_id does in validate_sleeper_ids.py: joining on it would
# make the fantasypros_id comparison circular, and that id is the main reason to
# be here. It stays in the list so a player whose other ids we lack is still
# checked, and so the orphan check below can trust a failed match.
MATCH_KEYS = {
    # PRISM key : FantasyPros key
    "sportradar_id": "sportradar_id",
    "yahoo_id": "yahoo_id",
    "espn_id": "espn_id",
    "fantasypros_id": "fantasypros_id",
}


def clean(value) -> str:
    return (str(value) if value is not None else "").strip()


def build_index(data: list[dict], key: str) -> dict:
    """Index FantasyPros records by one join key.

    Ambiguous keys are dropped rather than resolved -- the caller falls through
    to the next key instead of picking a winner.
    """
    rows_by_value = defaultdict(list)
    for row in data:
        value = clean(row.get(key, ""))
        if not value:
            continue
        rows_by_value[value].append(row)
    return {value: rows[0] for value, rows in rows_by_value.items() if len(rows) == 1}


def write_issues_txt(issues: list[dict], outfile_path: str = "issues.txt") -> None:
    """Writes a markdown-formatted issues file based on ID discrepancies."""
    now = datetime.utcnow().isoformat()
    header = [
        "## 📃 FantasyPros differences found",
        "",
        f"_Generated {now} UTC_",
        "",
        "The following FantasyPros mismatches found. FantasyPros is canonical "
        "for `fantasypros_id` and `fantasypros_slug`. `sportradar_id`, "
        "`espn_id` and `yahoo_id` are corroboration; whichever one the row was "
        "joined on is a tautology for that row -- see the note in the script.",
        "",
        "| Prism ID | Name | Key | FantasyPros Value | PRISM Value |",
        "|----------|------|-----|-------------------|-------------|",
    ]

    table_rows = []
    for issue in issues:
        table_rows.append(
            f"| {issue['prism_id']} | {issue['last_name']}, {issue['first_name']} "
            f"| {issue['prism_key']} | {issue['fantasypros_value']} "
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

    fp_data = fetch_fantasypros_data()
    fp_by_key = {
        our_key: build_index(fp_data, fp_key) for our_key, fp_key in MATCH_KEYS.items()
    }

    # Every value FantasyPros carries for the two columns it is canonical for,
    # used to catch ids and slugs of ours that resolve to nothing upstream. Note
    # this is deliberately the raw set rather than build_index's output, which
    # drops values appearing on more than one record -- a duplicate still means
    # the value exists, which is all this check asks.
    orphan_check_sets = {
        "fantasypros_id": {clean(p.get("fantasypros_id")) for p in fp_data},
        "fantasypros_slug": {clean(p.get("fantasypros_slug")) for p in fp_data},
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
                    found = fp_by_key[key].get(our_id, None)
                if found:
                    break
            if not found:
                # No FantasyPros record for this player. Two very different
                # situations land here and only one is benign:
                #
                #   * We hold no FantasyPros ids either -- the player simply is
                #     not covered. Expected, counted in the summary, not raised;
                #     FantasyPros carries fantasy-relevant players, so deep-bench
                #     absences are normal and raising each would bury real finds.
                #
                #   * We hold a fantasypros_id or slug that resolves to nothing.
                #     fantasypros_id is a join key, so failing to match on it is
                #     conclusive: the value does not exist upstream. For a column
                #     FantasyPros is canonical for, that is a stale or wrong value
                #     and an error, not a skip.
                for our_key in ("fantasypros_id", "fantasypros_slug"):
                    our_val = clean(row.get(our_key, ""))
                    if not our_val:
                        continue
                    if our_key not in orphan_check_sets:
                        continue
                    if our_val in orphan_check_sets[our_key]:
                        continue
                    is_ignore_key = False
                    if ignores.get(prism_id, None):
                        if our_key in ignores[prism_id] or ignores[prism_id] == our_key:
                            is_ignore_key = True
                    print(
                        f"Row {idx}, {prism_id}: Orphaned {our_key} {our_val} -- "
                        f"no FantasyPros record carries it. "
                        f"Ignoring: {is_ignore_key}"
                    )
                    if not is_ignore_key:
                        issues.append(
                            {
                                "prism_id": prism_id,
                                "last_name": row["last_name"],
                                "first_name": row["first_name"],
                                "prism_key": our_key,
                                "fantasypros_value": "(no record)",
                                "prism_value": our_val,
                            }
                        )
                continue
            else:
                matches += 1

            for fp_key, our_key in MAPPINGS.items():
                is_ignore_key = False
                if ignores.get(prism_id, None):
                    if our_key in ignores[prism_id] or ignores[prism_id] == our_key:
                        is_ignore_key = True
                fp_val = clean(found.get(fp_key, ""))
                our_val = row.get(our_key, None)

                # Note both branches require fp_val: a column we carry and
                # FantasyPros does not is silently skipped, never a finding.
                if not our_val and fp_val:
                    print(
                        f"Row {idx}, {prism_id}: Missing {our_key}, "
                        f"FantasyPros has {fp_val}. Ignoring: {is_ignore_key}"
                    )
                    if not is_ignore_key:
                        issues.append(
                            {
                                "prism_id": prism_id,
                                "last_name": row["last_name"],
                                "first_name": row["first_name"],
                                "prism_key": our_key,
                                "fantasypros_value": fp_val,
                                "prism_value": our_val,
                            }
                        )
                elif fp_val and fp_val != our_val:
                    print(
                        f"Row {idx}, {prism_id}: "
                        f"Diff {our_key}. FantasyPros: {fp_val}, Prism: {our_val}. "
                        f"Ignoring: {is_ignore_key}"
                    )
                    if not is_ignore_key:
                        issues.append(
                            {
                                "prism_id": prism_id,
                                "last_name": row["last_name"],
                                "first_name": row["first_name"],
                                "prism_key": our_key,
                                "fantasypros_value": fp_val,
                                "prism_value": our_val,
                            }
                        )

    if issues:
        if not quiet:
            print(
                f"\nFound {len(issues)} mismatches. "
                f"Matched: {matches}, skipped {rows - matches}"
            )
        if issues_file:
            write_issues_txt(issues, issues_file)
        return 1

    print(f"No mismatches found. Matched: {matches}, skipped {rows - matches}")
    return 0


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv_path", help="Path to the players CSV")
    parser.add_argument("--start", type=int, default=1, help="Row to start at")
    parser.add_argument("--quiet", action="store_true", help="Suppress the summary")
    parser.add_argument("--issues", dest="issues_file", help="Write findings here")
    parser.add_argument(
        "--ignores-file",
        dest="ignores_file",
        help="YAML of prism_id to lists of keys that should be ignored/skipped",
    )
    args = parser.parse_args()

    sys.exit(
        validate_csv(
            args.csv_path,
            start=args.start,
            quiet=args.quiet,
            issues_file=args.issues_file,
            ignores_file=args.ignores_file,
        )
    )
