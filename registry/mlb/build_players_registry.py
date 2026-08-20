"""MLB entry point for the shared players-registry builder.

Kept as a per-league script so consumers keep invoking
`tools/registry/mlb/build_players_registry.py` by path. The engine lives in
`registry/players_registry.py`; this file only supplies the MLB default
registry file and the `sources/` directory that source modules load from.
"""

import sys
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from players_registry import main  # noqa: E402


if __name__ == "__main__":
    main(
        default_registry_file="schema/leagues/mlb/players.yaml",
        source_dir=Path(__file__).resolve().parent,
    )
