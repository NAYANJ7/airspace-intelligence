"""
Run the Phase 7 SQL files and print every result.

    python -m src.run_sql analytics                  # create/refresh the views, print samples
    python -m src.run_sql data_quality               # create/refresh the checks, print results
    python -m src.run_sql explain                    # EXPLAIN ANALYZE of the key queries
    python -m src.run_sql analytics data_quality     # several files, in the order given

Each file in src/sql/ is split into blocks that start with a "-- @ <title>" line. Blocks
run one at a time; the first error stops the run with exit code 1. Connection settings come
from the same POSTGRES_* variables the loader uses (reused, not duplicated). This script only
reads data and (re)creates views/functions - it never changes rows or touches the loader.
"""

import re
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from .postgres_loader import connect

SQL_DIR = Path(__file__).with_name("sql")
MARKER = re.compile(r"^-- @(.+)$", re.MULTILINE)


def split_blocks(text: str) -> list[tuple[str, str]]:
    parts = MARKER.split(text)  # [preamble, title1, body1, title2, body2, ...]
    return [(parts[i].strip(), parts[i + 1].strip()) for i in range(1, len(parts), 2)]


def fmt(value) -> str:
    if value is None:
        return ""
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, float):
        return f"{value:.4f}".rstrip("0").rstrip(".")
    if isinstance(value, timedelta):
        return str(value)
    return str(value)


def print_rows(columns: list[str], rows: list[tuple]) -> None:
    if columns == ["QUERY PLAN"]:  # EXPLAIN output: print the plan text as it is
        for (line,) in rows:
            print("    " + line)
        return
    table = [[fmt(v) for v in row] for row in rows]
    widths = [max(len(c), *(len(r[i]) for r in table)) if table else len(c) for i, c in enumerate(columns)]
    print("    " + " | ".join(c.ljust(w) for c, w in zip(columns, widths)))
    print("    " + "-+-".join("-" * w for w in widths))
    for r in table:
        print("    " + " | ".join(v.ljust(w) for v, w in zip(r, widths)))
    print(f"    ({len(table)} row{'s' if len(table) != 1 else ''})")


def run_file(conn, name: str) -> int:
    path = SQL_DIR / f"{name}.sql"
    if not path.exists():
        print(f"ERROR: {path} not found. Available: {', '.join(sorted(p.stem for p in SQL_DIR.glob('*.sql')))}", file=sys.stderr)
        return 1
    blocks = split_blocks(path.read_text(encoding="utf-8"))
    print("=" * 70)
    print(f"{name}.sql  ({len(blocks)} blocks)")
    print("=" * 70)
    for title, sql in blocks:
        try:
            cur = conn.execute(sql)
            print(f"\n[{title}]")
            if cur.description is None:
                print("    ok")
            else:
                print_rows([d.name for d in cur.description], cur.fetchall())
        except Exception as e:
            print(f"\nERROR in block '{title}' of {path.name}:\n{e}", file=sys.stderr)
            return 1
    return 0


def main(argv: list[str]) -> int:
    if not argv:
        print(__doc__)
        return 1
    try:
        conn = connect()
    except Exception as e:
        print(f"ERROR: could not connect to PostgreSQL: {e}", file=sys.stderr)
        return 1
    with conn:
        for name in argv:
            if run_file(conn, name) != 0:
                return 1
    print("\nAll blocks executed successfully.")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
