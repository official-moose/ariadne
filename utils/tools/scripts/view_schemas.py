#!/usr/bin/env python3 
"""
View schemas for Ariadne SQLite databases (Simulation, Live, Ledger).

Usage:
    python scripts/view_schemas.py
"""

import os
import sqlite3
from typing import List, Tuple

# Resolve project root (one level up from /scripts)
ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

# Default paths (match your config)
DBS = [
    ("Simulation DB", os.path.join(ROOT, "data", "sims", "ariadne_sim.db")),
    ("Live DB",       os.path.join(ROOT, "data", "live", "ariadne_live.db")),
    ("Ledger DB",     os.path.join(ROOT, "data", "finance", "ledger.db")),
]

SEP = "═" * 80


def print_header(title: str, path: str) -> None:
    print(SEP)
    print(f"{title}  |  path: {path}")
    print(SEP)


def fetch_all(conn: sqlite3.Connection, query: str, params: Tuple = ()) -> List[Tuple]:
    cur = conn.cursor()
    cur.execute(query, params)
    return cur.fetchall()


def print_table_schema(conn: sqlite3.Connection, table: str) -> None:
    # CREATE statement (may be None for some system tables)
    create_sql_rows = fetch_all(
        conn,
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?;",
        (table,),
    )
    create_sql = (create_sql_rows[0][0] or "").strip() if create_sql_rows else ""
    print(f"\nTABLE: {table}")
    if create_sql:
        print("CREATE SQL:")
        print(create_sql)
    else:
        print("(No CREATE SQL found)")

    # Columns
    cols = fetch_all(conn, f"PRAGMA table_info('{table}');")
    if cols:
        # cols: cid, name, type, notnull, dflt_value, pk
        print("\nCOLUMNS:")
        print(f"{'cid':>3}  {'name':<24} {'type':<16} {'notnull':<7} {'default':<18} {'pk':<2}")
        print("-" * 80)
        for cid, name, coltype, notnull, dflt, pk in cols:
            dflt_str = "NULL" if dflt is None else str(dflt)
            print(f"{cid:>3}  {name:<24} {coltype:<16} {notnull!s:<7} {dflt_str:<18} {pk!s:<2}")
    else:
        print("\n(No columns found)")

    # Indexes (optional but useful)
    idxs = fetch_all(conn, f"PRAGMA index_list('{table}');")
    if idxs:
        print("\nINDEXES:")
        # rows: seq, name, unique, origin, partial
        print(f"{'seq':>3}  {'name':<32} {'unique':<6} {'origin':<6} {'partial':<7}")
        print("-" * 80)
        for seq, name, unique, origin, partial in idxs:
            print(f"{seq:>3}  {name:<32} {unique!s:<6} {origin:<6} {partial!s:<7}")


def describe_db(title: str, path: str) -> None:
    print_header(title, path)
    if not os.path.exists(path):
        print(f"!! Missing database: {path}\n")
        return

    try:
        conn = sqlite3.connect(path)
    except Exception as e:
        print(f"!! Failed to open {path}: {e}\n")
        return

    try:
        # List tables
        tables = fetch_all(conn, "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")
        table_names = [t[0] for t in tables]
        if not table_names:
            print("(No tables found)\n")
        else:
            print("TABLES:")
            print(", ".join(table_names) + "\n")
            # Print schema for each table
            for t in table_names:
                print_table_schema(conn, t)
                print()
    finally:
        conn.close()


def main():
    for title, path in DBS:
        describe_db(title, path)


if __name__ == "__main__":
    main()
