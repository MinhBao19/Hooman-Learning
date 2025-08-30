#!/usr/bin/env python3
import sqlite3
from pathlib import Path

DB = r"processed\groceries_DB.sqlite"

REQUIRED_FDC_COLUMNS = [
    "fdc_id","brand_owner","description","ingredients","gtin_upc",
    "serving_size","serving_size_unit","branded_food_category",
    "price_per_unit_aud","store"  # the two enrichment columns
]
# add near the top
ROW_PREVIEW = 5  # how many rows to show per table

def preview_rows(con, table, limit=ROW_PREVIEW):
    cols = [r[1] for r in con.execute(f"PRAGMA table_info({table});").fetchall()]
    rows = con.execute(f"SELECT * FROM {table} LIMIT {limit};").fetchall()
    if not rows:
        print(f"(no rows in {table})")
        return
    print(f"First {min(limit, len(rows))} row(s):")
    for i, row in enumerate(rows, 1):
        rec = dict(zip(cols, row))
        print(f"  [{i}] {rec}")

def list_tables(con):
    q = "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;"
    return [r[0] for r in con.execute(q).fetchall()]

def table_info(con, table):
    # cid, name, type, notnull, dflt_value, pk
    return con.execute(f"PRAGMA table_info({table});").fetchall()

def index_list(con, table):
    # seq, name, unique, origin, partial
    return con.execute(f"PRAGMA index_list({table});").fetchall()

def main():
    db_path = Path(DB)
    if not db_path.exists():
        print(f"[error] DB not found at: {db_path.resolve()}")
        return

    con = sqlite3.connect(db_path)
    try:
        print(f"[ok] Connected: {db_path.resolve()}")
        tables = list_tables(con)
        if not tables:
            print("[warn] No tables found.")
            return

        for t in tables:
            print(f"\n=== TABLE: {t} ===")
            cols = table_info(con, t)
            if not cols:
                print("(no columns?)")
                continue

            print("cid | name | type | notnull | default | pk")
            for cid, name, ctype, notnull, dflt, pk in cols:
                print(f"{cid:>3} | {name} | {ctype} | {notnull} | {dflt} | {pk}")

            idxs = index_list(con, t)
            if idxs:
                print("Indexes:")
                for row in idxs:
                    print("  ", row)

            # Optional: sanity check required columns on fdc_products
            if t == "fdc_products":
                have = {name for _, name, *_ in cols}
                missing = [c for c in REQUIRED_FDC_COLUMNS if c not in have]
                if missing:
                    print(f"[warn] Missing expected columns on {t}: {missing}")
                else:
                    print(f"[ok] {t} has all expected columns.")
            preview_rows(con, t, limit=ROW_PREVIEW)
    finally:
        con.close()

if __name__ == "__main__":
    main()
