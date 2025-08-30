# ===============================
# Block 1 — Imports
# ===============================
import os
import time
import json
import sqlite3
from pathlib import Path
from datetime import datetime

import pandas as pd
from pandas.errors import EmptyDataError
from pydantic import BaseModel, Field, conint, confloat
from openai import OpenAI


# ===============================
# Block 2 — Configuration
# ===============================

# Fixed locations (kept as you specified; will normalize for safety)
SOURCE = r"./data//fda_approved_food_items_w_nutrient_info.csv"
OUTPUT = r"./processed//cleaned_data.csv"
DB     = r"./processed//groceries_DB.sqlite"

OPENAI_MODEL = "gpt-4o-2024-08-06"
API_DELAY_SECONDS = 0.8
MAX_AI_ENRICH_ROWS = 50  # set to None to enrich all rows

# Minimal schema to use if the source is missing/empty/unreadable
# (use raw column names you referenced earlier so prompts still work)
MIN_SCHEMA = [
    "fdc_id",
    "brand_owner",
    "description",
    "ingredients",
    "gtin_upc",
    "serving_size",
    "serving_size_unit",
    "branded_food_category",
    "Energy-KCAL",
    "Protein-G",
    "Total lipid (fat)-G",
    "Carbohydrate, by difference-G",
    "Sodium, Na-MG",
]

# ---------- Structured output schema ----------
from pydantic import BaseModel, Field, conint, confloat

class HSRatings(BaseModel):
    healthiness: conint(ge=1, le=10) = Field(..., description="1 (unhealthy) to 10 (very healthy)")
    sustainability: conint(ge=1, le=10) = Field(..., description="1 (poor) to 10 (excellent)")

class ProductExtraction(BaseModel):
    price_per_unit: confloat(gt=0) = Field(..., description="AUD per 100 g or per 100 mL")
    unit_basis: str = Field(..., description="e.g., 'AUD per 100 g' or 'AUD per 100 mL'")
    store: str = Field(..., description="Likely AU retailer (Woolworths/Coles/Aldi)")
    ratings: HSRatings

SYSTEM_PROMPT = (
    "You are given information about a packaged food product. "
    "Return ONLY these in the provided schema: "
    "1) AUD price per standardized unit (per 100 g or per 100 mL), "
    "2) a likely Australian store (Woolworths/Coles/Aldi/etc.), "
    "3) TWO ratings (1-10): healthiness and sustainability. "
    "If uncertain, estimate consistently."
)



# ===============================
# Block 3 — Code Logic (functions)
# ===============================

def normpath(p: str) -> Path:
    # Make '/.data\...' behave like './data/...'
    p2 = p.replace("\\", "/")
    if p2.startswith("/.data"):
        p2 = "./data" + p2[len("/.data"):]
    return Path(os.path.normpath(p2))

def ensure_dirs_for(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)

def empty_schema_df() -> pd.DataFrame:
    # Create an empty DF with the minimal schema
    return pd.DataFrame(columns=MIN_SCHEMA)

def robust_read_table_or_empty(path: Path) -> pd.DataFrame:
    """
    Try to read the source; if it doesn't exist, is empty, or unreadable,
    return an empty DataFrame with the minimal schema (no crash).
    """
    try:
        if not path.exists() or path.is_dir() or path.stat().st_size == 0:
            print(f"[warn] Source not found/empty at {path}. Creating empty outputs.")
            return empty_schema_df()

        # Detect Excel/zip signature
        with open(path, "rb") as f:
            head = f.read(8)
        if head.startswith(b"PK\x03\x04") or str(path).lower().endswith((".xlsx", ".xlsm", ".xls")):
            try:
                df = pd.read_excel(path)
                return df if not df.empty else empty_schema_df()
            except Exception as e:
                print(f"[warn] Failed to read Excel ({e}). Using empty schema.")
                return empty_schema_df()

        # Try CSV normally, then fallback attempts
        try:
            df = pd.read_csv(path, low_memory=False)
            return df if not df.empty else empty_schema_df()
        except EmptyDataError:
            # Try sniffing sep/encoding
            try:
                df = pd.read_csv(path, sep=None, engine="python", low_memory=False)
                return df if not df.empty else empty_schema_df()
            except Exception:
                print(f"[warn] Could not parse CSV at {path}. Using empty schema.")
                return empty_schema_df()
        except Exception as e:
            print(f"[warn] Read failed ({e}). Using empty schema.")
            return empty_schema_df()
    except Exception as e:
        print(f"[warn] Unexpected error checking source ({e}). Using empty schema.")
        return empty_schema_df()

def write_db(df: pd.DataFrame, db_path: Path):
    ensure_dirs_for(db_path)
    conn = sqlite3.connect(db_path)
    try:
        # Ensure at least minimal schema exists
        if df.empty and not list(df.columns):
            df = empty_schema_df()
        df.to_sql("fdc_products", conn, if_exists="replace", index=False)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_fdc_id ON fdc_products (fdc_id);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_gtin ON fdc_products (gtin_upc);")
        conn.commit()
    finally:
        conn.close()

def ensure_enrichment_columns(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(fdc_products);").fetchall()]
        if "price_per_unit_aud" not in cols:
            conn.execute("ALTER TABLE fdc_products ADD COLUMN price_per_unit_aud REAL;")
        if "store" not in cols:
            conn.execute("ALTER TABLE fdc_products ADD COLUMN store TEXT;")
        conn.execute("""
          CREATE TABLE IF NOT EXISTS ai_product_insights (
            fdc_id TEXT PRIMARY KEY,
            rating_cost INTEGER,
            rating_healthiness INTEGER,
            rating_sustainability INTEGER,
            unit_basis TEXT,
            model TEXT,
            checked_at TEXT
          );
        """)
        conn.commit()
    finally:
        conn.close()

def update_main_row(db_path: Path, fdc_id: str, price_per_unit: float, store: str):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("""
            UPDATE fdc_products
               SET price_per_unit_aud = ?, store = ?
             WHERE fdc_id = ?;
        """, (price_per_unit, store, fdc_id))
        conn.commit()
    finally:
        conn.close()

def upsert_insight(db_path: Path, fdc_id: str, ratings: HSRatings, unit_basis: str, model: str):
    conn = sqlite3.connect(db_path)
    try:
        conn.execute("""
            INSERT INTO ai_product_insights
              (fdc_id, rating_healthiness, rating_sustainability, unit_basis, model, checked_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(fdc_id) DO UPDATE SET
              rating_healthiness=excluded.rating_healthiness,
              rating_sustainability=excluded.rating_sustainability,
              unit_basis=excluded.unit_basis,
              model=excluded.model,
              checked_at=excluded.checked_at;
        """, (
            fdc_id,
            int(ratings.healthiness),
            int(ratings.sustainability),
            unit_basis,
            model,
            datetime.now().isoformat(timespec="seconds"),
        ))
        conn.commit()
    finally:
        conn.close()


SYSTEM_PROMPT = (
    "You are given information about a packaged food product. "
    "Return ONLY these in the provided schema: "
    "1) AUD price per standardized unit (per 100 g or per 100 mL), "
    "2) a likely Australian store (Woolworths/Coles/Aldi/etc.), "
    "3) ratings 1-10 for cost, healthiness, sustainability. "
    "If uncertain, make a best estimate and be consistent."
)

def get_openai_client() -> OpenAI:
    key = os.environ.get("OPENAI_API_KEY")
    if not key:
        raise RuntimeError("OPENAI_API_KEY is not set.")
    return OpenAI(api_key=key)

def build_product_prompt(row: dict) -> str:
    # Build from whatever exists (no preprocessing required)
    candidate_cols = [
        "fdc_id", "brand_owner", "description", "ingredients",
        "serving_size", "serving_size_unit", "branded_food_category",
        "Energy-KCAL", "Protein-G", "Total lipid (fat)-G",
        "Carbohydrate, by difference-G", "Sodium, Na-MG",
    ]
    payload = {k: row.get(k) for k in candidate_cols if k in row and pd.notna(row.get(k))}
    return json.dumps(payload, ensure_ascii=False)

def enrich_database(db_path: Path, limit: int | None = MAX_AI_ENRICH_ROWS):
    ensure_enrichment_columns(db_path)

    # If there are zero rows, nothing to do — but the files exist (as you asked)
    conn = sqlite3.connect(db_path)
    try:
        cnt = conn.execute("SELECT COUNT(*) FROM fdc_products;").fetchone()[0]
        if cnt == 0:
            print("[info] DB has 0 rows; skipping API enrichment.")
            return
    finally:
        conn.close()

    client = get_openai_client()
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.cursor()
        sql = """
        SELECT *
          FROM fdc_products
         WHERE price_per_unit_aud IS NULL OR store IS NULL
        """
        if isinstance(limit, int) and limit > 0:
            sql += f" LIMIT {int(limit)}"
        rows = cur.execute(sql).fetchall()
        cols = [d[0] for d in cur.description]

        for r in rows:
            row = dict(zip(cols, r))
            fdc_id = str(row.get("fdc_id", "")) if row.get("fdc_id") is not None else ""
            if not fdc_id:
                # Require fdc_id to update
                continue

            prompt = build_product_prompt(row)
            try:
                resp = client.responses.parse(
                    model=OPENAI_MODEL,
                    input=[
                        {"role": "system", "content": SYSTEM_PROMPT},
                        {"role": "user", "content": prompt},
                    ],
                    text_format=ProductExtraction,
                )
                parsed: ProductExtraction = resp.output_parsed
            except Exception as e:
                print(f"[warn] parse failed for fdc_id={fdc_id}: {e}")
                continue

            update_main_row(db_path, fdc_id, float(parsed.price_per_unit), parsed.store)
            upsert_insight(db_path, fdc_id, parsed.ratings, parsed.unit_basis, OPENAI_MODEL)

            time.sleep(API_DELAY_SECONDS)
    finally:
        conn.close()

def ensure_enrichment_columns(db_path: Path):
    conn = sqlite3.connect(db_path)
    try:
        cols = [r[1] for r in conn.execute("PRAGMA table_info(fdc_products);").fetchall()]
        if "price_per_unit_aud" not in cols:
            conn.execute("ALTER TABLE fdc_products ADD COLUMN price_per_unit_aud REAL;")
        if "store" not in cols:
            conn.execute("ALTER TABLE fdc_products ADD COLUMN store TEXT;")

        # Two-rating insights table (OK if an older table already exists; this will no-op)
        conn.execute("""
          CREATE TABLE IF NOT EXISTS ai_product_insights (
            fdc_id TEXT PRIMARY KEY,
            rating_healthiness INTEGER,
            rating_sustainability INTEGER,
            unit_basis TEXT,
            model TEXT,
            checked_at TEXT
          );
        """)
        conn.commit()
    finally:
        conn.close()

# ---------- Runner ----------
def main():
    src = normpath(SOURCE)
    out_csv = normpath(OUTPUT)
    db_path = normpath(DB)

    # 1) Read source if possible, else create an empty DF with schema
    df = robust_read_table_or_empty(src)

    # 2) Always write CSV and DB (even if empty)
    ensure_dirs_for(out_csv)
    df.to_csv(out_csv, index=False, encoding="utf-8")
    print(f"[ok] Wrote CSV to {out_csv.resolve()}")

    write_db(df, db_path)
    print(f"[ok] Built DB at {db_path.resolve()}")

    # 3) Enrich only if there are rows
    enrich_database(db_path, limit=MAX_AI_ENRICH_ROWS)
    print("[done] Completed (created outputs even if source was missing).")


if __name__ == "__main__":
    main()
