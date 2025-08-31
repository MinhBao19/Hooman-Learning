# sql_store.py — DB-only helpers (read-only)
import sqlite3
from pathlib import Path
from typing import Optional, Dict, Any, List, Tuple

# Default DB location (override at runtime with set_db_path if you want)
_DB_PATH = Path(r"processed\groceries_DB.sqlite")


#SQL setup
def set_db_path(p: str | Path) -> None:
    """Optionally override the DB path at runtime."""
    global _DB_PATH
    _DB_PATH = Path(p)

def get_db_path() -> Path:
    return _DB_PATH

def _connect_ro() -> sqlite3.Connection | None:
    """
    Open the SQLite DB in read-only mode so we don't create an empty DB by mistake.
    Returns None if file doesn't exist.
    """
    dbp = get_db_path()
    if not dbp.exists():
        return None
    # read-only URI prevents accidental creation
    return sqlite3.connect(f"file:{dbp.as_posix()}?mode=ro", uri=True)

def _rows_to_dicts(cur: sqlite3.Cursor, rows: list[tuple]) -> List[Dict[str, Any]]:
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r)) for r in rows]

def _like_val(x: Optional[str]) -> Optional[str]:
    """Normalize empty strings to None so our SQL '(? IS NULL OR ...)' works nicely."""
    if x is None:
        return None
    x = x.strip()
    return x if x else None

def _tables_exist(con: sqlite3.Connection) -> bool:
    """Quick guard so we don't crash if tables are missing."""
    cur = con.execute("SELECT name FROM sqlite_master WHERE type='table'")
    have = {n for (n,) in cur.fetchall()}
    return "fdc_products" in have  # ai_product_insights is optional

#Public helpers

def db_count_matches(
    item: Optional[str],
    supplier: Optional[str],
) -> int:
    """
    Count candidate rows matching the filters (case-insensitive LIKE).
    Returns 0 if DB missing or table absent.
    """
    con = _connect_ro()
    if con is None:
        return 0
    try:
        if not _tables_exist(con):
            return 0
        item, supplier = _like_val(item), _like_val(supplier)
        sql = """
        SELECT COUNT(*)
          FROM fdc_products p
          LEFT JOIN ai_product_insights i ON i.fdc_id = p.fdc_id
         WHERE (? IS NULL OR p.fdc_category LIKE '%' || ? || '%' COLLATE NOCASE)
           AND (? IS NULL OR p.store        LIKE '%' || ? || '%' COLLATE NOCASE)
           AND (
                ? IS NULL OR
                p.description  LIKE '%' || ? || '%' COLLATE NOCASE OR
                p.brand_owner  LIKE '%' || ? || '%' COLLATE NOCASE
           )
        """
        params = [supplier, supplier, item, item, item]
        (count,) = con.execute(sql, params).fetchone()
        return int(count or 0)
    except sqlite3.Error:
        return 0
    finally:
        con.close()

def query_top_products(
    item: Optional[str],
    supplier: Optional[str],
    topn: int = 3,
) -> List[Dict[str, Any]]:
    """
    Return up to topn rows ranked by:
      1) average(rating_healthiness, rating_sustainability) DESC
      2) price_per_unit_aud ASC (NULLs last)
    All filters optional (case-insensitive LIKE). Returns [] if DB/tables missing.
    """
    con = _connect_ro()
    if con is None:
        return []
    try:
        if not _tables_exist(con):
            return []

        item, supplier = _like_val(item), _like_val(supplier)

        sql = """
        SELECT
          p.fdc_id,
          p.description,
          p.brand_owner,
          p.fdc_category,
          p.store,
          p.price_per_unit_aud,
          i.rating_healthiness,
          i.rating_sustainability,
          ((COALESCE(i.rating_healthiness,0) + COALESCE(i.rating_sustainability,0)) / 2.0) AS rating_overall
        FROM fdc_products p
        LEFT JOIN ai_product_insights i ON i.fdc_id = p.fdc_id
        WHERE (? IS NULL OR p.fdc_category LIKE '%' || ? || '%' COLLATE NOCASE)
          AND (? IS NULL OR p.store        LIKE '%' || ? || '%' COLLATE NOCASE)
          AND (
               ? IS NULL OR
               p.description  LIKE '%' || ? || '%' COLLATE NOCASE OR
               p.brand_owner  LIKE '%' || ? || '%' COLLATE NOCASE
          )
        ORDER BY rating_overall DESC,
                 (p.price_per_unit_aud IS NULL) ASC,
                 p.price_per_unit_aud ASC
        LIMIT ?
        """
        params = [ supplier, supplier, item, item, item, int(topn)]
        cur = con.cursor()
        rows = cur.execute(sql, params).fetchall()
        return _rows_to_dicts(cur, rows)
    except sqlite3.Error:
        return []
    finally:
        con.close()
