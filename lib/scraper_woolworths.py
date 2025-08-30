# woolworths_scraper.py
# Requirements:
#   pip install selenium beautifulsoup4
# Note:
#   - Uses configuration.ini (see keys in the CONFIG section below).
#   - Writes CSV with columns (incl. Ingredients, NutritionJSON).
#   - Handles Windows CSV locks with retry/fallback.

import sys
import csv
import os
import time
import random
import signal
import shutil
import configparser
import re
import json
from datetime import datetime

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.edge.service import Service as EdgeService
from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException, JavascriptException
from bs4 import BeautifulSoup

# ======================
# CONFIG
# ======================

CONFIG_FILE = "configuration.ini"
CFG = configparser.ConfigParser()
if not os.path.exists(CONFIG_FILE):
    raise FileNotFoundError(f"Missing {CONFIG_FILE}")

CFG.read(CONFIG_FILE)

SAVE_DIR = CFG.get('Global', 'SaveLocation', fallback='.')
DELAY_BASE = CFG.getint('Woolworths', 'DelaySeconds', fallback=2)
HEADLESS = CFG.get('Woolworths', 'Headless', fallback='TRUE').upper() == 'TRUE'
IGNORED_CATEGORIES_RAW = CFG.get('Woolworths', 'IgnoredCategories', fallback='')
RESUME_ACTIVE = CFG.get('Woolworths', 'Resume_Active', fallback='FALSE').upper() == 'TRUE'
RESUME_CATEGORY = CFG.get('Woolworths', 'Resume_Category', fallback='null')
RESUME_PAGE = CFG.getint('Woolworths', 'Resume_Page', fallback=1)

RESTART_EVERY_PAGES = CFG.getint('Woolworths', 'RestartEveryPages', fallback=50)
NAV_TIMEOUT = CFG.getint('Woolworths', 'NavTimeoutSeconds', fallback=20)
GRID_TIMEOUT = CFG.getint('Woolworths', 'GridTimeoutSeconds', fallback=20)

# Ingredients & Nutrition toggles/timeouts
FETCH_INGREDIENTS = CFG.get('Woolworths', 'FetchIngredients', fallback='TRUE').upper() == 'TRUE'
ING_TIMEOUT = CFG.getint('Woolworths', 'IngredientsTimeoutSeconds', fallback=20)
ING_RETRIES = CFG.getint('Woolworths', 'IngredientsMaxRetries', fallback=2)

FETCH_NUTRITION = CFG.get('Woolworths', 'FetchNutrition', fallback='TRUE').upper() == 'TRUE'
NUT_TIMEOUT = CFG.getint('Woolworths', 'NutritionTimeoutSeconds', fallback=20)
NUT_RETRIES = CFG.getint('Woolworths', 'NutritionMaxRetries', fallback=2)

URL_BASE = "https://www.woolworths.com.au"
CSV_NAME = "Woolworths.csv"
CSV_PATH = os.path.join(SAVE_DIR, CSV_NAME)

IGNORED_ENDPOINTS = [s.strip() for s in IGNORED_CATEGORIES_RAW.split(',') if s.strip()]

# In-run caches
_DETAILS_CACHE = {}

# CSV header (kept here so the safe writer can seed headers when creating/rolling files)
CSV_HEADER = [
    "Product Code", "Category", "Item Name",
    "Best Price", "Best Unit Price",
    "Item Price", "Unit Price",
    "Price Was", "Special Text", "Complex Promo Text", "Link",
    "Ingredients", "NutritionJSON"
]

# ======================
# FILE HELPERS (Windows lock-safe)
# ======================

def ensure_dir_writable(directory: str) -> bool:
    try:
        os.makedirs(directory, exist_ok=True)
        probe = os.path.join(directory, ".write_probe.tmp")
        with open(probe, "w", encoding="utf-8") as t:
            t.write("ok")
        os.remove(probe)
        return True
    except Exception:
        return False

def open_csv_writer_with_retry(base_path: str,
                               header: list,
                               max_retries: int = 8,
                               backoff_start: float = 0.4):
    """
    Try to open CSV for append. If PermissionError persists after retries,
    fall back to a timestamped file (keeps the run going).
    Returns (file_handle, csv_writer, final_path).
    """
    attempt = 0
    while attempt < max_retries:
        try:
            is_new = not os.path.exists(base_path)
            f = open(base_path, "a", newline="", encoding="utf-8")
            writer = csv.writer(f)
            if is_new and header:
                writer.writerow(header)
            return f, writer, base_path
        except PermissionError:
            time.sleep(backoff_start * (2 ** attempt))
            attempt += 1

    ts = datetime.now().strftime("%Y%m%d-%H%M%S")
    root, ext = os.path.splitext(base_path)
    alt_path = f"{root}_{ts}{ext or '.csv'}"
    f = open(alt_path, "a", newline="", encoding="utf-8")
    writer = csv.writer(f)
    writer.writerow(header)
    print(f"⚠️  Could not open '{base_path}' (locked or not writable). Writing to '{alt_path}' instead.")
    return f, writer, alt_path

# ======================
# GENERAL HELPERS
# ======================

def nap(base: int = DELAY_BASE, lo: float = 0.25, hi: float = 0.75):
    time.sleep(base + random.uniform(lo, hi))

def persist_resume(category: str, page: int, active: bool = True):
    CFG.set('Woolworths', 'Resume_Active', "TRUE" if active else "FALSE")
    CFG.set('Woolworths', 'Resume_Category', category)
    CFG.set('Woolworths', 'Resume_Page', str(page))
    with open(CONFIG_FILE, 'w') as cfgf:
        CFG.write(cfgf)

def wait_for(driver, by, selector, timeout=NAV_TIMEOUT):
    return WebDriverWait(driver, timeout).until(EC.presence_of_element_located((by, selector)))

def safe_js(driver, script, default=None):
    try:
        val = driver.execute_script(script)
        return default if val is None else val
    except (JavascriptException, WebDriverException):
        return default

# ======================
# ROBUST EDGE DRIVER
# ======================

def edge_driver(headless_override: bool = None):
    class _Ctx:
        def __enter__(self_inner):
            headless = HEADLESS if headless_override is None else headless_override

            def build_driver(hless: bool):
                opts = EdgeOptions()
                opts.add_argument("--window-size=1280,900")
                opts.add_experimental_option('excludeSwitches', ['enable-logging'])
                # stability flags
                opts.add_argument("--disable-extensions")
                opts.add_argument("--no-first-run")
                opts.add_argument("--no-default-browser-check")
                opts.add_argument("--disable-background-networking")
                opts.add_argument("--disable-sync")
                opts.add_argument("--disable-features=Translate,InterestFeedContent,OptimizationGuideModelDownloading")
                opts.add_argument("--metrics-recording-only")
                opts.add_argument("--password-store=basic")
                if hless:
                    opts.add_argument("--headless=new")
                    opts.add_argument("--disable-gpu")
                opts.add_argument(
                    "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0.0.0 Safari/537.36 Edg/124.0.0.0"
                )
                service = None
                msedgedriver_path = shutil.which("msedgedriver")
                if msedgedriver_path:
                    service = EdgeService(executable_path=msedgedriver_path)
                return webdriver.Edge(options=opts, service=service) if service else webdriver.Edge(options=opts)

            try:
                self_inner.driver = build_driver(hless=headless)
                return self_inner.driver
            except WebDriverException:
                if headless:
                    print("Headless launch failed; retrying in non-headless mode...")
                    self_inner.driver = build_driver(hless=False)
                    return self_inner.driver
                raise

        def __exit__(self_inner, exc_type, exc, tb):
            try:
                self_inner.driver.quit()
            except Exception:
                pass

    return _Ctx()

# ======================
# CATEGORY & TILE SCRAPING
# ======================

def get_categories(driver):
    driver.get(URL_BASE)
    wait_for(driver, By.CSS_SELECTOR, "button.wx-header__drawer-button.browseMenuDesktop")
    driver.find_element(By.CSS_SELECTOR, "button.wx-header__drawer-button.browseMenuDesktop").click()
    nap()
    page = BeautifulSoup(driver.page_source, "html.parser")
    raw = page.find_all("a", class_="item ng-star-inserted")
    cats = []
    for a in raw:
        name = a.get_text(strip=True)
        href = a.get("href") or ""
        if not name or not href.startswith("/shop/browse/"):
            continue
        endpoint = href.replace("/shop/browse/", "").strip("/")
        if any(ig.lower() in endpoint.lower() for ig in IGNORED_ENDPOINTS):
            continue
        cats.append({"name": name, "href": href, "endpoint": endpoint})
    return cats

def filter_for_resume(categories):
    if not RESUME_ACTIVE:
        return categories
    keep, found = [], False
    for c in categories:
        if not found and c["name"] == RESUME_CATEGORY:
            found = True
        if found:
            keep.append(c)
    return keep

def get_total_pages(driver):
    try:
        el = WebDriverWait(driver, 3).until(EC.presence_of_element_located((By.CSS_SELECTOR, "span.page-count")))
        t = (el.get_attribute("innerText") or "").strip()
        return int(t) if t.isdigit() else 1
    except (TimeoutException, ValueError):
        return 1

def open_category_page(driver, href: str, page_num: int):
    url = f"{URL_BASE}{href}?pageNumber={page_num}&sortBy=TraderRelevance&filter=SoldBy(Woolworths)"
    driver.get(url)
    try:
        WebDriverWait(driver, GRID_TIMEOUT).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "shared-grid.grid-v2"))
        )
    except TimeoutException:
        pass
    nap()

def count_tiles(driver):
    return len(driver.find_elements(By.CSS_SELECTOR, "wc-product-tile.ng-star-inserted"))

def get_tile_texts(driver, idx: int):
    js = f"""
    const host = document.getElementsByClassName('grid-v2')[0]?.getElementsByTagName('wc-product-tile')[{idx}];
    if (!host || !host.shadowRoot) return {{}};
    const root = host.shadowRoot.children[0];

    function txt(cls) {{
      const el = root.querySelector('.' + cls);
      return el ? (el.innerText || '').trim() : '';
    }}
    function hrefOf(selector) {{
      const a = root.querySelector(selector);
      return a ? (a.href || '') : '';
    }}

    const name = txt('title');
    const price = txt('primary');
    const unitprice = txt('price-per-cup');
    const special = txt('product-tile-label');
    const promo = txt('product-tile-promo-info');
    const was = txt('was-price ');
    const link = hrefOf('a');

    let code = "";
    if (link) {{
      const bits = link.split("/");
      code = bits[bits.length - 2] || "";
      if (code === "productdetails") code = bits[bits.length - 1] || "";
    }}
    return {{name, price, unitprice, special, promo, was, link, code}};
    """
    data = safe_js(driver, js, default={}) or {}
    for k in ["name","price","unitprice","special","promo","was","link","code"]:
        data[k] = (data.get(k) or "").strip()
    return data

def normalize_best_price(itemprice: str, unitprice: str, promo: str):
    best_price, best_unitprice, price_was = itemprice, unitprice, None
    if not promo:
        return best_price, best_unitprice, price_was
    p = promo
    if "Range was " in p or "Was " in p:
        try:
            price_was = p[p.find("$"):p.find(" - ")].strip() if " - " in p else p[p.find("$"):].split()[0]
        except Exception:
            price_was = None
    if "Member Price" in p or " for " in p:
        p2 = p.replace("Member Price", "").strip()
        try:
            if " for " in p2 and "$" in p2:
                qty_str = p2.split(" for ")[0].strip()
                qty = int(''.join(ch for ch in qty_str if ch.isdigit()) or "0")
                after_for = p2.split(" for ")[1]
                price_str = after_for.split(" - ")[0].strip()
                total = float(price_str.replace("$", "").strip())
                if qty > 0 and total > 0:
                    best_price = f"${round(total / qty, 2)}"
                if " - " in after_for:
                    best_unitprice = after_for.split(" - ", 1)[1].strip()
        except Exception:
            pass
    return best_price, best_unitprice, price_was

# ======================
# DETAILS (INGREDIENTS + NUTRITION)
# ======================

ING_HEADING_PATTERN = re.compile(r'^\s*ingredients\s*[:\-]?\s*$', re.IGNORECASE | re.MULTILINE)
NUT_HEADING_PATTERN = re.compile(r'^\s*nutrition(?:al)?\s+(?:information|facts)\s*[:\-]?\s*$',
                                 re.IGNORECASE | re.MULTILINE)

NEXT_HEADINGS = [
    "allergen", "allergy", "contains", "may contain", "nutrition", "nutritional",
    "storage", "directions", "warning", "warnings", "country of origin",
    "servings", "preparation", "usage", "safety", "manufacturer", "brand",
    "product warnings", "information", "dietary", "advisory", "ingredients"
]

NUTRIENT_KEYS = [
    ("energy", r"energy"),
    ("protein_g", r"protein"),
    ("fat_total_g", r"fat[, \-]*total|total\s+fat"),
    ("fat_saturated_g", r"fat[, \-]*saturate|saturated\s+fat"),
    ("carbohydrate_g", r"carbohydrate"),
    ("sugars_g", r"sugars?"),
    ("dietary_fibre_g", r"(dietary\s+)?fibre|fiber"),
    ("sodium_mg", r"sodium"),
]

VAL_PATTERN = re.compile(r'(\d+(?:[.,]\d+)?)\s*(kJ|kcal|g|mg|mcg|µg|kj)', re.IGNORECASE)

def _raw_details_text_js():
    return """
    function collectTextFrom(root) {
      if (!root) return "";
      try { return (root.innerText || root.textContent || ""); } catch (e) { return ""; }
    }
    let text = "";
    const trySel = sel => { const el = document.querySelector(sel); if (el && el.shadowRoot) text += " " + collectTextFrom(el.shadowRoot); };
    trySel('wc-product-details');
    trySel('wc-product-tabs');
    trySel('wc-nutrition-table');
    trySel('nutrition-information');
    trySel('product-nutrition');
    trySel('product-details');
    if (!text.trim()) text = collectTextFrom(document.body);
    return text;
    """

def _extract_nutrition_table_js():
    # Return the best-looking nutrition table found (including inside shadow DOM)
    return """
    function walk(root, out=[]) {
      if (!root) return out;
      out.push(root);
      const kids = root.children || [];
      for (let i=0;i<kids.length;i++) {
        const k = kids[i];
        out.push(k);
        if (k.shadowRoot) walk(k.shadowRoot, out);
        walk(k, out);
      }
      return out;
    }
    function text(el){ try { return (el.innerText || el.textContent || '').trim(); } catch(e){ return ''; } }
    function scoreTable(tblText){
      let s=0, low = tblText.toLowerCase();
      ['energy','protein','fat','carbo','sugar','sodium','per 100','kJ','kj'].forEach(k=>{ if(low.includes(k)) s++; });
      return s;
    }

    const nodes = walk(document);
    let best = null, bestScore = -1;

    for (const n of nodes) {
      if (n.tagName === 'TABLE') {
        const tblText = text(n);
        const sc = scoreTable(tblText);
        if (sc > bestScore) { bestScore = sc; best = n; }
      }
    }

    if (!best) return null;

    // Extract headers
    let headers = [];
    const thead = best.querySelector('thead');
    if (thead) {
      const ths = thead.querySelectorAll('th, td');
      headers = Array.from(ths).map(th => text(th));
    }
    if (!headers.length) {
      const fr = best.querySelector('tr');
      if (fr) headers = Array.from(fr.querySelectorAll('th,td')).map(td => text(td));
    }

    // Extract rows
    let rows = [];
    const trs = best.querySelectorAll('tbody tr');
    if (trs.length) {
      rows = Array.from(trs).map(tr => Array.from(tr.querySelectorAll('td,th')).map(td => text(td)));
    } else {
      const all = best.querySelectorAll('tr');
      rows = Array.from(all).slice(1).map(tr => Array.from(tr.querySelectorAll('td,th')).map(td => text(td)));
    }

    // Serving size near table
    let serving = '';
    let parent = best.parentElement;
    for (let i=0;i<5 && parent;i++, parent = parent.parentElement) {
      const t = text(parent);
      const m = t.match(/serving\\s*size\\s*[:\\-]?\\s*([^\\n]+)/i);
      if (m) { serving = m[1].trim(); break; }
    }

    return { headers, rows, serving_size: serving, table_text: (best.innerText||'').trim() };
    """

def _detect_column_order_from_headers(headers):
    hdrs = [h.lower() for h in headers]
    idx_nutr = 0
    if any('nutrient' in h or 'average quantity' in h or 'avg qty' in h for h in hdrs):
        idx_nutr = min(range(len(hdrs)), key=lambda i: (0 if 'nutrient' in hdrs[i] else 1))
    idx_serv = next((i for i,h in enumerate(hdrs) if 'per serving' in h or 'serving' in h), -1)
    idx_100 = next((i for i,h in enumerate(hdrs) if 'per 100' in h or '100g' in h or '100 ml' in h), -1)
    if idx_serv == -1 or idx_100 == -1:
        numericish = [i for i,h in enumerate(hdrs) if any(k in h for k in ['per 100','100','serv'])]
        if len(numericish) >= 2:
            idx_serv, idx_100 = numericish[0], numericish[1]
    return idx_nutr, idx_serv, idx_100

def _detect_order_from_table_text(table_text: str):
    low = table_text.lower()
    ps = low.find("per serving")
    p100 = low.find("per 100")
    if ps != -1 and p100 != -1:
        return ("per_serving", "per_100g") if ps < p100 else ("per_100g", "per_serving")
    if p100 != -1:
        return ("per_100g", "per_serving")
    return ("per_serving", "per_100g")

def parse_nutrition_from_table(table_obj: dict) -> dict:
    if not table_obj:
        return {}
    headers = table_obj.get("headers") or []
    rows = table_obj.get("rows") or []
    serving_size = (table_obj.get("serving_size") or "").strip()
    table_text = table_obj.get("table_text") or ""

    per_serving, per_100g = {}, {}
    if not rows:
        return {"serving_size": serving_size} if serving_size else {}

    idx_nutr, idx_serv, idx_100 = _detect_column_order_from_headers(headers) if headers else (0, -1, -1)
    if idx_serv == -1 and idx_100 == -1:
        order = _detect_order_from_table_text(table_text)
        infer_only = True
    else:
        order = ("per_serving", "per_100g")
        infer_only = False

    def put(key, value, which):
        if not value: return
        if which == "per_serving": per_serving[key] = value
        else: per_100g[key] = value

    for r in rows:
        if not r: continue
        name = (r[idx_nutr] if idx_nutr < len(r) else r[0]).strip()
        if not name: continue
        low = name.lower()

        ckey = None
        for key, pat in NUTRIENT_KEYS:
            if re.search(pat, low, re.I):
                ckey = key
                break
        if not ckey:
            continue

        if infer_only:
            vals = [f"{n} {u}".replace(" ,", ",") for (n, u) in VAL_PATTERN.findall(" ".join(r))]
            if len(vals) >= 1:
                which1, which2 = order
                put(ckey, vals[0], which1)
                if len(vals) >= 2:
                    put(ckey, vals[1], which2)
        else:
            v_serv = r[idx_serv].strip() if 0 <= idx_serv < len(r) else ""
            v_100 = r[idx_100].strip() if 0 <= idx_100 < len(r) else ""
            put(ckey, v_serv, "per_serving")
            put(ckey, v_100, "per_100g")

    out = {}
    if serving_size: out["serving_size"] = serving_size
    if per_serving:  out["per_serving"] = per_serving
    if per_100g:    out["per_100g"] = per_100g
    return out

def _slice_section(text: str, heading_regex: re.Pattern) -> str:
    if not text:
        return ""
    t = text.replace('\r', '')
    m = heading_regex.search(t)
    start_idx = None
    if m:
        start_idx = m.end()
    else:
        low = t.lower()
        key = "ingredient" if heading_regex is ING_HEADING_PATTERN else "nutrition"
        pos = low.find(key)
        if pos == -1:
            return ""
        line_end = t.find('\n', pos)
        start_idx = (line_end + 1) if line_end != -1 else pos
    tail = t[start_idx:].lstrip()
    next_idx = None
    low_tail = tail.lower()
    candidates = []
    for kw in NEXT_HEADINGS:
        kpos = low_tail.find("\n" + kw)
        if kpos != -1:
            candidates.append(kpos)
        if low_tail.startswith(kw):
            candidates.append(0)
    lines = tail.splitlines()
    cut_at_chars = None
    for i, line in enumerate(lines[:120]):
        s = line.strip()
        if not s: continue
        if (len(s) <= 60 and (s.istitle() or s.isupper())
            and not s.endswith(':') and not s.lower().startswith(("•","- "))):
            if i > 0:
                cut_at_chars = len("\n".join(lines[:i]))
                break
    if candidates:
        cand_min = min(candidates)
        next_idx = cand_min
    if cut_at_chars is not None and (next_idx is None or cut_at_chars < next_idx):
        next_idx = cut_at_chars
    return tail.strip() if next_idx is None else tail[:next_idx].strip()

def parse_ingredients_from_text(text: str) -> str:
    sect = _slice_section(text, ING_HEADING_PATTERN)
    if not sect: return ""
    return re.sub(r'[ \t]+', ' ', sect).strip()

def parse_nutrition_from_text(text: str) -> dict:
    sect = _slice_section(text, NUT_HEADING_PATTERN)
    if not sect: return {}
    m_ss = re.search(r"serving\s*size\s*[:\-]?\s*([^\n]+)", sect, flags=re.I)
    serving_size = m_ss.group(1).strip() if m_ss else ""
    lines = [ln.strip() for ln in sect.splitlines() if ln.strip()]
    def collect_vals(ln):
        return [f"{n} {u}".replace(" ,", ",") for (n, u) in VAL_PATTERN.findall(ln)]
    per_serving, per_100g = {}, {}
    order = _detect_order_from_table_text(sect)
    for ln in lines:
        low = ln.lower()
        vals = collect_vals(ln)
        for key, pat in NUTRIENT_KEYS:
            if re.search(pat, low, re.I):
                if vals:
                    which1, which2 = order
                    if len(vals) >= 1: (per_serving if which1=="per_serving" else per_100g)[key] = vals[0]
                    if len(vals) >= 2: (per_serving if which2=="per_serving" else per_100g)[key] = vals[1]
                break
    out = {}
    if serving_size: out["serving_size"] = serving_size
    if per_serving:  out["per_serving"] = per_serving
    if per_100g:    out["per_100g"] = per_100g
    return out

def _scroll_page_fully(driver, step=900, pause=0.25, max_loops=40):
    """Scroll down to trigger lazy-loaded content."""
    last = 0
    loops = 0
    while loops < max_loops:
        driver.execute_script(f"window.scrollBy(0,{step});")
        time.sleep(pause)
        now = driver.execute_script("return Math.round(window.scrollY)")
        height = driver.execute_script("return document.body.scrollHeight")
        if now == last or now + 5 >= height:
            break
        last = now
        loops += 1
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(0.35)

def _expand_nutrition_sections(driver):
    """Click anything that looks like a Nutrition accordion/tab, then wait a tick."""
    js = r"""
    (function(){
      const rx = /(nutrition\s*information|nutritional|nutrition)\b/i;
      const els = Array.from(document.querySelectorAll(
        'button, a, summary, [role="button"], [role="tab"], [aria-controls], [onclick]'
      ));
      let clicked = 0;
      for (const el of els) {
        const t = (el.innerText || el.textContent || '').trim();
        if (!t) continue;
        if (rx.test(t)) {
          try { el.click(); clicked++; } catch(e){}
        }
      }
      for (const det of Array.from(document.querySelectorAll('details'))) {
        const sum = det.querySelector('summary');
        if (!sum) continue;
        const t = (sum.innerText || sum.textContent || '').trim();
        if (t && rx.test(t)) { try { det.open = true; clicked++; } catch(e){} }
      }
      return clicked;
    })();
    """
    try:
        return driver.execute_script(js) or 0
    except Exception:
        return 0

def fetch_details_for_link(driver, url: str) -> dict:
    """
    Open product page in a new tab, reveal Nutrition info by scrolling & expanding,
    then parse Ingredients + Nutrition from the page's visible text.
    Falls back to JSON-LD and (least preferred) table-walker.
    """
    if not url:
        return {"ingredients": "", "nutrition": {}}
    if url in _DETAILS_CACHE:
        return _DETAILS_CACHE[url]

    orig = driver.current_window_handle
    driver.switch_to.new_window('tab')
    try:
        driver.get(url)
        try:
            WebDriverWait(driver, max(ING_TIMEOUT, NUT_TIMEOUT)).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "body"))
            )
        except TimeoutException:
            pass

        _scroll_page_fully(driver)
        for _ in range(max(1, NUT_RETRIES)):
            _expand_nutrition_sections(driver)
            time.sleep(0.35)

        text_block = safe_js(driver, "return document.body.innerText || document.body.textContent || ''", default="") or ""
        if "nutrition" not in (text_block.lower()):
            _scroll_page_fully(driver)
            _expand_nutrition_sections(driver)
            time.sleep(0.35)
            text_block = safe_js(driver, "return document.body.innerText || document.body.textContent || ''", default="") or ""

        ingredients = parse_ingredients_from_text(text_block) if FETCH_INGREDIENTS else ""

        nutrition = {}
        if FETCH_NUTRITION:
            nutrition = parse_nutrition_from_text(text_block)

            if not nutrition:
                ld_jsons = safe_js(driver, """
                const out = [];
                const nodes = document.querySelectorAll('script[type="application/ld+json"],script[type="application/json"]');
                for (const s of nodes) {
                  try { const txt = s.textContent || ''; if (/nutrit/i.test(txt)) out.push(txt); } catch(e){}
                }
                return out;
                """, default=[]) or []
                for blob in ld_jsons:
                    try:
                        data = json.loads(blob)
                    except Exception:
                        continue
                    def hunt(x):
                        if isinstance(x, dict):
                            if 'nutrition' in x: return x['nutrition']
                            if 'nutritionInformation' in x: return x['nutritionInformation']
                            for v in x.values():
                                res = hunt(v)
                                if res: return res
                        elif isinstance(x, list):
                            for v in x:
                                res = hunt(v)
                                if res: return res
                        return None
                    nut = hunt(data)
                    if isinstance(nut, dict) and nut:
                        nutrition = {"raw": nut}
                        break

            if not nutrition:
                table_obj = safe_js(driver, _extract_nutrition_table_js(), default=None)
                if table_obj:
                    nutrition = parse_nutrition_from_table(table_obj)

        result = {"ingredients": ingredients, "nutrition": nutrition}
        _DETAILS_CACHE[url] = result
        return result

    finally:
        try: driver.close()
        except Exception: pass
        try: driver.switch_to.window(orig)
        except Exception: pass

# ======================
# SIGNAL HANDLING
# ======================

def graceful_exit(signum, frame):
    try:
        CFG.set('Woolworths', 'Resume_Active', 'TRUE')
        with open(CONFIG_FILE, 'w') as cfgf:
            CFG.write(cfgf)
    finally:
        print("\nInterrupted. Current resume point saved.")
        sys.exit(1)

signal.signal(signal.SIGINT, graceful_exit)

# ======================
# MAIN
# ======================

def main():
    if RESUME_ACTIVE:
        print(f"Resuming at page {RESUME_PAGE} of {RESUME_CATEGORY}")
    else:
        print("Resume data not found, starting anew...")

    # Ensure output directory is writable
    if not ensure_dir_writable(SAVE_DIR):
        raise PermissionError(f"SaveLocation '{SAVE_DIR}' is not writable. "
                              f"Update [Global].SaveLocation in configuration.ini to a user-writable path.")

    # Open CSV with retry/fallback (handles Excel/OneDrive locks)
    f, writer, active_csv_path = open_csv_writer_with_retry(CSV_PATH, CSV_HEADER)
    print(f"Saving to: {active_csv_path}")

    # Gather categories with a short-lived driver
    with edge_driver() as d0:
        print("Starting Woolworths...")
        cats_all = get_categories(d0)

    if not cats_all:
        print("No categories found. Exiting.")
        try: f.close()
        except Exception: pass
        return

    categories = filter_for_resume(cats_all)

    print("Categories to Scrape:")
    for c in categories:
        print(f"- {c['name']}  (/shop/browse/{c['endpoint']})")

    pages_since_restart = 0
    DO_FETCH_DETAILS = (FETCH_INGREDIENTS or FETCH_NUTRITION)

    drv_ctx = edge_driver()
    driver = drv_ctx.__enter__()
    try:
        for cat in categories:
            cat_name = cat["name"]
            cat_href = cat["href"]

            persist_resume(cat_name, RESUME_PAGE if (RESUME_ACTIVE and cat_name == RESUME_CATEGORY) else 1, True)

            open_category_page(driver, cat_href, 1)
            total_pages = get_total_pages(driver)
            first_page = RESUME_PAGE if (RESUME_ACTIVE and cat_name == RESUME_CATEGORY) else 1

            for page in range(first_page, total_pages + 1):
                persist_resume(cat_name, page, True)
                open_category_page(driver, cat_href, page)

                tries, product_count = 0, count_tiles(driver)
                while product_count == 0 and tries < 2:
                    print("Grid empty; retrying after a longer wait...")
                    time.sleep(DELAY_BASE + 1.5)
                    product_count = count_tiles(driver)
                    tries += 1

                print(f"{cat_name}: Page {page} of {total_pages} | Products on this page: {product_count}")

                if product_count == 0:
                    pages_since_restart += 1
                    if pages_since_restart % RESTART_EVERY_PAGES == 0:
                        print("Restarting browser for hygiene...")
                        try: driver.quit()
                        except Exception: pass
                        drv_ctx.__exit__(None, None, None)
                        drv_ctx = edge_driver()
                        driver = drv_ctx.__enter__()
                    continue

                for idx in range(product_count):
                    data = get_tile_texts(driver, idx)
                    name = data["name"]; itemprice = data["price"]; unitprice = data["unitprice"]
                    specialtext = data["special"]; promotext = data["promo"]
                    price_was_struckout = data["was"]; productLink = data["link"]; productcode = data["code"]

                    if not (name and itemprice and productLink and productcode):
                        continue

                    best_price, best_unitprice, price_was_from_promo = normalize_best_price(itemprice, unitprice, promotext)
                    price_was = (price_was_struckout or "").strip() or price_was_from_promo

                    ingredients = ""
                    nutrition_json = ""
                    if DO_FETCH_DETAILS:
                        try:
                            details = fetch_details_for_link(driver, productLink)
                            if FETCH_INGREDIENTS:
                                ingredients = details.get("ingredients", "")
                            if FETCH_NUTRITION:
                                nd = details.get("nutrition", {})
                                nutrition_json = json.dumps(nd, ensure_ascii=False) if nd else ""
                        except Exception:
                            pass

                    writer.writerow([
                        productcode, cat_name, name,
                        best_price, best_unitprice,
                        itemprice, unitprice,
                        price_was, specialtext, promotext, productLink,
                        ingredients, nutrition_json
                    ])

                pages_since_restart += 1
                if pages_since_restart % RESTART_EVERY_PAGES == 0:
                    print("Restarting browser for hygiene...")
                    try: driver.quit()
                    except Exception: pass
                    drv_ctx.__exit__(None, None, None)
                    drv_ctx = edge_driver()
                    driver = drv_ctx.__enter__()

            nap()

    finally:
        try: driver.quit()
        except Exception: pass
        drv_ctx.__exit__(None, None, None)
        try: f.close()
        except Exception: pass

    CFG.set('Woolworths', 'Resume_Active', "FALSE")
    CFG.set('Woolworths', 'Resume_Category', "null")
    CFG.set('Woolworths', 'Resume_Page', "0")
    with open(CONFIG_FILE, 'w') as cfgf:
        CFG.write(cfgf)

    print("Finished")

if __name__ == "__main__":
    main()
