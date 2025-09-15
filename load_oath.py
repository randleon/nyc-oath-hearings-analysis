"""
Loads NYC OATH data into PostgreSQL 15 without hardcoded credentials.

Usage:
  1) Set env vars (see .env.example) or create a .env file in repo root.
  2) pip install -r requirements.txt
  3) python etl/oath_load_chunked.py

Env vars used:
  PGHOST, PGPORT, PGUSER, PGPASSWORD, PGDATABASE
  SOCRATA_APP_TOKEN (optional, improves reliability)

Requires:
  - requests
  - psycopg2-binary
  - python-dotenv (optional; only if you want .env file support)
"""

import os
import time
import io
import csv
import tempfile
import requests
import psycopg2

# --- Optional .env support (safe if missing) ---
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()  # will only work if python-dotenv is installed
except Exception:
    pass

# ---- CONFIG ----
PGHOST = os.getenv("PGHOST", "localhost")
PGPORT = int(os.getenv("PGPORT", "5432"))
PGUSER = os.getenv("PGUSER", "postgres")
PGPASSWORD = os.getenv("PGPASSWORD", "")
PGDATABASE = os.getenv("PGDATABASE", "nycdata")

# Build DSN from env (don’t print the password)
PG_DSN = f"host={PGHOST} port={PGPORT} dbname={PGDATABASE} user={PGUSER} password={PGPASSWORD}"

# Target table (6 columns)
TABLE = "oath_cases"

# Column selection from Socrata (order matters)
FIELDS = [
    "ticket_number",   # -> case_id
    "hearing_date",    # -> hearing_date
    "issuing_agency",  # -> violation_type
    "hearing_result",  # -> decision
    "penalty_imposed", # -> amount_due
    "paid_amount",     # -> amount_paid
]

SODA_CSV_URL = "https://data.cityofnewyork.us/resource/jz4z-kudi.csv"

TOTAL_TARGET = 50_000
PAGE_LIMIT   = 10_000  # 10k x 5 pages = 50k

# Optional date filter (leave False for max reliability)
USE_DATE_FILTER = False
DATE_FIELD = "hearing_date"
DATE_FROM  = "2024-09-14T00:00:00.000"

SOCRATA_APP_TOKEN = os.getenv("SOCRATA_APP_TOKEN")  # optional

HEADERS = {
    "Accept": "text/csv",
    "Accept-Encoding": "gzip, deflate",
    **({"X-App-Token": SOCRATA_APP_TOKEN} if SOCRATA_APP_TOKEN else {}),
}

# ---- Helpers ----
def ensure_table_exists(conn):
    ddl = f"""
    CREATE TABLE IF NOT EXISTS {TABLE} (
        case_id        text,
        hearing_date   date,
        violation_type text,
        decision       text,
        amount_due     numeric,
        amount_paid    numeric
    );
    """
    with conn.cursor() as cur:
        cur.execute(ddl)
        conn.commit()

def fetch_page(offset: int) -> str:
    params = {
        "$select": ",".join(FIELDS),
        "$limit": str(PAGE_LIMIT),
        "$offset": str(offset),
        # Avoid $order to reduce 406/timeouts; sort later in SQL if needed.
    }
    if USE_DATE_FILTER:
        params["$where"] = f"{DATE_FIELD} >= '{DATE_FROM}'"

    backoff = 2
    for attempt in range(6):  # ~2+4+8+16+32 = up to ~62s of backoff
        try:
            r = requests.get(SODA_CSV_URL, params=params, headers=HEADERS, timeout=(10,180))
            if r.status_code == 406:
                # Some Socrata stacks dislike Accept: text/csv; try plain
                r = requests.get(SODA_CSV_URL, params=params, headers={"Accept": "text/plain"}, timeout=(10,180))
            r.raise_for_status()
            text = r.text
            # If only header/no data
            if not text or text.count("\n") < 1:
                return ""
            return text
        except requests.exceptions.RequestException:
            if attempt == 5:
                raise
            time.sleep(backoff)
            backoff *= 2
    return ""

def build_local_csv() -> tuple[str, int]:
    rows_written = 0
    offset = 0
    header_written = False
    tmp_path = os.path.join(tempfile.gettempdir(), "oath_50k.csv")
    with open(tmp_path, "w", encoding="utf-8", newline="") as f:
        while rows_written < TOTAL_TARGET:
            chunk = fetch_page(offset)
            if not chunk:
                break
            lines = chunk.splitlines()
            if not lines:
                break

            header = lines[0]
            data_lines = lines[1:] if len(lines) > 1 else []

            # sanity: guarantee 6 columns in header
            hdr_cols = next(csv.reader([header]))
            if len(hdr_cols) != len(FIELDS):
                raise RuntimeError(f"Unexpected column count from Socrata: {len(hdr_cols)} (expected {len(FIELDS)})")

            if not header_written:
                f.write(header + "\n")
                header_written = True

            if data_lines:
                f.write("\n".join(data_lines) + "\n")
                rows_written += len(data_lines)
            else:
                break

            offset += PAGE_LIMIT
            if len(data_lines) < PAGE_LIMIT:
                # likely reached end
                break
    return tmp_path, rows_written

def copy_into_postgres(csv_path: str):
    """
    COPY order must match TABLE column list and CSV field order.
    CSV header names are skipped (HEADER true) and do not need to match.
    """
    copy_sql = f"""
    COPY {TABLE} (case_id, hearing_date, violation_type, decision, amount_due, amount_paid)
    FROM STDIN WITH (FORMAT csv, HEADER true)
    """
    with psycopg2.connect(PG_DSN) as conn, conn.cursor() as cur:
        ensure_table_exists(conn)
        with open(csv_path, "r", encoding="utf-8") as fh:
            cur.copy_expert(copy_sql, fh)
        conn.commit()

def main():
    print(f"Connecting to PostgreSQL at {PGHOST}:{PGPORT} db={PGDATABASE} user={PGUSER}")
    print("Downloading from Socrata (paged)…")
    csv_path, n_rows = build_local_csv()
    if n_rows == 0:
        print("⚠️ No data fetched. Try setting USE_DATE_FILTER=False or switching DATE_FIELD/DATE_FROM.")
        print("Also consider setting SOCRATA_APP_TOKEN to reduce throttling.")
        print("CSV path:", csv_path)
        raise SystemExit(1)

    print(f"Built CSV at {csv_path} with ~{n_rows} rows. Loading into Postgres table `{TABLE}`…")
    copy_into_postgres(csv_path)
    print("✅ Done.")

if __name__ == "__main__":
    main()
