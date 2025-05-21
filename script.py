#!/usr/bin/env python3
"""
EOD-daily loader (Azure SQL)

✓ Opens at most ONE database connection per worker thread.
✓ Commits once per ticker, not per chunk.
"""

import os
import sys
import struct
import logging
import requests
import pyodbc
import pandas as pd
from datetime import datetime, timezone, timedelta
from azure.identity import DefaultAzureCredential
from urllib.parse import quote_plus
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import threading   # <-- NEW

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)

# ---------------------------------------------------------------------------
# Thread-local connection cache
# ---------------------------------------------------------------------------
thread_local = threading.local()   # one instance for the whole process


def get_conn(odbc_connection_str, attrs):
    """
    Return a cached pyodbc connection tied to the current thread.
    A connection is opened the first time this thread calls the function
    and reused for all subsequent calls.
    """
    conn = getattr(thread_local, "conn", None)
    # `connected` exists on pyodbc ≥ 5.1; older versions fall back to try/except
    if conn is None or getattr(conn, "connected", True) is False:
        conn = pyodbc.connect(
            odbc_connection_str,
            attrs_before=attrs,
            autocommit=False,      # we call conn.commit() ourselves
        )
        thread_local.conn = conn
        logging.debug("Opened new DB connection for thread %s", threading.current_thread().name)
    return conn


def close_all_thread_conns():
    """Cleanly close any thread-local connections after ThreadPoolExecutor is done."""
    for t in threading.enumerate():
        conn = getattr(t, "conn", None)
        if conn:
            try:
                conn.close()
            except Exception:
                pass

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def get_pyodbc_attrs(access_token: str) -> dict:
    """Format the Azure AD access token for pyodbc's SQL_COPT_SS_ACCESS_TOKEN."""
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    enc_token = access_token.encode("utf-16-le")
    token_struct = struct.pack("=i", len(enc_token)) + enc_token
    return {SQL_COPT_SS_ACCESS_TOKEN: token_struct}


def run_tsql(cursor, sql, description="T-SQL"):
    """Execute arbitrary T-SQL and (optionally) log the first row."""
    if not sql:
        logging.info("No %s provided, skipping.", description)
        return
    logging.info("Running %s …", description)
    cursor.execute(sql)
    try:
        row = cursor.fetchone()
        if row is not None:
            logging.info("%s → %s", description, row)
    except pyodbc.ProgrammingError:
        pass


def get_tickers(engine, ticker_sql):
    """Return the ticker list produced by `ticker_sql` (assumes first column)."""
    if not ticker_sql:
        logging.error("TICKER_SQL is not set. Exiting.")
        sys.exit(1)
    try:
        df = pd.read_sql(ticker_sql, engine)
        tickers = df.iloc[:, 0].dropna().tolist()
        logging.info("Fetched %d tickers.", len(tickers))
        return tickers
    except Exception as e:
        logging.error("Failed to fetch tickers: %s", e)
        sys.exit(1)


def fetch_eod_data(ticker, from_date, to_date, api_token, max_retries=3):
    """GET /api/eod with simple back-off on HTTP 429."""
    url = f"https://eodhd.com/api/eod/{ticker}"
    params = {
        "from": from_date,
        "to": to_date,
        "period": "d",
        "api_token": api_token,
        "fmt": "json",
    }

    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            return r.json()
        except requests.exceptions.HTTPError as he:
            if he.response.status_code == 429 and attempt < max_retries:
                logging.warning("%s 429 (attempt %d/%d) → sleep 5 s", ticker, attempt, max_retries)
                time.sleep(5)
                continue
            logging.error("%s HTTP error: %s", ticker, he)
        except Exception as e:
            logging.error("%s fetch failed: %s", ticker, e)
        break
    return []  # exhausted retries or fatal error


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # Environment
    db_server = os.getenv("DB_SERVER")
    db_name = os.getenv("DB_NAME")
    pre_sql = os.getenv("PRE_SQL")
    post_sql = os.getenv("POST_SQL")
    api_token = os.getenv("EODHD_API_TOKEN")
    target_table = os.getenv("TARGET_TABLE")
    ticker_sql = os.getenv("TICKER_SQL")

    from_date = "2020-01-01"
    to_date = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    # Azure AD access token
    logging.info("Getting Azure AD token …")
    credential = DefaultAzureCredential()
    access_token = credential.get_token("https://database.windows.net/.default").token
    attrs = get_pyodbc_attrs(access_token)

    # Connection string & SQLAlchemy engine (for ticker query only)
    odbc_connection_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={db_server};"
        f"DATABASE={db_name};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )
    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_connection_str)}",
        connect_args={"attrs_before": attrs},
    )

    tickers = get_tickers(engine, ticker_sql)
    if not tickers:
        logging.info("No tickers → nothing to do.")
        return

    # -----------------------------------------------------------------------
    # Optional pre-import SQL
    # -----------------------------------------------------------------------
    try:
        with pyodbc.connect(odbc_connection_str, attrs_before=attrs) as c:
            run_tsql(c.cursor(), pre_sql, "pre-import SQL")
    except Exception as ex:
        logging.error("Pre-import SQL failed: %s", ex)

    # -----------------------------------------------------------------------
    # Prepared INSERT (parameter placeholders must match column order)
    # -----------------------------------------------------------------------
    insert_sql = f"""
    INSERT INTO {target_table} (
        ext2_ticker,
        timestamp_created_utc,
        update_date,
        [open],
        high,
        low,
        [close],
        adjusted_close,
        volume,
        currency,
        adjusted_close_usd,
        exchange_rate
    )
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """

    chunk_size = 5_000
    max_workers = 12

    def worker(ticker: str) -> int:
        """Fetch EOD data for ONE ticker and insert to DB in chunks."""
        raw = fetch_eod_data(ticker, from_date, to_date, api_token)
        if not raw:
            return 0

        now_utc = datetime.now(timezone.utc)
        df = pd.DataFrame(
            [
                {
                    "ext2_ticker": ticker,
                    "timestamp_created_utc": now_utc,
                    "update_date": row.get("date"),
                    "open": row.get("open"),
                    "high": row.get("high"),
                    "low": row.get("low"),
                    "close": row.get("close"),
                    "adjusted_close": row.get("adjusted_close"),
                    "volume": row.get("volume"),
                    "currency": row.get("exchange", None),
                    "adjusted_close_usd": None,
                    "exchange_rate": None,
                }
                for row in raw
            ]
        )
        if df.empty:
            return 0

        inserted = 0
        conn = get_conn(odbc_connection_str, attrs)
        cur = conn.cursor()
        cur.fast_executemany = True

        for start in range(0, len(df), chunk_size):
            rows = list(
                df.iloc[start : start + chunk_size][
                    [
                        "ext2_ticker",
                        "timestamp_created_utc",
                        "update_date",
                        "open",
                        "high",
                        "low",
                        "close",
                        "adjusted_close",
                        "volume",
                        "currency",
                        "adjusted_close_usd",
                        "exchange_rate",
                    ]
                ].itertuples(index=False, name=None)
            )
            cur.executemany(insert_sql, rows)
            inserted += len(rows)

        conn.commit()  # one commit per ticker
        return inserted

    # -----------------------------------------------------------------------
    # Parallel load
    # -----------------------------------------------------------------------
    logging.info("Loading %d tickers with %d threads …", len(tickers), max_workers)
    total = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(worker, t): t for t in tickers}
        for f in as_completed(futures):
            ticker = futures[f]
            try:
                n = f.result()
                total += n
                logging.info("%s → %d rows (total %d)", ticker, n, total)
            except Exception as e:
                logging.error("%s failed: %s", ticker, e)

    # -----------------------------------------------------------------------
    # Optional post-import SQL
    # -----------------------------------------------------------------------
    try:
        with pyodbc.connect(odbc_connection_str, attrs_before=attrs) as c:
            run_tsql(c.cursor(), post_sql, "post-import SQL")
    except Exception as ex:
        logging.error("Post-import SQL failed: %s", ex)

    # Clean-up
    close_all_thread_conns()
    logging.info("Finished. Total rows inserted: %d", total)


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error("Fatal: %s", e)
        sys.exit(1)


