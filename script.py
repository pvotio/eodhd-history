import os
import sys
import struct
import logging
import requests
import pyodbc
import pandas as pd
from datetime import datetime, timezone, timedelta

# Replaced InteractiveBrowserCredential with DefaultAzureCredential
from azure.identity import DefaultAzureCredential

from urllib.parse import quote_plus
from sqlalchemy import create_engine
from concurrent.futures import ThreadPoolExecutor, as_completed
import time  # <-- For sleep/backoff

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)

def get_pyodbc_attrs(access_token: str) -> dict:
    """Format the Azure AD access token for pyodbc's SQL_COPT_SS_ACCESS_TOKEN."""
    SQL_COPT_SS_ACCESS_TOKEN = 1256
    enc_token = access_token.encode('utf-16-le')
    token_struct = struct.pack('=i', len(enc_token)) + enc_token
    return {SQL_COPT_SS_ACCESS_TOKEN: token_struct}

def run_tsql(cursor, sql, description="T-SQL"):
    """Helper to execute T-SQL and optionally fetch one row."""
    if not sql:
        logging.info(f"No {description} provided, skipping.")
        return
    logging.info(f"Executing {description}: {sql}")
    try:
        cursor.execute(sql)
        try:
            row = cursor.fetchone()
            if row is not None:
                logging.info(f"{description} => Returned: {row}")
        except pyodbc.ProgrammingError:
            pass
    except Exception as ex:
        logging.error(f"Failed to execute {description}: {ex}")
        raise

def get_tickers(engine, ticker_sql):
    """Fetch a list of tickers from the database using the specified SQL query."""
    if not ticker_sql:
        logging.error("TICKER_SQL is not provided. Exiting.")
        sys.exit(1)

    try:
        df = pd.read_sql(ticker_sql, engine)
        # For safety, assume first column is ticker
        tickers = df.iloc[:, 0].dropna().tolist()
        logging.info(f"Fetched {len(tickers)} tickers from the database.")
        return tickers
    except Exception as e:
        logging.error(f"Error fetching tickers from the database: {e}")
        sys.exit(1)

def fetch_eod_data(ticker, from_date, to_date, api_token, max_retries=3):
    """
    Fetch daily OHLC data from EODHD for the given ticker & date range,
    retrying up to 'max_retries' times if a 429 (Too Many Requests) or
    certain other errors occur. We sleep for 5 seconds when we get a 429
    before retrying.
    """
    url = f"https://eodhd.com/api/eod/{ticker}"
    params = {
        "from": from_date,
        "to": to_date,
        "period": "d",
        "api_token": api_token,
        "fmt": "json"
    }

    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params)
            resp.raise_for_status()  # Raises HTTPError if status != 200
            return resp.json()       # Typically a list of OHLC objects

        except requests.exceptions.HTTPError as http_err:
            status_code = http_err.response.status_code
            if status_code == 429:
                # Too Many Requests
                if attempt < max_retries:
                    logging.warning(
                        f"Received 429 Too Many Requests for ticker {ticker} "
                        f"(Attempt {attempt}/{max_retries}). Sleeping 5s before retry..."
                    )
                    time.sleep(5)
                else:
                    logging.error(
                        f"Received 429 for ticker {ticker} on final attempt. Aborting."
                    )
                    return []
            else:
                logging.error(
                    f"HTTP error for ticker {ticker} (Attempt {attempt}/{max_retries}): {http_err}"
                )
                # If you only want to retry on 429, break immediately for other errors
                break

        except Exception as e:
            logging.error(
                f"Error fetching EOD data for ticker {ticker} (Attempt {attempt}/{max_retries}): {e}"
            )
            # Decide whether to keep retrying or not. Here, we break on first non-HTTPError.
            break

    # If we exit the loop, either used up all retries or had a non-retriable error
    return []

def main():
    # --- 1) Load environment variables ---
    db_server    = os.getenv("DB_SERVER")
    db_name      = os.getenv("DB_NAME")
    pre_sql      = os.getenv("PRE_SQL")
    post_sql     = os.getenv("POST_SQL")
    api_token    = os.getenv("EODHD_API_TOKEN")
    target_table = os.getenv("TARGET_TABLE")
    ticker_sql   = os.getenv("TICKER_SQL")

    # 2) Configure date range
    from_date = "2020-01-01"
    to_date   = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

    # 3) Obtain Azure AD token using DefaultAzureCredential
    logging.info("Obtaining Azure AD token via DefaultAzureCredential...")
    try:
        credential = DefaultAzureCredential()
        token = credential.get_token("https://database.windows.net/.default")
        access_token = token.token
        logging.info("Successfully obtained access token for SQL Database.")
    except Exception as e:
        logging.error(f"Failed to obtain access token for SQL Database: {e}")
        sys.exit(1)

    # 4) Build SQLAlchemy engine
    attrs = get_pyodbc_attrs(access_token)
    odbc_connection_str = (
        "DRIVER={ODBC Driver 18 for SQL Server};"
        f"SERVER={db_server};"
        f"DATABASE={db_name};"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
    )

    engine = create_engine(
        f"mssql+pyodbc:///?odbc_connect={quote_plus(odbc_connection_str)}",
        connect_args={'attrs_before': attrs}
    )

    # 5) Fetch tickers
    tickers = get_tickers(engine, ticker_sql)
    if not tickers:
        logging.info("No tickers found. Exiting.")
        return

    # 6) Optional: run pre-import SQL if provided
    try:
        with pyodbc.connect(odbc_connection_str, attrs_before=attrs) as pre_conn:
            pre_cursor = pre_conn.cursor()
            run_tsql(pre_cursor, pre_sql, description="Pre-import SQL")
    except Exception as ex:
        logging.error(f"Failed to run pre-import: {ex}")

    # 7) Prepare the insert statement (common for all tickers)
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

    # 8) Define a function that fetches + inserts *one* ticker at a time
    def fetch_and_insert_ticker(ticker):
        """Fetch data for a single ticker, and insert in chunks to save memory."""
        # (A) fetch data with retry logic
        raw_data = fetch_eod_data(ticker, from_date, to_date, api_token, max_retries=3)
        if not raw_data:
            return 0  # no data or error

        # (B) transform into a DataFrame
        now_utc = datetime.now(timezone.utc)
        records = []
        for row in raw_data:
            records.append({
                "ext2_ticker": ticker,
                "timestamp_created_utc": now_utc,
                "date": row.get("date"),
                "open": row.get("open"),
                "high": row.get("high"),
                "low": row.get("low"),
                "close": row.get("close"),
                "adjusted_close": row.get("adjusted_close"),
                "volume": row.get("volume"),
                "currency": row.get("exchange", None),
                "adjusted_close_usd": None,
                "exchange_rate": None
            })
        df = pd.DataFrame(records)
        if df.empty:
            return 0

        # (C) chunked insert
        chunk_size = 5000
        inserted_count = 0
        for start_idx in range(0, len(df), chunk_size):
            end_idx = start_idx + chunk_size
            chunk_df = df.iloc[start_idx:end_idx]

            # Insert this chunk
            try:
                with pyodbc.connect(odbc_connection_str, attrs_before=attrs) as conn:
                    cursor = conn.cursor()
                    cursor.fast_executemany = True

                    data_tuples = list(
                        chunk_df[[
                            "ext2_ticker",
                            "timestamp_created_utc",
                            "date",
                            "open",
                            "high",
                            "low",
                            "close",
                            "adjusted_close",
                            "volume",
                            "currency",
                            "adjusted_close_usd",
                            "exchange_rate"
                        ]].itertuples(index=False, name=None)
                    )
                    cursor.executemany(insert_sql, data_tuples)
                    conn.commit()
                    inserted_count += len(data_tuples)
            except Exception as e:
                logging.error(f"Error inserting chunk for ticker {ticker}: {e}")
                raise

        return inserted_count

    # 9) Parallel fetch + insert across all tickers
    logging.info(f"Fetching & inserting EOD data for {len(tickers)} tickers in parallel...")
    total_inserted = 0
    max_workers = 12

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {executor.submit(fetch_and_insert_ticker, t): t for t in tickers}
        for future in as_completed(future_map):
            ticker = future_map[future]
            try:
                inserted = future.result()
                total_inserted += inserted
                logging.info(f"Ticker {ticker}: inserted {inserted} row(s). (Running total: {total_inserted})")
            except Exception as exc:
                logging.error(f"Ticker {ticker} failed: {exc}")

    logging.info(f"All tickers completed. Total rows inserted: {total_inserted}.")

    # 10) Optional: run post-import SQL if provided
    try:
        with pyodbc.connect(odbc_connection_str, attrs_before=attrs) as post_conn:
            post_cursor = post_conn.cursor()
            run_tsql(post_cursor, post_sql, description="Post-import SQL")
    except Exception as ex:
        logging.error(f"Failed to run post-import: {ex}")

    logging.info("Script completed successfully.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.error(f"Script terminated with an error: {e}")
        sys.exit(1)

