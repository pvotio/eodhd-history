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

def fetch_eod_data(ticker, from_date, to_date, api_token):
    """Fetch daily OHLC data from EODHD for the given ticker & date range."""
    url = f"https://eodhd.com/api/eod/{ticker}"
    params = {
        "from": from_date,
        "to": to_date,
        "period": "d",
        "api_token": api_token,
        "fmt": "json"
    }
    try:
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logging.error(f"Error fetching EOD data for ticker {ticker}: {e}")
        return []

def main():
    # --- 1) Load environment variables ---
    db_server = os.getenv("DB_SERVER")
    db_name   = os.getenv("DB_NAME")
    pre_sql   = os.getenv("PRE_SQL")
    post_sql  = os.getenv("POST_SQL")
    api_token = os.getenv("EODHD_API_TOKEN")
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

    # 6) Fetch EOD data in parallel
    logging.info(f"Fetching EOD data from {from_date} to {to_date}...")
    eod_records = []
    max_workers = 12
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(fetch_eod_data, t, from_date, to_date, api_token): t
            for t in tickers
        }
        for future in as_completed(future_map):
            ticker = future_map[future]
            try:
                data = future.result()
                for row in data:
                    eod_records.append({
                        "ext2_ticker": ticker,
                        "timestamp_created_utc": datetime.now(timezone.utc),
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
            except Exception as exc:
                logging.error(f"Error fetching data for ticker {ticker}: {exc}")

    if not eod_records:
        logging.info("No EOD records fetched. Exiting.")
        return

    df_eod = pd.DataFrame(eod_records)
    logging.info(f"Fetched {len(df_eod)} EOD rows in total.")

    # 7) Pre-SQL if provided
    try:
        with pyodbc.connect(odbc_connection_str, attrs_before=attrs) as pre_conn:
            pre_cursor = pre_conn.cursor()
            run_tsql(pre_cursor, pre_sql, description="Pre-import SQL")
    except Exception as ex:
        logging.error(f"Failed to run pre-import: {ex}")

    # 8) Bulk insert into the target table
    insert_sql = f"""
    INSERT INTO {target_table} (
        ext2_ticker,
        timestamp_created_utc,
        date,
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

    def insert_chunk(chunk_df):
        if chunk_df.empty:
            return 0
        local_conn = None
        local_cursor = None
        try:
            local_conn = pyodbc.connect(odbc_connection_str, attrs_before=attrs)
            local_cursor = local_conn.cursor()
            local_cursor.fast_executemany = True

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
            local_cursor.executemany(insert_sql, data_tuples)
            local_conn.commit()
            return len(data_tuples)
        except Exception as e:
            logging.error(f"Error inserting chunk: {e}")
            raise
        finally:
            if local_cursor:
                local_cursor.close()
            if local_conn:
                local_conn.close()

    chunk_size = 5000
    total_inserted = 0
    chunks = []
    for start_idx in range(0, len(df_eod), chunk_size):
        end_idx = start_idx + chunk_size
        chunks.append(df_eod.iloc[start_idx:end_idx])

    logging.info(f"Beginning multithreaded insert of {len(df_eod)} rows, in {len(chunks)} chunk(s).")

    with ThreadPoolExecutor(max_workers=7) as executor:
        future_map = {executor.submit(insert_chunk, c): i for i, c in enumerate(chunks, start=1)}
        for future in as_completed(future_map):
            chunk_num = future_map[future]
            try:
                inserted = future.result()
                total_inserted += inserted
                logging.info(f"Chunk {chunk_num} inserted {inserted} rows. (Total so far: {total_inserted})")
            except Exception as ex:
                logging.error(f"Chunk {chunk_num} failed: {ex}")

    logging.info(f"All chunks completed. Total rows inserted: {total_inserted}.")

    # 9) Post-SQL if provided
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
