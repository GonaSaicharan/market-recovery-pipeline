# Databricks notebook source
# MAGIC %pip install yfinance

# COMMAND ----------

import yfinance as yf
import datetime

end_date = datetime.date.today().isoformat()
test_tickers = ["DIS", "SKYD", "VMRK"]
results = {}

for t in test_tickers:
    try:
        results[t] = yf.download(t, start="2019-01-01", end=end_date,
                                 auto_adjust=False, progress=False)
    except Exception as e:
        results[t] = None
        print(t, "-> ERROR:", type(e).__name__, str(e)[:100])

for t, df in results.items():
    if df is None:
        continue
    if df.empty:
        print(t, "-> EMPTY, no rows returned")
    else:
        print(t, "->", df.shape, "first:", df.index.min().date(), "last:", df.index.max().date())

print(results["DIS"].columns.tolist())
print(results["DIS"].head(2))

# COMMAND ----------

for t, df in results.items():
    close = df["Close"][t]
    print(t, "| first:", round(close.iloc[0], 2), "| last:", round(close.iloc[-1], 2),
          "| min:", round(close.min(), 2), "| max:", round(close.max(), 2),
          "| missing:", int(df.isna().sum().sum()))

dis_close = results["DIS"]["Close"]["DIS"].to_numpy()
print("SKYD same as DIS on every day:", (results["SKYD"]["Close"]["SKYD"].to_numpy() == dis_close).all())
print("VMRK same as DIS on every day:", (results["VMRK"]["Close"]["VMRK"].to_numpy() == dis_close).all())

# COMMAND ----------

for t in ["SKYD", "VMRK", "DIS"]:
    try:
        info = yf.Ticker(t).info
        print(t, "|", info.get("longName"), "|", info.get("quoteType"), "|", info.get("exchange"))
    except Exception as e:
        print(t, "-> ERROR:", type(e).__name__, str(e)[:100])

# COMMAND ----------

import pandas as pd
bad_tickers = ["VMRK", "SKYD"]
mapping_old = pd.read_csv("/Volumes/workspace/market_pipeline/raw/reference/ticker_sector_mapping.csv")

kept = mapping_old[~mapping_old["ticker"].isin(bad_tickers)]
new_rows = []
change_log = []

for bad in bad_tickers:
    sector = mapping_old.loc[mapping_old["ticker"] == bad, "sector"].iloc[0]
    pool = eligible[(eligible["GICS Sector"] == sector) & (~eligible["Symbol"].isin(mapping_old["ticker"]))]
    pick = pool.sample(n=1, random_state=42).iloc[0]
    new_rows.append({"ticker": pick["Symbol"], "company": pick["Security"], "sector": sector})
    change_log.append({"old_ticker": bad, "new_ticker": pick["Symbol"], "sector": sector,
                       "pool_size": len(pool)})

mapping_v2 = pd.concat([kept, pd.DataFrame(new_rows)], ignore_index=True)
mapping_v2 = mapping_v2.sort_values(["sector", "ticker"]).reset_index(drop=True)

print(pd.DataFrame(change_log))
print("Rows:", len(mapping_v2), "| Duplicate tickers:", mapping_v2["ticker"].duplicated().sum())
print(mapping_v2["sector"].value_counts())

# COMMAND ----------

pip install pandas

# COMMAND ----------

import os
import time
import datetime
import pandas as pd
import yfinance as yf

raw_root = "/Volumes/workspace/market_pipeline/raw"
mapping = pd.read_csv(f"{raw_root}/reference/ticker_sector_mapping_v2.csv")

stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
run_dir = f"{raw_root}/prices/run_{stamp}"
os.makedirs(run_dir, exist_ok=False)

start_date = "2019-01-01"
end_date = datetime.date.today().isoformat()
print("Tickers to pull:", len(mapping))
print("Run folder:", run_dir)
print("Date range:", start_date, "to", end_date, "(end is exclusive)")

# COMMAND ----------

log_rows = []
consecutive_failures = 0

for i, row in mapping.iterrows():
    ticker = row["ticker"]
    entry = {"ticker": ticker, "wiki_name": row["company"], "yahoo_name": None,
             "status": None, "rows": 0, "first_date": None, "last_date": None,
             "file": None, "error": None}
    try:
        df = yf.download(ticker, start=start_date, end=end_date,
                         auto_adjust=False, progress=False)
        if df.empty:
            entry["status"] = "empty"
        else:
            df = df.droplevel("Ticker", axis=1)
            file_name = f"{ticker}_{stamp}.csv"
            df.to_csv(f"{run_dir}/{file_name}")
            entry.update(status="ok", rows=len(df),
                         first_date=str(df.index.min().date()),
                         last_date=str(df.index.max().date()), file=file_name)
    except Exception as e:
        entry["status"] = "error"
        entry["error"] = f"{type(e).__name__}: {str(e)[:100]}"

    try:
        entry["yahoo_name"] = yf.Ticker(ticker).info.get("longName")
    except Exception:
        entry["yahoo_name"] = None

    log_rows.append(entry)
    print(f"{i + 1}/{len(mapping)} {ticker}: {entry['status']}")

    if entry["status"] == "ok":
        consecutive_failures = 0
    else:
        consecutive_failures += 1
        if consecutive_failures >= 5:
            print("STOPPING: 5 failures in a row, likely rate-limited or a code problem")
            break
    time.sleep(1)

log_df = pd.DataFrame(log_rows)
log_path = f"{run_dir}/pull_log_{stamp}.csv"
log_df.to_csv(log_path, index=False)
print("Log saved:", log_path)

# COMMAND ----------

print(log_df["status"].value_counts())
print("Total rows saved:", log_df["rows"].sum())
print(len(os.listdir(run_dir)), "files in run folder (expect ok-count + 1 log)")

print("--- Not ok ---")
print(log_df[log_df["status"] != "ok"][["ticker", "status", "error"]].to_string())

print("--- Short history (starts after 2019-01-10 or under 1900 rows) ---")
short = log_df[(log_df["status"] == "ok") &
               ((log_df["first_date"] > "2019-01-10") | (log_df["rows"] < 1900))]
print(short[["ticker", "rows", "first_date"]].to_string())

print("--- Names: Wikipedia vs Yahoo ---")
print(log_df[["ticker", "wiki_name", "yahoo_name"]].to_string())