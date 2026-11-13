# Databricks notebook source
pip install yfinance

# COMMAND ----------

import os
import time
import datetime
import pandas as pd
import yfinance as yf

raw_root = "/Volumes/workspace/market_pipeline/raw"
clean_dir = "/Volumes/workspace/market_pipeline/clean"
target_table = "workspace.market_pipeline.fact_prices"

end_date = datetime.date.today().isoformat()
latest = spark.sql(f"SELECT ticker, MAX(date) AS latest_date FROM {target_table} GROUP BY ticker").toPandas()
latest["fetch_from"] = (pd.to_datetime(latest["latest_date"]) + pd.Timedelta(days=1)).dt.strftime("%Y-%m-%d")
latest["needs_fetch"] = latest["fetch_from"] < end_date

print("Target table:", target_table)
print("Fetch window ends before:", end_date)
print(latest.groupby(["fetch_from", "needs_fetch"]).size())

# COMMAND ----------

stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
inc_dir = f"{raw_root}/prices/incremental_{stamp}"
os.makedirs(inc_dir, exist_ok=False)

new_frames = []
fetch_log = []
for _, row in latest[latest["needs_fetch"]].iterrows():
    ticker = row["ticker"]
    entry = {"ticker": ticker, "fetch_from": row["fetch_from"], "status": None, "rows": 0, "error": None}
    try:
        df = yf.download(ticker, start=row["fetch_from"], end=end_date, auto_adjust=False, progress=False)
        if df.empty:
            entry["status"] = "no new data"
        else:
            df = df.droplevel("Ticker", axis=1)
            df.to_csv(f"{inc_dir}/{ticker}_{stamp}.csv")
            df["ticker"] = ticker
            new_frames.append(df.reset_index())
            entry.update(status="ok", rows=len(df))
    except Exception as e:
        entry["status"] = "error"
        entry["error"] = f"{type(e).__name__}: {str(e)[:100]}"
    fetch_log.append(entry)
    time.sleep(1)

fetch_log_df = pd.DataFrame(fetch_log)
fetch_log_df.to_csv(f"{inc_dir}/fetch_log_{stamp}.csv", index=False)
all_new = pd.concat(new_frames, ignore_index=True)

print(fetch_log_df["status"].value_counts().to_dict())
print("Rows fetched:", len(all_new), "| tickers skipped (already up to date):", int((~latest["needs_fetch"]).sum()))
print("New data dates:", all_new["Date"].min().date(), "to", all_new["Date"].max().date())
print(sorted(os.listdir(inc_dir))[:3], "...", len(os.listdir(inc_dir)), "files in folder")

# COMMAND ----------

def log_step(step, rows_in, rows_out, errors=""):
    spark.sql(f"""
        INSERT INTO workspace.market_pipeline.pipeline_log
        VALUES (current_timestamp(), '{step}', {rows_in}, {rows_out}, '{errors}')
    """)

last_stored = spark.sql(f"""
    SELECT ticker, adj_close AS last_adj_close FROM (
        SELECT ticker, adj_close,
               ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) AS rn
        FROM {target_table}
    ) WHERE rn = 1
""").toPandas()
last_map = dict(zip(last_stored["ticker"], last_stored["last_adj_close"]))
print("Tickers with a stored last price:", len(last_map))

# COMMAND ----------

new = all_new.rename(columns={"Date": "date", "Adj Close": "adj_close", "Close": "close",
                              "High": "high", "Low": "low", "Open": "open", "Volume": "volume"})
new["date"] = pd.to_datetime(new["date"], errors="coerce")
new = new[["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]]
new = new.sort_values(["ticker", "date"]).reset_index(drop=True)

price_cols = ["open", "high", "low", "close", "adj_close"]
rules = {
    "date could not be read": new["date"].isna(),
    "date is in the future": new["date"] > pd.Timestamp.today(),
    "a price was zero or negative": (new[price_cols] <= 0).any(axis=1),
    "high was below low": new["high"] < new["low"],
    "duplicate (date, ticker)": new.duplicated(subset=["date", "ticker"], keep="first"),
}
drop_mask = pd.Series(False, index=new.index)
for reason, mask in rules.items():
    print(f"{reason}: {int(mask.sum())}")
    drop_mask = drop_mask | mask
new_clean = new[~drop_mask].copy()
print("Rows in:", len(new), "| dropped:", int(drop_mask.sum()), "| rows out:", len(new_clean))

prev = new_clean.groupby("ticker")["adj_close"].shift(1)
prev = prev.fillna(new_clean["ticker"].map(last_map))
new_clean["flag_big_move"] = ((new_clean["adj_close"] / prev - 1) * 100).abs() > 20
new_clean["flag_zero_volume"] = new_clean["volume"] == 0
print("Big moves flagged:", int(new_clean["flag_big_move"].sum()),
      "| zero-volume flagged:", int(new_clean["flag_zero_volume"].sum()))

# COMMAND ----------

upload = new_clean.assign(date=new_clean["date"].dt.strftime("%Y-%m-%d"))
spark.createDataFrame(upload).createOrReplaceTempView("incoming_inc_text")
spark.sql("""
CREATE OR REPLACE TEMP VIEW incoming_inc AS
SELECT CAST(date AS DATE) AS date, ticker, CAST(open AS DOUBLE) AS open, CAST(high AS DOUBLE) AS high,
       CAST(low AS DOUBLE) AS low, CAST(close AS DOUBLE) AS close, CAST(adj_close AS DOUBLE) AS adj_close,
       CAST(volume AS BIGINT) AS volume, CAST(flag_big_move AS BOOLEAN) AS flag_big_move,
       CAST(flag_zero_volume AS BOOLEAN) AS flag_zero_volume
FROM incoming_inc_text
""")

before = spark.table(target_table).count()
merge_result = spark.sql(f"""
MERGE INTO {target_table} AS t
USING incoming_inc AS s
ON t.date = s.date AND t.ticker = s.ticker
WHEN NOT MATCHED THEN INSERT *
""")
inserted = merge_result.collect()[0]["num_inserted_rows"]
after = spark.table(target_table).count()

fetch_errors = int((fetch_log_df["status"] == "error").sum())
log_step(f"incremental load {target_table.split('.')[-1]}", len(all_new), inserted,
         f"{fetch_errors} fetch errors, {int(drop_mask.sum())} rows dropped")
print("Rows before:", before, "| inserted:", inserted, "| rows after:", after)

spark.sql(f"""
SELECT COUNT(*) AS total_rows, COUNT(DISTINCT date, ticker) AS distinct_date_ticker,
       MIN(date) AS first_date, MAX(date) AS last_date
FROM {target_table}
""").show()
spark.sql("SELECT * FROM workspace.market_pipeline.pipeline_log ORDER BY run_time").show(truncate=False)

# COMMAND ----------

real = "workspace.market_pipeline.fact_prices"
test = "workspace.market_pipeline.fact_prices_test"

spark.sql(f"""
SELECT (SELECT COUNT(*) FROM (SELECT * FROM {real} EXCEPT SELECT * FROM {test})) AS in_real_not_in_test,
       (SELECT COUNT(*) FROM (SELECT * FROM {test} EXCEPT SELECT * FROM {real})) AS in_test_not_in_real
""").show()

spark.sql(f"""
SELECT COUNT(*) AS compared_rows,
       SUM(CASE WHEN ABS(t.close - r.close) > 0.000001 THEN 1 ELSE 0 END) AS close_differs,
       SUM(CASE WHEN ABS(t.adj_close - r.adj_close) > 0.000001 THEN 1 ELSE 0 END) AS adj_close_differs,
       SUM(CASE WHEN t.volume <> r.volume THEN 1 ELSE 0 END) AS volume_differs,
       SUM(CASE WHEN t.flag_big_move <> r.flag_big_move
                  OR t.flag_zero_volume <> r.flag_zero_volume THEN 1 ELSE 0 END) AS flags_differ
FROM {test} t JOIN {real} r ON t.date = r.date AND t.ticker = r.ticker
""").show()

spark.sql(f"""
SELECT t.ticker, t.date, t.close, r.close AS real_close,
       t.adj_close, r.adj_close AS real_adj_close,
       ROUND((t.adj_close / r.adj_close - 1) * 100, 4) AS adj_diff_pct
FROM {test} t JOIN {real} r ON t.date = r.date AND t.ticker = r.ticker
WHERE ABS(t.adj_close - r.adj_close) > 0.000001
ORDER BY t.ticker, t.date
LIMIT 15
""").show()

# COMMAND ----------

spark.sql(f"""
SELECT COUNT(*) AS compared_rows,
       SUM(CASE WHEN t.open <> r.open THEN 1 ELSE 0 END) AS open_diff,
       SUM(CASE WHEN t.high <> r.high THEN 1 ELSE 0 END) AS high_diff,
       SUM(CASE WHEN t.low <> r.low THEN 1 ELSE 0 END) AS low_diff,
       SUM(CASE WHEN t.close <> r.close THEN 1 ELSE 0 END) AS close_diff,
       SUM(CASE WHEN t.adj_close <> r.adj_close THEN 1 ELSE 0 END) AS adj_close_diff,
       SUM(CASE WHEN t.volume <> r.volume THEN 1 ELSE 0 END) AS volume_diff,
       MAX(GREATEST(ABS(t.open - r.open), ABS(t.high - r.high), ABS(t.low - r.low),
                    ABS(t.close - r.close), ABS(t.adj_close - r.adj_close))) AS biggest_gap
FROM {test} t JOIN {real} r ON t.date = r.date AND t.ticker = r.ticker
""").show()

spark.sql(f"""
SELECT t.date, COUNT(*) AS rows_that_differ
FROM {test} t JOIN {real} r ON t.date = r.date AND t.ticker = r.ticker
WHERE t.open <> r.open OR t.high <> r.high OR t.low <> r.low OR t.close <> r.close
   OR t.adj_close <> r.adj_close OR t.volume <> r.volume
GROUP BY t.date ORDER BY t.date
""").show()

spark.sql(f"""
SELECT t.ticker, t.date, t.open, r.open AS real_open, t.high, r.high AS real_high,
       t.low, r.low AS real_low, t.close, r.close AS real_close
FROM {test} t JOIN {real} r ON t.date = r.date AND t.ticker = r.ticker
WHERE t.open <> r.open OR t.high <> r.high OR t.low <> r.low OR t.close <> r.close
   OR t.adj_close <> r.adj_close OR t.volume <> r.volume
ORDER BY t.date, t.ticker LIMIT 10
""").show(truncate=False)


# COMMAND ----------

def log_step(step, rows_in, rows_out, errors=""):
    spark.sql(f"""
        INSERT INTO workspace.market_pipeline.pipeline_log
        VALUES (current_timestamp(), '{step}', {rows_in}, {rows_out}, '{errors}')
    """)

stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
inc_dir = f"{raw_root}/prices/incremental_{stamp}"
os.makedirs(inc_dir, exist_ok=False)

new_frames = []
fetch_log = []
for _, row in latest[latest["needs_fetch"]].iterrows():
    ticker = row["ticker"]
    entry = {"ticker": ticker, "fetch_from": row["fetch_from"], "status": None, "rows": 0, "error": None}
    try:
        df = yf.download(ticker, start=row["fetch_from"], end=end_date, auto_adjust=False, progress=False)
        if df.empty:
            entry["status"] = "no new data"
        else:
            df = df.droplevel("Ticker", axis=1)
            df.to_csv(f"{inc_dir}/{ticker}_{stamp}.csv")
            df["ticker"] = ticker
            new_frames.append(df.reset_index())
            entry.update(status="ok", rows=len(df))
    except Exception as e:
        entry["status"] = "error"
        entry["error"] = f"{type(e).__name__}: {str(e)[:100]}"
    fetch_log.append(entry)
    time.sleep(1)

fetch_log_df = pd.DataFrame(fetch_log)
fetch_log_df.to_csv(f"{inc_dir}/fetch_log_{stamp}.csv", index=False)
print(fetch_log_df["status"].value_counts().to_dict())

if not new_frames:
    n_errors = int((fetch_log_df["status"] == "error").sum())
    print("No new data for any ticker. Nothing to load.")
    log_step(f"incremental load {target_table.split('.')[-1]}", 0, 0, f"no new data; {n_errors} fetch errors")
    dbutils.notebook.exit("no new data")

all_new = pd.concat(new_frames, ignore_index=True)
print("Rows fetched:", len(all_new), "| dates:", all_new["Date"].min().date(), "to", all_new["Date"].max().date())

# COMMAND ----------

chk = all_new.copy()
print("Date column type:", chk["Date"].dtype)
print("Dates in fetched data:", chk["Date"].dt.date.value_counts().to_dict())
chk["date"] = pd.to_datetime(chk["Date"]).dt.strftime("%Y-%m-%d")

stored = spark.sql(f"""
    SELECT ticker, CAST(date AS STRING) AS date, close AS close_stored,
           adj_close AS adj_stored, volume AS volume_stored
    FROM {target_table} WHERE date = DATE'2026-10-30'
""").toPandas()

cmp = chk.merge(stored, on=["ticker", "date"], how="left")
print("Fetched rows:", len(cmp), "| found in table:", int(cmp["close_stored"].notna().sum()))
print("Close differs:", int(((cmp["Close"] - cmp["close_stored"]).abs() > 1e-9).sum()))
print("Adj Close differs:", int(((cmp["Adj Close"] - cmp["adj_stored"]).abs() > 1e-9).sum()))
print("Volume differs:", int((cmp["Volume"] != cmp["volume_stored"]).sum()))

# COMMAND ----------



# COMMAND ----------

cmp["volume_ratio"] = (cmp["Volume"] / cmp["volume_stored"]).round(4)
print(cmp["volume_ratio"].describe())
print(cmp[["ticker", "Volume", "volume_stored", "volume_ratio"]].head(8).to_string())

bad = cmp[((cmp["Close"] - cmp["close_stored"]).abs() > 1e-9) |
          ((cmp["Adj Close"] - cmp["adj_stored"]).abs() > 1e-9)]
print(bad[["ticker", "Open", "High", "Low", "Close", "close_stored", "Adj Close", "adj_stored"]].to_string())

# COMMAND ----------

print(cmp.sort_values("volume_ratio").head(3)[["ticker", "Volume", "volume_stored", "volume_ratio"]].to_string())
print("Tickers with volume ratio below 0.99:", int((cmp["volume_ratio"] < 0.99).sum()))

# COMMAND ----------

def log_step(step, rows_in, rows_out, errors=""):
    spark.sql(f"""
        INSERT INTO workspace.market_pipeline.pipeline_log
        VALUES (current_timestamp(), '{step}', {rows_in}, {rows_out}, '{errors}')
    """)

stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
inc_dir = f"{raw_root}/prices/incremental_{stamp}"
os.makedirs(inc_dir, exist_ok=False)

new_frames = []
fetch_log = []
for _, row in latest[latest["needs_fetch"]].iterrows():
    ticker = row["ticker"]
    entry = {"ticker": ticker, "fetch_from": row["fetch_from"], "status": None,
             "rows": 0, "rows_before_filter": 0, "error": None}
    try:
        df = yf.download(ticker, start=row["fetch_from"], end=end_date, auto_adjust=False, progress=False)
        entry["rows_before_filter"] = len(df)
        if not df.empty:
            df = df[df.index >= pd.Timestamp(row["fetch_from"])]
        if df.empty:
            entry["status"] = "no new data"
        else:
            df = df.droplevel("Ticker", axis=1)
            df.to_csv(f"{inc_dir}/{ticker}_{stamp}.csv")
            df["ticker"] = ticker
            new_frames.append(df.reset_index())
            entry.update(status="ok", rows=len(df))
    except Exception as e:
        entry["status"] = "error"
        entry["error"] = f"{type(e).__name__}: {str(e)[:100]}"
    fetch_log.append(entry)
    time.sleep(1)

fetch_log_df = pd.DataFrame(fetch_log)
fetch_log_df.to_csv(f"{inc_dir}/fetch_log_{stamp}.csv", index=False)
print(fetch_log_df["status"].value_counts().to_dict())
print("Rows returned by Yahoo before filtering:", int(fetch_log_df["rows_before_filter"].sum()))

if not new_frames:
    n_errors = int((fetch_log_df["status"] == "error").sum())
    print("No new data for any ticker. Nothing to load.")
    log_step(f"incremental load {target_table.split('.')[-1]}", 0, 0, f"no new data; {n_errors} fetch errors")
    dbutils.notebook.exit("no new data")

all_new = pd.concat(new_frames, ignore_index=True)
print("Rows fetched:", len(all_new), "| dates:", all_new["Date"].min().date(), "to", all_new["Date"].max().date())

# COMMAND ----------

print(fetch_log_df["status"].value_counts().to_dict())
print("Rows returned by Yahoo before filtering:", int(fetch_log_df["rows_before_filter"].sum()))
print("Rows kept after filter:", int(fetch_log_df["rows"].sum()))
print(fetch_log_df[fetch_log_df["status"] == "error"][["ticker", "error"]].head(5).to_string())

spark.sql("SELECT * FROM workspace.market_pipeline.pipeline_log ORDER BY run_time DESC LIMIT 3").show(truncate=False)

print(cmp[cmp["volume_ratio"] < 0.99][["ticker", "volume_ratio"]].sort_values("volume_ratio").to_string())

# COMMAND ----------

