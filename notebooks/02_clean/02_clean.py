# Databricks notebook source
import os
import pandas as pd

run_dir = "/Volumes/workspace/market_pipeline/raw/prices/run_20261031_151755"
all_files = sorted(os.listdir(run_dir))
price_files = [f for f in all_files if f.endswith(".csv") and not f.startswith("pull_log")]
print("Files in folder:", len(all_files), "| price files:", len(price_files))
print("First file:", price_files[0])

with open(f"{run_dir}/{price_files[0]}", "r", encoding="utf-8") as f:
    for _ in range(3):
        print(f.readline().rstrip())

# COMMAND ----------

frames = []
for file_name in price_files:
    df = pd.read_csv(f"{run_dir}/{file_name}")
    df["ticker"] = file_name.split("_")[0]
    frames.append(df)

raw_prices = pd.concat(frames, ignore_index=True)
print(raw_prices.shape)
print(raw_prices.dtypes)
print(raw_prices.head(3))
print("Tickers:", raw_prices["ticker"].nunique())

# COMMAND ----------

prices = raw_prices.copy()
prices = prices.rename(columns={"Date": "date", "Adj Close": "adj_close", "Close": "close",
                                "High": "high", "Low": "low", "Open": "open", "Volume": "volume"})
prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
prices = prices[["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]]

print(prices.dtypes)

# COMMAND ----------

print("Dates that failed to convert:", prices["date"].isna().sum())
print("Missing values per column:")
print(prices.isna().sum())
print("Duplicate (date, ticker) rows:", prices.duplicated(subset=["date", "ticker"]).sum())
print("Date range:", prices["date"].min().date(), "to", prices["date"].max().date())
print("Future dates:", (prices["date"] > pd.Timestamp.today()).sum())
print("Weekend dates:", (prices["date"].dt.dayofweek >= 5).sum())

rows_per_ticker = prices.groupby("ticker").size()
print("Rows per ticker: min", rows_per_ticker.min(), "| max", rows_per_ticker.max())
print("Unique dates:", prices["date"].nunique())
print("Rows if every ticker had every date:", prices["date"].nunique() * prices["ticker"].nunique(), "| actual rows:", len(prices))

# COMMAND ----------

price_cols = ["open", "high", "low", "close", "adj_close"]
flag_nonpositive = (prices[price_cols] <= 0).any(axis=1)
flag_high_below_low = prices["high"] < prices["low"]
flag_close_outside = (prices["close"] > prices["high"] + 0.01) | (prices["close"] < prices["low"] - 0.01)
flag_zero_volume = prices["volume"] == 0

print("Any price <= 0:", flag_nonpositive.sum())
print("High below low:", flag_high_below_low.sum())
print("Close outside high-low range (1 cent tolerance):", flag_close_outside.sum())
print("Zero volume days:", flag_zero_volume.sum())

# COMMAND ----------

sorted_prices = prices.sort_values(["ticker", "date"])
daily_change = sorted_prices.groupby("ticker")["adj_close"].pct_change()
big = sorted_prices.assign(daily_change_pct=(daily_change * 100).round(1))
big = big[big["daily_change_pct"].abs() > 20]
top = big.loc[big["daily_change_pct"].abs().sort_values(ascending=False).index]

print("Days with a move over 20%:", len(big), "| tickers affected:", big["ticker"].nunique())
print(top[["ticker", "date", "adj_close", "daily_change_pct"]].head(15).to_string())

unique_dates = pd.Series(sorted(prices["date"].unique()))
gap_days = unique_dates.diff().dt.days
print("Largest gap between trading dates (calendar days):", int(gap_days.max()))
print("Gaps over 4 days:", (gap_days > 4).sum())

# COMMAND ----------

print(prices[flag_zero_volume][["ticker", "date", "open", "high", "low", "close", "volume"]].to_string())

print("--- Dates with the most moves over 20% ---")
print(big["date"].dt.date.value_counts().head(8))

print("--- VTRS around Nov 2020 ---")
vtrs = sorted_prices[(sorted_prices["ticker"] == "VTRS") &
                     (sorted_prices["date"] >= "2020-11-02") &
                     (sorted_prices["date"] <= "2020-11-24")]
print(vtrs[["date", "close", "adj_close", "volume"]].to_string())

# COMMAND ----------

for tkr, day in [("CCI", "2020-07-31"), ("GEN", "2019-11-05")]:
    t = sorted_prices[sorted_prices["ticker"] == tkr].reset_index(drop=True)
    pos = t.index[t["date"] == day][0]
    print(tkr)
    print(t.loc[pos - 2:pos + 2, ["date", "open", "high", "low", "close", "volume"]].to_string())

# COMMAND ----------

import os
import datetime

ref_dir = "/Volumes/workspace/market_pipeline/raw/reference"
v3_path = f"{ref_dir}/ticker_sector_mapping_v3.csv"
log3_path = f"{ref_dir}/mapping_change_log_v3.csv"

mapping_v2 = pd.read_csv(f"{ref_dir}/ticker_sector_mapping_v2.csv")
old_log = pd.read_csv(f"{ref_dir}/mapping_change_log.csv")

sector = mapping_v2.loc[mapping_v2["ticker"] == "VTRS", "sector"].iloc[0]
already_used = set(mapping_v2["ticker"]) | {"VMRK", "SKYD"}
pool = eligible[(eligible["GICS Sector"] == sector) & (~eligible["Symbol"].isin(already_used))]
pick = pool.sample(n=1, random_state=42).iloc[0]

kept = mapping_v2[mapping_v2["ticker"] != "VTRS"]
new_row = pd.DataFrame([{"ticker": pick["Symbol"], "company": pick["Security"], "sector": sector}])
mapping_v3 = pd.concat([kept, new_row], ignore_index=True).sort_values(["sector", "ticker"]).reset_index(drop=True)

new_log = pd.DataFrame([{
    "old_ticker": "VTRS", "new_ticker": pick["Symbol"], "sector": sector, "pool_size": len(pool),
    "reason": "Viatris formed by merger on 2020-11-16 (first VTRS trading day 2020-11-17); Yahoo history before that belongs to a different company",
    "changed_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")}])
log_v3 = pd.concat([old_log, new_log], ignore_index=True)

print("Replacement:", pick["Symbol"], "|", pick["Security"], "|", sector, "| pool size:", len(pool))
print("Rows:", len(mapping_v3), "| Duplicate tickers:", mapping_v3["ticker"].duplicated().sum())
print(mapping_v3["sector"].value_counts().to_string())

if os.path.exists(v3_path) or os.path.exists(log3_path):
    print("STOP: a v3 file already exists, nothing written")
else:
    mapping_v3.to_csv(v3_path, index=False)
    log_v3.to_csv(log3_path, index=False)
    print("Saved both v3 files")
print(sorted(os.listdir(ref_dir)))

# COMMAND ----------

# MAGIC %pip install lxml

# COMMAND ----------

import pandas as pd
from io import StringIO

html_path = "/Volumes/workspace/market_pipeline/raw/wikipedia/sp500_list_20261031_144046.html"
with open(html_path, "r", encoding="utf-8") as f:
    html_text = f.read()

sp500 = pd.read_html(StringIO(html_text))[0]

drop_second_class = ["GOOG", "FOX", "NWS"]
stocks = sp500.copy()
stocks["Date added"] = pd.to_datetime(stocks["Date added"], errors="coerce")
stocks = stocks[~stocks["Symbol"].str.contains(".", regex=False)]
stocks = stocks[~stocks["Symbol"].isin(drop_second_class)]
eligible = stocks[stocks["Date added"] <= "2019-01-01"]

print("sp500 rows:", len(sp500), "| Eligible:", len(eligible))

# COMMAND ----------

import os
import time
import datetime
import pandas as pd
import yfinance as yf

raw_root = "/Volumes/workspace/market_pipeline/raw"
first_run_dir = f"{raw_root}/prices/run_20261031_151755"
mapping_v3 = pd.read_csv(f"{raw_root}/reference/ticker_sector_mapping_v3.csv")

have = {f.split("_")[0] for f in os.listdir(first_run_dir)
        if f.endswith(".csv") and not f.startswith("pull_log")}
missing = mapping_v3[~mapping_v3["ticker"].isin(have)]

print("Files already saved:", len(have), "| v3 mapping rows:", len(mapping_v3))
print("In v3 mapping but no price file yet:", missing["ticker"].tolist())
print("Saved but no longer in mapping:", sorted(have - set(mapping_v3["ticker"])))

# COMMAND ----------

pip install yfinance

# COMMAND ----------

start_date = "2019-01-01"
end_date = "2026-10-31"
stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
run_dir = f"{raw_root}/prices/run_{stamp}"
os.makedirs(run_dir, exist_ok=False)

log_rows = []
for ticker, wiki_name in zip(missing["ticker"], missing["company"]):
    entry = {"ticker": ticker, "wiki_name": wiki_name, "yahoo_name": None, "status": None,
             "rows": 0, "first_date": None, "last_date": None, "file": None, "error": None}
    try:
        df = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False, progress=False)
        if df.empty:
            entry["status"] = "empty"
        else:
            df = df.droplevel("Ticker", axis=1)
            file_name = f"{ticker}_{stamp}.csv"
            df.to_csv(f"{run_dir}/{file_name}")
            entry.update(status="ok", rows=len(df), first_date=str(df.index.min().date()),
                         last_date=str(df.index.max().date()), file=file_name)
    except Exception as e:
        entry["status"] = "error"
        entry["error"] = f"{type(e).__name__}: {str(e)[:100]}"
    try:
        entry["yahoo_name"] = yf.Ticker(ticker).info.get("longName")
    except Exception:
        entry["yahoo_name"] = None
    log_rows.append(entry)
    time.sleep(1)

log_new = pd.DataFrame(log_rows)
log_new.to_csv(f"{run_dir}/pull_log_{stamp}.csv", index=False)
print(log_new.to_string())
print(sorted(os.listdir(run_dir)))

# COMMAND ----------

import os
import pandas as pd

raw_root = "/Volumes/workspace/market_pipeline/raw"
mapping_v3 = pd.read_csv(f"{raw_root}/reference/ticker_sector_mapping_v3.csv")

run_dirs = [f"{raw_root}/prices/run_20261031_151755",
            f"{raw_root}/prices/run_20261101_110053"]

file_for_ticker = {}
for folder in run_dirs:
    for name in sorted(os.listdir(folder)):
        if name.endswith(".csv") and not name.startswith("pull_log"):
            file_for_ticker[name.split("_")[0]] = f"{folder}/{name}"

frames = []
for ticker in mapping_v3["ticker"]:
    df = pd.read_csv(file_for_ticker[ticker])
    df["ticker"] = ticker
    frames.append(df)

raw_v3 = pd.concat(frames, ignore_index=True)
print(raw_v3.shape, "| tickers:", raw_v3["ticker"].nunique())
print("VTRS in table:", "VTRS" in set(raw_v3["ticker"]), "| INCY in table:", "INCY" in set(raw_v3["ticker"]))

# COMMAND ----------

prices = raw_v3.rename(columns={"Date": "date", "Adj Close": "adj_close", "Close": "close",
                                "High": "high", "Low": "low", "Open": "open", "Volume": "volume"})
prices["date"] = pd.to_datetime(prices["date"], errors="coerce")
prices = prices[["date", "ticker", "open", "high", "low", "close", "adj_close", "volume"]]
prices = prices.sort_values(["ticker", "date"]).reset_index(drop=True)

prices["daily_change_pct"] = (prices.groupby("ticker")["adj_close"].pct_change() * 100).round(2)
prices["flag_big_move"] = prices["daily_change_pct"].abs() > 20
prices["flag_zero_volume"] = prices["volume"] == 0

price_cols = ["open", "high", "low", "close", "adj_close"]
print("Rows:", len(prices), "| Failed dates:", prices["date"].isna().sum(),
      "| Missing values:", prices[["open", "high", "low", "close", "adj_close", "volume"]].isna().sum().sum())
print("Duplicate (date, ticker):", prices.duplicated(subset=["date", "ticker"]).sum())
print("Date range:", prices["date"].min().date(), "to", prices["date"].max().date(),
      "| Future:", (prices["date"] > pd.Timestamp.today()).sum(),
      "| Weekend:", (prices["date"].dt.dayofweek >= 5).sum())
per_ticker = prices.groupby("ticker").size()
print("Rows per ticker: min", per_ticker.min(), "| max", per_ticker.max())
print("Price <= 0:", (prices[price_cols] <= 0).any(axis=1).sum(),
      "| High below low:", (prices["high"] < prices["low"]).sum(),
      "| Close outside range:", ((prices["close"] > prices["high"] + 0.01) | (prices["close"] < prices["low"] - 0.01)).sum())
print("Flag zero volume:", prices["flag_zero_volume"].sum(), "| Flag big move:", prices["flag_big_move"].sum(),
      "| Tickers with a big move:", prices.loc[prices["flag_big_move"], "ticker"].nunique())
print(prices.loc[prices["flag_zero_volume"], ["ticker", "date", "close", "volume"]].to_string())
print("INCY flagged rows:", prices.loc[(prices["ticker"] == "INCY") & (prices["flag_big_move"]), ["date", "daily_change_pct"]].to_string())

# COMMAND ----------

drop_rules = {
    "date could not be read": prices["date"].isna(),
    "a price was zero or negative": (prices[price_cols] <= 0).any(axis=1),
    "high was below low": prices["high"] < prices["low"],
    "duplicate (date, ticker), later copy": prices.duplicated(subset=["date", "ticker"], keep="first"),
}

dropped_parts = []
drop_mask = pd.Series(False, index=prices.index)
for reason, mask in drop_rules.items():
    print(f"{reason}: {mask.sum()}")
    part = prices.loc[mask, ["ticker", "date"]].copy()
    part["reason"] = reason
    dropped_parts.append(part)
    drop_mask = drop_mask | mask

drop_log = pd.concat(dropped_parts, ignore_index=True)

clean_cols = ["date", "ticker", "open", "high", "low", "close", "adj_close", "volume",
              "flag_big_move", "flag_zero_volume"]
prices_clean = prices.loc[~drop_mask, clean_cols].reset_index(drop=True)

print("Rows in:", len(prices), "| rows dropped:", int(drop_mask.sum()), "| rows out:", len(prices_clean))
print("Drop log shape:", drop_log.shape)

# COMMAND ----------

import datetime

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.market_pipeline.clean")
clean_dir = "/Volumes/workspace/market_pipeline/clean"

summary = pd.DataFrame([
    ("cleaned_at", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    ("mapping_file", "ticker_sector_mapping_v3.csv"),
    ("source_runs", "run_20261031_151755 + run_20261101_110053"),
    ("rows_in", len(prices)),
    ("rows_dropped", int(drop_mask.sum())),
    ("rows_out", len(prices_clean)),
    ("tickers", prices_clean["ticker"].nunique()),
    ("flag_zero_volume", int(prices_clean["flag_zero_volume"].sum())),
    ("flag_big_move", int(prices_clean["flag_big_move"].sum())),
], columns=["item", "value"])

prices_clean.to_csv(f"{clean_dir}/prices_clean.csv", index=False)
drop_log.to_csv(f"{clean_dir}/drop_log.csv", index=False)
summary.to_csv(f"{clean_dir}/cleaning_summary.csv", index=False)

for name in sorted(os.listdir(clean_dir)):
    print(name, "|", round(os.path.getsize(f"{clean_dir}/{name}") / 1_000_000, 1), "MB")

check = pd.read_csv(f"{clean_dir}/prices_clean.csv", parse_dates=["date"])
print(check.shape, "| dates are real dates:", str(check["date"].dtype))
print(check.head(2).to_string())
print(pd.read_csv(f"{clean_dir}/cleaning_summary.csv").to_string())

# COMMAND ----------

