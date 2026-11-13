# Databricks notebook source
import os
import json
import pandas as pd

clean_dir = "/Volumes/workspace/market_pipeline/clean"
checks = []

def add_check(name, actual, expected):
    status = "PASS" if actual == expected else "FAIL"
    checks.append({"check": name, "actual": actual, "expected": expected, "status": status})
    print(status, "|", name, "| actual:", actual, "| expected:", expected)

prices = pd.read_csv(f"{clean_dir}/prices_clean.csv", parse_dates=["date"])
mapping = pd.read_csv("/Volumes/workspace/market_pipeline/raw/reference/ticker_sector_mapping_v3.csv")
price_cols = ["open", "high", "low", "close", "adj_close"]

add_check("duplicate (date, ticker) rows", int(prices.duplicated(subset=["date", "ticker"]).sum()), 0)
add_check("future dates", int((prices["date"] > pd.Timestamp.today()).sum()), 0)
add_check("weekend dates", int((prices["date"].dt.dayofweek >= 5).sum()), 0)
add_check("missing values in price/volume columns", int(prices[price_cols + ["volume"]].isna().sum().sum()), 0)
add_check("high below low", int((prices["high"] < prices["low"]).sum()), 0)
add_check("close outside high-low range (1 cent tolerance)",
          int(((prices["close"] > prices["high"] + 0.01) | (prices["close"] < prices["low"] - 0.01)).sum()), 0)
add_check("prices at or below zero", int((prices[price_cols] <= 0).any(axis=1).sum()), 0)
add_check("tickers in prices match the mapping", set(prices["ticker"]) == set(mapping["ticker"]), True)

rows_per_ticker = prices.groupby("ticker").size()
add_check("every ticker has at least 95% of the longest ticker's rows",
          bool(rows_per_ticker.min() >= 0.95 * rows_per_ticker.max()), True)

# COMMAND ----------

sector_counts = mapping.groupby("sector").size()
add_check("every sector has exactly 9 stocks", bool((sector_counts == 9).all()), True)

macro = pd.read_csv(f"{clean_dir}/macro_clean.csv", parse_dates=["date"])
add_check("duplicate (date, series_id) rows in macro", int(macro.duplicated(subset=["date", "series_id"]).sum()), 0)
add_check("macro dates in the future", int((macro["date"] > pd.Timestamp.today()).sum()), 0)
add_check("macro values exactly 0 (a dot turned into 0 would show here)", int((macro["value"] == 0).sum()), 0)

raw_prices_dir = "/Volumes/workspace/market_pipeline/raw/prices"
run_dirs = [f"{raw_prices_dir}/run_20261031_151755", f"{raw_prices_dir}/run_20261101_110053"]
file_for_ticker = {}
for folder in run_dirs:
    for name in sorted(os.listdir(folder)):
        if name.endswith(".csv") and not name.startswith("pull_log"):
            file_for_ticker[name.split("_")[0]] = f"{folder}/{name}"

raw_row_total = 0
for ticker in mapping["ticker"]:
    raw_row_total += len(pd.read_csv(file_for_ticker[ticker]))
dropped = len(pd.read_csv(f"{clean_dir}/drop_log.csv"))
add_check("price rows: raw files = clean rows + dropped rows", raw_row_total, len(prices) + dropped)

fred_dir = "/Volumes/workspace/market_pipeline/raw/fred/run_20261031_153601"
raw_dots = 0
for sid in ["FEDFUNDS", "CPIAUCSL", "DGS10", "UNRATE"]:
    with open(f"{fred_dir}/{sid}_20261031_153601.json", "r", encoding="utf-8") as f:
        observations = json.load(f)["observations"]
    raw_dots += sum(1 for o in observations if o["value"] == ".")
add_check("FRED empty values: dots in raw = empty cells in clean", raw_dots, int(macro["value"].isna().sum()))

results = pd.DataFrame(checks)
print()
print("Summary:", results["status"].value_counts().to_dict())
print(results[results["status"] == "FAIL"].to_string())

# COMMAND ----------

import datetime

now = datetime.datetime.now()
results["run_at"] = now.strftime("%Y-%m-%d %H:%M:%S")
log_path = f"{clean_dir}/validation_log_{now.strftime('%Y%m%d_%H%M%S')}.csv"

results.to_csv(log_path, index=False)
print("Saved:", log_path)
print(pd.read_csv(log_path).to_string())

# COMMAND ----------

