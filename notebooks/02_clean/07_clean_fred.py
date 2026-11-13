# Databricks notebook source
import json
import pandas as pd

fred_dir = "/Volumes/workspace/market_pipeline/raw/fred/run_20261031_153601"
stamp = "20261031_153601"
series_ids = ["FEDFUNDS", "CPIAUCSL", "DGS10", "UNRATE"]

parts = []
for sid in series_ids:
    with open(f"{fred_dir}/{sid}_{stamp}.json", "r", encoding="utf-8") as f:
        observations = json.load(f)["observations"]
    part = pd.DataFrame(observations)[["date", "value"]]
    part["series_id"] = sid
    parts.append(part)

macro_raw = pd.concat(parts, ignore_index=True)
print(macro_raw.shape)
print(macro_raw.dtypes)
print(macro_raw.groupby("series_id").size())

# COMMAND ----------

macro = macro_raw.copy()
macro["date"] = pd.to_datetime(macro["date"], errors="coerce")
macro["value_num"] = pd.to_numeric(macro["value"], errors="coerce")
macro = macro[["date", "series_id", "value", "value_num"]]

is_dot = macro["value"] == "."
unexpected_text = (~is_dot) & macro["value_num"].isna()

print("Failed dates:", macro["date"].isna().sum())
print("Dots per series:", macro[is_dot].groupby("series_id").size().to_dict())
print("Non-dot values that failed to become numbers:", unexpected_text.sum())
print("Duplicate (date, series_id):", macro.duplicated(subset=["date", "series_id"]).sum())

summary = macro.groupby("series_id").agg(rows=("date", "size"), first=("date", "min"),
                                         last=("date", "max"), low=("value_num", "min"),
                                         high=("value_num", "max"))
print(summary.to_string())

monthly = macro[macro["series_id"] != "DGS10"]
print("Monthly series dates not on the 1st:", (monthly["date"].dt.day != 1).sum())
dgs = macro[macro["series_id"] == "DGS10"]
print("DGS10 weekend dates:", (dgs["date"].dt.dayofweek >= 5).sum())

# COMMAND ----------

import os
import datetime

clean_dir = "/Volumes/workspace/market_pipeline/clean"

macro_clean = macro[["date", "series_id", "value_num"]].rename(columns={"value_num": "value"})
macro_clean = macro_clean.sort_values(["series_id", "date"]).reset_index(drop=True)

nulls_per_series = macro_clean[macro_clean["value"].isna()].groupby("series_id").size().to_dict()

summary_macro = pd.DataFrame([
    ("cleaned_at", datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
    ("source_run", "fred/run_20261031_153601"),
    ("rows_in", len(macro_raw)),
    ("rows_dropped", 0),
    ("rows_out", len(macro_clean)),
    ("null_values_total", int(macro_clean["value"].isna().sum())),
    ("null_values_by_series", str(nulls_per_series)),
], columns=["item", "value"])

macro_clean.to_csv(f"{clean_dir}/macro_clean.csv", index=False)
summary_macro.to_csv(f"{clean_dir}/macro_cleaning_summary.csv", index=False)

print(sorted(os.listdir(clean_dir)))
check = pd.read_csv(f"{clean_dir}/macro_clean.csv", parse_dates=["date"])
print(check.shape, "| date type:", str(check["date"].dtype))
print(check.groupby("series_id")["value"].apply(lambda s: s.isna().sum()).to_dict())
print(check[check["value"].isna() & (check["series_id"] != "DGS10")].to_string())
print(pd.read_csv(f"{clean_dir}/macro_cleaning_summary.csv").to_string())

# COMMAND ----------

unrate_rows = macro_clean[(macro_clean["series_id"] == "UNRATE") &
                          (macro_clean["date"] >= "2025-08-01") &
                          (macro_clean["date"] <= "2025-12-01")]
print(unrate_rows.to_string())

dgs_rows = macro_clean[(macro_clean["series_id"] == "DGS10") &
                       (macro_clean["date"] >= "2025-10-13") &
                       (macro_clean["date"] <= "2025-10-21")]
print(dgs_rows.to_string())

# COMMAND ----------

