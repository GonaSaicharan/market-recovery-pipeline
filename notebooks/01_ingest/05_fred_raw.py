# Databricks notebook source
import requests
from getpass import getpass

fred_key = getpass("Paste your FRED API key: ").strip()
print("Key received, length:", len(fred_key))

series_ids = ["FEDFUNDS", "CPIAUCSL", "DGS10", "UNRATE"]
meta_url = "https://api.stlouisfed.org/fred/series"

for sid in series_ids:
    resp = requests.get(meta_url, params={"series_id": sid, "api_key": fred_key, "file_type": "json"}, timeout=30)
    if resp.status_code != 200:
        print(sid, "-> status", resp.status_code)
        continue
    s = resp.json()["seriess"][0]
    print(sid, "|", s["title"])
    print("   frequency:", s["frequency"], "| units:", s["units"], "| seasonal:", s["seasonal_adjustment_short"])
    print("   data from", s["observation_start"], "to", s["observation_end"], "| last updated:", s["last_updated"])

# COMMAND ----------

import os
import datetime
import requests
import pandas as pd

raw_root = "/Volumes/workspace/market_pipeline/raw"
stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
run_dir = f"{raw_root}/fred/run_{stamp}"
os.makedirs(run_dir, exist_ok=False)

obs_url = "https://api.stlouisfed.org/fred/series/observations"
log_rows = []

for sid in series_ids:
    entry = {"series_id": sid, "http_status": None, "rows": 0, "dot_values": 0,
             "first_date": None, "last_date": None, "file": None, "error": None}
    try:
        resp = requests.get(obs_url,
                            params={"series_id": sid, "api_key": fred_key,
                                    "file_type": "json", "observation_start": "2018-01-01"},
                            timeout=30)
        entry["http_status"] = resp.status_code
        if resp.status_code == 200:
            file_name = f"{sid}_{stamp}.json"
            with open(f"{run_dir}/{file_name}", "w", encoding="utf-8") as f:
                f.write(resp.text)
            obs = resp.json()["observations"]
            entry.update(rows=len(obs),
                         dot_values=sum(1 for o in obs if o["value"] == "."),
                         first_date=obs[0]["date"], last_date=obs[-1]["date"],
                         file=file_name)
    except Exception as e:
        entry["error"] = f"{type(e).__name__}: {str(e)[:100]}"
    log_rows.append(entry)
    print(sid, "->", entry["http_status"], "| rows:", entry["rows"])

fred_log = pd.DataFrame(log_rows)
fred_log.to_csv(f"{run_dir}/fred_pull_log_{stamp}.csv", index=False)
print(fred_log.to_string())
print(os.listdir(run_dir))

# COMMAND ----------

import json
from collections import Counter

for sid in series_ids:
    path = f"{run_dir}/{sid}_{stamp}.json"
    with open(path, "r", encoding="utf-8") as f:
        text = f.read()
    obs = json.loads(text)["observations"]

    dot_dates = []
    for o in obs:
        if o["value"] == ".":
            dot_dates.append(o["date"])

    print(sid, "| dots:", len(dot_dates), "| key found inside file:", fred_key in text)
    if sid == "DGS10":
        years = Counter(d[:4] for d in dot_dates)
        print("   dots per year:", dict(years))
    else:
        print("   dot dates:", dot_dates)

# COMMAND ----------

