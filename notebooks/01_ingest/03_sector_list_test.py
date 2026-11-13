# Databricks notebook source
import requests
import pandas as pd
from io import StringIO

url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
headers = {"User-Agent": "Mozilla/5.0 (learning project)"}

page = requests.get(url, headers=headers, timeout=30)
print("Status code:", page.status_code)

tables = pd.read_html(StringIO(page.text))
print("Tables found:", len(tables))

sp500 = tables[0]
print(sp500.shape)
print(sp500.columns.tolist())
print(sp500.head(3))

# COMMAND ----------

# MAGIC %pip install lxml

# COMMAND ----------

print(sp500["GICS Sector"].value_counts())
print("Unique sectors:", sp500["GICS Sector"].nunique())
print("Missing sector:", sp500["GICS Sector"].isna().sum())
print("Duplicate symbols:", sp500["Symbol"].duplicated().sum())
dotted = sp500.loc[sp500["Symbol"].str.contains(".", regex=False), "Symbol"]
print("Symbols with a dot:", dotted.tolist())
print("Date added type:", sp500["Date added"].dtype)
print("Rows with no Date added:", sp500["Date added"].isna().sum())

# COMMAND ----------

stocks = sp500.copy()
stocks["Date added"] = pd.to_datetime(stocks["Date added"], errors="coerce")
print("Dates that failed to convert:", stocks["Date added"].isna().sum())

stocks = stocks[~stocks["Symbol"].str.contains(".", regex=False)]
eligible = stocks[stocks["Date added"] <= "2019-01-01"]

print("Eligible stocks:", len(eligible))
print(eligible["GICS Sector"].value_counts())

# COMMAND ----------

selected = eligible.groupby("GICS Sector").sample(n=9, random_state=42)
selected = selected.sort_values(["GICS Sector", "Symbol"]).reset_index(drop=True)

print("Selected stocks:", len(selected))
print("Duplicate symbols:", selected["Symbol"].duplicated().sum())
print(selected["GICS Sector"].value_counts())
print(selected["Symbol"].tolist())

# COMMAND ----------

class_rows = sp500[sp500["Security"].str.contains("Class", case=False)]
print(len(class_rows), "rows with 'Class' in the company name")
print(class_rows[["Symbol", "Security", "GICS Sector"]].to_string())

# COMMAND ----------

drop_second_class = ["GOOG", "FOX", "NWS"]

stocks = sp500.copy()
stocks["Date added"] = pd.to_datetime(stocks["Date added"], errors="coerce")
stocks = stocks[~stocks["Symbol"].str.contains(".", regex=False)]
stocks = stocks[~stocks["Symbol"].isin(drop_second_class)]
eligible = stocks[stocks["Date added"] <= "2019-01-01"]

print("Eligible stocks:", len(eligible))
print(eligible["GICS Sector"].value_counts())

selected = eligible.groupby("GICS Sector").sample(n=9, random_state=42)
selected = selected.sort_values(["GICS Sector", "Symbol"]).reset_index(drop=True)

print("Selected:", len(selected))
print("Duplicate symbols:", selected["Symbol"].duplicated().sum())
print(selected["GICS Sector"].value_counts())
print(selected["Symbol"].tolist())

# COMMAND ----------

check = sp500[sp500["Symbol"].isin(["SKYD", "VMRK"])]
print(check[["Symbol", "Security", "GICS Sector", "GICS Sub-Industry", "Date added"]].to_string())

# COMMAND ----------

import os

print("Working folder:", os.getcwd())
print("/Volumes exists:", os.path.exists("/Volumes"))
spark.sql("SHOW CATALOGS").show()

# COMMAND ----------

spark.sql("SHOW SCHEMAS IN workspace").show()


# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS workspace.market_pipeline")
spark.sql("CREATE VOLUME IF NOT EXISTS workspace.market_pipeline.raw")

spark.sql("SHOW SCHEMAS IN workspace").show()
spark.sql("SHOW VOLUMES IN workspace.market_pipeline").show()

# COMMAND ----------

import os
import datetime

raw_root = "/Volumes/workspace/market_pipeline/raw"
os.makedirs(f"{raw_root}/wikipedia", exist_ok=True)
os.makedirs(f"{raw_root}/reference", exist_ok=True)

stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
html_path = f"{raw_root}/wikipedia/sp500_list_{stamp}.html"

with open(html_path, "w", encoding="utf-8") as f:
    f.write(page.text)
print("Saved:", html_path)

# COMMAND ----------

mapping = selected[["Symbol", "Security", "GICS Sector"]].copy()
mapping.columns = ["ticker", "company", "sector"]

mapping_path = f"{raw_root}/reference/ticker_sector_mapping.csv"
if os.path.exists(mapping_path):
    print("STOP: mapping file already exists, not overwriting:", mapping_path)
else:
    mapping.to_csv(mapping_path, index=False)
    print("Saved:", mapping_path)

check = pd.read_csv(mapping_path)
print(check.shape)
print(check.head(3))
print("Sectors:", check["sector"].nunique(), "| Duplicate tickers:", check["ticker"].duplicated().sum())
print(os.listdir(f"{raw_root}/wikipedia"))
print(os.listdir(f"{raw_root}/reference"))

# COMMAND ----------

for name in ["page", "sp500", "stocks", "eligible", "selected", "mapping"]:
    print(name, "exists:", name in globals())

# COMMAND ----------

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

import os
import datetime

ref_dir = "/Volumes/workspace/market_pipeline/raw/reference"
v2_path = f"{ref_dir}/ticker_sector_mapping_v2.csv"
log_path = f"{ref_dir}/mapping_change_log.csv"

reasons = {
    "VMRK": "Yahoo name (AvalonBay Communities) did not match Wikipedia name (Vivmark Residential)",
    "SKYD": "Name matched, but price history looked like a different older company (suspected, not verified); replaced to be safe",
}
log_df = pd.DataFrame(change_log)
log_df["reason"] = log_df["old_ticker"].map(reasons)
log_df["changed_at"] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")

if os.path.exists(v2_path) or os.path.exists(log_path):
    print("STOP: a v2 file or the change log already exists, nothing written")
else:
    mapping_v2.to_csv(v2_path, index=False)
    log_df.to_csv(log_path, index=False)
    print("Saved both files")

print(os.listdir(ref_dir))
print(pd.read_csv(v2_path).shape)
print(pd.read_csv(log_path).to_string())

# COMMAND ----------

