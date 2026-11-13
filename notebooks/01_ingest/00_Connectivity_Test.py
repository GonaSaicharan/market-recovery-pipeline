# Databricks notebook source
# MAGIC %pip install yfinance

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import yfinance as yf
tickers = ["AAPL", "MSFT", "JPM", "XOM", "JNJ"]
df = yf.download(tickers, start="2024-01-01", end="2024-02-01")

print(df.shape)
print(df.columns)
print(df.index[:5])
print(df.head())


# COMMAND ----------

import requests

try:
    r = requests.get("https://api.stlouisfed.org/fred/series", params={"series_id": "FEDFUNDS"}, timeout=15)
    print(r.status_code)
    print(r.text[:300])
except Exception as e :
    print("FAILED:" ,repr(e))
    

# COMMAND ----------

import yfinance as yf

tickers = ["AAPL", "MSFT", "JPM", "XOM", "JNJ"]

adj = yf.download(tickers, start="2024-01-01", end="2024-02-01", auto_adjust=True)
raw = yf.download(tickers, start="2024-01-01", end="2024-02-01", auto_adjust=False)

print(adj.shape, raw.shape)
print(adj.columns.tolist()[:6])
print(raw.columns.get_level_values(0).unique().tolist())
print(adj.index.min(), adj.index.max(), len(adj))
print(adj["Close"]["AAPL"].head(3))
print(raw["Close"]["AAPL"].head(3))
print(adj.isna().sum().sum(), "missing values in adjusted pull")

# COMMAND ----------

import pandas as pd

all_weekdays = pd.bdate_range("2024-01-01", "2024-01-31")
missing = all_weekdays.difference(adj.index)
print(len(all_weekdays), len(adj))
print(missing)


# COMMAND ----------

