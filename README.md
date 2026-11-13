# Market Recovery Pipeline: S&P 500 sectors

I built a pipeline that pulls S&P 500 stock prices (yfinance) and macro data (FRED), loads them into SQL with duplicate and quality checks, and measures how deep each sector fell in selloffs and how long it took to recover. It shows what happened. It does not predict anything and it is not investment advice.

The data is a snapshot up to 30 October 2026.

## What I built
- Pulled daily prices for 99 stocks (9 per sector, 11 sectors) from 2019-01-02 to 2026-10-30, and 4 FRED series (FEDFUNDS, CPIAUCSL, DGS10, UNRATE).
- Saved every raw API response untouched, with a timestamp in the file name.
- Cleaned the data in Python and wrote 15 validation checks (duplicates, future dates, price rules, raw-to-clean row counts).
- Loaded 194,931 price rows into Databricks tables. Re-running the load adds 0 rows.
- Wrote the analysis in SQL with window functions: returns, drawdown, recovery time, 30-day volatility, and a macro overlay.
- Built a 3-page Power BI dashboard and checked its numbers against SQL and an Excel pivot.

## How to run it (in order)
1. notebooks/01_ingest: 00_Connectivity_Test, 03_sector_list_test, 04_prices_raw, 05_fred_raw
2. notebooks/02_clean: 02_clean (prices), 07_clean_fred, 08_validation
3. notebooks/03_load: 09_create_tables, 10_incremental_load
4. notebooks/04_analysis: 11_returns

You need a Databricks workspace and a free FRED API key. The notebooks ask for the key when they run. It is never saved in the code. Raw and clean data files are not in this repo. The notebooks rebuild them.

## What I found (from my 99 stocks)
- The market index I built (equal-weighted, 99 stocks) had 4 selloffs deeper than 10%. The deepest was 39.38%, peak 2020-02-20 to bottom 2020-03-23. It took 161 trading days to get back to the old peak.
- Energy fell the most in the 2020 selloff (69.86%) and was the most volatile sector on average (32.5% annualized).
- Consumer Staples was the least volatile sector (15.6%) but had the longest recovery in my table: 844 trading days after the 2022 selloff.
- Within each of the 4 selloffs, sectors that fell deeper tended to take longer to recover (correlations 0.555, 0.349, 0.614, 0.637). With 11 sectors per selloff, I treat this as a loose tendency only.

## Limits
- Survivorship bias: I used today's S&P 500 list, so companies that left the index are not in my data. Results likely look better than a full list would.
- Macro data shows what lined up, not what caused it. Four selloffs is too few to draw rules from.
- Sector numbers are equal-weighted averages of 9 stocks, so they will not match official sector indexes.
- yfinance is unofficial and can change. When I asked for the latest day on its own, volumes differed from my stored data for 8 of 99 stocks. I kept the stored data and do not know the cause.
- I replaced 3 tickers (VMRK, SKYD, VTRS) because their price history did not match the company. The change log is in reference/. I could not check the other 96 beyond name and price-history checks.
- My load only inserts. It never updates a stored row, so if Yahoo later changes an adjusted price, my table will not pick it up.
- I run the pipeline by hand. I wrote code to log a failed ticker and carry on, but none of my 99 tickers failed, so I never tested that path.
- FRED data is a separate manual pull. October 2025 has no CPI or unemployment value, and I left it empty instead of filling it.

## Folders
- notebooks/: the pipeline code
- reference/: ticker-to-sector mappings (v1 to v3) and the change log
- dashboard/: dashboard PDF and page screenshots
