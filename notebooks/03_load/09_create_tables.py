# Databricks notebook source
table_sql = {
"dim_stock": """
    CREATE TABLE IF NOT EXISTS workspace.market_pipeline.dim_stock (
        ticker STRING, company STRING, sector STRING
    )""",
"dim_date": """
    CREATE TABLE IF NOT EXISTS workspace.market_pipeline.dim_date (
        date DATE, year INT, month INT, quarter INT, is_trading_day BOOLEAN
    )""",
"fact_prices": """
    CREATE TABLE IF NOT EXISTS workspace.market_pipeline.fact_prices (
        date DATE, ticker STRING, open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
        adj_close DOUBLE, volume BIGINT, flag_big_move BOOLEAN, flag_zero_volume BOOLEAN
    )""",
"fact_macro": """
    CREATE TABLE IF NOT EXISTS workspace.market_pipeline.fact_macro (
        date DATE, series_id STRING, value DOUBLE
    )""",
"pipeline_log": """
    CREATE TABLE IF NOT EXISTS workspace.market_pipeline.pipeline_log (
        run_time TIMESTAMP, step STRING, rows_in BIGINT, rows_out BIGINT, errors STRING
    )""",
}

for name, sql_text in table_sql.items():
    spark.sql(sql_text)
    print("ok:", name)

spark.sql("SHOW TABLES IN workspace.market_pipeline").show(truncate=False)
spark.sql("DESCRIBE TABLE workspace.market_pipeline.fact_prices").show(truncate=False)

# COMMAND ----------

ref_path = "/Volumes/workspace/market_pipeline/raw/reference/ticker_sector_mapping_v3.csv"
spark.read.option("header", True).csv(ref_path).createOrReplaceTempView("incoming_stock")

result = spark.sql("""
MERGE INTO workspace.market_pipeline.dim_stock AS t
USING incoming_stock AS s
ON t.ticker = s.ticker
WHEN MATCHED THEN UPDATE SET t.company = s.company, t.sector = s.sector
WHEN NOT MATCHED THEN INSERT (ticker, company, sector) VALUES (s.ticker, s.company, s.sector)
""")
result.show()

spark.sql("""SELECT COUNT(*) AS rows, COUNT(DISTINCT ticker) AS tickers
             FROM workspace.market_pipeline.dim_stock""").show()

# COMMAND ----------

clean_path = "/Volumes/workspace/market_pipeline/clean/prices_clean.csv"
before = spark.table("workspace.market_pipeline.fact_prices").count()

spark.read.option("header", True).csv(clean_path).createOrReplaceTempView("incoming_prices_text")
spark.sql("""
CREATE OR REPLACE TEMP VIEW incoming_prices AS
SELECT CAST(date AS DATE) AS date, ticker,
       CAST(open AS DOUBLE) AS open, CAST(high AS DOUBLE) AS high,
       CAST(low AS DOUBLE) AS low, CAST(close AS DOUBLE) AS close,
       CAST(adj_close AS DOUBLE) AS adj_close, CAST(volume AS BIGINT) AS volume,
       CAST(flag_big_move AS BOOLEAN) AS flag_big_move,
       CAST(flag_zero_volume AS BOOLEAN) AS flag_zero_volume
FROM incoming_prices_text
""")

merge_result = spark.sql("""
MERGE INTO workspace.market_pipeline.fact_prices AS t
USING incoming_prices AS s
ON t.date = s.date AND t.ticker = s.ticker
WHEN NOT MATCHED THEN INSERT *
""")
merge_result.show()

after = spark.table("workspace.market_pipeline.fact_prices").count()
print("Rows before:", before, "| rows after:", after, "| added this run:", after - before)
spark.sql("""
SELECT COUNT(*) AS total_rows,
       COUNT(DISTINCT date, ticker) AS distinct_date_ticker,
       COUNT(DISTINCT ticker) AS tickers,
       MIN(date) AS first_date, MAX(date) AS last_date
FROM workspace.market_pipeline.fact_prices
""").show()

# COMMAND ----------

def log_step(step, rows_in, rows_out, errors=""):
    spark.sql(f"""
        INSERT INTO workspace.market_pipeline.pipeline_log
        VALUES (current_timestamp(), '{step}', {rows_in}, {rows_out}, '{errors}')
    """)

macro_path = "/Volumes/workspace/market_pipeline/clean/macro_clean.csv"
macro_text = spark.read.option("header", True).csv(macro_path)
macro_rows_in = macro_text.count()
macro_text.createOrReplaceTempView("incoming_macro_text")

spark.sql("""
CREATE OR REPLACE TEMP VIEW incoming_macro AS
SELECT CAST(date AS DATE) AS date, series_id, CAST(value AS DOUBLE) AS value
FROM incoming_macro_text
""")

macro_result = spark.sql("""
MERGE INTO workspace.market_pipeline.fact_macro AS t
USING incoming_macro AS s
ON t.date = s.date AND t.series_id = s.series_id
WHEN NOT MATCHED THEN INSERT *
""")
macro_inserted = macro_result.collect()[0]["num_inserted_rows"]
log_step("load fact_macro", macro_rows_in, macro_inserted)
print("Macro rows in file:", macro_rows_in, "| inserted this run:", macro_inserted)

spark.sql("""
SELECT series_id, COUNT(*) AS rows,
       SUM(CASE WHEN value IS NULL THEN 1 ELSE 0 END) AS null_values,
       MIN(date) AS first_date, MAX(date) AS last_date
FROM workspace.market_pipeline.fact_macro
GROUP BY series_id ORDER BY series_id
""").show()

# COMMAND ----------

dd_result = spark.sql("""
MERGE INTO workspace.market_pipeline.dim_date AS t
USING (
    SELECT c.d AS date, YEAR(c.d) AS year, MONTH(c.d) AS month, QUARTER(c.d) AS quarter,
           (p.date IS NOT NULL) AS is_trading_day
    FROM (SELECT explode(sequence(DATE'2019-01-01', DATE'2026-10-30', INTERVAL 1 DAY)) AS d) c
    LEFT JOIN (SELECT DISTINCT date FROM workspace.market_pipeline.fact_prices) p
           ON c.d = p.date
) AS s
ON t.date = s.date
WHEN NOT MATCHED THEN INSERT *
""")
dd_inserted = dd_result.collect()[0]["num_inserted_rows"]
log_step("load dim_date", 0, dd_inserted)
print("dim_date rows inserted this run:", dd_inserted)

spark.sql("""
SELECT COUNT(*) AS calendar_days,
       SUM(CASE WHEN is_trading_day THEN 1 ELSE 0 END) AS trading_days,
       MIN(date) AS first_date, MAX(date) AS last_date
FROM workspace.market_pipeline.dim_date
""").show()

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS prices_with_no_stock_row
FROM workspace.market_pipeline.fact_prices p
LEFT JOIN workspace.market_pipeline.dim_stock s ON p.ticker = s.ticker
WHERE s.ticker IS NULL
""").show()

spark.sql("""
SELECT sector, COUNT(*) AS stocks
FROM workspace.market_pipeline.dim_stock GROUP BY sector ORDER BY sector
""").show(truncate=False)

spark.sql("SELECT * FROM workspace.market_pipeline.pipeline_log ORDER BY run_time").show(truncate=False)

# COMMAND ----------

spark.sql("""
SELECT ticker, MAX(date) AS latest_date
FROM workspace.market_pipeline.fact_prices
GROUP BY ticker
""").createOrReplaceTempView("latest_dates")

spark.sql("""
SELECT latest_date, COUNT(*) AS tickers
FROM latest_dates
GROUP BY latest_date
ORDER BY latest_date
""").show()

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE workspace.market_pipeline.fact_prices_test AS
SELECT * FROM workspace.market_pipeline.fact_prices
WHERE date <= DATE'2026-10-23'
""")

spark.sql("""
SELECT COUNT(*) AS rows, COUNT(DISTINCT ticker) AS tickers,
       MIN(date) AS first_date, MAX(date) AS last_date
FROM workspace.market_pipeline.fact_prices_test
""").show()

spark.sql("SELECT COUNT(*) AS rows_in_real_table FROM workspace.market_pipeline.fact_prices").show()

# COMMAND ----------

