# Databricks notebook source
spark.sql("""
CREATE OR REPLACE VIEW workspace.market_pipeline.v_daily_returns AS
SELECT p.date, p.ticker, s.sector, p.adj_close,
       p.adj_close / LAG(p.adj_close) OVER (PARTITION BY p.ticker ORDER BY p.date) - 1 AS daily_return
FROM workspace.market_pipeline.fact_prices p
JOIN workspace.market_pipeline.dim_stock s ON p.ticker = s.ticker
""")
print("view created")

# COMMAND ----------

spark.sql("""
SELECT date, ticker, sector, ROUND(daily_return, 4) AS daily_return
FROM workspace.market_pipeline.v_daily_returns
WHERE daily_return IS NOT NULL
ORDER BY daily_return
LIMIT 5
""").show(truncate=False)

spark.sql("""
SELECT date, ticker, sector, ROUND(daily_return, 4) AS daily_return
FROM workspace.market_pipeline.v_daily_returns
WHERE daily_return IS NOT NULL
ORDER BY daily_return DESC
LIMIT 5
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW workspace.market_pipeline.v_sector_daily_returns AS
SELECT date, sector, COUNT(*) AS n_stocks, AVG(daily_return) AS sector_return
FROM workspace.market_pipeline.v_daily_returns
WHERE daily_return IS NOT NULL
GROUP BY date, sector
""")
print("view created")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS rows, COUNT(DISTINCT sector) AS sectors, COUNT(DISTINCT date) AS dates,
       MIN(n_stocks) AS min_stocks, MAX(n_stocks) AS max_stocks,
       MIN(date) AS first_date, MAX(date) AS last_date
FROM workspace.market_pipeline.v_sector_daily_returns
""").show()

spark.sql("""
SELECT date, sector, n_stocks, ROUND(sector_return, 4) AS sector_return
FROM workspace.market_pipeline.v_sector_daily_returns
ORDER BY sector_return LIMIT 5
""").show(truncate=False)

spark.sql("""
SELECT date, sector, n_stocks, ROUND(sector_return, 4) AS sector_return
FROM workspace.market_pipeline.v_sector_daily_returns
ORDER BY sector_return DESC LIMIT 5
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW workspace.market_pipeline.v_monthly_returns AS
WITH ranked AS (
    SELECT ticker, CAST(date_trunc('month', date) AS DATE) AS month, adj_close,
           ROW_NUMBER() OVER (PARTITION BY ticker, date_trunc('month', date) ORDER BY date DESC) AS rn
    FROM workspace.market_pipeline.fact_prices
),
month_end AS (
    SELECT ticker, month, adj_close FROM ranked WHERE rn = 1
)
SELECT m.ticker, s.sector, m.month, m.adj_close AS month_end_adj_close,
       m.adj_close / LAG(m.adj_close) OVER (PARTITION BY m.ticker ORDER BY m.month) - 1 AS monthly_return
FROM month_end m
JOIN workspace.market_pipeline.dim_stock s ON m.ticker = s.ticker
""")

spark.sql("""
CREATE OR REPLACE VIEW workspace.market_pipeline.v_sector_monthly_returns AS
SELECT month, sector, COUNT(*) AS n_stocks, AVG(monthly_return) AS sector_return
FROM workspace.market_pipeline.v_monthly_returns
WHERE monthly_return IS NOT NULL
GROUP BY month, sector
""")
print("views created")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS stock_months,
       SUM(CASE WHEN monthly_return IS NULL THEN 1 ELSE 0 END) AS null_returns,
       MIN(month) AS first_month, MAX(month) AS last_month
FROM workspace.market_pipeline.v_monthly_returns
""").show()

spark.sql("""
SELECT COUNT(*) AS sector_months, MIN(n_stocks) AS min_stocks, MAX(n_stocks) AS max_stocks
FROM workspace.market_pipeline.v_sector_monthly_returns
""").show()

spark.sql("""
WITH compounded AS (
    SELECT ticker, CAST(date_trunc('month', date) AS DATE) AS month,
           EXP(SUM(LN(1 + daily_return))) - 1 AS from_daily
    FROM workspace.market_pipeline.v_daily_returns
    WHERE daily_return IS NOT NULL
    GROUP BY ticker, date_trunc('month', date)
)
SELECT COUNT(*) AS months_compared, MAX(ABS(c.from_daily - m.monthly_return)) AS biggest_gap
FROM compounded c
JOIN workspace.market_pipeline.v_monthly_returns m ON c.ticker = m.ticker AND c.month = m.month
WHERE m.monthly_return IS NOT NULL
""").show()

spark.sql("""
SELECT month, sector, ROUND(sector_return, 4) AS sector_return
FROM workspace.market_pipeline.v_sector_monthly_returns ORDER BY sector_return LIMIT 5
""").show(truncate=False)

spark.sql("""
SELECT month, sector, ROUND(sector_return, 4) AS sector_return
FROM workspace.market_pipeline.v_sector_monthly_returns ORDER BY sector_return DESC LIMIT 5
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW workspace.market_pipeline.v_drawdown AS
SELECT p.date, p.ticker, s.sector, p.adj_close,
       MAX(p.adj_close) OVER (PARTITION BY p.ticker ORDER BY p.date
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_peak,
       p.adj_close / MAX(p.adj_close) OVER (PARTITION BY p.ticker ORDER BY p.date
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) - 1 AS drawdown
FROM workspace.market_pipeline.fact_prices p
JOIN workspace.market_pipeline.dim_stock s ON p.ticker = s.ticker
""")
print("view created")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS rows,
       ROUND(MIN(drawdown), 4) AS deepest,
       MAX(drawdown) AS highest,
       SUM(CASE WHEN drawdown > 0 THEN 1 ELSE 0 END) AS rows_above_zero,
       SUM(CASE WHEN drawdown = 0 THEN 1 ELSE 0 END) AS rows_at_new_peak
FROM workspace.market_pipeline.v_drawdown
""").show()

spark.sql("""
SELECT ticker, sector, date, ROUND(drawdown, 4) AS drawdown, ROUND(adj_close, 2) AS adj_close,
       ROUND(running_peak, 2) AS running_peak
FROM workspace.market_pipeline.v_drawdown
ORDER BY drawdown LIMIT 8
""").show(truncate=False)

spark.sql("""
SELECT ticker, date, ROUND(drawdown, 4) AS drawdown
FROM workspace.market_pipeline.v_drawdown
WHERE ticker = 'DIS' AND date BETWEEN '2019-01-02' AND '2019-01-08'
ORDER BY date
""").show()

# COMMAND ----------

spark.sql("""
SELECT month, ROUND(month_end_adj_close, 2) AS month_end_price, ROUND(monthly_return, 4) AS monthly_return
FROM workspace.market_pipeline.v_monthly_returns
WHERE ticker = 'CHTR' AND month >= '2025-06-01'
ORDER BY month
""").show(20)

spark.sql("""
SELECT date, ROUND(daily_return, 4) AS daily_return, ROUND(adj_close, 2) AS adj_close
FROM workspace.market_pipeline.v_daily_returns
WHERE ticker = 'CHTR' AND ABS(daily_return) > 0.10
ORDER BY date
""").show(20)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW workspace.market_pipeline.v_index_drawdown AS
WITH idx AS (
    SELECT date, AVG(daily_return) AS index_return, COUNT(*) AS n_stocks
    FROM workspace.market_pipeline.v_daily_returns
    WHERE daily_return IS NOT NULL
    GROUP BY date
),
lvl AS (
    SELECT date, index_return, n_stocks,
           100 * EXP(SUM(LN(1 + index_return)) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS index_level
    FROM idx
),
pk AS (
    SELECT *, MAX(index_level) OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_peak
    FROM lvl
)
SELECT date, n_stocks, index_return, index_level, running_peak, index_level / running_peak - 1 AS drawdown
FROM pk
""")

spark.sql("""
CREATE OR REPLACE VIEW workspace.market_pipeline.v_index_episodes AS
WITH marked AS (
    SELECT date, drawdown,
           SUM(CASE WHEN drawdown = 0 THEN 1 ELSE 0 END)
               OVER (ORDER BY date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS episode_id
    FROM workspace.market_pipeline.v_index_drawdown
),
ranked AS (
    SELECT episode_id, date, drawdown,
           ROW_NUMBER() OVER (PARTITION BY episode_id ORDER BY drawdown ASC, date ASC) AS rn_low,
           ROW_NUMBER() OVER (PARTITION BY episode_id ORDER BY date ASC) AS rn_first
    FROM marked
),
summary AS (
    SELECT episode_id,
           MAX(CASE WHEN rn_first = 1 THEN date END) AS peak_date,
           MAX(CASE WHEN rn_low = 1 THEN date END) AS trough_date,
           MIN(drawdown) AS depth
    FROM ranked
    GROUP BY episode_id
)
SELECT episode_id, peak_date, trough_date, depth,
       LEAD(peak_date) OVER (ORDER BY episode_id) AS recovery_date
FROM summary
""")
print("views created")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS days, MIN(n_stocks) AS min_stocks, MAX(n_stocks) AS max_stocks,
       MIN(date) AS first_date, MAX(date) AS last_date,
       ROUND(MIN(drawdown), 4) AS deepest_index_drawdown,
       SUM(CASE WHEN drawdown > 0 THEN 1 ELSE 0 END) AS rows_above_zero
FROM workspace.market_pipeline.v_index_drawdown
""").show()

spark.sql("""
SELECT peak_date, trough_date, recovery_date, ROUND(depth, 4) AS depth
FROM workspace.market_pipeline.v_index_episodes
WHERE depth < -0.10
ORDER BY depth
LIMIT 10
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW workspace.market_pipeline.v_selloffs AS
WITH d AS (
    SELECT date, ROW_NUMBER() OVER (ORDER BY date) AS day_no
    FROM workspace.market_pipeline.v_index_drawdown
)
SELECT ROW_NUMBER() OVER (ORDER BY e.peak_date) AS selloff_no,
       e.peak_date, e.trough_date, e.recovery_date, e.depth,
       dt.day_no - dp.day_no AS trading_days_peak_to_trough,
       dr.day_no - dt.day_no AS trading_days_trough_to_recovery,
       dr.day_no - dp.day_no AS trading_days_total,
       DATEDIFF(e.recovery_date, e.trough_date) AS calendar_days_trough_to_recovery
FROM workspace.market_pipeline.v_index_episodes e
JOIN d dp ON e.peak_date = dp.date
JOIN d dt ON e.trough_date = dt.date
LEFT JOIN d dr ON e.recovery_date = dr.date
WHERE e.depth < -0.10
""")

spark.sql("SELECT * FROM workspace.market_pipeline.v_selloffs ORDER BY selloff_no").show(truncate=False)

spark.sql("""
SELECT COUNT(*) AS selloffs,
       SUM(CASE WHEN trading_days_total = trading_days_peak_to_trough + trading_days_trough_to_recovery
                THEN 1 ELSE 0 END) AS totals_add_up
FROM workspace.market_pipeline.v_selloffs
""").show()

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW workspace.market_pipeline.v_sector_index AS
SELECT date, sector,
       100 * EXP(SUM(LN(1 + sector_return)) OVER (PARTITION BY sector ORDER BY date
                 ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)) AS sector_level
FROM workspace.market_pipeline.v_sector_daily_returns
""")

spark.sql("""
SELECT COUNT(*) AS rows, COUNT(DISTINCT sector) AS sectors,
       MIN(date) AS first_date, MAX(date) AS last_date,
       ROUND(MIN(sector_level), 1) AS lowest_level, ROUND(MAX(sector_level), 1) AS highest_level
FROM workspace.market_pipeline.v_sector_index
""").show()

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW workspace.market_pipeline.v_sector_selloffs AS
WITH days AS (
    SELECT date, ROW_NUMBER() OVER (ORDER BY date) AS day_no
    FROM workspace.market_pipeline.v_index_drawdown
),
ref AS (
    SELECT s.selloff_no, s.peak_date, s.recovery_date, x.sector, x.sector_level AS ref_level
    FROM workspace.market_pipeline.v_selloffs s
    JOIN workspace.market_pipeline.v_sector_index x ON x.date = s.peak_date
),
win AS (
    SELECT r.selloff_no, r.sector, r.ref_level, r.peak_date, x.date AS d, x.sector_level
    FROM ref r
    JOIN workspace.market_pipeline.v_sector_index x
      ON x.sector = r.sector AND x.date BETWEEN r.peak_date AND r.recovery_date
),
bottom AS (
    SELECT selloff_no, sector, ref_level, peak_date, d AS trough_date, sector_level AS trough_level,
           ROW_NUMBER() OVER (PARTITION BY selloff_no, sector ORDER BY sector_level ASC, d ASC) AS rn
    FROM win
),
bot AS (SELECT * FROM bottom WHERE rn = 1),
rec AS (
    SELECT b.selloff_no, b.sector, MIN(x.date) AS sector_recovery_date
    FROM bot b
    JOIN workspace.market_pipeline.v_sector_index x
      ON x.sector = b.sector AND x.date >= b.trough_date AND x.sector_level >= b.ref_level
    GROUP BY b.selloff_no, b.sector
)
SELECT b.selloff_no, b.sector, b.peak_date, b.trough_date AS sector_trough_date,
       r.sector_recovery_date,
       LEAST(b.trough_level / b.ref_level - 1, 0) AS depth,
       dt.day_no - dp.day_no AS trading_days_peak_to_trough,
       dr.day_no - dt.day_no AS trading_days_trough_to_recovery
FROM bot b
LEFT JOIN rec r ON b.selloff_no = r.selloff_no AND b.sector = r.sector
JOIN days dp ON dp.date = b.peak_date
JOIN days dt ON dt.date = b.trough_date
LEFT JOIN days dr ON dr.date = r.sector_recovery_date
""")
print("view created")

# COMMAND ----------

spark.sql("""
SELECT selloff_no, sector, peak_date, sector_trough_date, sector_recovery_date,
       ROUND(depth, 4) AS depth, trading_days_peak_to_trough AS fall_days,
       trading_days_trough_to_recovery AS recovery_days
FROM workspace.market_pipeline.v_sector_selloffs
ORDER BY selloff_no, depth
""").show(50, truncate=False)

spark.sql("""
SELECT COUNT(*) AS rows,
       SUM(CASE WHEN sector_recovery_date IS NULL THEN 1 ELSE 0 END) AS not_recovered,
       SUM(CASE WHEN depth > 0 THEN 1 ELSE 0 END) AS depth_above_zero
FROM workspace.market_pipeline.v_sector_selloffs
""").show()

spark.sql("""
SELECT ROUND(MIN(x.sector_level) / MAX(CASE WHEN x.date = DATE'2020-02-20' THEN x.sector_level END) - 1, 4)
       AS energy_depth_check
FROM workspace.market_pipeline.v_sector_index x
WHERE x.sector = 'Energy' AND x.date BETWEEN DATE'2020-02-20' AND DATE'2020-11-09'
""").show()

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW workspace.market_pipeline.v_sector_volatility AS
SELECT date, sector, sector_return,
       CASE WHEN COUNT(*) OVER (PARTITION BY sector ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) = 30
            THEN STDDEV_SAMP(sector_return) OVER (PARTITION BY sector ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW)
       END AS vol_30d,
       CASE WHEN COUNT(*) OVER (PARTITION BY sector ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) = 30
            THEN STDDEV_SAMP(sector_return) OVER (PARTITION BY sector ORDER BY date ROWS BETWEEN 29 PRECEDING AND CURRENT ROW) * SQRT(252)
       END AS vol_30d_annualized
FROM workspace.market_pipeline.v_sector_daily_returns
""")
print("view created")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS rows,
       SUM(CASE WHEN vol_30d IS NULL THEN 1 ELSE 0 END) AS null_days,
       ROUND(MIN(vol_30d), 4) AS lowest_vol, ROUND(MAX(vol_30d), 4) AS highest_vol
FROM workspace.market_pipeline.v_sector_volatility
""").show()

spark.sql("""
SELECT date, sector, ROUND(vol_30d, 4) AS vol_30d, ROUND(vol_30d_annualized, 3) AS annualized
FROM workspace.market_pipeline.v_sector_volatility
WHERE vol_30d IS NOT NULL
ORDER BY vol_30d DESC LIMIT 5
""").show(truncate=False)

spark.sql("""
SELECT sector, ROUND(AVG(vol_30d_annualized), 3) AS average_annualized_vol
FROM workspace.market_pipeline.v_sector_volatility
WHERE vol_30d IS NOT NULL
GROUP BY sector ORDER BY average_annualized_vol DESC
""").show(truncate=False)

spark.sql("""
WITH e AS (
    SELECT date, sector_return, ROW_NUMBER() OVER (ORDER BY date) AS rn
    FROM workspace.market_pipeline.v_sector_daily_returns WHERE sector = 'Energy'
),
t AS (SELECT rn FROM e WHERE date = DATE'2020-04-30')
SELECT ROUND(STDDEV_SAMP(e.sector_return), 8) AS recomputed,
       (SELECT ROUND(vol_30d, 8) FROM workspace.market_pipeline.v_sector_volatility
        WHERE sector = 'Energy' AND date = DATE'2020-04-30') AS from_view
FROM e, t WHERE e.rn BETWEEN t.rn - 29 AND t.rn
""").show()

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW workspace.market_pipeline.v_macro_daily AS
WITH days AS (
    SELECT date, CAST(date_trunc('month', date) AS DATE) AS month
    FROM workspace.market_pipeline.dim_date
    WHERE is_trading_day
),
monthly AS (
    SELECT date AS month,
           MAX(CASE WHEN series_id = 'FEDFUNDS' THEN value END) AS fed_funds_rate,
           MAX(CASE WHEN series_id = 'CPIAUCSL' THEN value END) AS cpi_index,
           MAX(CASE WHEN series_id = 'UNRATE' THEN value END) AS unemployment_rate
    FROM workspace.market_pipeline.fact_macro
    WHERE series_id IN ('FEDFUNDS', 'CPIAUCSL', 'UNRATE')
    GROUP BY date
),
cpi AS (
    SELECT month, cpi_index, LAG(cpi_index, 12) OVER (ORDER BY month) AS cpi_12m_ago
    FROM monthly
),
dgs AS (
    SELECT date, value FROM workspace.market_pipeline.fact_macro WHERE series_id = 'DGS10'
),
dd AS (
    SELECT d.date, d.month,
           LAST_VALUE(g.value, TRUE) OVER (ORDER BY d.date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS treasury_10y
    FROM days d
    LEFT JOIN dgs g ON d.date = g.date
)
SELECT dd.date, dd.treasury_10y, m.fed_funds_rate, m.unemployment_rate, c.cpi_index,
       c.cpi_index / c.cpi_12m_ago - 1 AS cpi_yoy
FROM dd
LEFT JOIN monthly m ON dd.month = m.month
LEFT JOIN cpi c ON dd.month = c.month
""")
print("view created")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS rows, COUNT(DISTINCT date) AS dates,
       SUM(CASE WHEN treasury_10y IS NULL THEN 1 ELSE 0 END) AS null_treasury,
       SUM(CASE WHEN fed_funds_rate IS NULL THEN 1 ELSE 0 END) AS null_fed_funds,
       SUM(CASE WHEN unemployment_rate IS NULL THEN 1 ELSE 0 END) AS null_unemployment,
       SUM(CASE WHEN cpi_index IS NULL THEN 1 ELSE 0 END) AS null_cpi,
       SUM(CASE WHEN cpi_yoy IS NULL THEN 1 ELSE 0 END) AS null_cpi_yoy
FROM workspace.market_pipeline.v_macro_daily
""").show()

spark.sql("""
SELECT date, treasury_10y, fed_funds_rate, unemployment_rate
FROM workspace.market_pipeline.v_macro_daily
WHERE date BETWEEN DATE'2025-10-09' AND DATE'2025-10-15'
ORDER BY date
""").show()

spark.sql("""
SELECT ROUND(m.cpi_yoy, 6) AS view_value,
       ROUND((SELECT value FROM workspace.market_pipeline.fact_macro WHERE series_id = 'CPIAUCSL' AND date = DATE'2019-01-01')
           / (SELECT value FROM workspace.market_pipeline.fact_macro WHERE series_id = 'CPIAUCSL' AND date = DATE'2018-01-01') - 1, 6) AS recomputed
FROM workspace.market_pipeline.v_macro_daily m WHERE m.date = DATE'2019-01-02'
""").show()

spark.sql("""
SELECT s.selloff_no, 'peak' AS point, s.peak_date AS date, m.treasury_10y, m.fed_funds_rate,
       ROUND(m.cpi_yoy * 100, 2) AS cpi_yoy_pct, m.unemployment_rate
FROM workspace.market_pipeline.v_selloffs s
JOIN workspace.market_pipeline.v_macro_daily m ON m.date = s.peak_date
UNION ALL
SELECT s.selloff_no, 'trough', s.trough_date, m.treasury_10y, m.fed_funds_rate,
       ROUND(m.cpi_yoy * 100, 2), m.unemployment_rate
FROM workspace.market_pipeline.v_selloffs s
JOIN workspace.market_pipeline.v_macro_daily m ON m.date = s.trough_date
ORDER BY selloff_no, date
""").show()

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE VIEW workspace.market_pipeline.v_sector_macro_monthly AS
WITH me AS (
    SELECT CAST(date_trunc('month', date) AS DATE) AS month, treasury_10y, fed_funds_rate,
           unemployment_rate, cpi_yoy,
           ROW_NUMBER() OVER (PARTITION BY date_trunc('month', date) ORDER BY date DESC) AS rn
    FROM workspace.market_pipeline.v_macro_daily
)
SELECT r.month, r.sector, r.n_stocks, r.sector_return,
       me.treasury_10y AS treasury_10y_month_end, me.fed_funds_rate,
       me.unemployment_rate, me.cpi_yoy
FROM workspace.market_pipeline.v_sector_monthly_returns r
JOIN me ON r.month = me.month AND me.rn = 1
""")
print("view created")

# COMMAND ----------

spark.sql("""
SELECT COUNT(*) AS rows, COUNT(DISTINCT sector) AS sectors, COUNT(DISTINCT month) AS months,
       SUM(CASE WHEN treasury_10y_month_end IS NULL THEN 1 ELSE 0 END) AS null_treasury,
       SUM(CASE WHEN fed_funds_rate IS NULL THEN 1 ELSE 0 END) AS null_fed_funds,
       SUM(CASE WHEN unemployment_rate IS NULL THEN 1 ELSE 0 END) AS null_unemployment,
       SUM(CASE WHEN cpi_yoy IS NULL THEN 1 ELSE 0 END) AS null_cpi_yoy
FROM workspace.market_pipeline.v_sector_macro_monthly
""").show()

spark.sql("""
SELECT month, ROUND(sector_return, 4) AS sector_return, treasury_10y_month_end, fed_funds_rate,
       ROUND(cpi_yoy * 100, 2) AS cpi_yoy_pct, unemployment_rate
FROM workspace.market_pipeline.v_sector_macro_monthly
WHERE sector = 'Energy' AND month BETWEEN DATE'2022-03-01' AND DATE'2022-07-01'
ORDER BY month
""").show()

spark.sql("""
SELECT (SELECT treasury_10y_month_end FROM workspace.market_pipeline.v_sector_macro_monthly
        WHERE sector = 'Energy' AND month = DATE'2019-03-01') AS from_view,
       (SELECT value FROM workspace.market_pipeline.fact_macro
        WHERE series_id = 'DGS10' AND date = (SELECT MAX(date) FROM workspace.market_pipeline.dim_date
                                              WHERE is_trading_day AND date BETWEEN DATE'2019-03-01' AND DATE'2019-03-31')) AS direct
""").show()

# COMMAND ----------

export = spark.sql("""
SELECT month, ticker, sector, monthly_return
FROM workspace.market_pipeline.v_monthly_returns
ORDER BY sector, ticker, month
""").toPandas()

export_path = "/Volumes/workspace/market_pipeline/clean/export_monthly_returns.csv"
export.to_csv(export_path, index=False)
print(export.shape)
print(export.head(3).to_string())

# COMMAND ----------

spark.sql("""
SELECT a.sector, a.stock_months,
       ROUND(a.avg_stock_months, 6) AS avg_from_stock_months,
       ROUND(b.avg_sector_months, 6) AS avg_from_sector_months
FROM (SELECT sector, COUNT(monthly_return) AS stock_months, AVG(monthly_return) AS avg_stock_months
      FROM workspace.market_pipeline.v_monthly_returns GROUP BY sector) a
JOIN (SELECT sector, AVG(sector_return) AS avg_sector_months
      FROM workspace.market_pipeline.v_sector_monthly_returns GROUP BY sector) b
  ON a.sector = b.sector
ORDER BY a.sector
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
SELECT sector, ROUND(AVG(sector_return), 4) AS avg_monthly_return_2020
FROM workspace.market_pipeline.v_sector_monthly_returns
WHERE month BETWEEN DATE'2020-01-01' AND DATE'2020-12-01'
GROUP BY sector ORDER BY sector
""").show(truncate=False)

# COMMAND ----------

spark.sql("""
SELECT sector, ROUND(AVG(vol_30d_annualized), 3) AS avg_annualized_vol_2020
FROM workspace.market_pipeline.v_sector_volatility
WHERE vol_30d IS NOT NULL AND date BETWEEN DATE'2020-01-01' AND DATE'2020-12-31'
GROUP BY sector ORDER BY avg_annualized_vol_2020 DESC
""").show(truncate=False)

spark.sql("""
SELECT ROUND(AVG(vol_30d_annualized), 3) AS overall_2020
FROM workspace.market_pipeline.v_sector_volatility
WHERE vol_30d IS NOT NULL AND date BETWEEN DATE'2020-01-01' AND DATE'2020-12-31'
""").show()

# COMMAND ----------

spark.sql("""
SELECT selloff_no, COUNT(*) AS sectors,
       ROUND(CORR(-depth, trading_days_trough_to_recovery), 3) AS corr_fall_vs_recovery_days
FROM workspace.market_pipeline.v_sector_selloffs
GROUP BY selloff_no ORDER BY selloff_no
""").show()

spark.sql("""
SELECT sector, ROUND(AVG(depth), 4) AS avg_depth,
       ROUND(AVG(trading_days_trough_to_recovery), 0) AS avg_recovery_days,
       MIN(trading_days_trough_to_recovery) AS fastest, MAX(trading_days_trough_to_recovery) AS slowest
FROM workspace.market_pipeline.v_sector_selloffs
GROUP BY sector ORDER BY avg_recovery_days
""").show(truncate=False)

spark.sql("""
SELECT s.selloff_no,
       SUM(CASE WHEN x.sector_recovery_date < s.recovery_date THEN 1 ELSE 0 END) AS before_market,
       SUM(CASE WHEN x.sector_recovery_date = s.recovery_date THEN 1 ELSE 0 END) AS same_day,
       SUM(CASE WHEN x.sector_recovery_date > s.recovery_date THEN 1 ELSE 0 END) AS after_market
FROM workspace.market_pipeline.v_sector_selloffs x
JOIN workspace.market_pipeline.v_selloffs s ON x.selloff_no = s.selloff_no
GROUP BY s.selloff_no ORDER BY s.selloff_no
""").show()

# COMMAND ----------

spark.sql("""
SELECT ROUND(AVG(sector_return), 4) AS avg_all_sectors_jun_2022
FROM workspace.market_pipeline.v_sector_monthly_returns
WHERE month = DATE'2022-06-01'
""").show()

# COMMAND ----------

