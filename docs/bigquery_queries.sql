SELECT 
    symbol,
    DATE(open_time) as trade_date,
    open,
    high,
    low,
    close,
    volume,
    daily_return_pct,
    price_range_pct
FROM `{project_id}.crypto_market.klines_daily`
WHERE DATE(open_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
ORDER BY symbol, trade_date DESC;

SELECT 
    symbol,
    DATE_TRUNC(DATE(open_time), MONTH) as month,
    COUNT(*) as trading_days,
    AVG(close) as avg_close_price,
    MIN(low) as month_low,
    MAX(high) as month_high,
    SUM(volume) as total_volume,
    AVG(daily_return_pct) as avg_daily_return,
    STDDEV(daily_return_pct) as volatility
FROM `{project_id}.crypto_market.klines_daily`
GROUP BY symbol, month
ORDER BY symbol, month DESC;

WITH ranked_days AS (
    SELECT 
        symbol,
        DATE(open_time) as trade_date,
        close,
        volume,
        quote_volume,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY volume DESC) as volume_rank
    FROM `{project_id}.crypto_market.klines_daily`
)
SELECT 
    symbol,
    trade_date,
    close,
    ROUND(volume, 2) as volume,
    ROUND(quote_volume, 2) as quote_volume
FROM ranked_days
WHERE volume_rank <= 10
ORDER BY symbol, volume_rank;

WITH daily_returns AS (
    SELECT 
        DATE(open_time) as trade_date,
        symbol,
        daily_return_pct
    FROM `{project_id}.crypto_market.klines_daily`
    WHERE daily_return_pct IS NOT NULL
)
SELECT 
    a.symbol as symbol_1,
    b.symbol as symbol_2,
    CORR(a.daily_return_pct, b.daily_return_pct) as correlation
FROM daily_returns a
JOIN daily_returns b 
    ON a.trade_date = b.trade_date 
    AND a.symbol < b.symbol
GROUP BY a.symbol, b.symbol
ORDER BY correlation DESC;

SELECT 
    symbol,
    DATE(open_time) as trade_date,
    close,
    AVG(close) OVER (
        PARTITION BY symbol 
        ORDER BY DATE(open_time) 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as ma_30,
    AVG(volume) OVER (
        PARTITION BY symbol 
        ORDER BY DATE(open_time) 
        ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
    ) as ma_30_volume
FROM `{project_id}.crypto_market.klines_daily`
ORDER BY symbol, trade_date DESC;

SELECT 
    symbol,
    FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY open_time) as year_start_price,
    LAST_VALUE(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) as current_price,
    ROUND(
        ((LAST_VALUE(close) OVER (PARTITION BY symbol ORDER BY open_time ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) - 
          FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY open_time)) / 
          FIRST_VALUE(close) OVER (PARTITION BY symbol ORDER BY open_time)) * 100, 
        2
    ) as ytd_return_pct
FROM `{project_id}.crypto_market.klines_daily`
WHERE EXTRACT(YEAR FROM open_time) = EXTRACT(YEAR FROM CURRENT_DATE())
QUALIFY ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY open_time DESC) = 1
ORDER BY ytd_return_pct DESC;

SELECT 
    symbol,
    COUNT(*) as trading_days,
    AVG(daily_return_pct) as avg_daily_return,
    STDDEV(daily_return_pct) as daily_volatility,
    MIN(daily_return_pct) as worst_day,
    MAX(daily_return_pct) as best_day,
    MAX(high) / MIN(low) - 1 as max_drawdown_pct
FROM `{project_id}.crypto_market.klines_daily`
WHERE DATE(open_time) >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)
    AND daily_return_pct IS NOT NULL
GROUP BY symbol
ORDER BY daily_volatility DESC;
