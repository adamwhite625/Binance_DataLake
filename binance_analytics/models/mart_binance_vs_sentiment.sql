{{config(materialized ='table')}}

WITH daily AS (
    SELECT
        DATE(candle_start_time) as date,
        UPPER(REPLACE(symbol, 'usdt', '')) as symbol,
        AVG(close_price) as avg_price,
        MAX(high_price) as max_price,
        MIN(low_price) as min_price,
        SUM(total_volume) as total_volume
    FROM {{source('warehouse', 'fact_market_candles')}}
    WHERE symbol = 'BTCUSDT' OR symbol = 'btcusdt'
    GROUP BY 1,2
),
sentiment AS (
    SELECT
        date,
        fng_value,
        fng_classification
    FROM {{ source('warehouse','fact_market_sentiment')}}
)
SELECT
    a.date,
    a.symbol,
    a.avg_price,
    a.max_price,
    a.min_price,
    a.total_volume,
    b.fng_value,
    b.fng_classification
FROM daily a
LEFT JOIN sentiment b
ON a.date = b.date::date
ORDER BY a.date DESC