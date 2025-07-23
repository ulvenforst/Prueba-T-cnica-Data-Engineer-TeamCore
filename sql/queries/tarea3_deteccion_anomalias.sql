-- TAREA 3: Detección de anomalías en conteos diarios
-- Compara conteos diarios y alerta sobre incrementos significativos

WITH daily_counts AS (
    SELECT 
        date,
        COUNT(*) as daily_transaction_count
    FROM transactions
    GROUP BY date
),
daily_stats AS (
    SELECT 
        date,
        daily_transaction_count,
        LAG(daily_transaction_count) OVER (ORDER BY date) as previous_day_count,
        AVG(daily_transaction_count) OVER (
            ORDER BY date 
            ROWS BETWEEN 6 PRECEDING AND 1 PRECEDING
        ) as avg_last_7_days
    FROM daily_counts
)
SELECT 
    date,
    daily_transaction_count,
    previous_day_count,
    ROUND(avg_last_7_days, 2) as avg_last_7_days,
    ROUND(
        (daily_transaction_count - avg_last_7_days) * 100.0 / avg_last_7_days, 
        2
    ) as percent_increase_vs_avg,
    CASE 
        WHEN daily_transaction_count > avg_last_7_days * 1.5 THEN 'ALERT: +50% increase'
        WHEN daily_transaction_count > avg_last_7_days * 1.3 THEN 'WARNING: +30% increase'
        WHEN daily_transaction_count < avg_last_7_days * 0.7 THEN 'WARNING: -30% decrease'
        ELSE 'NORMAL'
    END as anomaly_status
FROM daily_stats
WHERE avg_last_7_days IS NOT NULL
ORDER BY date;
