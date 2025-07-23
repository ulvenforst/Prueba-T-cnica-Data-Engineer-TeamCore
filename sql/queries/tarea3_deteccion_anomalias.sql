-- Tarea 3: Detección de anomalías en montos
-- Encuentra transacciones con montos atípicos usando desviación estándar

WITH transaction_stats AS (
    SELECT 
        AVG(amount) as mean_amount,
        STDEV(amount) as stddev_amount
    FROM transactions
    WHERE status = 'completed'
),
anomalies AS (
    SELECT 
        t.*,
        ts.mean_amount,
        ts.stddev_amount,
        ABS(t.amount - ts.mean_amount) / ts.stddev_amount as z_score
    FROM transactions t
    CROSS JOIN transaction_stats ts
    WHERE t.status = 'completed'
    AND ABS(t.amount - ts.mean_amount) / ts.stddev_amount > 2.0
)
SELECT 
    order_id,
    user_id,
    amount,
    timestamp,
    status,
    ROUND(z_score, 2) as z_score,
    CASE 
        WHEN z_score > 3.0 THEN 'Extrema'
        WHEN z_score > 2.5 THEN 'Alta'
        ELSE 'Moderada'
    END as anomaly_level
FROM anomalies
ORDER BY z_score DESC
LIMIT 100;
