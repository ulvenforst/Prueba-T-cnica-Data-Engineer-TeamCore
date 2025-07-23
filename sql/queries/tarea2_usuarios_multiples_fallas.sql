-- TAREA 2: Detectar usuarios con más de 3 transacciones fallidas en los últimos 7 días
-- Query que identifica usuarios problemáticos con múltiples fallas recientes

SELECT 
    user_id,
    COUNT(*) as failed_transactions_last_7_days,
    MIN(date) as first_failure_date,
    MAX(date) as last_failure_date,
    ROUND(SUM(amount), 2) as total_failed_amount
FROM transactions
WHERE status = 'failed' 
    AND date >= date('now', '-7 days')
GROUP BY user_id
HAVING COUNT(*) > 3
ORDER BY failed_transactions_last_7_days DESC;
