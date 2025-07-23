-- Tarea 2: Usuarios con múltiples transacciones fallidas
-- Identifica usuarios con patrones de fallas

SELECT 
    user_id,
    COUNT(*) as failed_transactions,
    COUNT(DISTINCT DATE(timestamp)) as days_with_failures,
    AVG(amount) as avg_failed_amount,
    MIN(timestamp) as first_failure,
    MAX(timestamp) as last_failure
FROM transactions
WHERE status = 'failed'
GROUP BY user_id
HAVING COUNT(*) >= 2
ORDER BY failed_transactions DESC, user_id
LIMIT 50;
