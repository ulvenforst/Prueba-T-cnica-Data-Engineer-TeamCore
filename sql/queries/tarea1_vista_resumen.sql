-- Tarea 1: Vista resumen de transacciones
-- Agrupa transacciones por usuario y status, calculando totales

SELECT 
    user_id,
    status,
    COUNT(*) as total_transactions,
    SUM(amount) as total_amount,
    AVG(amount) as avg_amount,
    MIN(amount) as min_amount,
    MAX(amount) as max_amount,
    DATE(timestamp) as transaction_date
FROM transactions
GROUP BY user_id, status, DATE(timestamp)
ORDER BY user_id, status, transaction_date
LIMIT 100;
