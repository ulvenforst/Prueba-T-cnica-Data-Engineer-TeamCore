-- Tarea 2: Usuarios con >3 transacciones fallidas en últimos 7 días

SELECT 
    user_id,
    COUNT(*) as total_fallas,
    MIN(timestamp) as primera_falla,
    MAX(timestamp) as ultima_falla,
    ROUND(SUM(amount), 2) as monto_total_fallido
FROM transactions
WHERE status = 'failed' 
    AND timestamp >= datetime('now', '-7 days')
GROUP BY user_id
HAVING COUNT(*) > 3
ORDER BY total_fallas DESC;
