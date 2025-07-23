-- Tarea 1: Vista/tabla resumen por día y estado

SELECT 
    DATE(timestamp) as fecha,
    status as estado,
    COUNT(*) as total_transacciones,
    COUNT(DISTINCT user_id) as usuarios_unicos,
    ROUND(SUM(amount), 2) as volumen_total,
    ROUND(AVG(amount), 2) as monto_promedio,
    MIN(amount) as monto_minimo,
    MAX(amount) as monto_maximo
FROM transactions
WHERE timestamp >= datetime('now', '-30 days')  -- Solo últimos 30 días para mejor rendimiento
GROUP BY DATE(timestamp), status
ORDER BY fecha DESC, estado
LIMIT 100;
