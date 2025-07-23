-- Tarea 3: Detección de anomalías - incrementos significativos en conteos diarios

SELECT 
    fecha_actual,
    conteo_actual,
    conteo_anterior,
    incremento_absoluto,
    ROUND(incremento_porcentual, 2) as incremento_porcentual,
    CASE 
        WHEN incremento_porcentual > 50 THEN 'ALERTA_CRITICA'
        WHEN incremento_porcentual > 25 THEN 'ALERTA_ALTA'
        WHEN incremento_porcentual > 10 THEN 'ALERTA_MEDIA'
        ELSE 'NORMAL'
    END as nivel_alerta
FROM (
    SELECT 
        DATE(timestamp) as fecha_actual,
        COUNT(*) as conteo_actual,
        LAG(COUNT(*)) OVER (ORDER BY DATE(timestamp)) as conteo_anterior,
        COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY DATE(timestamp)) as incremento_absoluto,
        CASE 
            WHEN LAG(COUNT(*)) OVER (ORDER BY DATE(timestamp)) > 0 
            THEN (COUNT(*) - LAG(COUNT(*)) OVER (ORDER BY DATE(timestamp))) * 100.0 / LAG(COUNT(*)) OVER (ORDER BY DATE(timestamp))
            ELSE 0 
        END as incremento_porcentual
    FROM transactions
    WHERE timestamp >= datetime('now', '-30 days')  -- Solo últimos 30 días
    GROUP BY DATE(timestamp)
) 
WHERE conteo_anterior IS NOT NULL 
    AND incremento_porcentual > 10
ORDER BY fecha_actual DESC
LIMIT 50;
