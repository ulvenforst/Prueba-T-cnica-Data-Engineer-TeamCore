-- Tarea 4: Índices y triggers para optimización y validación

-- Análisis de duplicados potenciales (limitado para rendimiento)
SELECT 
    'DUPLICADOS_POTENCIALES' as metrica,
    COUNT(*) as cantidad,
    'Transacciones con mismo user_id y amount en ventana de 1 hora' as descripcion
FROM (
    SELECT user_id, amount, timestamp 
    FROM transactions 
    WHERE timestamp >= datetime('now', '-7 days')  -- Solo últimos 7 días
    LIMIT 10000  -- Limitar muestra para análisis
) t1
WHERE EXISTS (
    SELECT 1 FROM transactions t2 
    WHERE t2.user_id = t1.user_id 
        AND t2.amount = t1.amount 
        AND t2.timestamp >= datetime('now', '-7 days')
        AND ABS(julianday(t2.timestamp) - julianday(t1.timestamp)) < 0.042  -- 1 hora
)

UNION ALL

-- Análisis de valores fuera de rango
SELECT 
    'VALORES_FUERA_RANGO' as metrica,
    COUNT(*) as cantidad,
    'Transacciones con amount <= 0 o user_id <= 0' as descripcion
FROM transactions
WHERE amount <= 0 OR user_id <= 0

UNION ALL

-- Estadísticas para diseño de índices
SELECT 
    'CARDINALIDAD_USER_ID' as metrica,
    COUNT(DISTINCT user_id) as cantidad,
    'Usuarios únicos - alta selectividad para índice' as descripcion
FROM transactions

UNION ALL

SELECT 
    'CARDINALIDAD_STATUS' as metrica,
    COUNT(DISTINCT status) as cantidad,
    'Estados únicos - baja selectividad' as descripcion
FROM transactions

UNION ALL

-- Recomendación de particionamiento
SELECT 
    'PARTICION_MENSUAL' as metrica,
    COUNT(DISTINCT strftime('%Y-%m', timestamp)) as cantidad,
    'Meses únicos - base para particionamiento temporal' as descripcion
FROM transactions;
