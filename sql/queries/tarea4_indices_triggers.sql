-- Tarea 4: Optimización con índices y triggers
-- Análisis de rendimiento y recomendaciones de índices

-- Análisis de consultas frecuentes
SELECT 
    'user_id_queries' as query_type,
    COUNT(DISTINCT user_id) as distinct_values,
    COUNT(*) as total_rows,
    'CREATE INDEX idx_user_id ON transactions(user_id)' as recommended_index
FROM transactions

UNION ALL

SELECT 
    'status_queries' as query_type,
    COUNT(DISTINCT status) as distinct_values,
    COUNT(*) as total_rows,
    'CREATE INDEX idx_status ON transactions(status)' as recommended_index
FROM transactions

UNION ALL

SELECT 
    'timestamp_queries' as query_type,
    COUNT(DISTINCT DATE(timestamp)) as distinct_values,
    COUNT(*) as total_rows,
    'CREATE INDEX idx_timestamp ON transactions(timestamp)' as recommended_index
FROM transactions

UNION ALL

SELECT 
    'amount_queries' as query_type,
    NULL as distinct_values,
    COUNT(*) as total_rows,
    'CREATE INDEX idx_amount ON transactions(amount)' as recommended_index
FROM transactions;
