-- TAREA 1: Vista con resumen por día y estado
-- Crea una vista que muestra estadísticas diarias agrupadas por estado

DROP VIEW IF EXISTS daily_status_summary;

CREATE VIEW daily_status_summary AS
SELECT 
    date,
    status,
    COUNT(*) as transaction_count,
    ROUND(SUM(amount), 2) as total_amount,
    ROUND(AVG(amount), 2) as avg_amount,
    COUNT(DISTINCT user_id) as unique_users
FROM transactions
GROUP BY date, status
ORDER BY date DESC, status;

CREATE VIEW daily_status_summary AS
SELECT 
    date,
    status,
    COUNT(*) as transaction_count,
    ROUND(SUM(amount), 2) as total_amount,
    ROUND(AVG(amount), 2) as avg_amount,
    COUNT(DISTINCT user_id) as unique_users
FROM transactions
GROUP BY date, status
ORDER BY date DESC, status;
