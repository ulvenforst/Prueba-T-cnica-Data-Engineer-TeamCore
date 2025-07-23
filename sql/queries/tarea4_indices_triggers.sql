-- TAREA 4: Índices y triggers para duplicados y validación de rangos
-- Optimización de consultas y validación automática de datos

-- Índices para optimización de consultas
CREATE INDEX IF NOT EXISTS idx_date_status ON transactions(date, status);
CREATE INDEX IF NOT EXISTS idx_status_date_user ON transactions(status, date, user_id);
CREATE INDEX IF NOT EXISTS idx_amount ON transactions(amount);

-- Trigger para prevenir order_id duplicados
CREATE TRIGGER IF NOT EXISTS prevent_duplicate_orders
    BEFORE INSERT ON transactions
    FOR EACH ROW
    WHEN EXISTS (SELECT 1 FROM transactions WHERE order_id = NEW.order_id)
BEGIN
    SELECT RAISE(ABORT, 'Duplicate order_id detected');
END;

-- Trigger para validar rangos de montos
CREATE TRIGGER IF NOT EXISTS validate_transaction_values
    BEFORE INSERT ON transactions
    FOR EACH ROW
    WHEN NEW.amount < 0 OR NEW.amount > 50000
BEGIN
    SELECT RAISE(ABORT, 'Amount out of valid range (0-50000)');
END;