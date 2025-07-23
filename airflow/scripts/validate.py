"""
Script de validación para el pipeline de transacciones
"""
import sys
from pathlib import Path
from sqlalchemy import create_engine, text

# Añadir directorio padre para importar config
# Añadir directorios padre para importar config y utils
current_dir = Path(__file__).parent
project_root = current_dir.parent.parent.parent
sys.path.append(str(current_dir.parent.parent))  # Para config
sys.path.append(str(project_root / "shared"))    # Para utils

from config import DB_CONNECTION_STRING, TABLE_NAME
from utils import setup_logger, send_alert, validate_table_not_empty

def validate_pipeline():
    """
    Ejecuta validaciones finales del pipeline:
    - Verifica que la tabla no esté vacía
    - Ejecuta checks de calidad de datos básicos
    - Genera alertas si hay problemas
    """
    logger = setup_logger(__name__, 'validate.log')
    
    try:
        logger.info("Iniciando validaciones del pipeline")
        
        # Crear conexión
        engine = create_engine(DB_CONNECTION_STRING)
        
        # Validación 1: Tabla no vacía
        if not validate_table_not_empty(engine, TABLE_NAME):
            raise ValueError(f"La tabla {TABLE_NAME} está vacía")
        
        with engine.connect() as conn:
            # Obtener estadísticas básicas
            stats_query = text(f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT user_id) as unique_users,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as completed_transactions,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_transactions,
                    SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending_transactions,
                    ROUND(AVG(amount), 2) as avg_amount,
                    MIN(date) as min_date,
                    MAX(date) as max_date
                FROM {TABLE_NAME}
            """)
            
            result = conn.execute(stats_query)
            stats = result.fetchone()
            
            # Log estadísticas
            logger.info("Estadísticas de la carga:")
            logger.info(f"- Total filas: {stats[0]:,}")
            logger.info(f"- Usuarios únicos: {stats[1]:,}")
            logger.info(f"- Transacciones completadas: {stats[2]:,}")
            logger.info(f"- Transacciones fallidas: {stats[3]:,}")
            logger.info(f"- Transacciones pendientes: {stats[4]:,}")
            logger.info(f"- Monto promedio: ${stats[5]}")
            logger.info(f"- Rango de fechas: {stats[6]} a {stats[7]}")
            
            # Validación 2: Datos básicos de calidad
            failed_rate = stats[3] / stats[0] * 100 if stats[0] > 0 else 0
            
            # Validación 3: Revisar datos duplicados
            duplicates_query = text(f"""
                SELECT COUNT(*) as duplicates
                FROM (
                    SELECT order_id, COUNT(*) as cnt
                    FROM {TABLE_NAME}
                    GROUP BY order_id
                    HAVING COUNT(*) > 1
                ) dup
            """)
            
            dup_result = conn.execute(duplicates_query)
            duplicates = dup_result.scalar()
            
            # Validación 4: Revisar valores negativos
            negative_amounts_query = text(f"""
                SELECT COUNT(*) FROM {TABLE_NAME} WHERE amount <= 0
            """)
            
            neg_result = conn.execute(negative_amounts_query)
            negative_amounts = neg_result.scalar()
            
            # Generar alertas si es necesario
            alerts = []
            
            if stats[0] < 1000:  # Muy pocas transacciones
                alerts.append(f"Pocas transacciones cargadas: {stats[0]}")
            
            if failed_rate > 50:  # Tasa de fallo muy alta
                alerts.append(f"Tasa de fallos alta: {failed_rate:.1f}%")
            
            if duplicates > 0:
                alerts.append(f"Se encontraron {duplicates} order_ids duplicados")
            
            if negative_amounts > 0:
                alerts.append(f"Se encontraron {negative_amounts} montos negativos/cero")
            
            # Enviar alertas si las hay
            if alerts:
                alert_message = "\\n".join(alerts)
                send_alert("Alertas de calidad de datos", alert_message, logger)
                logger.warning("Se generaron alertas de calidad de datos")
            else:
                logger.info("Todas las validaciones pasaron exitosamente")
        
        logger.info("Validaciones completadas")
        return True
        
    except Exception as e:
        error_msg = f"Error en validaciones: {str(e)}"
        logger.error(error_msg)
        send_alert("Error crítico en validaciones", error_msg, logger)
        raise
