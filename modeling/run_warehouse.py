#!/usr/bin/env python3
"""
Script principal para el Ejercicio 4: Modelado de datos
Implementa ETL completo con esquema star, SCD y particionamiento
"""

import sys
import logging
from pathlib import Path

# Añadir src al path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir / "src"))

from src.database import DatabaseManager, setup_logging
from src.etl import TransactionETL
from src.scd_manager import SCDManager, PartitionManager
from src.config import DATABASE_FILE


def initialize_warehouse() -> DatabaseManager:
    """Inicializa el data warehouse con el esquema"""
    logger = logging.getLogger(__name__)
    logger.info("Inicializando data warehouse")
    
    db_manager = DatabaseManager()
    
    # Crear esquema
    schema_file = current_dir / "sql" / "01_create_schema.sql"
    db_manager.execute_sql_file(schema_file)
    logger.info("Esquema creado exitosamente")
    
    # Poblar dimensiones base
    dimensions_file = current_dir / "sql" / "02_populate_dimensions.sql"
    db_manager.execute_sql_file(dimensions_file)
    logger.info("Dimensiones base pobladas")
    
    return db_manager


def run_etl_process(db_manager: DatabaseManager) -> dict:
    """Ejecuta el proceso ETL completo"""
    logger = logging.getLogger(__name__)
    logger.info("Iniciando proceso ETL")
    
    etl = TransactionETL(db_manager)
    stats = etl.run_etl()
    
    return stats


def run_scd_process(db_manager: DatabaseManager) -> dict:
    """Ejecuta el proceso de gestión SCD"""
    logger = logging.getLogger(__name__)
    logger.info("Iniciando proceso SCD")
    
    scd_manager = SCDManager(db_manager)
    stats = scd_manager.run_scd_updates()
    
    return stats


def run_partition_process(db_manager: DatabaseManager) -> dict:
    """Ejecuta el proceso de particionamiento"""
    logger = logging.getLogger(__name__)
    logger.info("Iniciando proceso de particionamiento")
    
    partition_manager = PartitionManager(db_manager)
    stats = partition_manager.archive_old_partitions(months_to_keep=12)
    
    return stats


def validate_warehouse(db_manager: DatabaseManager) -> dict:
    """Valida la integridad del data warehouse"""
    logger = logging.getLogger(__name__)
    logger.info("Validando data warehouse")
    
    validation_queries = {
        'total_transactions': "SELECT COUNT(*) as count FROM fact_transactions",
        'total_users': "SELECT COUNT(*) as count FROM dim_user",
        'total_dates': "SELECT COUNT(*) as count FROM dim_date",
        'total_statuses': "SELECT COUNT(*) as count FROM dim_status",
        'revenue_by_status': """
            SELECT ds.status_code, 
                   COUNT(*) as transaction_count,
                   ROUND(SUM(ft.revenue), 2) as total_revenue
            FROM fact_transactions ft
            JOIN dim_status ds ON ft.status_key = ds.status_key
            GROUP BY ds.status_code
        """,
        'user_segments': """
            SELECT user_segment, 
                   COUNT(*) as user_count,
                   ROUND(AVG(total_lifetime_value), 2) as avg_ltv
            FROM dim_user
            GROUP BY user_segment
        """
    }
    
    results = {}
    for name, query in validation_queries.items():
        try:
            result = db_manager.execute_query(query)
            results[name] = result
            logger.info(f"Validación {name}: {len(result)} registros")
        except Exception as e:
            logger.error(f"Error en validación {name}: {e}")
            results[name] = {"error": str(e)}
    
    return results


def main():
    """Función principal"""
    setup_logging("INFO")
    logger = logging.getLogger(__name__)
    
    logger.info("=== EJERCICIO 4: MODELADO DE DATOS ===")
    logger.info("Iniciando construcción de data warehouse")
    
    try:
        # Inicializar warehouse
        db_manager = initialize_warehouse()
        
        # Ejecutar ETL
        etl_stats = run_etl_process(db_manager)
        
        # Ejecutar SCD
        scd_stats = run_scd_process(db_manager)
        
        # Ejecutar particionamiento
        partition_stats = run_partition_process(db_manager)
        
        # Validar resultados
        validation_results = validate_warehouse(db_manager)
        
        # Resumen final
        print("\n" + "="*80)
        print("RESUMEN DEL MODELADO DE DATOS")
        print("="*80)
        print(f"Base de datos creada en: {DATABASE_FILE}")
        print(f"Registros ETL procesados: {etl_stats['loaded_records']:,}")
        print(f"Tasa de éxito ETL: {etl_stats['success_rate']:.2f}%")
        print(f"Actualizaciones SCD: {scd_stats['total_updates']}")
        print(f"Particiones archivadas: {partition_stats['archived_partitions']}")
        print("\nESTADÍSTICAS DEL WAREHOUSE:")
        
        for name, data in validation_results.items():
            if isinstance(data, list) and data:
                if name in ['total_transactions', 'total_users', 'total_dates', 'total_statuses']:
                    print(f"  {name}: {data[0]['count']:,} registros")
                elif name == 'revenue_by_status':
                    print(f"  Revenue por estado:")
                    for row in data:
                        print(f"    {row['status_code']}: {row['transaction_count']:,} trans, ${row['total_revenue']:,}")
                elif name == 'user_segments':
                    print(f"  Segmentos de usuario:")
                    for row in data:
                        print(f"    {row['user_segment']}: {row['user_count']:,} usuarios, LTV promedio: ${row['avg_ltv']:,}")
        
        print("="*80)
        print("Modelado completado exitosamente")
        
        return True
        
    except Exception as e:
        logger.error(f"Error en el proceso principal: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
