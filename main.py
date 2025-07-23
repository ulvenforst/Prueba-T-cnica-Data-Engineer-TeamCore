#!/usr/bin/env python3
"""
Script principal para ejecutar el pipeline de data engineering
"""

import argparse
import logging
import sys
from pathlib import Path

# Configurar logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_etl():
    """Ejecutar proceso ETL"""
    logger.info("Iniciando proceso ETL")
    
    try:
        from etl.processor import TransactionProcessor
        
        processor = TransactionProcessor()
        
        # Extraer
        extract_stats = processor.extract()
        logger.info(f"Extracción completada: {extract_stats}")
        
        # Transformar
        transform_stats = processor.transform(extract_stats)
        logger.info(f"Transformación completada: {transform_stats}")
        
        # Exportar
        output_file = Path("output") / "processed_transactions.parquet"
        export_stats = processor.export_to_parquet(output_file)
        logger.info(f"Exportación completada: {export_stats}")
        
        logger.info("Proceso ETL completado exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error en proceso ETL: {e}")
        return False


def run_warehouse():
    """Ejecutar proceso de data warehouse"""
    logger.info("Iniciando proceso de data warehouse")
    
    try:
        from modeling.warehouse import DataWarehouse
        from etl.processor import TransactionProcessor
        
        # Procesar datos primero
        processor = TransactionProcessor()
        processor.extract()
        processor.transform({})
        data = processor.get_processed_data()
        
        # Inicializar warehouse
        warehouse = DataWarehouse()
        warehouse.initialize_schema()
        
        # Cargar datos
        load_stats = warehouse.load(data)
        logger.info(f"Carga completada: {load_stats}")
        
        # Actualizar dimensiones
        update_stats = warehouse.update_dimensions()
        logger.info(f"Actualización SCD completada: {update_stats}")
        
        # Validar calidad
        validation = warehouse.validate_data_quality()
        logger.info(f"Validación completada: {validation}")
        
        logger.info("Proceso de data warehouse completado exitosamente")
        return True
        
    except Exception as e:
        logger.error(f"Error en proceso de warehouse: {e}")
        return False


def run_full_pipeline():
    """Ejecutar pipeline completo"""
    logger.info("Iniciando pipeline completo")
    
    success = True
    
    # Ejecutar ETL
    if not run_etl():
        success = False
    
    # Ejecutar warehouse
    if not run_warehouse():
        success = False
    
    if success:
        logger.info("Pipeline completo ejecutado exitosamente")
    else:
        logger.error("Pipeline completado con errores")
    
    return success


def main():
    """Función principal"""
    parser = argparse.ArgumentParser(
        description="Pipeline de data engineering",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Ejemplos:
  %(prog)s etl                 # Ejecutar solo ETL
  %(prog)s warehouse           # Ejecutar solo data warehouse
  %(prog)s pipeline            # Ejecutar pipeline completo
  %(prog)s --help              # Mostrar esta ayuda
        """
    )
    
    parser.add_argument(
        'command',
        choices=['etl', 'warehouse', 'pipeline'],
        help='Comando a ejecutar'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Mostrar información detallada'
    )
    
    args = parser.parse_args()
    
    # Configurar nivel de logging
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Ejecutar comando
    success = False
    
    if args.command == 'etl':
        success = run_etl()
    elif args.command == 'warehouse':
        success = run_warehouse()
    elif args.command == 'pipeline':
        success = run_full_pipeline()
    
    # Salir con código apropiado
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
