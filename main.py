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
        from etl.processor import ETLProcessor
        processor = ETLProcessor()
        
        # Buscar archivo de entrada
        input_file = Path("data/raw/sample_transactions.csv")
        if not input_file.exists():
            logger.warning(f"Archivo no encontrado: {input_file}")
            return False
            
        # Procesar archivo
        result = processor.process_file(input_file)
        logger.info(f"ETL completado: {result}")
        
        logger.info("Proceso ETL completado exitosamente")
        return result['status'] == 'success'
    except Exception as e:
        logger.error(f"Error en proceso ETL: {e}")
        return False

def run_warehouse():
    """Ejecutar proceso de data warehouse"""
    logger.info("Iniciando proceso de data warehouse")
    try:
        from modeling.warehouse import DataWarehouse
        import pandas as pd
        
        # Inicializar warehouse
        warehouse = DataWarehouse()
        init_result = warehouse.initialize_schema()
        logger.info(f"Inicialización: {init_result}")
        
        # Cargar datos si existe archivo procesado
        processed_file = Path("data/processed/cleaned_transactions.csv")
        if processed_file.exists():
            df = pd.read_csv(processed_file)
            load_result = warehouse.load_transactions(df)
            logger.info(f"Carga completada: {load_result}")
        else:
            logger.warning(f"Archivo procesado no encontrado: {processed_file}")
        
        # Obtener estadísticas
        stats = warehouse.get_summary_stats()
        logger.info(f"Estadísticas: {stats}")
        
        logger.info("Proceso de data warehouse completado exitosamente")
        return True
    except Exception as e:
        logger.error(f"Error en proceso de warehouse: {e}")
        return False

def run_sql_analysis():
    """Ejecutar análisis SQL - Ejercicio 2"""
    logger.info("Iniciando análisis SQL - Ejercicio 2")
    try:
        from sql.analysis_runner import main as run_analysis
        result = run_analysis()
        logger.info("Análisis SQL completado exitosamente")
        return result
    except Exception as e:
        logger.error(f"Error en análisis SQL: {e}")
        return False

def run_streaming_benchmark():
    """Ejecutar benchmark de streaming - Ejercicio 3"""
    logger.info("Iniciando benchmark de streaming - Ejercicio 3")
    try:
        from etl.streaming_processor import main as run_streaming
        result = run_streaming()
        logger.info("Benchmark de streaming completado exitosamente")
        return result
    except Exception as e:
        logger.error(f"Error en benchmark de streaming: {e}")
        return False

def run_all_exercises():
    """Ejecutar todos los ejercicios en secuencia"""
    logger.info("=== EJECUTANDO TODOS LOS EJERCICIOS ===")
    
    results = {}
    
    # Ejercicio 1: Orquestación (ETL)
    logger.info("--- Ejercicio 1: Orquestación ---")
    results['ejercicio_1'] = run_etl()
    
    # Ejercicio 2: SQL y análisis
    logger.info("--- Ejercicio 2: SQL y análisis ---")
    results['ejercicio_2'] = run_sql_analysis()
    
    # Ejercicio 3: ETL streaming
    logger.info("--- Ejercicio 3: ETL streaming ---")
    results['ejercicio_3'] = run_streaming_benchmark()
    
    # Ejercicio 4: Modelado de datos
    logger.info("--- Ejercicio 4: Modelado de datos ---")
    results['ejercicio_4'] = run_warehouse()
    
    # Resumen
    successful = sum(1 for result in results.values() if result)
    total = len(results)
    
    logger.info(f"=== RESUMEN: {successful}/{total} ejercicios completados ===")
    
    for ejercicio, resultado in results.items():
        status = "EXITOSO" if resultado else "FALLIDO"
        logger.info(f"{ejercicio}: {status}")
    
    return successful == total

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
  %(prog)s etl                 # Ejecutar solo ETL (Ejercicio 1)
  %(prog)s warehouse           # Ejecutar solo data warehouse (Ejercicio 4)
  %(prog)s sql                 # Ejecutar análisis SQL (Ejercicio 2)
  %(prog)s streaming           # Ejecutar benchmark streaming (Ejercicio 3)
  %(prog)s pipeline            # Ejecutar pipeline básico (ETL + Warehouse)
  %(prog)s all                 # Ejecutar TODOS los ejercicios
  %(prog)s --help              # Mostrar esta ayuda
        """
    )
    
    parser.add_argument(
        'command',
        choices=['etl', 'warehouse', 'pipeline', 'sql', 'streaming', 'all'],
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
    elif args.command == 'sql':
        success = run_sql_analysis()
    elif args.command == 'streaming':
        success = run_streaming_benchmark()
    elif args.command == 'all':
        success = run_all_exercises()
    
    # Salir con código apropiado
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()
