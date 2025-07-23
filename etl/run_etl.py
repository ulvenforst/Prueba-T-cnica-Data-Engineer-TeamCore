#!/usr/bin/env python3
"""
Script principal para el ETL de procesamiento de logs
Ejercicio 3: ETL Python para archivo grande
"""

import sys
import logging
from pathlib import Path

# Añadir src al path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir / "src"))

from src.config import LOG_LEVEL, LOG_FORMAT
from src.utils import setup_logging
from src.processor import LogProcessor


def main():
    """Función principal del ETL"""
    
    # Configurar logging
    setup_logging(LOG_LEVEL, LOG_FORMAT)
    logger = logging.getLogger(__name__)
    
    logger.info("=== EJERCICIO 3: ETL PYTHON PARA ARCHIVO GRANDE ===")
    logger.info("Iniciando procesamiento de logs con streaming")
    
    try:
        # Crear y ejecutar el procesador
        processor = LogProcessor()
        stats = processor.process_streaming()
        
        # Mostrar resumen final
        print("\n" + "="*60)
        print("RESUMEN DEL PROCESAMIENTO")
        print("="*60)
        print(f"Total de registros leídos: {stats['total_records']:,}")
        print(f"Registros filtrados (status >= 500): {stats['filtered_records']:,}")
        print(f"Registros procesados exitosamente: {stats['processed_records']:,}")
        print(f"Registros con errores: {stats['error_records']:,}")
        print(f"Tiempo de procesamiento: {stats['processing_time_seconds']} segundos")
        print(f"Memoria utilizada: {stats['memory_used_mb']} MB")
        print(f"Registros por segundo: {stats['records_per_second']:,}")
        print("="*60)
        
        return True
        
    except Exception as e:
        logger.error(f"Error en el procesamiento principal: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
