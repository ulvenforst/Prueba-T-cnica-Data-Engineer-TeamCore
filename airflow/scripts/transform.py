"""
Script de transformación de datos para el pipeline de transacciones
"""
import pandas as pd
import sys
from pathlib import Path
from datetime import datetime

# Añadir directorio padre para importar config
# Añadir directorios padre para importar config y utils
current_dir = Path(__file__).parent
project_root = current_dir.parent.parent.parent
sys.path.append(str(current_dir.parent.parent))  # Para config
sys.path.append(str(project_root / "shared"))    # Para utils

from config import CSV_FILE_PATH, CLEANED_CSV_PATH, CHUNK_SIZE
from utils import setup_logger, send_alert

def transform_data():
    """
    Transforma los datos de transacciones:
    - Limpia valores nulos
    - Convierte tipos de datos
    - Añade campos calculados
    - Procesa en chunks para eficiencia de memoria
    """
    logger = setup_logger(__name__, 'transform.log')
    
    try:
        logger.info(f"Iniciando transformación de: {CSV_FILE_PATH}")
        
        if not CSV_FILE_PATH.exists():
            raise FileNotFoundError(f"Archivo fuente no encontrado: {CSV_FILE_PATH}")
        
        processed_chunks = []
        total_rows = 0
        chunks_processed = 0
        
        # Leer y procesar en chunks
        chunk_reader = pd.read_csv(CSV_FILE_PATH, chunksize=CHUNK_SIZE)
        
        for chunk in chunk_reader:
            chunks_processed += 1
            logger.info(f"Procesando chunk {chunks_processed} con {len(chunk)} filas")
            
            # Limpiar datos
            initial_rows = len(chunk)
            
            # Eliminar filas con valores nulos críticos
            chunk = chunk.dropna(subset=['order_id', 'user_id', 'amount', 'status'])
            
            # Convertir tipos
            chunk['order_id'] = chunk['order_id'].astype(int)
            chunk['user_id'] = chunk['user_id'].astype(int)
            chunk['amount'] = pd.to_numeric(chunk['amount'], errors='coerce')
            
            # Eliminar filas con amount inválido
            chunk = chunk.dropna(subset=['amount'])
            
            # Validar que amount sea positivo
            chunk = chunk[chunk['amount'] > 0]
            
            # Convertir timestamp
            chunk['ts'] = pd.to_datetime(chunk['ts'], errors='coerce')
            chunk = chunk.dropna(subset=['ts'])
            
            # Añadir campos calculados
            chunk['processed_at'] = datetime.now()
            chunk['date'] = chunk['ts'].dt.date
            chunk['hour'] = chunk['ts'].dt.hour
            
            # Validar status values
            valid_statuses = ['completed', 'failed', 'pending']
            chunk = chunk[chunk['status'].isin(valid_statuses)]
            
            final_rows = len(chunk)
            rows_cleaned = initial_rows - final_rows
            
            if rows_cleaned > 0:
                logger.info(f"Chunk {chunks_processed}: eliminadas {rows_cleaned} filas inválidas")
            
            processed_chunks.append(chunk)
            total_rows += final_rows
        
        # Concatenar todos los chunks
        logger.info("Concatenando chunks procesados...")
        final_df = pd.concat(processed_chunks, ignore_index=True)
        
        # Guardar datos limpios
        final_df.to_csv(CLEANED_CSV_PATH, index=False)
        
        logger.info(f"Transformación completada:")
        logger.info(f"- Chunks procesados: {chunks_processed}")
        logger.info(f"- Filas finales: {total_rows}")
        logger.info(f"- Archivo guardado: {CLEANED_CSV_PATH}")
        
        # Validar resultado
        if total_rows == 0:
            raise ValueError("No se generaron filas válidas después de la transformación")
        
        return str(CLEANED_CSV_PATH)
        
    except Exception as e:
        error_msg = f"Error en transformación: {str(e)}"
        logger.error(error_msg)
        send_alert("Error en transformación de datos", error_msg, logger)
        raise

