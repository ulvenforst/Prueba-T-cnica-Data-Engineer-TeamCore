"""
Script de carga de datos para el pipeline de transacciones
"""
import pandas as pd
import sys
from pathlib import Path
from sqlalchemy import create_engine, text

# Añadir directorio padre para importar config
# Añadir directorios padre para importar config y utils
current_dir = Path(__file__).parent
project_root = current_dir.parent.parent.parent
sys.path.append(str(current_dir.parent.parent))  # Para config
sys.path.append(str(project_root / "shared"))    # Para utils

from config import CLEANED_CSV_PATH, DB_CONNECTION_STRING, TABLE_NAME, LOAD_CHUNK_SIZE
from utils import setup_logger, send_alert

def load_to_sqlite():
    """
    Carga los datos transformados a SQLite
    Procesa en chunks para eficiencia de memoria
    """
    logger = setup_logger(__name__, 'load.log')
    
    try:
        logger.info(f"Iniciando carga desde: {CLEANED_CSV_PATH}")
        
        if not CLEANED_CSV_PATH.exists():
            raise FileNotFoundError(f"Archivo de datos limpios no encontrado: {CLEANED_CSV_PATH}")
        
        # Crear conexión a SQLite
        engine = create_engine(DB_CONNECTION_STRING)
        
        # Leer y cargar en chunks
        chunk_reader = pd.read_csv(CLEANED_CSV_PATH, chunksize=LOAD_CHUNK_SIZE)
        
        total_rows = 0
        chunks_loaded = 0
        
        for chunk in chunk_reader:
            chunks_loaded += 1
            logger.info(f"Cargando chunk {chunks_loaded} con {len(chunk)} filas")
            
            # Cargar chunk a la base de datos
            chunk.to_sql(
                TABLE_NAME, 
                engine, 
                if_exists='append' if chunks_loaded > 1 else 'replace',
                index=False,
                method=None  # Usar método por defecto, más lento pero más compatible
            )
            
            total_rows += len(chunk)
        
        # Crear índices para mejorar performance
        logger.info("Creando índices...")
        with engine.connect() as conn:
            # Índice en user_id para queries de usuario
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_user_id ON {TABLE_NAME}(user_id)"))
            
            # Índice en date para queries temporales
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_date ON {TABLE_NAME}(date)"))
            
            # Índice en status para filtros por estado
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_{TABLE_NAME}_status ON {TABLE_NAME}(status)"))
            
            # Verificar datos cargados
            result = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE_NAME}"))
            db_count = result.scalar()
            
            conn.commit()
        
        logger.info(f"Carga completada:")
        logger.info(f"- Chunks cargados: {chunks_loaded}")
        logger.info(f"- Filas procesadas: {total_rows}")
        logger.info(f"- Filas en BD: {db_count}")
        logger.info(f"- Base de datos: {DB_CONNECTION_STRING}")
        
        # Validar que se cargaron datos
        if db_count == 0:
            raise ValueError("No se cargaron datos a la base de datos")
        
        if db_count != total_rows:
            logger.warning(f"Discrepancia en conteo: procesadas={total_rows}, en_bd={db_count}")
        
        return db_count
        
    except Exception as e:
        error_msg = f"Error en carga: {str(e)}"
        logger.error(error_msg)
        send_alert("Error en carga de datos", error_msg, logger)
        raise

