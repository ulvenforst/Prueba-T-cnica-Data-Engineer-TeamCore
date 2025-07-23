"""
Scripts modulares para el pipeline ETL
Ejercicio 1: Modularizar el pipeline en scripts/paquetes reutilizables
"""

import pandas as pd
import sqlite3
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, Any

logger = logging.getLogger(__name__)

def extract_csv(file_path: Path, chunk_size: int = 10000) -> Dict[str, Any]:
    """
    Extrae datos de CSV con lectura por chunks
    
    Args:
        file_path: Ruta al archivo CSV
        chunk_size: Tamaño de chunk para lectura
    
    Returns:
        Dict con datos y métricas
    """
    logger.info(f"📥 Extrayendo datos de: {file_path}")
    
    if not file_path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
    
    # Verificar tamaño mínimo
    file_size = file_path.stat().st_size
    min_size = 1024 * 100  # 100KB mínimo
    
    if file_size < min_size:
        raise ValueError(f"Archivo muy pequeño: {file_size} bytes")
    
    chunks = []
    total_rows = 0
    
    try:
        # Lectura por chunks para eficiencia de memoria
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            chunks.append(chunk)
            total_rows += len(chunk)
            
            if total_rows % 50000 == 0:
                logger.info(f"📊 Procesadas {total_rows} filas...")
        
        df = pd.concat(chunks, ignore_index=True)
        
        metrics = {
            'rows_extracted': len(df),
            'columns': list(df.columns),
            'file_size_mb': file_size / (1024*1024),
            'extraction_time': datetime.now().isoformat(),
            'status': 'success'
        }
        
        logger.info(f"✅ Extracción completada: {metrics}")
        
        return {
            'data': df,
            'metrics': metrics
        }
        
    except Exception as e:
        logger.error(f"❌ Error en extracción: {e}")
        raise

def transform_transactions(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Transforma y limpia datos de transacciones
    
    Args:
        df: DataFrame con datos raw
    
    Returns:
        Dict con datos transformados y métricas
    """
    logger.info(f"🔄 Transformando {len(df)} registros")
    
    initial_count = len(df)
    df_clean = df.copy()
    
    try:
        # Limpiar datos nulos
        df_clean = df_clean.dropna()
        
        # Validar montos positivos
        if 'amount' in df_clean.columns:
            df_clean = df_clean[df_clean['amount'] > 0]
        
        # Validar user_id
        if 'user_id' in df_clean.columns:
            df_clean = df_clean[df_clean['user_id'].notna()]
        
        # Normalizar timestamp
        if 'timestamp' in df_clean.columns:
            df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'], errors='coerce')
            df_clean = df_clean[df_clean['timestamp'].notna()]
        
        # Agregar metadatos
        df_clean['processed_at'] = datetime.now()
        df_clean['data_quality_score'] = 1.0
        
        final_count = len(df_clean)
        
        metrics = {
            'rows_input': initial_count,
            'rows_output': final_count,
            'data_quality_ratio': final_count / initial_count if initial_count > 0 else 0,
            'transformation_time': datetime.now().isoformat(),
            'status': 'success'
        }
        
        logger.info(f"✅ Transformación completada: {metrics}")
        
        return {
            'data': df_clean,
            'metrics': metrics
        }
        
    except Exception as e:
        logger.error(f"❌ Error en transformación: {e}")
        raise

def load_to_sqlite(df: pd.DataFrame, db_path: Path, table_name: str = 'transactions') -> Dict[str, Any]:
    """
    Carga datos a SQLite
    
    Args:
        df: DataFrame a cargar
        db_path: Ruta a la base de datos
        table_name: Nombre de la tabla
    
    Returns:
        Dict con métricas de carga
    """
    logger.info(f"💾 Cargando {len(df)} registros a: {db_path}")
    
    try:
        # Crear directorio si no existe
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Cargar a SQLite
        with sqlite3.connect(db_path) as conn:
            df.to_sql(table_name, conn, if_exists='replace', index=False)
            
            # Verificar carga
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            if row_count == 0:
                raise ValueError("❌ Tabla destino está vacía")
            
            if row_count != len(df):
                logger.warning(f"⚠️  Discrepancia en conteo: esperado {len(df)}, cargado {row_count}")
            
            metrics = {
                'rows_loaded': row_count,
                'table_name': table_name,
                'database_path': str(db_path),
                'database_size_mb': db_path.stat().st_size / (1024*1024) if db_path.exists() else 0,
                'load_time': datetime.now().isoformat(),
                'status': 'success'
            }
            
            logger.info(f"✅ Carga completada: {metrics}")
            return metrics
            
    except Exception as e:
        logger.error(f"❌ Error en carga: {e}")
        raise

def validate_data_quality(db_path: Path, table_name: str = 'transactions') -> Dict[str, Any]:
    """
    Valida calidad de datos en la tabla
    
    Args:
        db_path: Ruta a la base de datos
        table_name: Nombre de la tabla
    
    Returns:
        Dict con resultados de validación
    """
    logger.info(f"🔍 Validando calidad de datos en: {db_path}")
    
    try:
        with sqlite3.connect(db_path) as conn:
            # Verificar existencia y contenido
            cursor = conn.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            
            if row_count == 0:
                raise ValueError("❌ VALIDACIÓN FALLIDA: Tabla vacía")
            
            # Estadísticas detalladas
            cursor = conn.execute(f"""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT user_id) as unique_users,
                    MIN(amount) as min_amount,
                    MAX(amount) as max_amount,
                    AVG(amount) as avg_amount,
                    COUNT(CASE WHEN amount <= 0 THEN 1 END) as invalid_amounts,
                    COUNT(CASE WHEN user_id IS NULL THEN 1 END) as null_users
                FROM {table_name}
            """)
            
            stats = cursor.fetchone()
            
            validation_result = {
                'total_rows': stats[0],
                'unique_users': stats[1],
                'min_amount': stats[2],
                'max_amount': stats[3],
                'avg_amount': stats[4],
                'invalid_amounts': stats[5],
                'null_users': stats[6],
                'validation_time': datetime.now().isoformat(),
                'status': 'PASSED'
            }
            
            # Aplicar reglas de validación
            warnings = []
            
            if stats[5] > 0:  # Montos inválidos
                warnings.append(f"Encontrados {stats[5]} montos inválidos")
            
            if stats[6] > 0:  # Usuarios nulos
                warnings.append(f"Encontrados {stats[6]} usuarios nulos")
            
            if stats[1] < 10:  # Pocos usuarios únicos
                warnings.append(f"Solo {stats[1]} usuarios únicos")
            
            if warnings:
                validation_result['status'] = 'WARNING'
                validation_result['warnings'] = warnings
                for warning in warnings:
                    logger.warning(f"⚠️  {warning}")
            
            logger.info(f"✅ Validación completada: {validation_result}")
            return validation_result
            
    except Exception as e:
        logger.error(f"❌ Error en validación: {e}")
        # Simular alerta
        logger.error("🚨 ALERTA: Validación fallida - notificación enviada")
        raise

def run_etl_pipeline(input_file: Path, output_db: Path) -> Dict[str, Any]:
    """
    Ejecuta pipeline ETL completo
    
    Args:
        input_file: Archivo CSV de entrada
        output_db: Base de datos de salida
    
    Returns:
        Dict con métricas completas del pipeline
    """
    logger.info(f"🚀 Iniciando pipeline ETL: {input_file} -> {output_db}")
    
    pipeline_start = datetime.now()
    pipeline_metrics = {}
    
    try:
        # Extracción
        extract_result = extract_csv(input_file)
        pipeline_metrics['extract'] = extract_result['metrics']
        
        # Transformación
        transform_result = transform_transactions(extract_result['data'])
        pipeline_metrics['transform'] = transform_result['metrics']
        
        # Carga
        load_metrics = load_to_sqlite(transform_result['data'], output_db)
        pipeline_metrics['load'] = load_metrics
        
        # Validación
        validation_metrics = validate_data_quality(output_db)
        pipeline_metrics['validation'] = validation_metrics
        
        # Métricas finales
        pipeline_end = datetime.now()
        pipeline_duration = (pipeline_end - pipeline_start).total_seconds()
        
        pipeline_metrics['summary'] = {
            'status': 'SUCCESS',
            'duration_seconds': pipeline_duration,
            'start_time': pipeline_start.isoformat(),
            'end_time': pipeline_end.isoformat(),
            'total_rows_processed': load_metrics['rows_loaded']
        }
        
        logger.info(f"🎉 Pipeline ETL completado exitosamente en {pipeline_duration:.2f}s")
        
        return pipeline_metrics
        
    except Exception as e:
        pipeline_end = datetime.now()
        pipeline_duration = (pipeline_end - pipeline_start).total_seconds()
        
        pipeline_metrics['summary'] = {
            'status': 'FAILED',
            'error': str(e),
            'duration_seconds': pipeline_duration,
            'start_time': pipeline_start.isoformat(),
            'end_time': pipeline_end.isoformat()
        }
        
        logger.error(f"❌ Pipeline ETL falló después de {pipeline_duration:.2f}s: {e}")
        raise

if __name__ == "__main__":
    # Configurar logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    # Ejecutar pipeline de ejemplo
    input_path = Path("../../data/raw/sample_transactions.csv")
    output_path = Path("../../data/processed/transactions.db")
    
    if input_path.exists():
        metrics = run_etl_pipeline(input_path, output_path)
        print(f"Pipeline completado: {metrics['summary']}")
    else:
        print(f"Archivo de entrada no encontrado: {input_path}")
