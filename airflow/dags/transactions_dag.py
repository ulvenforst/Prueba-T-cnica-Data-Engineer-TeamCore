"""
DAG principal para procesamiento de transacciones
Ejercicio 1: Orquestación local end-to-end
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.dates import days_ago
import pandas as pd
import sqlite3
from pathlib import Path
import logging

# Configuración del DAG
default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'transactions_etl_pipeline',
    default_args=default_args,
    description='Pipeline ETL para transacciones - Ejercicio 1',
    schedule_interval=timedelta(hours=1),
    catchup=False,
    tags=['etl', 'transactions', 'ejercicio1'],
)

# Configuración de rutas
DATA_DIR = Path('/opt/airflow/data')
RAW_DIR = DATA_DIR / 'raw'
PROCESSED_DIR = DATA_DIR / 'processed'
DB_PATH = PROCESSED_DIR / 'transactions.db'

def check_file_size(**context):
    """
    Sensor personalizado: verificar que el archivo tenga tamaño mínimo
    Requisito: sensores (espera de archivo, tamaño mínimo)
    """
    file_path = RAW_DIR / 'sample_transactions.csv'
    
    if not file_path.exists():
        raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
    
    file_size = file_path.stat().st_size
    min_size = 1024 * 1024  # 1MB mínimo
    
    if file_size < min_size:
        raise ValueError(f"Archivo muy pequeño: {file_size} bytes < {min_size} bytes")
    
    logging.info(f"✅ Archivo válido: {file_size} bytes")
    return file_path

def extract_data(**context):
    """
    Extrae datos del CSV con lectura por chunks
    Requisitos: lectura por chunks para eficiencia de memoria
    """
    file_path = context['task_instance'].xcom_pull(task_ids='check_file_size')
    
    logging.info(f"📥 Extrayendo datos de: {file_path}")
    
    # Lectura por chunks para archivos grandes
    chunk_size = 10000
    chunks = []
    total_rows = 0
    
    try:
        for chunk in pd.read_csv(file_path, chunksize=chunk_size):
            chunks.append(chunk)
            total_rows += len(chunk)
            
            if total_rows % 100000 == 0:
                logging.info(f"📊 Procesadas {total_rows} filas...")
        
        df = pd.concat(chunks, ignore_index=True)
        
        # Métricas de extracción
        metrics = {
            'rows_extracted': len(df),
            'columns': list(df.columns),
            'file_size_mb': file_path.stat().st_size / (1024*1024),
            'extraction_time': datetime.now().isoformat()
        }
        
        logging.info(f"✅ Extracción completada: {metrics}")
        
        # Guardar datos temporalmente
        temp_path = PROCESSED_DIR / 'temp_extracted.csv'
        df.to_csv(temp_path, index=False)
        
        return {
            'temp_file': str(temp_path),
            'metrics': metrics
        }
        
    except Exception as e:
        logging.error(f"❌ Error en extracción: {e}")
        raise

def transform_data(**context):
    """
    Transforma y limpia los datos
    Requisitos: transforme datos con Python
    """
    extract_result = context['task_instance'].xcom_pull(task_ids='extract_data')
    temp_file = extract_result['temp_file']
    
    logging.info(f"🔄 Transformando datos de: {temp_file}")
    
    try:
        df = pd.read_csv(temp_file)
        
        # Transformaciones
        initial_count = len(df)
        
        # Limpiar datos nulos
        df = df.dropna()
        
        # Validar tipos de datos
        if 'amount' in df.columns:
            df = df[df['amount'] > 0]
        
        if 'user_id' in df.columns:
            df = df[df['user_id'].notna()]
        
        # Normalizar timestamp
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
            df = df[df['timestamp'].notna()]
        
        # Agregar columnas calculadas
        df['processed_at'] = datetime.now()
        df['data_quality_score'] = 1.0  # Simplificado
        
        final_count = len(df)
        
        # Métricas de transformación
        metrics = {
            'rows_input': initial_count,
            'rows_output': final_count,
            'data_quality_ratio': final_count / initial_count if initial_count > 0 else 0,
            'transformation_time': datetime.now().isoformat()
        }
        
        logging.info(f"✅ Transformación completada: {metrics}")
        
        # Guardar datos transformados
        transformed_path = PROCESSED_DIR / 'transformed_transactions.csv'
        df.to_csv(transformed_path, index=False)
        
        return {
            'transformed_file': str(transformed_path),
            'metrics': metrics
        }
        
    except Exception as e:
        logging.error(f"❌ Error en transformación: {e}")
        raise

def load_to_database(**context):
    """
    Carga datos a SQLite/PostgreSQL
    Requisitos: cargue a SQLite/PostgreSQL
    """
    transform_result = context['task_instance'].xcom_pull(task_ids='transform_data')
    transformed_file = transform_result['transformed_file']
    
    logging.info(f"💾 Cargando datos a base: {DB_PATH}")
    
    try:
        df = pd.read_csv(transformed_file)
        
        # Crear directorio si no existe
        DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        
        # Cargar a SQLite
        with sqlite3.connect(DB_PATH) as conn:
            df.to_sql('transactions', conn, if_exists='replace', index=False)
            
            # Verificar carga
            cursor = conn.execute("SELECT COUNT(*) FROM transactions")
            row_count = cursor.fetchone()[0]
            
            if row_count == 0:
                raise ValueError("❌ Tabla destino está vacía - ALERTA GENERADA")
            
            # Métricas de carga
            metrics = {
                'rows_loaded': row_count,
                'table_name': 'transactions',
                'database_size_mb': DB_PATH.stat().st_size / (1024*1024) if DB_PATH.exists() else 0,
                'load_time': datetime.now().isoformat()
            }
            
            logging.info(f"✅ Carga completada: {metrics}")
            
            return metrics
            
    except Exception as e:
        logging.error(f"❌ Error en carga: {e}")
        # Generar alerta simulada
        logging.error("🚨 ALERTA: Error en pipeline - notificación enviada")
        raise

def validate_data_quality(**context):
    """
    Validar que la tabla destino no esté vacía
    Requisitos: Validar que la tabla destino no esté vacía; si lo está o hay error, generar alerta
    """
    logging.info(f"🔍 Validando calidad de datos en: {DB_PATH}")
    
    try:
        with sqlite3.connect(DB_PATH) as conn:
            # Verificar que la tabla existe y tiene datos
            cursor = conn.execute("SELECT COUNT(*) FROM transactions")
            row_count = cursor.fetchone()[0]
            
            if row_count == 0:
                raise ValueError("❌ VALIDACIÓN FALLIDA: Tabla vacía")
            
            # Validaciones adicionales
            cursor = conn.execute("""
                SELECT 
                    COUNT(*) as total_rows,
                    COUNT(DISTINCT user_id) as unique_users,
                    MIN(amount) as min_amount,
                    MAX(amount) as max_amount,
                    COUNT(CASE WHEN amount <= 0 THEN 1 END) as invalid_amounts
                FROM transactions
            """)
            
            stats = cursor.fetchone()
            
            validation_result = {
                'total_rows': stats[0],
                'unique_users': stats[1], 
                'min_amount': stats[2],
                'max_amount': stats[3],
                'invalid_amounts': stats[4],
                'validation_time': datetime.now().isoformat(),
                'status': 'PASSED'
            }
            
            # Verificar reglas de negocio
            if stats[4] > 0:  # Montos inválidos
                validation_result['status'] = 'WARNING'
                logging.warning(f"⚠️  Encontrados {stats[4]} montos inválidos")
            
            if stats[1] < 10:  # Muy pocos usuarios únicos
                validation_result['status'] = 'WARNING'
                logging.warning(f"⚠️  Solo {stats[1]} usuarios únicos encontrados")
            
            logging.info(f"✅ Validación completada: {validation_result}")
            
            return validation_result
            
    except Exception as e:
        logging.error(f"❌ Error en validación: {e}")
        # Generar alerta (simulada)
        logging.error("🚨 ALERTA: Validación fallida - notificación enviada")
        raise

# Definir tareas del DAG

# Sensor de archivo
file_sensor = FileSensor(
    task_id='wait_for_file',
    filepath=str(RAW_DIR / 'sample_transactions.csv'),
    fs_conn_id='fs_default',
    poke_interval=30,
    timeout=300,
    dag=dag,
)

# Verificar tamaño de archivo
check_size = PythonOperator(
    task_id='check_file_size',
    python_callable=check_file_size,
    dag=dag,
)

# Extracción
extract = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    dag=dag,
)

# Transformación
transform = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    dag=dag,
)

# Carga
load = PythonOperator(
    task_id='load_to_database',
    python_callable=load_to_database,
    dag=dag,
)

# Validación
validate = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

# Cleanup temporal
cleanup = BashOperator(
    task_id='cleanup_temp_files',
    bash_command=f'rm -f {PROCESSED_DIR}/temp_*.csv',
    dag=dag,
)

# Definir dependencias
file_sensor >> check_size >> extract >> transform >> load >> validate >> cleanup
