"""
ETL Processor para manejo de transacciones
"""

import logging
import pandas as pd
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import gzip
import json
from datetime import datetime

logger = logging.getLogger(__name__)


class ETLProcessor:
    """Procesador ETL para archivos de transacciones"""
    
    def __init__(self, data_dir: Path = None):
        self.data_dir = data_dir or Path("data")
        self.raw_dir = self.data_dir / "raw"
        self.processed_dir = self.data_dir / "processed"
        
        # Crear directorios si no existen
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.processed_dir.mkdir(parents=True, exist_ok=True)
    
    def extract(self, file_path: Path) -> pd.DataFrame:
        """
        Extrae datos de archivo CSV o comprimido
        
        Args:
            file_path: Ruta al archivo
            
        Returns:
            DataFrame con los datos extraídos
        """
        logger.info(f"Extrayendo datos de: {file_path}")
        
        if not file_path.exists():
            raise FileNotFoundError(f"Archivo no encontrado: {file_path}")
        
        try:
            if file_path.suffix == '.gz':
                # Manejar archivos comprimidos
                if file_path.name.endswith('.log.gz'):
                    return self._extract_log_gz(file_path)
                else:
                    with gzip.open(file_path, 'rt') as f:
                        df = pd.read_csv(f)
            else:
                df = pd.read_csv(file_path)
            
            logger.info(f"Extraídas {len(df)} filas")
            return df
            
        except Exception as e:
            logger.error(f"Error extrayendo datos: {e}")
            raise
    
    def _extract_log_gz(self, file_path: Path) -> pd.DataFrame:
        """Extrae datos de archivo log comprimido"""
        records = []
        
        with gzip.open(file_path, 'rt') as f:
            for line_num, line in enumerate(f, 1):
                try:
                    # Parsear línea de log (formato JSON esperado)
                    if line.strip():
                        record = json.loads(line.strip())
                        records.append(record)
                        
                        # Procesar en lotes para archivos grandes
                        if line_num % 100000 == 0:
                            logger.info(f"Procesadas {line_num} líneas")
                            
                except json.JSONDecodeError:
                    # Si no es JSON, parsear como log estructurado
                    record = self._parse_log_line(line)
                    if record:
                        records.append(record)
        
        return pd.DataFrame(records)
    
    def _parse_log_line(self, line: str) -> Optional[Dict]:
        """Parsea línea de log en formato estándar"""
        try:
            # Formato esperado: timestamp level message
            parts = line.strip().split(' ', 2)
            if len(parts) >= 3:
                return {
                    'timestamp': parts[0],
                    'level': parts[1],
                    'message': parts[2] if len(parts) > 2 else ''
                }
        except Exception:
            pass
        return None
    
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Transforma y limpia los datos
        
        Args:
            df: DataFrame original
            
        Returns:
            DataFrame transformado
        """
        logger.info(f"Transformando {len(df)} registros")
        
        if len(df) == 0:
            logging.warning("DataFrame vacío recibido para transformación")
            return df
        
        df_clean = df.copy()
        
        # Limpiar y normalizar columnas
        if 'timestamp' in df_clean.columns:
            df_clean['timestamp'] = pd.to_datetime(df_clean['timestamp'], errors='coerce')
        elif 'ts' in df_clean.columns:
            df_clean['timestamp'] = pd.to_datetime(df_clean['ts'], errors='coerce')
            df_clean = df_clean.drop('ts', axis=1)
        
        # Limpiar valores nulos
        df_clean = df_clean.dropna()
        
        # Normalizar nombres de columnas solo si hay columnas
        if len(df_clean.columns) > 0:
            df_clean.columns = df_clean.columns.str.lower().str.strip()
        
        # Validaciones específicas para transacciones
        if 'amount' in df_clean.columns:
            df_clean = df_clean[df_clean['amount'] > 0]
        
        if 'user_id' in df_clean.columns:
            df_clean = df_clean[df_clean['user_id'].notna()]
        
        logger.info(f"Transformación completa: {len(df_clean)} registros válidos")
        return df_clean
    
    def load(self, df: pd.DataFrame, output_path: Path = None) -> Dict:
        """
        Carga datos transformados
        
        Args:
            df: DataFrame a cargar
            output_path: Ruta de salida (opcional)
            
        Returns:
            Dict con resultado de la carga
        """
        if output_path is None:
            output_path = self.processed_dir / "cleaned_transactions.csv"
        
        logger.info(f"Cargando {len(df)} registros a: {output_path}")
        
        try:
            # Guardar como CSV
            df.to_csv(output_path, index=False)
            
            # También guardar en SQLite para queries
            db_path = self.processed_dir / "transactions.db"
            with sqlite3.connect(db_path) as conn:
                df.to_sql('transactions', conn, if_exists='replace', index=False)
            
            result = {
                'status': 'success',
                'records': len(df),
                'output_file': str(output_path),
                'database': str(db_path)
            }
            
            logger.info(f"Carga exitosa: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error en carga: {e}")
            raise
    
    def process_file(self, input_path: Path, output_path: Path = None) -> Dict:
        """
        Procesa archivo completo ETL
        
        Args:
            input_path: Archivo de entrada
            output_path: Archivo de salida (opcional)
            
        Returns:
            Dict con métricas del procesamiento
        """
        start_time = datetime.now()
        logger.info(f"Iniciando procesamiento ETL: {input_path}")
        
        try:
            # Extract
            df_raw = self.extract(input_path)
            
            # Transform
            df_clean = self.transform(df_raw)
            
            # Load
            result = self.load(df_clean, output_path)
            
            # Métricas
            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()
            
            metrics = {
                'input_file': str(input_path),
                'output_file': result['output_file'],
                'records_input': len(df_raw),
                'records_output': len(df_clean),
                'processing_time_seconds': processing_time,
                'compression_ratio': len(df_clean) / len(df_raw) if len(df_raw) > 0 else 0,
                'status': 'success'
            }
            
            logger.info(f"ETL completado: {metrics}")
            return metrics
            
        except Exception as e:
            logger.error(f"Error en procesamiento ETL: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'input_file': str(input_path)
            }


def main():
    """Función principal para testing"""
    logging.basicConfig(level=logging.INFO)
    
    processor = ETLProcessor()
    
    # Buscar archivo de ejemplo
    sample_file = processor.raw_dir / "sample_transactions.csv"
    if sample_file.exists():
        result = processor.process_file(sample_file)
        print(f"Resultado: {result}")
    else:
        print(f"Archivo de ejemplo no encontrado: {sample_file}")


if __name__ == "__main__":
    main()
