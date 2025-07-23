"""
Tests para el pipeline ETL de transacciones
"""
import unittest
import pandas as pd
import tempfile
import sys
from pathlib import Path
from sqlalchemy import create_engine, text
import os

# Añadir directorio padre para importar módulos
sys.path.append(str(Path(__file__).parent.parent.parent))

from airflow.scripts.transform import transform_data
from airflow.scripts.load import load_to_sqlite
from utils import validate_file_size, validate_table_not_empty, setup_logger

class TestETLPipeline(unittest.TestCase):
    
    def setUp(self):
        """Configurar tests con datos temporales"""
        self.test_dir = Path(tempfile.mkdtemp())
        self.test_csv = self.test_dir / "test_transactions.csv"
        self.test_cleaned_csv = self.test_dir / "test_cleaned.csv"
        self.test_db = self.test_dir / "test.db"
        
        # Crear datos de prueba
        test_data = {
            'order_id': [1, 2, 3, 4, 5],
            'user_id': [100, 200, 300, 400, 500],
            'amount': [50.0, 100.0, -10.0, 200.0, 'invalid'],  # Incluir datos inválidos
            'ts': ['2025-01-01T10:00:00', '2025-01-01T11:00:00', '2025-01-01T12:00:00', 
                   '2025-01-01T13:00:00', '2025-01-01T14:00:00'],
            'status': ['completed', 'failed', 'completed', 'pending', 'completed']
        }
        
        df = pd.DataFrame(test_data)
        df.to_csv(self.test_csv, index=False)
    
    def tearDown(self):
        """Limpiar archivos temporales"""
        import shutil
        shutil.rmtree(self.test_dir)
    
    def test_file_size_validation(self):
        """Test validación de tamaño de archivo"""
        # Archivo válido
        self.assertTrue(validate_file_size(self.test_csv, 0.001))  # 1KB mínimo
        
        # Archivo muy pequeño
        empty_file = self.test_dir / "empty.csv"
        empty_file.touch()
        self.assertFalse(validate_file_size(empty_file, 1))  # 1MB mínimo
    
    def test_data_transformation(self):
        """Test básico de transformación de datos"""
        # Mockear configuración para test
        import config
        original_csv = config.CSV_FILE_PATH
        original_cleaned = config.CLEANED_CSV_PATH
        
        try:
            config.CSV_FILE_PATH = self.test_csv
            config.CLEANED_CSV_PATH = self.test_cleaned_csv
            
            # No debería fallar con datos de prueba válidos
            result = transform_data()
            self.assertTrue(self.test_cleaned_csv.exists())
            
            # Verificar que se limpiaron datos inválidos
            cleaned_df = pd.read_csv(self.test_cleaned_csv)
            self.assertTrue(len(cleaned_df) < 5)  # Debería filtrar filas inválidas
            self.assertTrue(all(cleaned_df['amount'] > 0))  # Solo montos positivos
            
        finally:
            # Restaurar configuración
            config.CSV_FILE_PATH = original_csv
            config.CLEANED_CSV_PATH = original_cleaned
    
    def test_database_loading(self):
        """Test básico de carga a base de datos"""
        # Crear archivo limpio para cargar
        clean_data = {
            'order_id': [1, 2, 3],
            'user_id': [100, 200, 300],
            'amount': [50.0, 100.0, 200.0],
            'ts': ['2025-01-01T10:00:00', '2025-01-01T11:00:00', '2025-01-01T12:00:00'],
            'status': ['completed', 'failed', 'pending'],
            'processed_at': ['2025-01-01T15:00:00', '2025-01-01T15:00:00', '2025-01-01T15:00:00'],
            'date': ['2025-01-01', '2025-01-01', '2025-01-01'],
            'hour': [10, 11, 12]
        }
        
        df = pd.DataFrame(clean_data)
        df.to_csv(self.test_cleaned_csv, index=False)
        
        # Mockear configuración
        import config
        original_cleaned = config.CLEANED_CSV_PATH
        original_db = config.DB_CONNECTION_STRING
        original_table = config.TABLE_NAME
        
        try:
            config.CLEANED_CSV_PATH = self.test_cleaned_csv
            config.DB_CONNECTION_STRING = f"sqlite:///{self.test_db}"
            config.TABLE_NAME = "test_transactions"
            
            # Cargar datos
            rows_loaded = load_to_sqlite()
            self.assertEqual(rows_loaded, 3)
            
            # Verificar que los datos están en la BD
            engine = create_engine(config.DB_CONNECTION_STRING)
            self.assertTrue(validate_table_not_empty(engine, config.TABLE_NAME))
            
        finally:
            # Restaurar configuración
            config.CLEANED_CSV_PATH = original_cleaned
            config.DB_CONNECTION_STRING = original_db
            config.TABLE_NAME = original_table
    
    def test_logger_setup(self):
        """Test configuración de logger"""
        logger = setup_logger("test_logger")
        self.assertIsNotNone(logger)
        self.assertEqual(logger.name, "test_logger")

if __name__ == '__main__':
    unittest.main()