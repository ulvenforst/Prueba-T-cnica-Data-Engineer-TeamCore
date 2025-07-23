"""
Suite de tests ampliada para validación completa del sistema
Tests unitarios e integración para todos los componentes
"""

import pytest
import pandas as pd
import sqlite3
import json
import gzip
from pathlib import Path
import tempfile
import os
from datetime import datetime

def test_transaction_generator():
    """Test del generador de transacciones"""
    from scripts.generate_transactions import generate_transactions
    
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)
    
    try:
        stats = generate_transactions(1000, tmp_path)
        
        assert stats['records_generated'] == 1000
        assert tmp_path.exists()
        assert tmp_path.stat().st_size > 0
        
        # Verificar contenido
        df = pd.read_csv(tmp_path)
        assert len(df) == 1000
        assert all(col in df.columns for col in ['order_id', 'user_id', 'amount', 'status', 'timestamp'])
        assert df['amount'].min() > 0
        assert df['status'].isin(['completed', 'failed', 'pending']).all()
        
        print("Test generador de transacciones: PASSED")
        
    finally:
        if tmp_path.exists():
            os.unlink(tmp_path)

def test_log_generator():
    """Test del generador de logs"""
    from scripts.generate_logs import generate_logs
    
    with tempfile.NamedTemporaryFile(suffix='.log.gz', delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)
    
    try:
        stats = generate_logs(1000, tmp_path)
        
        assert stats['records_generated'] == 1000
        assert tmp_path.exists()
        assert tmp_path.stat().st_size > 0
        
        # Verificar contenido comprimido
        with gzip.open(tmp_path, 'rt') as f:
            lines = f.readlines()
            assert len(lines) == 1000
            
            # Verificar formato JSON
            first_log = json.loads(lines[0])
            required_fields = ['timestamp', 'level', 'status_code', 'endpoint']
            assert all(field in first_log for field in required_fields)
        
        print("Test generador de logs: PASSED")
        
    finally:
        if tmp_path.exists():
            os.unlink(tmp_path)

def test_etl_processor():
    """Test completo del procesador ETL"""
    from etl.processor import ETLProcessor
    
    processor = ETLProcessor()
    
    # Crear datos de prueba
    test_data = pd.DataFrame({
        'order_id': [1, 2, 3, 4, 5],
        'user_id': [1, 2, 3, 1, 2],
        'amount': [100.0, 200.0, 300.0, -50.0, 0.0],  # Incluir datos inválidos
        'status': ['completed', 'failed', 'pending', 'completed', 'failed'],
        'timestamp': [
            '2025-01-01 12:00:00', 
            '2025-01-02 13:00:00', 
            '2025-01-03 14:00:00',
            'invalid_date',
            '2025-01-05 16:00:00'
        ]
    })
    
    # Test transform (debe filtrar datos inválidos)
    result = processor.transform(test_data)
    
    # Verificar que se filtraron los datos inválidos
    assert len(result) == 3  # Solo registros válidos
    assert 'timestamp' in result.columns
    assert result['amount'].min() > 0
    assert result['timestamp'].notna().all()
    
    print("Test ETL processor: PASSED")

def test_warehouse_operations():
    """Test completo de operaciones del data warehouse"""
    from modeling.warehouse import DataWarehouse
    
    # Usar base temporal
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_file:
        db_path = Path(tmp_file.name)
    
    try:
        warehouse = DataWarehouse(db_path)
        
        # Test inicialización de schema
        result = warehouse.initialize_schema()
        assert result['status'] == 'success'
        
        # Verificar que se crearon las tablas
        with sqlite3.connect(db_path) as conn:
            cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row[0] for row in cursor.fetchall()]
            expected_tables = ['dim_user', 'dim_time', 'dim_status', 'fact_transactions']
            assert all(table in tables for table in expected_tables)
        
        # Test carga de datos
        test_df = pd.DataFrame({
            'order_id': [1, 2, 3],
            'user_id': [1, 2, 3],
            'amount': [100.0, 200.0, 300.0],
            'status': ['completed', 'failed', 'pending'],
            'timestamp': ['2025-01-01 12:00:00', '2025-01-02 13:00:00', '2025-01-03 14:00:00']
        })
        
        load_result = warehouse.load_transactions(test_df)
        assert load_result['status'] == 'success'
        assert load_result['facts_loaded'] == 3
        
        # Test estadísticas
        stats = warehouse.get_summary_stats()
        assert stats['status'] == 'success'
        assert stats['stats']['fact_transactions_count'] == 3
        
        print("Test warehouse operations: PASSED")
        
    finally:
        if db_path.exists():
            try:
                os.unlink(db_path)
            except PermissionError:
                pass  # Windows file lock issue

def test_sql_analysis():
    """Test del motor de análisis SQL"""
    from sql.analysis_runner import SQLAnalysis
    
    # Crear base de datos temporal con datos
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_file:
        db_path = Path(tmp_file.name)
    
    try:
        # Crear datos de prueba
        test_data = pd.DataFrame({
            'order_id': range(1, 101),
            'user_id': [i % 10 + 1 for i in range(100)],
            'amount': [100.0 + i * 10 for i in range(100)],
            'status': ['completed' if i % 3 != 0 else 'failed' for i in range(100)],
            'timestamp': [f'2025-01-{(i % 28) + 1:02d} 12:00:00' for i in range(100)]
        })
        
        # Cargar a SQLite
        with sqlite3.connect(db_path) as conn:
            test_data.to_sql('transactions', conn, if_exists='replace', index=False)
        
        # Test análisis
        analyzer = SQLAnalysis(db_path)
        
        # Test info de tablas
        info = analyzer.get_table_info()
        assert info is not None
        assert 'tables' in info
        assert 'transactions' in info['tables']
        
        print("Test SQL analysis: PASSED")
        
    finally:
        if db_path.exists():
            try:
                os.unlink(db_path)
            except PermissionError:
                pass

def test_integration_pipeline():
    """Test de integración del pipeline completo"""
    from etl.processor import ETLProcessor
    from modeling.warehouse import DataWarehouse
    
    # Crear datos de prueba
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp_csv:
        csv_path = Path(tmp_csv.name)
    
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_db:
        db_path = Path(tmp_db.name)
    
    try:
        # Generar datos de prueba
        test_data = pd.DataFrame({
            'order_id': range(1, 101),
            'user_id': [i % 20 + 1 for i in range(100)],
            'amount': [50.0 + i * 5 for i in range(100)],
            'status': [['completed', 'failed', 'pending'][i % 3] for i in range(100)],
            'timestamp': [f'2025-01-{(i % 28) + 1:02d} {(i % 24):02d}:00:00' for i in range(100)]
        })
        test_data.to_csv(csv_path, index=False)
        
        # Test ETL
        processor = ETLProcessor()
        etl_result = processor.process_file(csv_path)
        assert etl_result['status'] == 'success'
        assert etl_result['records_output'] > 0
        
        # Test Warehouse
        warehouse = DataWarehouse(db_path)
        init_result = warehouse.initialize_schema()
        assert init_result['status'] == 'success'
        
        # Cargar datos procesados
        processed_data = pd.read_csv(etl_result['output_file'])
        load_result = warehouse.load_transactions(processed_data)
        assert load_result['status'] == 'success'
        
        # Verificar estadísticas finales
        stats = warehouse.get_summary_stats()
        assert stats['status'] == 'success'
        assert stats['stats']['fact_transactions_count'] > 0
        
        print("Test integración pipeline: PASSED")
        
    finally:
        for path in [csv_path, db_path]:
            if path.exists():
                try:
                    os.unlink(path)
                except PermissionError:
                    pass

def test_performance_benchmark():
    """Test de rendimiento con dataset moderado"""
    from scripts.generate_transactions import generate_transactions
    from etl.processor import ETLProcessor
    
    with tempfile.NamedTemporaryFile(suffix='.csv', delete=False) as tmp_file:
        tmp_path = Path(tmp_file.name)
    
    try:
        # Generar 10K registros para test de rendimiento
        start_time = datetime.now()
        stats = generate_transactions(10000, tmp_path)
        generation_time = (datetime.now() - start_time).total_seconds()
        
        # Verificar que la generación fue eficiente
        assert stats['records_generated'] == 10000
        assert generation_time < 10  # Menos de 10 segundos
        
        # Test procesamiento ETL
        processor = ETLProcessor()
        start_time = datetime.now()
        result = processor.process_file(tmp_path)
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Verificar eficiencia del procesamiento
        assert result['status'] == 'success'
        assert processing_time < 5  # Menos de 5 segundos para 10K registros
        
        throughput = result['records_output'] / processing_time
        assert throughput > 1000  # Más de 1K registros/segundo
        
        print(f"Test performance: PASSED - {throughput:.0f} registros/segundo")
        
    finally:
        if tmp_path.exists():
            try:
                os.unlink(tmp_path)
            except PermissionError:
                pass

def test_error_handling():
    """Test manejo de errores y casos edge"""
    from etl.processor import ETLProcessor
    
    processor = ETLProcessor()
    
    # Test con archivo inexistente
    with pytest.raises(FileNotFoundError):
        processor.extract(Path("archivo_inexistente.csv"))
    
    # Test con datos vacíos
    empty_df = pd.DataFrame()
    result = processor.transform(empty_df)
    assert len(result) == 0
    
    # Test con datos completamente inválidos
    invalid_df = pd.DataFrame({
        'order_id': [None, None],
        'user_id': [None, None],
        'amount': [-100, None],
        'status': [None, 'invalid'],
        'timestamp': ['invalid', None]
    })
    
    result = processor.transform(invalid_df)
    assert len(result) == 0  # Todos los registros deben ser filtrados
    
    print("Test error handling: PASSED")

if __name__ == "__main__":
    print("Ejecutando suite de tests completa...")
    
    test_transaction_generator()
    test_log_generator()
    test_etl_processor()
    test_warehouse_operations()
    test_sql_analysis()
    test_integration_pipeline()
    test_performance_benchmark()
    test_error_handling()
    
    print("\nTodos los tests completados exitosamente!")
