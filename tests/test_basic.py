"""
Tests básicos para verificar funcionamiento
"""

import pytest
import pandas as pd
from pathlib import Path
import tempfile
import os

# Test ETL
def test_etl_processor():
    """Test básico del procesador ETL"""
    from etl.processor import ETLProcessor
    
    processor = ETLProcessor()
    
    # Crear datos de prueba
    test_data = pd.DataFrame({
        'order_id': [1, 2, 3],
        'user_id': [1, 2, 3],
        'amount': [100.0, 200.0, 300.0],
        'status': ['completed', 'failed', 'pending'],
        'timestamp': ['2025-01-01 12:00:00', '2025-01-02 13:00:00', '2025-01-03 14:00:00']
    })
    
    # Test transform
    result = processor.transform(test_data)
    assert len(result) == 3
    assert 'timestamp' in result.columns
    
    print("✅ Test ETL pasado")


def test_warehouse():
    """Test básico del warehouse"""
    from modeling.warehouse import DataWarehouse
    
    # Usar base temporal
    with tempfile.NamedTemporaryFile(suffix='.db', delete=False) as tmp_file:
        db_path = Path(tmp_file.name)
    
    try:
        warehouse = DataWarehouse(db_path)
        
        # Test inicialización
        result = warehouse.initialize_schema()
        assert result['status'] == 'success'
        
        # Test estadísticas
        stats = warehouse.get_summary_stats()
        assert stats['status'] == 'success'
        
        print("✅ Test Warehouse pasado")
        
    finally:
        if db_path.exists():
            os.unlink(db_path)


def test_sql_analysis():
    """Test básico del análisis SQL"""
    from sql.analysis_runner import SQLAnalysis
    
    # Verificar que existe la base de datos
    db_path = Path("data/processed/transactions.db")
    if db_path.exists():
        analyzer = SQLAnalysis(db_path)
        
        # Test info de tablas
        info = analyzer.get_table_info()
        assert info is not None
        assert 'tables' in info
        
        print("✅ Test SQL Analysis pasado")
    else:
        print("⚠️  Base de datos no encontrada, saltando test SQL")


def test_integration():
    """Test de integración simple"""
    # Verificar archivos principales
    files_to_check = [
        "main.py",
        "etl/processor.py",
        "modeling/warehouse.py",
        "sql/analysis_runner.py"
    ]
    
    for file_path in files_to_check:
        assert Path(file_path).exists(), f"Archivo faltante: {file_path}"
    
    print("✅ Test de integración pasado")


if __name__ == "__main__":
    print("🧪 Ejecutando tests básicos...")
    
    test_etl_processor()
    test_warehouse()
    test_sql_analysis()
    test_integration()
    
    print("\n🎉 Todos los tests básicos pasaron!")
