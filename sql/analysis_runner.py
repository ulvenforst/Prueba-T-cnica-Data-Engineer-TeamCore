"""
Análisis SQL Runner
"""

import sqlite3
import logging
from pathlib import Path
import pandas as pd

logger = logging.getLogger(__name__)


class SQLAnalysis:
    """Runner para análisis SQL"""
    
    def __init__(self, db_path: Path = None):
        self.db_path = db_path or Path("data/processed/transactions.db")
        self.queries_dir = Path("sql/queries")
    
    def run_analysis(self, query_name: str = None):
        """Ejecuta análisis SQL específico o todos"""
        logger.info(f"Ejecutando análisis SQL: {query_name or 'todos'}")
        
        if not self.db_path.exists():
            logger.error(f"Base de datos no encontrada: {self.db_path}")
            return None
        
        results = {}
        
        if query_name:
            results[query_name] = self._run_single_query(query_name)
        else:
            # Ejecutar todas las queries
            for query_file in self.queries_dir.glob("*.sql"):
                query_name = query_file.stem
                results[query_name] = self._run_single_query(query_name)
        
        return results
    
    def _run_single_query(self, query_name: str):
        """Ejecuta una query específica"""
        query_file = self.queries_dir / f"{query_name}.sql"
        
        if not query_file.exists():
            logger.error(f"Query no encontrada: {query_file}")
            return None
        
        try:
            with open(query_file, 'r') as f:
                query = f.read()
            
            with sqlite3.connect(self.db_path) as conn:
                df = pd.read_sql_query(query, conn)
            
            logger.info(f"Query {query_name} ejecutada: {len(df)} resultados")
            return df
            
        except Exception as e:
            logger.error(f"Error ejecutando {query_name}: {e}")
            return None
    
    def get_table_info(self):
        """Obtiene información de las tablas"""
        try:
            with sqlite3.connect(self.db_path) as conn:
                # Listar tablas
                tables_df = pd.read_sql_query(
                    "SELECT name FROM sqlite_master WHERE type='table'", 
                    conn
                )
                
                info = {'tables': tables_df['name'].tolist()}
                
                # Información de columnas para cada tabla
                for table in info['tables']:
                    columns_df = pd.read_sql_query(f"PRAGMA table_info({table})", conn)
                    info[f"{table}_columns"] = columns_df
                
                return info
                
        except Exception as e:
            logger.error(f"Error obteniendo info de tablas: {e}")
            return None


def main():
    """Función principal"""
    logging.basicConfig(level=logging.INFO)
    
    analyzer = SQLAnalysis()
    
    # Mostrar información de tablas
    info = analyzer.get_table_info()
    if info:
        print("Tablas disponibles:", info['tables'])
        for table in info['tables']:
            columns = info.get(f"{table}_columns")
            if columns is not None:
                print(f"\nColumnas de {table}:")
                print(columns[['name', 'type']])
    
    # Ejecutar análisis
    results = analyzer.run_analysis()
    if results:
        for query_name, df in results.items():
            if df is not None:
                print(f"\n=== Resultados de {query_name} ===")
                print(df.head(10))


if __name__ == "__main__":
    main()
