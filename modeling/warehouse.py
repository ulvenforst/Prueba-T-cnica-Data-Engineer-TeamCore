"""
Data Warehouse principal
"""

import logging
import sqlite3
from pathlib import Path
from typing import Dict, List, Optional
import pandas as pd

logger = logging.getLogger(__name__)


class DataWarehouse:
    """Gestor del Data Warehouse dimensional"""
    
    def __init__(self, db_path: Optional[Path] = None):
        self.db_path = db_path or Path("data/warehouse") / "transactions_warehouse.db"
        self.schema_path = Path("modeling/schemas")
        
        # Crear directorios si no existen
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.schema_path.mkdir(parents=True, exist_ok=True)
    
    def get_connection(self):
        """Obtiene conexión a la base de datos"""
        return sqlite3.connect(str(self.db_path))
    
    def initialize_schema(self) -> Dict:
        """
        Inicializa el esquema del data warehouse
        
        Returns:
            Dict: Resultado de la inicialización
        """
        logger.info("Inicializando esquema del data warehouse")
        
        try:
            with self.get_connection() as conn:
                # Crear tablas dimensionales
                self._create_dimension_tables(conn)
                
                # Crear tabla de hechos
                self._create_fact_table(conn)
                
                # Crear índices
                self._create_indexes(conn)
                
                conn.commit()
            
            logger.info("Esquema inicializado exitosamente")
            return {'status': 'success', 'message': 'Esquema creado'}
            
        except Exception as e:
            logger.error(f"Error inicializando esquema: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def _create_dimension_tables(self, conn):
        """Crea tablas dimensionales"""
        
        # Dimensión Usuario
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_user (
                user_key INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER UNIQUE NOT NULL,
                user_type VARCHAR(50),
                registration_date DATE,
                status VARCHAR(20),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Dimensión Tiempo
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_time (
                time_key INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE UNIQUE NOT NULL,
                year INTEGER,
                month INTEGER,
                day INTEGER,
                quarter INTEGER,
                day_of_week INTEGER,
                week_number INTEGER,
                is_weekend BOOLEAN
            )
        """)
        
        # Dimensión Status
        conn.execute("""
            CREATE TABLE IF NOT EXISTS dim_status (
                status_key INTEGER PRIMARY KEY AUTOINCREMENT,
                status_code VARCHAR(20) UNIQUE NOT NULL,
                status_description VARCHAR(100),
                status_category VARCHAR(50)
            )
        """)
    
    def _create_fact_table(self, conn):
        """Crea tabla de hechos de transacciones"""
        
        conn.execute("""
            CREATE TABLE IF NOT EXISTS fact_transactions (
                transaction_key INTEGER PRIMARY KEY AUTOINCREMENT,
                order_id INTEGER NOT NULL,
                user_key INTEGER,
                time_key INTEGER,
                status_key INTEGER,
                amount DECIMAL(10,2),
                transaction_date TIMESTAMP,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                
                FOREIGN KEY (user_key) REFERENCES dim_user(user_key),
                FOREIGN KEY (time_key) REFERENCES dim_time(time_key),
                FOREIGN KEY (status_key) REFERENCES dim_status(status_key)
            )
        """)
    
    def _create_indexes(self, conn):
        """Crea índices para optimización"""
        
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_fact_user_key ON fact_transactions(user_key)",
            "CREATE INDEX IF NOT EXISTS idx_fact_time_key ON fact_transactions(time_key)",
            "CREATE INDEX IF NOT EXISTS idx_fact_status_key ON fact_transactions(status_key)",
            "CREATE INDEX IF NOT EXISTS idx_fact_date ON fact_transactions(transaction_date)",
            "CREATE INDEX IF NOT EXISTS idx_dim_user_id ON dim_user(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_dim_time_date ON dim_time(date)",
            "CREATE INDEX IF NOT EXISTS idx_dim_status_code ON dim_status(status_code)"
        ]
        
        for index_sql in indexes:
            conn.execute(index_sql)
    
    def load_transactions(self, df: pd.DataFrame) -> Dict:
        """
        Carga transacciones al data warehouse
        
        Args:
            df: DataFrame con transacciones
            
        Returns:
            Dict: Resultado de la carga
        """
        logger.info(f"Cargando {len(df)} transacciones al warehouse")
        
        try:
            with self.get_connection() as conn:
                # Cargar dimensiones primero
                self._load_dimensions(conn, df)
                
                # Luego cargar hechos
                fact_count = self._load_facts(conn, df)
                
                conn.commit()
            
            result = {
                'status': 'success',
                'facts_loaded': fact_count,
                'total_records': len(df)
            }
            
            logger.info(f"Carga completada: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error cargando transacciones: {e}")
            return {'status': 'error', 'message': str(e)}
    
    def _load_dimensions(self, conn, df):
        """Carga datos a tablas dimensionales"""
        
        # Cargar dim_user
        users = df[['user_id']].drop_duplicates()
        for _, user in users.iterrows():
            conn.execute("""
                INSERT OR IGNORE INTO dim_user (user_id, user_type, status)
                VALUES (?, 'regular', 'active')
            """, (user['user_id'],))
        
        # Cargar dim_time
        if 'timestamp' in df.columns:
            dates = pd.to_datetime(df['timestamp']).dt.date.drop_duplicates()
            for date in dates:
                dt = pd.to_datetime(date)
                conn.execute("""
                    INSERT OR IGNORE INTO dim_time 
                    (date, year, month, day, quarter, day_of_week, week_number, is_weekend)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    date,
                    dt.year,
                    dt.month,
                    dt.day,
                    dt.quarter,
                    dt.dayofweek,
                    dt.isocalendar().week,
                    dt.dayofweek >= 5
                ))
        
        # Cargar dim_status
        if 'status' in df.columns:
            statuses = df[['status']].drop_duplicates()
            for _, status in statuses.iterrows():
                conn.execute("""
                    INSERT OR IGNORE INTO dim_status (status_code, status_description)
                    VALUES (?, ?)
                """, (status['status'], status['status'].title()))
    
    def _load_facts(self, conn, df) -> int:
        """Carga hechos de transacciones"""
        
        fact_count = 0
        
        for _, row in df.iterrows():
            # Obtener keys de dimensiones
            user_key = self._get_user_key(conn, row['user_id'])
            
            time_key = None
            if 'timestamp' in row:
                time_key = self._get_time_key(conn, pd.to_datetime(row['timestamp']).date())
            
            status_key = None
            if 'status' in row:
                status_key = self._get_status_key(conn, row['status'])
            
            # Insertar hecho
            conn.execute("""
                INSERT INTO fact_transactions 
                (order_id, user_key, time_key, status_key, amount, transaction_date)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                row.get('order_id'),
                user_key,
                time_key,
                status_key,
                row.get('amount'),
                row.get('timestamp')
            ))
            
            fact_count += 1
        
        return fact_count
    
    def _get_user_key(self, conn, user_id) -> Optional[int]:
        """Obtiene user_key por user_id"""
        cursor = conn.execute("SELECT user_key FROM dim_user WHERE user_id = ?", (user_id,))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def _get_time_key(self, conn, date) -> Optional[int]:
        """Obtiene time_key por fecha"""
        cursor = conn.execute("SELECT time_key FROM dim_time WHERE date = ?", (date,))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def _get_status_key(self, conn, status_code) -> Optional[int]:
        """Obtiene status_key por código de status"""
        cursor = conn.execute("SELECT status_key FROM dim_status WHERE status_code = ?", (status_code,))
        result = cursor.fetchone()
        return result[0] if result else None
    
    def get_summary_stats(self) -> Dict:
        """Obtiene estadísticas resumidas del warehouse"""
        
        try:
            with self.get_connection() as conn:
                stats = {}
                
                # Contar registros en cada tabla
                tables = ['dim_user', 'dim_time', 'dim_status', 'fact_transactions']
                for table in tables:
                    cursor = conn.execute(f"SELECT COUNT(*) FROM {table}")
                    stats[f"{table}_count"] = cursor.fetchone()[0]
                
                # Estadísticas adicionales
                cursor = conn.execute("""
                    SELECT 
                        MIN(amount) as min_amount,
                        MAX(amount) as max_amount,
                        AVG(amount) as avg_amount,
                        COUNT(DISTINCT user_key) as unique_users
                    FROM fact_transactions
                """)
                
                result = cursor.fetchone()
                if result:
                    stats.update({
                        'min_amount': result[0],
                        'max_amount': result[1],
                        'avg_amount': result[2],
                        'unique_users': result[3]
                    })
                
                return {'status': 'success', 'stats': stats}
                
        except Exception as e:
            logger.error(f"Error obteniendo estadísticas: {e}")
            return {'status': 'error', 'message': str(e)}


def main():
    """Función principal para testing"""
    logging.basicConfig(level=logging.INFO)
    
    warehouse = DataWarehouse()
    
    # Inicializar esquema
    result = warehouse.initialize_schema()
    print(f"Inicialización: {result}")
    
    # Obtener estadísticas
    stats = warehouse.get_summary_stats()
    print(f"Estadísticas: {stats}")


if __name__ == "__main__":
    main()
