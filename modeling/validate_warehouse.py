"""
Script de validación final del data warehouse
"""

import sqlite3
from pathlib import Path

def validate_warehouse():
    db_path = Path('output/transactions_warehouse.db')
    if not db_path.exists():
        print("❌ Base de datos no encontrada")
        return
    
    conn = sqlite3.connect(str(db_path))
    
    print("=== ESTRUCTURA DEL DATA WAREHOUSE ===")
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name").fetchall()
    for table in tables:
        print(f"✅ Tabla: {table[0]}")
    
    print("\n=== CONTEOS POR TABLA ===")
    for table in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM {table[0]}").fetchone()[0]
        print(f"{table[0]}: {count:,} registros")
    
    print("\n=== ANÁLISIS DE REVENUE POR MES (TOP 6) ===")
    revenue_query = """
    SELECT 
        dd.year,
        dd.month,
        dd.month_name,
        COUNT(*) as transacciones,
        ROUND(SUM(ft.amount), 2) as revenue_total
    FROM fact_transactions ft
    JOIN dim_date dd ON ft.date_key = dd.date_key
    JOIN dim_status ds ON ft.status_key = ds.status_key
    WHERE ds.is_successful = 1
    GROUP BY dd.year, dd.month, dd.month_name
    ORDER BY dd.year, dd.month
    LIMIT 6
    """
    
    results = conn.execute(revenue_query).fetchall()
    for row in results:
        print(f"{row[2]} {row[0]}: {row[3]:,} trans, ${row[4]:,.2f}")
    
    print("\n=== TOP 5 USUARIOS POR REVENUE ===")
    top_users_query = """
    SELECT 
        du.user_id,
        du.user_segment,
        COUNT(*) as total_transacciones,
        ROUND(SUM(ft.amount), 2) as revenue_total
    FROM fact_transactions ft
    JOIN dim_user du ON ft.user_key = du.user_key
    JOIN dim_status ds ON ft.status_key = ds.status_key
    WHERE ds.is_successful = 1
    GROUP BY du.user_id, du.user_segment
    ORDER BY revenue_total DESC
    LIMIT 5
    """
    
    top_users = conn.execute(top_users_query).fetchall()
    for user in top_users:
        print(f"Usuario {user[0]} ({user[1]}): {user[2]:,} trans, ${user[3]:,.2f}")
    
    print("\n=== VERIFICACIÓN DE ÍNDICES ===")
    indexes = conn.execute("SELECT name FROM sqlite_master WHERE type='index' AND name NOT LIKE 'sqlite_%'").fetchall()
    print(f"Total índices creados: {len(indexes)}")
    for idx in indexes[:5]:  # Mostrar solo los primeros 5
        print(f"✅ {idx[0]}")
    if len(indexes) > 5:
        print(f"... y {len(indexes) - 5} índices más")
    
    print("\n=== VERIFICACIÓN SCD ===")
    # Verificar tabla de historia
    history_count = conn.execute("SELECT COUNT(*) FROM dim_user_history").fetchone()[0]
    print(f"Registros en dim_user_history: {history_count}")
    
    # Verificar registros actuales
    current_records = conn.execute("SELECT COUNT(*) FROM dim_user_history WHERE is_current = 1").fetchone()[0]
    print(f"Registros SCD actuales: {current_records}")
    
    conn.close()
    print("\n✅ VALIDACIÓN COMPLETADA EXITOSAMENTE")

if __name__ == "__main__":
    validate_warehouse()
