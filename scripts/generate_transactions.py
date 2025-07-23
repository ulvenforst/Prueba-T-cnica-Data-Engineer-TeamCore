"""
Script de generación de datos de transacciones
Genera datasets de prueba para los ejercicios de ETL y análisis
"""

import argparse
import pandas as pd
import random
import logging
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def generate_transactions(num_records: int, output_path: Path) -> dict:
    """
    Genera dataset de transacciones sintéticas
    
    Args:
        num_records: Número de registros a generar
        output_path: Ruta del archivo de salida
    
    Returns:
        Dict con estadísticas de generación
    """
    logger.info(f"Generando {num_records:,} transacciones en {output_path}")
    
    start_time = datetime.now()
    
    # Configurar parámetros de generación
    user_ids = list(range(1, min(10000, num_records // 10) + 1))
    statuses = ['completed', 'failed', 'pending']
    status_weights = [0.7, 0.2, 0.1]  # Distribución realista
    
    # Generar datos en lotes para eficiencia
    batch_size = 50000
    all_data = []
    
    for batch_start in range(0, num_records, batch_size):
        batch_end = min(batch_start + batch_size, num_records)
        batch_size_actual = batch_end - batch_start
        
        # Generar lote de datos
        batch_data = []
        for i in range(batch_size_actual):
            record_id = batch_start + i + 1
            
            # Timestamp con distribución temporal realista
            days_back = random.randint(0, 30)
            hours = random.randint(0, 23)
            minutes = random.randint(0, 59)
            seconds = random.randint(0, 59)
            
            timestamp = datetime.now() - timedelta(
                days=days_back, 
                hours=hours, 
                minutes=minutes, 
                seconds=seconds
            )
            
            # Monto con distribución log-normal más realista
            amount = round(random.lognormvariate(mu=5.5, sigma=1.2), 2)
            amount = max(1.0, min(amount, 50000.0))  # Límites realistas
            
            # Status con distribución ponderada
            status = random.choices(statuses, weights=status_weights)[0]
            
            # User ID con algunos usuarios más activos
            if random.random() < 0.1:  # 10% usuarios muy activos
                user_id = random.choice(user_ids[:100])
            else:
                user_id = random.choice(user_ids)
            
            batch_data.append({
                'order_id': record_id,
                'user_id': user_id,
                'amount': amount,
                'status': status,
                'timestamp': timestamp.strftime('%Y-%m-%d %H:%M:%S')
            })
        
        all_data.extend(batch_data)
        
        if batch_end % 100000 == 0:
            logger.info(f"Generados {batch_end:,} registros...")
    
    # Crear DataFrame y guardar
    df = pd.DataFrame(all_data)
    
    # Crear directorio si no existe
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Guardar CSV
    df.to_csv(output_path, index=False)
    
    end_time = datetime.now()
    generation_time = (end_time - start_time).total_seconds()
    
    # Estadísticas de generación
    stats = {
        'records_generated': len(df),
        'file_size_mb': output_path.stat().st_size / (1024 * 1024),
        'generation_time_seconds': generation_time,
        'records_per_second': len(df) / generation_time if generation_time > 0 else 0,
        'unique_users': df['user_id'].nunique(),
        'status_distribution': df['status'].value_counts().to_dict(),
        'amount_stats': {
            'min': df['amount'].min(),
            'max': df['amount'].max(),
            'mean': df['amount'].mean(),
            'median': df['amount'].median()
        },
        'date_range': {
            'start': df['timestamp'].min(),
            'end': df['timestamp'].max()
        }
    }
    
    logger.info(f"Generación completada: {stats}")
    
    return stats

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(
        description="Generador de datos de transacciones sintéticas"
    )
    
    parser.add_argument(
        '--records', '-r',
        type=int,
        default=1000000,
        help='Número de registros a generar (default: 1M)'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=Path,
        default=Path('data/raw/sample_transactions.csv'),
        help='Archivo de salida'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Logging detallado'
    )
    
    args = parser.parse_args()
    
    # Configurar logging
    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    try:
        stats = generate_transactions(args.records, args.output)
        
        print("\nEstadísticas de generación:")
        print(f"Registros: {stats['records_generated']:,}")
        print(f"Tamaño archivo: {stats['file_size_mb']:.1f} MB")
        print(f"Tiempo: {stats['generation_time_seconds']:.2f} segundos")
        print(f"Rendimiento: {stats['records_per_second']:,.0f} registros/segundo")
        print(f"Usuarios únicos: {stats['unique_users']:,}")
        print(f"Distribución status: {stats['status_distribution']}")
        print(f"Monto promedio: ${stats['amount_stats']['mean']:.2f}")
        print(f"Rango temporal: {stats['date_range']['start']} - {stats['date_range']['end']}")
        
        print(f"\nArchivo generado exitosamente: {args.output}")
        
    except Exception as e:
        logger.error(f"Error generando datos: {e}")
        raise

if __name__ == "__main__":
    main()
