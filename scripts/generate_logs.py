"""
Script de generación de logs simulados
Genera archivos de log comprimidos para testing de ETL streaming
"""

import argparse
import json
import gzip
import random
import logging
from pathlib import Path
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)

def generate_logs(num_records: int, output_path: Path) -> dict:
    """
    Genera archivo de logs simulados en formato JSONL comprimido
    
    Args:
        num_records: Número de registros de log a generar
        output_path: Ruta del archivo de salida (.gz)
    
    Returns:
        Dict con estadísticas de generación
    """
    logger.info(f"Generando {num_records:,} logs en {output_path}")
    
    start_time = datetime.now()
    
    # Configuración de generación
    endpoints = [
        '/api/transactions', '/api/users', '/api/reports', '/api/analytics',
        '/api/auth', '/api/payments', '/api/admin', '/api/notifications'
    ]
    
    methods = ['GET', 'POST', 'PUT', 'DELETE']
    method_weights = [0.6, 0.25, 0.1, 0.05]
    
    status_codes = [200, 201, 400, 401, 403, 404, 500, 502, 503]
    # Distribución realista: más 2xx, algunos 4xx, pocos 5xx
    status_weights = [0.65, 0.15, 0.08, 0.03, 0.02, 0.02, 0.02, 0.015, 0.015]
    
    log_levels = ['INFO', 'WARN', 'ERROR', 'DEBUG']
    level_weights = [0.7, 0.15, 0.1, 0.05]
    
    # Crear directorio si no existe
    output_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Generar logs en lotes para eficiencia
    batch_size = 10000
    total_written = 0
    
    with gzip.open(output_path, 'wt', encoding='utf-8') as f:
        for batch_start in range(0, num_records, batch_size):
            batch_end = min(batch_start + batch_size, num_records)
            
            for i in range(batch_start, batch_end):
                # Timestamp con distribución temporal
                hours_back = random.randint(0, 72)  # Últimas 3 días
                minutes = random.randint(0, 59)
                seconds = random.randint(0, 59)
                microseconds = random.randint(0, 999999)
                
                timestamp = datetime.now() - timedelta(
                    hours=hours_back,
                    minutes=minutes,
                    seconds=seconds,
                    microseconds=microseconds
                )
                
                # Generar entrada de log
                endpoint = random.choice(endpoints)
                method = random.choices(methods, weights=method_weights)[0]
                status_code = random.choices(status_codes, weights=status_weights)[0]
                level = random.choices(log_levels, weights=level_weights)[0]
                
                # Response time correlacionado con status code
                if status_code >= 500:
                    response_time = random.uniform(1000, 10000)  # Errores son lentos
                elif status_code >= 400:
                    response_time = random.uniform(100, 1000)
                else:
                    response_time = random.uniform(10, 500)
                
                # User agent simulado
                user_agents = [
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                    'curl/7.68.0',
                    'python-requests/2.28.1'
                ]
                
                # IP address simulada
                ip_address = f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}"
                
                # Mensaje de log específico por status
                if status_code >= 500:
                    message = f"Internal server error processing {endpoint}"
                elif status_code >= 400:
                    message = f"Client error for {endpoint}: validation failed"
                else:
                    message = f"Successfully processed {method} {endpoint}"
                
                log_entry = {
                    'timestamp': timestamp.isoformat() + 'Z',
                    'level': level,
                    'method': method,
                    'endpoint': endpoint,
                    'status_code': status_code,
                    'response_time_ms': round(response_time, 2),
                    'ip_address': ip_address,
                    'user_agent': random.choice(user_agents),
                    'message': message,
                    'request_id': f"req_{i+1:08d}",
                    'user_id': random.randint(1, 10000) if random.random() < 0.8 else None
                }
                
                # Escribir línea JSON
                json_line = json.dumps(log_entry, separators=(',', ':'))
                f.write(json_line + '\n')
                total_written += 1
            
            if batch_end % 50000 == 0:
                logger.info(f"Generados {batch_end:,} logs...")
    
    end_time = datetime.now()
    generation_time = (end_time - start_time).total_seconds()
    
    # Estadísticas de generación
    file_size = output_path.stat().st_size
    
    stats = {
        'records_generated': total_written,
        'file_size_mb': file_size / (1024 * 1024),
        'file_size_compressed': file_size,
        'generation_time_seconds': generation_time,
        'records_per_second': total_written / generation_time if generation_time > 0 else 0,
        'compression_ratio': 'N/A',  # Sería calculable comparando con versión sin comprimir
        'status_code_distribution': {
            '2xx': int(total_written * 0.8),
            '4xx': int(total_written * 0.15),
            '5xx': int(total_written * 0.05)
        },
        'endpoints_count': len(endpoints),
        'date_range': {
            'hours_back': 72,
            'end_time': datetime.now().isoformat()
        }
    }
    
    logger.info(f"Generación de logs completada: {stats}")
    
    return stats

def main():
    """Función principal"""
    parser = argparse.ArgumentParser(
        description="Generador de logs simulados para testing ETL"
    )
    
    parser.add_argument(
        '--records', '-r',
        type=int,
        default=100000,
        help='Número de registros de log a generar (default: 100K)'
    )
    
    parser.add_argument(
        '--output', '-o',
        type=Path,
        default=Path('data/raw/sample.log.gz'),
        help='Archivo de salida comprimido'
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
        stats = generate_logs(args.records, args.output)
        
        print("\nEstadísticas de generación de logs:")
        print(f"Registros: {stats['records_generated']:,}")
        print(f"Tamaño archivo: {stats['file_size_mb']:.1f} MB")
        print(f"Tiempo: {stats['generation_time_seconds']:.2f} segundos")
        print(f"Rendimiento: {stats['records_per_second']:,.0f} registros/segundo")
        print(f"Distribución status codes: {stats['status_code_distribution']}")
        print(f"Endpoints únicos: {stats['endpoints_count']}")
        
        print(f"\nArchivo de logs generado exitosamente: {args.output}")
        
    except Exception as e:
        logger.error(f"Error generando logs: {e}")
        raise

if __name__ == "__main__":
    main()
