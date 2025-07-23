#!/usr/bin/env python3
"""
Implementación completa del Ejercicio 3: ETL Python para archivo grande
Procesa sample.log.gz (~5M registros) en streaming con diferentes variantes:
- multiprocessing
- polars 
- dask
- Exporta a Parquet con compresión snappy
"""

import os
import sys
import time
import json
import gzip
import psutil
import logging
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime, timedelta
import pandas as pd

# Intentar importar librerías opcionales
try:
    import polars as pl
    HAS_POLARS = True
except ImportError:
    HAS_POLARS = False

try:
    import dask.dataframe as dd
    from dask import delayed
    HAS_DASK = True
except ImportError:
    HAS_DASK = False

try:
    import pyarrow as pa
    import pyarrow.parquet as pq
    HAS_PYARROW = True
except ImportError:
    HAS_PYARROW = False

from multiprocessing import Pool, cpu_count

logger = logging.getLogger(__name__)


class StreamingLogProcessor:
    """Procesador de logs streaming con múltiples implementaciones"""
    
    def __init__(self, input_file: Path = None, output_dir: Path = None):
        self.input_file = input_file or Path("data/raw/sample.log.gz")
        self.output_dir = output_dir or Path("data/processed")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Configuración de procesamiento
        self.chunk_size = 100000
        self.min_status_code = 500
        
    def process_with_pandas_streaming(self) -> Dict[str, Any]:
        """Implementación base con pandas streaming"""
        logger.info("Iniciando procesamiento con pandas streaming")
        
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        records = []
        total_records = 0
        filtered_records = 0
        error_records = 0
        
        try:
            with gzip.open(self.input_file, 'rt', encoding='utf-8') as f:
                batch = []
                
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        record = json.loads(line)
                        total_records += 1
                        
                        # Filtrar por status_code >= 500
                        if record.get('status_code', 0) >= self.min_status_code:
                            filtered_records += 1
                            
                            # Limpiar y parsear campos
                            cleaned_record = self._clean_log_record(record)
                            if cleaned_record:
                                batch.append(cleaned_record)
                        
                        # Procesar batch cuando alcance el tamaño objetivo
                        if len(batch) >= self.chunk_size:
                            batch_df = pd.DataFrame(batch)
                            aggregated = self._aggregate_by_hour_endpoint(batch_df)
                            records.append(aggregated)
                            batch = []
                            
                            if line_num % 500000 == 0:
                                logger.info(f"Procesadas {line_num:,} líneas, {filtered_records:,} filtradas")
                    
                    except json.JSONDecodeError:
                        error_records += 1
                        if error_records % 10000 == 0:
                            logger.warning(f"Errores JSON acumulados: {error_records}")
                
                # Procesar último batch
                if batch:
                    batch_df = pd.DataFrame(batch)
                    aggregated = self._aggregate_by_hour_endpoint(batch_df)
                    records.append(aggregated)
            
            # Consolidar resultados
            if records:
                final_df = pd.concat(records, ignore_index=True)
                final_df = final_df.groupby(['hour', 'endpoint']).agg({
                    'count': 'sum',
                    'avg_response_time': 'mean',
                    'error_rate': 'mean'
                }).reset_index()
            else:
                final_df = pd.DataFrame()
            
            # Exportar a Parquet
            output_file = self.output_dir / "log_analysis_pandas_streaming.parquet"
            if HAS_PYARROW and len(final_df) > 0:
                final_df.to_parquet(output_file, compression='snappy', engine='pyarrow')
            elif len(final_df) > 0:
                final_df.to_csv(output_file.with_suffix('.csv'), index=False)
            
            end_time = time.time()
            end_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            stats = {
                'method': 'pandas_streaming',
                'total_records': total_records,
                'filtered_records': filtered_records,
                'processed_records': len(final_df),
                'error_records': error_records,
                'processing_time_seconds': round(end_time - start_time, 2),
                'memory_used_mb': round(end_memory - start_memory, 2),
                'records_per_second': round(total_records / (end_time - start_time)),
                'output_file': str(output_file),
                'compression': 'snappy' if HAS_PYARROW else 'none'
            }
            
            logger.info(f"Procesamiento pandas completado: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error en procesamiento pandas: {e}")
            raise
    
    def process_with_multiprocessing(self) -> Dict[str, Any]:
        """Implementación con multiprocessing"""
        logger.info("Iniciando procesamiento con multiprocessing")
        
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        # Dividir archivo en chunks para procesar en paralelo
        chunks = self._split_log_file_for_multiprocessing()
        
        # Procesar chunks en paralelo
        num_workers = min(cpu_count(), len(chunks))
        logger.info(f"Usando {num_workers} workers para {len(chunks)} chunks")
        
        with Pool(num_workers) as pool:
            results = pool.map(self._process_chunk_multiprocessing, chunks)
        
        # Consolidar resultados
        all_stats = []
        total_records = 0
        filtered_records = 0
        error_records = 0
        processed_dfs = []
        
        for result in results:
            if result['status'] == 'success':
                total_records += result['stats']['total_records']
                filtered_records += result['stats']['filtered_records']
                error_records += result['stats']['error_records']
                if 'data' in result and len(result['data']) > 0:
                    processed_dfs.append(result['data'])
        
        # Agregar resultados finales
        if processed_dfs:
            final_df = pd.concat(processed_dfs, ignore_index=True)
            final_df = final_df.groupby(['hour', 'endpoint']).agg({
                'count': 'sum',
                'avg_response_time': 'mean',
                'error_rate': 'mean'
            }).reset_index()
        else:
            final_df = pd.DataFrame()
        
        # Exportar resultados
        output_file = self.output_dir / "log_analysis_multiprocessing.parquet"
        if HAS_PYARROW and len(final_df) > 0:
            final_df.to_parquet(output_file, compression='snappy', engine='pyarrow')
        elif len(final_df) > 0:
            final_df.to_csv(output_file.with_suffix('.csv'), index=False)
        
        end_time = time.time()
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        stats = {
            'method': 'multiprocessing',
            'num_workers': num_workers,
            'total_records': total_records,
            'filtered_records': filtered_records,
            'processed_records': len(final_df),
            'error_records': error_records,
            'processing_time_seconds': round(end_time - start_time, 2),
            'memory_used_mb': round(end_memory - start_memory, 2),
            'records_per_second': round(total_records / (end_time - start_time)),
            'output_file': str(output_file),
            'compression': 'snappy' if HAS_PYARROW else 'none'
        }
        
        logger.info(f"Procesamiento multiprocessing completado: {stats}")
        return stats
    
    def process_with_polars(self) -> Dict[str, Any]:
        """Implementación con polars (si está disponible)"""
        if not HAS_POLARS:
            logger.warning("Polars no está disponible, saltando implementación")
            return {'method': 'polars', 'status': 'skipped', 'reason': 'library_not_available'}
        
        logger.info("Iniciando procesamiento con polars")
        
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        try:
            # Leer archivo con polars lazy evaluation
            records = []
            total_records = 0
            filtered_records = 0
            error_records = 0
            
            with gzip.open(self.input_file, 'rt', encoding='utf-8') as f:
                for line_num, line in enumerate(f, 1):
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        record = json.loads(line)
                        total_records += 1
                        
                        if record.get('status_code', 0) >= self.min_status_code:
                            filtered_records += 1
                            cleaned_record = self._clean_log_record(record)
                            if cleaned_record:
                                records.append(cleaned_record)
                    
                    except json.JSONDecodeError:
                        error_records += 1
                    
                    if line_num % 500000 == 0:
                        logger.info(f"Polars: procesadas {line_num:,} líneas")
            
            if records:
                # Crear DataFrame con polars
                df = pl.DataFrame(records)
                
                # Agregar por hora y endpoint usando polars lazy API
                result_df = (
                    df.lazy()
                    .group_by(['hour', 'endpoint'])
                    .agg([
                        pl.len().alias('count'),
                        pl.col('response_time_ms').mean().alias('avg_response_time'),
                        pl.col('error_rate').mean().alias('error_rate')
                    ])
                    .collect()
                )
                
                # Convertir a pandas para exportar (compatibilidad)
                final_df = result_df.to_pandas()
            else:
                final_df = pd.DataFrame()
            
            # Exportar resultados
            output_file = self.output_dir / "log_analysis_polars.parquet"
            if HAS_PYARROW and len(final_df) > 0:
                final_df.to_parquet(output_file, compression='snappy', engine='pyarrow')
            elif len(final_df) > 0:
                final_df.to_csv(output_file.with_suffix('.csv'), index=False)
            
            end_time = time.time()
            end_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            stats = {
                'method': 'polars',
                'total_records': total_records,
                'filtered_records': filtered_records,
                'processed_records': len(final_df),
                'error_records': error_records,
                'processing_time_seconds': round(end_time - start_time, 2),
                'memory_used_mb': round(end_memory - start_memory, 2),
                'records_per_second': round(total_records / (end_time - start_time)),
                'output_file': str(output_file),
                'compression': 'snappy' if HAS_PYARROW else 'none'
            }
            
            logger.info(f"Procesamiento polars completado: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error en procesamiento polars: {e}")
            raise
    
    def process_with_dask(self) -> Dict[str, Any]:
        """Implementación con dask (si está disponible)"""
        if not HAS_DASK:
            logger.warning("Dask no está disponible, saltando implementación")
            return {'method': 'dask', 'status': 'skipped', 'reason': 'library_not_available'}
        
        logger.info("Iniciando procesamiento con dask")
        
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        try:
            # Procesar en chunks con dask delayed
            chunks = self._split_log_file_for_dask()
            
            # Crear delayed tasks
            delayed_tasks = [delayed(self._process_chunk_dask)(chunk) for chunk in chunks]
            
            # Ejecutar tareas
            results = dd.compute(*delayed_tasks)
            
            # Consolidar resultados
            total_records = 0
            filtered_records = 0
            error_records = 0
            processed_dfs = []
            
            for result in results:
                if result['status'] == 'success':
                    total_records += result['stats']['total_records']
                    filtered_records += result['stats']['filtered_records']
                    error_records += result['stats']['error_records']
                    if 'data' in result and len(result['data']) > 0:
                        processed_dfs.append(result['data'])
            
            if processed_dfs:
                final_df = pd.concat(processed_dfs, ignore_index=True)
                final_df = final_df.groupby(['hour', 'endpoint']).agg({
                    'count': 'sum',
                    'avg_response_time': 'mean',
                    'error_rate': 'mean'
                }).reset_index()
            else:
                final_df = pd.DataFrame()
            
            # Exportar resultados
            output_file = self.output_dir / "log_analysis_dask.parquet"
            if HAS_PYARROW and len(final_df) > 0:
                final_df.to_parquet(output_file, compression='snappy', engine='pyarrow')
            elif len(final_df) > 0:
                final_df.to_csv(output_file.with_suffix('.csv'), index=False)
            
            end_time = time.time()
            end_memory = psutil.Process().memory_info().rss / 1024 / 1024
            
            stats = {
                'method': 'dask',
                'total_records': total_records,
                'filtered_records': filtered_records,
                'processed_records': len(final_df),
                'error_records': error_records,
                'processing_time_seconds': round(end_time - start_time, 2),
                'memory_used_mb': round(end_memory - start_memory, 2),
                'records_per_second': round(total_records / (end_time - start_time)),
                'output_file': str(output_file),
                'compression': 'snappy' if HAS_PYARROW else 'none'
            }
            
            logger.info(f"Procesamiento dask completado: {stats}")
            return stats
            
        except Exception as e:
            logger.error(f"Error en procesamiento dask: {e}")
            raise
    
    def _clean_log_record(self, record: Dict) -> Dict:
        """Limpia y parsea campos de registro de log"""
        try:
            # Extraer timestamp y convertir a hora
            timestamp_str = record.get('timestamp', '')
            if timestamp_str:
                # Parsear timestamp ISO
                dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
                hour = dt.strftime('%Y-%m-%d %H:00:00')
            else:
                hour = '1970-01-01 00:00:00'
            
            # Extraer campos relevantes
            cleaned = {
                'hour': hour,
                'endpoint': record.get('endpoint', 'unknown'),
                'status_code': record.get('status_code', 0),
                'response_time_ms': record.get('response_time_ms', 0),
                'method': record.get('method', 'GET'),
                'level': record.get('level', 'INFO')
            }
            
            # Calcular error rate (1 si es error, 0 si no)
            cleaned['error_rate'] = 1.0 if cleaned['status_code'] >= 500 else 0.0
            
            return cleaned
            
        except Exception as e:
            logger.debug(f"Error limpiando registro: {e}")
            return None
    
    def _aggregate_by_hour_endpoint(self, df: pd.DataFrame) -> pd.DataFrame:
        """Agrupa datos por hora y endpoint"""
        if len(df) == 0:
            return pd.DataFrame()
        
        try:
            aggregated = df.groupby(['hour', 'endpoint']).agg({
                'status_code': 'count',
                'response_time_ms': 'mean',
                'error_rate': 'mean'
            }).reset_index()
            
            aggregated.columns = ['hour', 'endpoint', 'count', 'avg_response_time', 'error_rate']
            return aggregated
            
        except Exception as e:
            logger.error(f"Error en agregación: {e}")
            return pd.DataFrame()
    
    def _split_log_file_for_multiprocessing(self) -> List[Path]:
        """Divide archivo de log en chunks para multiprocessing"""
        chunks = []
        chunk_size = 1000000  # 1M líneas por chunk
        
        temp_dir = self.output_dir / "temp_chunks"
        temp_dir.mkdir(exist_ok=True)
        
        try:
            with gzip.open(self.input_file, 'rt', encoding='utf-8') as f:
                chunk_num = 0
                current_chunk = []
                
                for line_num, line in enumerate(f, 1):
                    current_chunk.append(line)
                    
                    if len(current_chunk) >= chunk_size:
                        # Guardar chunk
                        chunk_file = temp_dir / f"chunk_{chunk_num}.json"
                        with open(chunk_file, 'w', encoding='utf-8') as cf:
                            cf.writelines(current_chunk)
                        
                        chunks.append(chunk_file)
                        current_chunk = []
                        chunk_num += 1
                
                # Guardar último chunk
                if current_chunk:
                    chunk_file = temp_dir / f"chunk_{chunk_num}.json"
                    with open(chunk_file, 'w', encoding='utf-8') as cf:
                        cf.writelines(current_chunk)
                    chunks.append(chunk_file)
            
            logger.info(f"Archivo dividido en {len(chunks)} chunks")
            return chunks
            
        except Exception as e:
            logger.error(f"Error dividiendo archivo: {e}")
            return []
    
    def _process_chunk_multiprocessing(self, chunk_file: Path) -> Dict:
        """Procesa un chunk específico para multiprocessing"""
        try:
            records = []
            total_records = 0
            filtered_records = 0
            error_records = 0
            
            with open(chunk_file, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    
                    try:
                        record = json.loads(line)
                        total_records += 1
                        
                        if record.get('status_code', 0) >= self.min_status_code:
                            filtered_records += 1
                            cleaned_record = self._clean_log_record(record)
                            if cleaned_record:
                                records.append(cleaned_record)
                    
                    except json.JSONDecodeError:
                        error_records += 1
            
            # Agregar datos del chunk
            if records:
                df = pd.DataFrame(records)
                aggregated_df = self._aggregate_by_hour_endpoint(df)
            else:
                aggregated_df = pd.DataFrame()
            
            return {
                'status': 'success',
                'stats': {
                    'total_records': total_records,
                    'filtered_records': filtered_records,
                    'error_records': error_records
                },
                'data': aggregated_df
            }
            
        except Exception as e:
            logger.error(f"Error procesando chunk {chunk_file}: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'stats': {
                    'total_records': 0,
                    'filtered_records': 0,
                    'error_records': 0
                }
            }
    
    def _split_log_file_for_dask(self) -> List[Path]:
        """Divide archivo para procesamiento con dask"""
        return self._split_log_file_for_multiprocessing()
    
    def _process_chunk_dask(self, chunk_file: Path) -> Dict:
        """Procesa chunk con dask delayed"""
        return self._process_chunk_multiprocessing(chunk_file)
    
    def run_all_benchmarks(self) -> Dict[str, Any]:
        """Ejecuta todos los benchmarks y compara resultados"""
        logger.info("=== INICIANDO BENCHMARKS COMPLETOS ===")
        
        results = {}
        
        # Verificar que el archivo de entrada existe
        if not self.input_file.exists():
            logger.error(f"Archivo de entrada no encontrado: {self.input_file}")
            return {'error': 'input_file_not_found'}
        
        file_size_mb = self.input_file.stat().st_size / (1024 * 1024)
        logger.info(f"Archivo de entrada: {self.input_file} ({file_size_mb:.1f} MB)")
        
        # 1. Pandas streaming (baseline)
        try:
            results['pandas_streaming'] = self.process_with_pandas_streaming()
        except Exception as e:
            logger.error(f"Error en pandas streaming: {e}")
            results['pandas_streaming'] = {'status': 'error', 'error': str(e)}
        
        # 2. Multiprocessing
        try:
            results['multiprocessing'] = self.process_with_multiprocessing()
        except Exception as e:
            logger.error(f"Error en multiprocessing: {e}")
            results['multiprocessing'] = {'status': 'error', 'error': str(e)}
        
        # 3. Polars (si está disponible)
        try:
            results['polars'] = self.process_with_polars()
        except Exception as e:
            logger.error(f"Error en polars: {e}")
            results['polars'] = {'status': 'error', 'error': str(e)}
        
        # 4. Dask (si está disponible)
        try:
            results['dask'] = self.process_with_dask()
        except Exception as e:
            logger.error(f"Error en dask: {e}")
            results['dask'] = {'status': 'error', 'error': str(e)}
        
        # Generar reporte comparativo
        self._generate_benchmark_report(results)
        
        return results
    
    def _generate_benchmark_report(self, results: Dict[str, Any]):
        """Genera reporte comparativo de benchmarks"""
        report_file = self.output_dir / "benchmark_report.md"
        
        with open(report_file, 'w') as f:
            f.write("# Reporte de Benchmarks - ETL Streaming Log Processing\n\n")
            f.write(f"**Archivo procesado**: {self.input_file}\n")
            f.write(f"**Fecha**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            f.write("## Resultados por Método\n\n")
            f.write("| Método | Records/seg | Tiempo (s) | Memoria (MB) | Status |\n")
            f.write("|--------|-------------|------------|--------------|--------|\n")
            
            for method, result in results.items():
                if 'status' in result and result['status'] == 'error':
                    f.write(f"| {method} | ERROR | ERROR | ERROR | {result.get('error', 'Unknown')} |\n")
                elif 'status' in result and result['status'] == 'skipped':
                    f.write(f"| {method} | SKIPPED | SKIPPED | SKIPPED | {result.get('reason', 'Unknown')} |\n")
                else:
                    rps = result.get('records_per_second', 0)
                    time_s = result.get('processing_time_seconds', 0)
                    memory_mb = result.get('memory_used_mb', 0)
                    f.write(f"| {method} | {rps:,} | {time_s} | {memory_mb} | SUCCESS |\n")
            
            f.write("\n## Análisis de Rendimiento\n\n")
            
            # Encontrar el método más rápido
            valid_results = {k: v for k, v in results.items() 
                           if 'status' not in v or v.get('status') not in ['error', 'skipped']}
            
            if valid_results:
                fastest = max(valid_results.items(), 
                            key=lambda x: x[1].get('records_per_second', 0))
                f.write(f"**Método más rápido**: {fastest[0]} ({fastest[1].get('records_per_second', 0):,} records/seg)\n\n")
                
                most_efficient = min(valid_results.items(), 
                                   key=lambda x: x[1].get('memory_used_mb', float('inf')))
                f.write(f"**Más eficiente en memoria**: {most_efficient[0]} ({most_efficient[1].get('memory_used_mb', 0)} MB)\n\n")
            
            f.write("## Configuración del Sistema\n\n")
            f.write(f"- CPU cores: {cpu_count()}\n")
            f.write(f"- Polars disponible: {HAS_POLARS}\n")
            f.write(f"- Dask disponible: {HAS_DASK}\n")
            f.write(f"- PyArrow disponible: {HAS_PYARROW}\n")
        
        logger.info(f"Reporte generado: {report_file}")


def main():
    """Función principal"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    print("=== EJERCICIO 3: ETL PYTHON PARA ARCHIVO GRANDE ===")
    print("Evaluando variantes: pandas streaming, multiprocessing, polars, dask")
    print("Exportando a Parquet con compresión snappy")
    print("Midiendo tiempos y memoria (profiling)")
    print("=" * 60)
    
    try:
        # Verificar si existe archivo de entrada
        input_file = Path("data/raw/sample.log.gz")
        if not input_file.exists():
            print(f"⚠️  Archivo de entrada no encontrado: {input_file}")
            print("Generando archivo de muestra...")
            
            # Generar archivo de muestra
            from scripts.generate_logs import generate_logs
            stats = generate_logs(5000000, input_file)  # 5M registros
            print(f"✅ Archivo generado: {stats}")
        
        # Ejecutar benchmarks
        processor = StreamingLogProcessor(input_file)
        results = processor.run_all_benchmarks()
        
        print("\n" + "=" * 60)
        print("RESUMEN DE BENCHMARKS")
        print("=" * 60)
        
        for method, result in results.items():
            if 'status' in result and result['status'] == 'error':
                print(f"❌ {method}: ERROR - {result.get('error', 'Unknown')}")
            elif 'status' in result and result['status'] == 'skipped':
                print(f"⏭️  {method}: SKIPPED - {result.get('reason', 'Unknown')}")
            else:
                rps = result.get('records_per_second', 0)
                time_s = result.get('processing_time_seconds', 0)
                memory_mb = result.get('memory_used_mb', 0)
                processed = result.get('processed_records', 0)
                print(f"✅ {method}: {rps:,} rec/seg, {time_s}s, {memory_mb}MB, {processed:,} records")
        
        print("=" * 60)
        print("✅ Benchmarking completado exitosamente")
        print("📊 Revisa data/processed/benchmark_report.md para análisis detallado")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Error durante benchmarking: {e}")
        logger.error(f"Error en main: {e}")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
