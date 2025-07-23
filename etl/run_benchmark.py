#!/usr/bin/env python3
"""
Script principal para benchmarking completo de implementaciones ETL
Ejercicio 3: ETL Python para archivo grande - Evaluación de variantes
"""

import sys
import logging
from pathlib import Path

# Añadir src al path
current_dir = Path(__file__).parent
sys.path.append(str(current_dir / "src"))

from src.benchmark import main as benchmark_main


if __name__ == "__main__":
    print("EJERCICIO 3: ETL PYTHON - BENCHMARKING DE IMPLEMENTACIONES")
    print("=" * 70)
    print("Evaluando variantes: multiprocessing, polars, dask")
    print("Midiendo tiempos y memoria (profiling)")
    print("=" * 70)
    
    success = benchmark_main()
    
    if success:
        print("\n✓ Benchmarking completado exitosamente")
        print("✓ Revisa los archivos de salida para resultados detallados")
    else:
        print("\n✗ Error durante el benchmarking")
    
    sys.exit(0 if success else 1)
