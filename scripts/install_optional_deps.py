#!/usr/bin/env python3
"""
Script de configuración para dependencias opcionales del Ejercicio 3
Instala polars, dask, pyarrow para benchmarking completo
"""

import subprocess
import sys
from pathlib import Path

def install_optional_dependencies():
    """Instala dependencias opcionales para benchmarking"""
    
    optional_packages = [
        'polars>=0.20.0',
        'dask[dataframe]>=2024.1.0', 
        'pyarrow>=14.0.0',
        'psutil>=5.9.0',
        'memory-profiler>=0.61.0'
    ]
    
    print("=== INSTALANDO DEPENDENCIAS OPCIONALES PARA EJERCICIO 3 ===")
    print("Paquetes a instalar:")
    for pkg in optional_packages:
        print(f"  - {pkg}")
    
    for package in optional_packages:
        try:
            print(f"\nInstalando {package}...")
            subprocess.check_call([
                sys.executable, "-m", "pip", "install", package
            ])
            print(f"✅ {package} instalado exitosamente")
            
        except subprocess.CalledProcessError as e:
            print(f"❌ Error instalando {package}: {e}")
            print("Continuando con siguientes paquetes...")
    
    print("\n=== VERIFICANDO INSTALACIÓN ===")
    
    # Verificar cada paquete
    verification_results = {}
    
    try:
        import polars as pl
        verification_results['polars'] = f"✅ Polars {pl.__version__}"
    except ImportError:
        verification_results['polars'] = "❌ Polars no disponible"
    
    try:
        import dask
        verification_results['dask'] = f"✅ Dask {dask.__version__}"
    except ImportError:
        verification_results['dask'] = "❌ Dask no disponible"
    
    try:
        import pyarrow as pa
        verification_results['pyarrow'] = f"✅ PyArrow {pa.__version__}"
    except ImportError:
        verification_results['pyarrow'] = "❌ PyArrow no disponible"
    
    try:
        import psutil
        verification_results['psutil'] = f"✅ PSUtil {psutil.__version__}"
    except ImportError:
        verification_results['psutil'] = "❌ PSUtil no disponible"
    
    for package, status in verification_results.items():
        print(status)
    
    # Contar éxitos
    successful = sum(1 for status in verification_results.values() if "✅" in status)
    total = len(verification_results)
    
    print(f"\nResultado: {successful}/{total} paquetes instalados correctamente")
    
    if successful == total:
        print("\n🎉 ¡Todas las dependencias opcionales están listas!")
        print("Ahora puedes ejecutar el benchmark completo:")
        print("python etl/streaming_processor.py")
    else:
        print("\n⚠️  Algunas dependencias fallaron, pero el benchmark básico funcionará")
        print("Las implementaciones faltantes serán omitidas automáticamente")

if __name__ == "__main__":
    install_optional_dependencies()
