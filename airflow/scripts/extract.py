"""
Script de extracción de datos para el pipeline de transacciones
"""
import requests
import os
import sys
from pathlib import Path

# Añadir directorios padre para importar config y utils
current_dir = Path(__file__).parent
project_root = current_dir.parent.parent.parent
sys.path.append(str(current_dir.parent.parent))  # Para config
sys.path.append(str(project_root / "shared"))    # Para utils

from config import CSV_FILE_PATH, DROPBOX_URL, MIN_FILE_SIZE_MB, SKIP_DOWNLOAD
from utils import setup_logger, send_alert, validate_file_size

def download_file():
    """
    Descarga el archivo de transacciones desde Dropbox
    Si el archivo ya existe localmente y tiene el tamaño correcto, lo usa
    """
    logger = setup_logger(__name__, 'extract.log')
    
    try:
        # Verificar si el archivo ya existe y es válido
        if CSV_FILE_PATH.exists() and validate_file_size(CSV_FILE_PATH, MIN_FILE_SIZE_MB):
            logger.info(f"Archivo ya existe y es válido: {CSV_FILE_PATH}")
            return str(CSV_FILE_PATH)
        
        # Si está configurado para saltarse la descarga y no hay archivo, fallar
        if SKIP_DOWNLOAD and not CSV_FILE_PATH.exists():
            raise FileNotFoundError(f"Archivo no encontrado y descarga deshabilitada: {CSV_FILE_PATH}")
        
        # Solo descargar si no está configurado para saltarse
        if not SKIP_DOWNLOAD:
            logger.info(f"Descargando archivo desde: {DROPBOX_URL}")
            
            # Crear directorio si no existe
            CSV_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
            
            # Descargar archivo
            response = requests.get(DROPBOX_URL, stream=True, timeout=300)
            response.raise_for_status()
            
            with open(CSV_FILE_PATH, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
            
            # Validar archivo descargado
            if not validate_file_size(CSV_FILE_PATH, MIN_FILE_SIZE_MB):
                raise ValueError(f"Archivo descargado es muy pequeño: {CSV_FILE_PATH.stat().st_size / (1024*1024):.2f} MB")
            
            logger.info(f"Archivo descargado exitosamente: {CSV_FILE_PATH}")
        
        return str(CSV_FILE_PATH)
        
    except Exception as e:
        error_msg = f"Error descargando archivo: {str(e)}"
        logger.error(error_msg)
        send_alert("Error en descarga de datos", error_msg, logger)
        raise

