"""
Configuración centralizada para Data Engineering Pipeline
"""

import os
from pathlib import Path

# Rutas base
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"
WAREHOUSE_DATA_DIR = DATA_DIR / "warehouse"

# Base de datos
DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{WAREHOUSE_DATA_DIR}/warehouse.db")

# Configuración de logging
LOGGING_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'standard': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        },
    },
    'handlers': {
        'default': {
            'level': 'INFO',
            'formatter': 'standard',
            'class': 'logging.StreamHandler',
        },
    },
    'loggers': {
        '': {
            'handlers': ['default'],
            'level': 'INFO',
            'propagate': False
        }
    }
}

# Configuración de Airflow
AIRFLOW_CONFIG = {
    'DAG_DIR': BASE_DIR / "airflow" / "dags",
    'SCRIPTS_DIR': BASE_DIR / "airflow" / "scripts"
}

# Configuración ETL
ETL_CONFIG = {
    'CHUNK_SIZE': 10000,
    'COMPRESSION': 'snappy',
    'OUTPUT_FORMAT': 'parquet'
}
