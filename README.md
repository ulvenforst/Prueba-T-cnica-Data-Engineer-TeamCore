# Data Engineering Pipeline - TeamCore

Pipeline de procesamiento de datos para análisis de transacciones financieras desarrollado como parte de la prueba técnica para Data Engineer en TeamCore.

## Estructura del Proyecto

```
data-engineering-pipeline/
├── data/                           # Datos del proyecto
│   ├── raw/                       # Datos originales (Git LFS)
│   │   ├── sample_transactions.csv # Dataset de transacciones
│   │   └── sample.log.gz          # Logs comprimidos
│   ├── processed/                 # Datos procesados
│   │   └── transactions.db        # Base de datos SQLite
│   └── warehouse/                 # Data warehouse
│       └── transactions_warehouse.db
├── sql/                           # Consultas SQL (Ejercicio 2)
│   ├── queries/                   
│   │   ├── tarea1_vista_resumen.sql          # Vista diaria por estado
│   │   ├── tarea2_usuarios_multiples_fallas.sql # Usuarios con >3 fallas
│   │   ├── tarea3_deteccion_anomalias.sql    # Detección de anomalías
│   │   └── tarea4_indices_triggers.sql       # Optimización BD
│   └── analysis_runner.py          # Ejecutor de consultas
├── etl/                           # Procesamiento ETL (Ejercicio 1)
│   ├── processor.py               # Procesador principal
│   ├── run_etl.py                # Script de ejecución
│   └── streaming_processor.py     # Procesamiento en streaming
├── modeling/                      # Modelado de datos (Ejercicio 3)
│   ├── warehouse.py              # Modelo dimensional
│   └── run_warehouse.py          # Construcción del warehouse
├── airflow/                       # Orquestación (Ejercicio 4)
│   ├── dags/
│   │   └── transactions_dag.py    # DAG principal
│   └── scripts/                   # Scripts de ETL
├── tests/                         # Pruebas automatizadas (Ejercicio 5)
│   ├── test_basic.py             # Pruebas básicas
│   └── test_comprehensive.py     # Pruebas integrales
├── scripts/                       # Utilidades
│   ├── generate_transactions.py   # Generador de datos
│   └── generate_logs.py          # Generador de logs
├── main.py                        # Script principal
├── validate_exercises.py          # Validador de ejercicios
└── requirements.txt               # Dependencias Python
```

## Requisitos del Sistema

- Python 3.8+
- Git LFS (para archivos de datos)
- SQLite3
- Docker (opcional)

## Instalación

### 1. Clonar el Repositorio

```bash
git clone https://github.com/ulvenforst/Prueba-T-cnica-Data-Engineer-TeamCore.git
cd data-engineering-pipeline
```

### 2. Configurar Git LFS

Los archivos de datos grandes están gestionados con Git LFS:

```bash
# Instalar Git LFS si no está instalado
git lfs install

# Descargar archivos LFS
git lfs pull
```

### 3. Instalar Dependencias

```bash
pip install -r requirements.txt
```

### 4. Verificar Datos

Confirmar que los archivos de datos se descargaron correctamente:

```bash
# Verificar archivos principales
ls -la data/raw/
# Debería mostrar:
# - sample_transactions.csv (~48MB)
# - sample.log.gz (~1MB)
```

## Ejecución

### Script Principal

```bash
# Ejecutar pipeline completo
python main.py --all

# Ejecutar solo ETL
python main.py --etl

# Ejecutar solo análisis SQL
python main.py --sql

# Ejecutar solo warehouse
python main.py --warehouse
```

### Ejercicios Individuales

#### Ejercicio 1: ETL
```bash
# Procesamiento ETL básico
python etl/run_etl.py

# Procesamiento con métricas
python etl/run_benchmark.py

# Procesamiento streaming
python etl/streaming_processor.py
```

#### Ejercicio 2: SQL Analytics
```bash
# Ejecutar todas las consultas
python sql/analysis_runner.py

# Resultados incluyen:
# - Vista resumen diaria por estado
# - Usuarios con >3 fallas en 7 días
# - Detección de anomalías en volúmenes
# - Análisis de optimización (índices/triggers)
```

#### Ejercicio 3: Data Warehouse
```bash
# Construir modelo dimensional
python modeling/run_warehouse.py

# Validar integridad del warehouse
python modeling/validate_warehouse.py
```

#### Ejercicio 4: Airflow
```bash
# Con Docker
docker-compose up airflow

# Sin Docker (desarrollo)
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow dags list
airflow dags trigger transactions_dag
```

#### Ejercicio 5: Testing
```bash
# Pruebas básicas
python -m pytest tests/test_basic.py -v

# Pruebas integrales
python -m pytest tests/test_comprehensive.py -v

# Cobertura completa
python -m pytest tests/ --cov=. --cov-report=html
```

### Validación Completa

```bash
# Ejecutar validador de todos los ejercicios
python validate_exercises.py
```

## Características Técnicas

### Gestión de Datos
- **Git LFS**: Archivos grandes (CSV, DB, logs comprimidos)
- **SQLite**: Base de datos ligera para desarrollo
- **Particionamiento**: Estrategia por fecha para escalabilidad

### Calidad de Código
- **Black**: Formateo automático
- **Flake8**: Linting
- **MyPy**: Verificación de tipos
- **Pytest**: Testing automatizado

### Arquitectura
- **ETL**: Procesamiento por lotes y streaming
- **Star Schema**: Modelo dimensional optimizado
- **SQL Analítico**: Consultas optimizadas para 1M+ registros
- **Airflow**: Orquestación y scheduling
- **Docker**: Contenedorización opcional

## Datos de Muestra

### sample_transactions.csv
- **Registros**: ~1,000,000 transacciones
- **Periodo**: 30 días de datos sintéticos
- **Campos**: order_id, user_id, amount, status, timestamp
- **Estados**: completed, failed, pending

### sample.log.gz
- **Formato**: Logs de aplicación comprimidos
- **Contenido**: Eventos del sistema, errores, métricas

## Rendimiento

### Métricas Clave
- **ETL**: ~50,000 registros/segundo
- **SQL**: Consultas <2 segundos (con índices)
- **Warehouse**: Construcción <30 segundos
- **Tests**: Cobertura >85%

### Optimizaciones
- Filtros temporales (últimos 30 días)
- Índices estratégicos por fecha y usuario
- Particionamiento lógico mensual
- Límites en consultas analíticas

## Troubleshooting

### Error: "File not found: sample_transactions.csv"
```bash
# Verificar Git LFS
git lfs status
git lfs pull

# Si persiste, regenerar datos
python scripts/generate_transactions.py
```

### Error: "Database locked"
```bash
# Cerrar conexiones SQLite abiertas
rm data/processed/*.db-wal data/processed/*.db-shm
```

### Error: "Memory error en pandas"
```bash
# Procesar en chunks más pequeños
export CHUNK_SIZE=10000
python etl/run_etl.py
```

## Contribución

### Estándares de Código
```bash
# Formatear código
black .

# Verificar estilo
flake8 .

# Verificar tipos
mypy .

# Ejecutar tests
pytest
```

### Commit Guidelines
```bash
# Formato: [EJERCICIO] Descripción
git commit -m "[ETL] Optimizar procesamiento por chunks"
git commit -m "[SQL] Agregar índice para consulta de anomalías"
git commit -m "[TEST] Mejorar cobertura de validaciones"
```

## Licencia

Este proyecto es parte de una prueba técnica para TeamCore.

---

**Nota**: Este pipeline procesa datos sintéticos generados para fines de evaluación. Los algoritmos y optimizaciones son aplicables a datos reales de producción.
