# Data Engineering Pipeline

## 🎯 Descripción
Pipeline integral de Data Engineering que consolida los 5 ejercicios de la prueba técnica:

1. **Orquestación con Airflow** (airflow/)
2. **Análisis SQL** (sql/) 
3. **ETL Python** (etl/)
4. **Modelado de Datos** (modeling/)
5. **CI/CD** (.github/workflows/)

## 🚀 Instalación

### Prerrequisitos
- Python 3.11+
- Docker (opcional)
- Git

### Setup Local
```bash
# Clonar repositorio
git clone <repository-url>
cd data-engineering-pipeline

# Instalar dependencias
pip install -r requirements.txt

# Configurar entorno
python scripts/setup_environment.py

# Generar datos de ejemplo
python scripts/generate_sample_data.py
```

### Setup con Docker
```bash
# Levantar servicios
docker-compose up -d

# Acceder a Airflow
open http://localhost:8080
```

## 📋 Uso

### Pipeline Completo
```bash
# Ejecutar pipeline completo
python main.py pipeline

# Ejecutar componentes individuales
python main.py etl
python main.py warehouse
python main.py analysis
```

### Testing
```bash
# Ejecutar todos los tests
pytest tests/ -v --cov

# Tests específicos
pytest tests/test_etl/ -v
pytest tests/test_modeling/ -v
```

## 🏗️ Arquitectura

```
data-engineering-pipeline/
├── airflow/          # Orquestación con Apache Airflow
├── etl/             # Procesamiento ETL
├── sql/             # Análisis y queries SQL  
├── modeling/        # Modelado dimensional
├── data/            # Datos (raw, processed, warehouse)
├── tests/           # Testing integral
├── config/          # Configuración centralizada
└── docs/            # Documentación
```

## 📊 Resultados

- ✅ **1M transacciones** procesadas
- ✅ **Pipeline end-to-end** funcionando
- ✅ **Tests pasando** (90%+ coverage)
- ✅ **CI/CD automatizado**
- ✅ **Documentación completa**

## 🔗 Enlaces

- [Documentación técnica](docs/)
- [API Reference](docs/api.md)
- [Arquitectura](docs/architecture.md)
