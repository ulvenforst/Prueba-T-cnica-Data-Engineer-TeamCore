# Dockerfile para el pipeline de data engineering
FROM python:3.11-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y \
    sqlite3 \
    git \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements y instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar código del proyecto
COPY . .

# Crear directorios necesarios
RUN mkdir -p data/raw data/processed data/warehouse

# Exponer puerto para aplicaciones web (si es necesario)
EXPOSE 8080

# Comando por defecto
CMD ["python", "main.py", "--help"]
