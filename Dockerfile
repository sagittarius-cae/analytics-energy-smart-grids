FROM python:3.10-slim

# Instalar dependencias esenciales del sistema de forma segura
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Copiar e instalar tus librerías de analítica y desarrollo
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Instalar Apache Airflow sobre el mismo entorno
RUN pip install --no-cache-dir "apache-airflow==2.7.3"

# Comando por defecto para el contenedor de Jupyter
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]
