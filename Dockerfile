# Usa uma imagem base Python slim (menor e mais rápida)
FROM python:3.11-slim

# Define o diretório de trabalho dentro do contêiner
WORKDIR /app

# Instala pacotes do sistema operacional necessários para algumas bibliotecas Python (como pandas/pyarrow)
# Embora python:3.11-slim já traga a maioria, é bom garantir o gcc e o wheel.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copia o arquivo de requisitos e instala as dependências
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY Scraping.Dataframe_populate.py .

ENTRYPOINT ["python", "Scraping.Dataframe_populate.py"]
