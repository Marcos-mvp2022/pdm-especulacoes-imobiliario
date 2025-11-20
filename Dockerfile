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
# O --no-cache-dir garante que o contêiner final seja menor
RUN pip install --no-cache-dir -r requirements.txt

# Copia seu script Python (assumindo que o nome do seu arquivo é scraper_job.py ou algo similar)
# Substitua 'seu_script.py' pelo nome real do seu arquivo, ex: 'zap_scraper.py'
COPY seu_script.py .

# Define o comando de entrada. O ENTRYPOINT garante que a execução use a função main()
# e os argumentos (variáveis de ambiente) injetados pelo Cloud Run
ENTRYPOINT ["python", "Scraping.Dataframe_populate.py"]
