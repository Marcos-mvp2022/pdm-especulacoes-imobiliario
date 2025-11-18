FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copia dependências e instala
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY Scraping.Dataframe_pupulate.py .

ENV PYTHONUNBUFFERED=1

CMD ["python", "Scraping.Dataframe_pupulate.py"]