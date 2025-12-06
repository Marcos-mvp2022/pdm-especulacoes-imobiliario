FROM python:3.10-slim

WORKDIR /app

# Instala as bibliotecas
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia o código do site
COPY . .

# O Streamlit roda na porta 8501 por padrão, mas o Cloud Run exige que
# a gente use a porta que ele manda (geralmente 8080).
# Vamos configurar o Streamlit para obedecer o Cloud Run.
CMD streamlit run frontend.py --server.port=$PORT --server.address=0.0.0.0
