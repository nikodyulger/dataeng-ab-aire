FROM python:3.13-slim

WORKDIR /transformer

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.json .
COPY transformer_plata.py .

CMD ["python", "transformer_plata.py"]