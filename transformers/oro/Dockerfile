FROM python:3.13-slim

WORKDIR /transformer

COPY ../requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY transformer_oro.py .

CMD ["python", "transformer_oro.py"]