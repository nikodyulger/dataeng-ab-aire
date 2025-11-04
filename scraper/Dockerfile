FROM mcr.microsoft.com/playwright/python:v1.55.0-noble

WORKDIR /scraper

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENV LOG_LEVEL=INFO

CMD ["python", "extract_data.py"]