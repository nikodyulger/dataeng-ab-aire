
# 🌦️ Scraper de Datos de Estaciones Meteorológicas de Albacete

Este módulo automatiza la extracción de datos meteorológicos y de contaminación desde el portal [Troposfera Albacete](https://troposfera.es/datos/dev-albacete/#/dashboard).

El script `extract_data.py`:
- Lee variables desde `.env` o desde el entorno Docker
- Inicia un navegador Chromium con Playwright
- Rellena el formulario de fechas, estación y parámetros
- Ejecuta la consulta y descarga el resultado en Excel
- Guarda el archivo localmente
- Sube el Excel a MinIO para que el pipeline de Airflow continue

## Cómo funciona

1. El script carga variables de entorno y configura los parámetros.
2. Accede al portal y selecciona la estación.
3. Selecciona los parámetros (`METEO` o `CONTAMINANTE`).
4. Lanza la consulta y espera el botón de descarga.
5. Descarga el archivo `xlsx` y lo guarda en `OUTPUT_DIR`.
6. Sube ese archivo a MinIO en el bucket definido por `MINIO_BUCKET`.

> Nota: en el DAG de Airflow, este script usa las variables `SLUG` y `ESTACION` para nombrar el archivo y enviar el resultado al siguiente task vía XCom.

## Uso desde Docker

Construir la imagen:

```bash
docker build -t scraper-ab-aire .
```

Ejecutar el contenedor:

```bash
docker run --rm \
  --env-file .env \
  -v $(pwd)/data:/scraper/data \
  scraper-ab-aire
```

Esto generará archivos locales en `./data` y, además, subirá el Excel resultante a MinIO.

## Uso en local

Crear y activar el entorno virtual:

```bash
python -m venv .venv
source .venv/bin/activate      # En Mac/Linux
# .venv\Scripts\activate     # En Windows
```

Instalar dependencias:

```bash
pip install -r requirements.txt
playwright install
```

Ejecutar el script:

```bash
python extract_data.py
```

## Variables de entorno

Este script utiliza las siguientes variables:

| Variable | Descripción | Ejemplo |
| --- | --- | --- |
| `URL_PORTAL` | URL del portal Troposfera | `https://troposfera.es/datos/dev-albacete/#/analisis-de-datos` |
| `FECHA_INICIAL` | Fecha inicial en formato `YYYY-MM-DD` | `2025-09-01` |
| `FECHA_FINAL` | Fecha final en formato `YYYY-MM-DD` | `2025-09-30` |
| `HORA_INICIAL` | Hora inicial en formato `HH:MM` | `00:00` |
| `HORA_FINAL` | Hora final en formato `HH:MM` | `23:59` |
| `ESTACION` | Nombre exacto de la estación a seleccionar en el portal | `Avda. Isabel La Católica (Isleta)` |
| `SLUG` | Identificador corto de la estación para el nombre del archivo | `avda_isabel_la_catolica_isleta` |
| `TIPO_PARAMETROS` | `METEO` o `CONTAMINANTE` | `METEO` |
| `OUTPUT_DIR` | Carpeta local donde se guarda el Excel descargado | `data` |
| `MINIO_ENDPOINT` | Dirección y puerto de MinIO | `host.docker.internal:9000` |
| `MINIO_ACCESS_KEY` | Usuario MinIO | `minioadmin` |
| `MINIO_SECRET_KEY` | Contraseña MinIO | `minioadmin` |
| `MINIO_BUCKET` | Bucket de MinIO donde se sube el Excel | `bronce` |
| `LOGGING_LEVEL` | Nivel de logs | `INFO` |

### Ejemplo mínimo de `.env`

```ini
URL_PORTAL=https://troposfera.es/datos/dev-albacete/#/analisis-de-datos
FECHA_INICIAL=2025-09-01
FECHA_FINAL=2025-09-30
HORA_INICIAL=00:00
HORA_FINAL=23:59
ESTACION=Avda. Isabel La Católica (Isleta)
SLUG=avda_isabel_la_catolica_isleta
TIPO_PARAMETROS=METEO
OUTPUT_DIR=data
MINIO_ENDPOINT=localhost:9000 **si ejecutas por fuera del docker compose, sino host.docker.internal**
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
MINIO_BUCKET=bronce
LOGGING_LEVEL=INFO
```

## Salida y formato

- El archivo local se guarda en `OUTPUT_DIR/<SLUG>.xlsx`.
- El contenido se sube a MinIO en:
  `MINIO_BUCKET/<TIPO_PARAMETROS>/<AÑO>/<MES>/<SLUG>.xlsx`.
- El script imprime la clave `object_key` al final, lo que permite a Airflow usar esa ruta en la siguiente etapa.


## Observaciones
- `SLUG` y `OUTPUT_DIR` son necesarios para que el archivo tenga nombre único.
- Si el portal cambia de selectores o de estructura, puede ser necesario actualizar los selectores de Playwright.
