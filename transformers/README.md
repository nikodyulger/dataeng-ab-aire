# 🔧 Transformers - ETL Plata y Oro

Este directorio contiene los transformadores que normalizan y consolidan los datos extraídos por el scraper.

- `transformers/plata/transformer_plata.py` procesa los datos brutos de MinIO (`bronce`) y los normaliza en `plata`.
- `transformers/oro/transformer_oro.py` consolida los datos de `plata` en un dataset final en `oro`.

## Arquitectura

### Plata

El transformer de plata:
- Descarga el archivo Excel desde MinIO (`bucket bronce`)
- Renombra y normaliza columnas según `config.json`
- Convierte valores numéricos
- Imputa nulos
- Añade nuevas características según el tipo de datos
- Vuelve a guardar el resultado en MinIO en formato CSV en el bucket `plata`

### Oro

El transformer de oro:
- Lee los CSV de `plata` para el mes y tipo de parámetro actual
- Concatena todos los ficheros de estaciones
- Aplica transformaciones de alto nivel
- Escribe un archivo Parquet consolidado en el bucket `oro`

## Configuración y variables necesarias

### Variables comunes

| Variable | Descripción |
| --- | --- |
| `MINIO_ENDPOINT` | URL/host de MinIO | 
| `MINIO_ACCESS_KEY` | Access key de MinIO | 
| `MINIO_SECRET_KEY` | Secret key de MinIO | 
| `LOGGING_LEVEL` | Nivel de log (INFO, DEBUG, WARNING, ERROR) |

### Variables de `transformer_plata.py`

| Variable | Uso |
| --- | --- |
| `MINIO_BUCKET_BRONCE` | Bucket origen de datos brutos |
| `MINIO_BUCKET_PLATA` | Bucket destino para datos normalizados |
| `OBJECT_KEY` | Ruta completa del Excel en `bronce` |
| `TIPO_PARAMETROS` | `METEO` o `CONTAMINANTE` |

`OBJECT_KEY` se recibe normalmente desde el task anterior del DAG de Airflow como XCom.

### Variables de `transformer_oro.py`

| Variable | Uso |
| --- | --- |
| `MINIO_BUCKET_PLATA` | Bucket origen con CSV normalizados |
| `MINIO_BUCKET_ORO` | Bucket destino para datos consolidados |
| `TIPO_PARAMETROS` | `METEO` o `CONTAMINANTE` |
| `YEAR` | Año objetivo de la consolidación |
| `MES` | Mes objetivo de la consolidación |

## Ejecución desde contenedor

### Transformer Plata

```bash
cd transformers/plata
docker build -t transformer-plata .
docker run --rm --env-file .env transformer-plata
```

### Transformer Oro

```bash
cd transformers/oro
docker build -t transformer-oro .
docker run --rm --env-file .env transformer-oro
```

> En Airflow, estos contenedores se ejecutan a través de `DockerOperator` con las variables necesarias inyectadas desde el DAG.

## Detalles de `config.json`

El archivo `transformers/plata/config.json` define:
- Mapeos de nombre de columnas para `METEO` y `CONTAMINANTE`
- Límites del Índice de Calidad del Aire (`ICA_LIMITS`)

### Renombramientos

- `METEO`: convierte columnas como `TMP (ºC)` a `temp_c`, `VV (km/h)` a `vel_viento`, `HR (%)` a `humedad`, etc.
- `CONTAMINANTE`: convierte columnas como `PM10 (µg/m³)` a `pm10`, `NO2 (µg/m³)` a `no2`, etc.

### Indicador de calidad del aire

`ICA_LIMITS` define las categorías para los contaminantes en función de su valor horario.

## Qué hace cada transformer

### `transformer_plata.py`

- Carga el Excel desde MinIO
- Renombra columnas con `load_config()` y `apply_rename()`
- Convierte los datos numéricos a `Float64`
- Imputa valores nulos con relleno hacia adelante y ceros
- Añade características adicionales:
  - `METEO`: velocidad de viento en m/s, `beaufort`, `punto_rocio`, vectores de viento, etc.
  - `CONTAMINANTE`: `pm_total` y categorías de calidad del aire por contaminante
- Redondea valores numéricos a 2 decimales
- Guarda el resultado en el bucket `plata`

### `transformer_oro.py`

- Enumera objetos de `plata` para el mes y tipo seleccionados
- Lee cada CSV y agrega la columna `station_slug`
- Concatena todos los datos en un único DataFrame
- Convierte `fecha` a datetime
- Aplica transformaciones finales:
  - `METEO`: cálculo de wind chill y agregados diarios por estación
  - `CONTAMINANTE`: conteo de excedencias y agregados diarios por estación
- Escribe `consolidated_data.parquet` en el bucket `oro`