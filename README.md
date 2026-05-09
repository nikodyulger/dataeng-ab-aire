# Ingeniería de datos: Calidad del aire de Albacete

Este repositorio implementa un pipeline ETL que extrae datos históricos de estaciones meteorológicas de Albacete, los transforma y consolida en distintos niveles de calidad (`bronce`, `plata`, `oro`).

## Resumen del flujo

El proyecto se compone de:

- `airflow/`: orquestación del DAG principal con Airflow
- `scraper/`: extracción de datos del portal de la [Red de Estaciones de Control de Calidad del Aire](https://troposfera.es/datos/dev-albacete/#/dashboard)
- `transformers/`: transformación y normalización de datos
- `analytics/`: notebooks y CSV finales para análisis

### Pipeline general

1. **Scraper**: descarga datos desde el portal web y sube Excel a MinIO
2. **Transformador Plata**: normaliza y limpia los datos en formato CSV
3. **Transformador Oro**: concatena y consolida datos finales en Parquet
4. **Análisis**: notebooks y CSV producidos a partir del bucket `oro`

## Requisitos del sistema

- **Docker Engine y Docker Compose** (versión 2.x o superior)

### Recursos recomendados

- **Mínimo:** 4 GB de RAM, 2 núcleos de CPU
- **Recomendado:** 8 GB de RAM, 4 núcleos de CPU
- **Óptimo:** 16 GB+ de RAM, 8+ núcleos de CPU

> ℹ️ Nota de rendimiento: el scraping completo de las 5 estaciones (10 tareas: 5 × 2 parámetros) toma aproximadamente **1.5 horas en un Mac M1 con 8GB de RAM**.

## Arquitectura del entorno

Este proyecto levanta una configuración de Airflow ligera con los servicios necesarios para ejecutar el DAG.

| Servicio | Propósito | Imagen |
| :--- | :--- | :--- |
| `airflow-apiserver` | UI de Airflow | `apache/airflow:3.0.1` |
| `airflow-scheduler` | Scheduler de Airflow | `apache/airflow:3.0.1` |
| `postgres` | Metadatos de Airflow | `postgres:16` |
| `minio` | Almacenamiento de archivos | `quay.io/minio/minio` |

## Accesos por defecto

| Plataforma | URL | Usuario | Contraseña |
| :--- | :--- | :--- | :--- |
| **Airflow UI** | `http://localhost:8080` | `airflow` | `airflow` |
| **MinIO Console** | `http://localhost:9001` | `minioadmin` | `minioadmin` |

## Arranque rápido

1. Inicializar Airflow:

```bash
docker compose up airflow-init
```

2. Levantar el entorno:

```bash
docker compose up -d
```

3. Detener los servicios conservando datos:

```bash
docker compose down
```

4. Detener y limpiar todo (datos + volúmenes + imágenes):

```bash
docker compose down --volumes --rmi all
```

## Documentación interna

- `airflow/README.md` — Configuración del DAG, variables, pools y detalles de ejecución
- `scraper/README.md` — Cómo ejecutar el scraper, variables de entorno y salida de datos
- `transformers/README.md` — Funcionamiento de los transformers `plata` y `oro`
- `analytics/README.md` — Descripción de los CSV finales y notebooks de análisis

## Archivos de análisis finales

El directorio `analytics/` incluye:

- `meteo_ab.csv` — Dataset meteorológico agregado
- `contaminante_ab.csv` — Dataset de contaminantes agregado

Estos archivos se generan a partir de los datos consolidados en `oro` y están pensados para análisis posteriores.

## Uso recomendado

- Ejecuta primero `docker compose up airflow-init`
- Levanta `docker compose up -d`
- Importa `airflow/config/vars.json` y corrige `STATION_MAPPING` si hace falta
- Activa el DAG `etl_ab_aire` en Airflow
- Revisa `scraper/README.md` y `transformers/README.md` para detalles de ejecución y debug