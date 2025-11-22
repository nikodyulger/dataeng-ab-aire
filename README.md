# Ingeniería de datos. Calidad del aire de Albacete

Este repositorio contiene una solución de ETL (Extract, Transform, Load) diseñada para extraer datos históricos de las estaciones meteorológicas de Albacete


## Requisitos del Sistema
Esta configuración utiliza Docker Compose para levantar todos los servicios, por lo que los requisitos principales son:

Docker Engine y Docker Compose (versión 2.x o superior).

Recursos: Se recomienda un mínimo de 8 GB de RAM y 2 núcleos de CPU dedicados a Docker para asegurar la estabilidad, especialmente cuando se ejecutan los contenedores de scraping en paralelo.

## Arquitectura del Entorno
Este entorno utiliza una versión minimizada de Airflow que solo incluye los servicios esenciales para correr tu ETL.
| Servicio | Propósito | Imagen |
| :--- | :--- | :--- |
| `airflow-apiserver` | Interfaz de usuario web (UI). | `apache/airflow:3.0.1` |
| `airflow-scheduler` | Monitorea los DAGs y lanza las tareas. | `apache/airflow:3.0.1` |
| `postgres` | Base de datos de metadatos de Airflow. | `postgres:16` |
| `minio` | Almacenamiento de datos (capa Bronze/Landing). | `quay.io/minio/minio` |

## Accesos por Defecto

Los accesos a los servicios se ha mantenido usuarios por defecto por facilidad de uso dado que esto no es un entorno productivo, sino local

| Plataforma | URL | Usuario | Contraseña |
| :--- | :--- | :--- | :--- |
| **Airflow UI** | `http://localhost:8080` | `airflow` | `airflow` |
| **MinIO Console**| `http://localhost:9001` | `minioadmin` | `minioadmin` |


## Guía de Uso Rápido

1. **Inicializar la Base de Datos**
Antes de iniciar los servicios principales, es fundamental inicializar la base de datos de Airflow y crear el usuario administrador.
```bash
docker compose up airflow-init
```
2. **Levantar el Entorno**
Una vez inicializado, levanta todos los servicios en segundo plano
```bash
docker compose up -d 
```

3. **Parar entorno**

Para detener todos los servicios y seguir trabajando despues con todos los datos:
```bash
docker compose down 
```

Para detener y eliminar todos los servicios, incluyendo la base de datos de Airflow y los volúmenes de MinIO, es decir, **borrar todo el historial y los datos**:
```bash
docker compose down --volumes --rmi all
```

## Sobre el DAG y el Web Scraper
1. El DAG (dag_scraper.py)

Está programado para ejecutarse mensualmente (@monthly) desde el 01/01/2023 hasta el 01/11/2025 que son las fechas desde la cual hay datos disponibles

**Backfill**: Está habilitado (`catchup=True`), lo que permite recuperar datos desde el inicio de forma automática una vez habilitado el DAG desde la interfaz de Airflow

**Ejecución**: Se utiliza `DockerOperator`para ejecutar el script de scraping de manera aislada.

**Red**: Está configurado con network_mode: `bridge`

**Variables**: Hay un fichero `vars.json` que debe ser importando desde la sección `Admin` -> `Variables` para que el DAG recoja dichos parámetros

2. El Scraper (extract_data.py)
El script utiliza Playwright en modo headless para interactuar con el portal web.

**Función**: Extrae datos para el rango de fechas y la estación definidas en las Variables de Airflow.

**Salida de Datos**: Los datos se guardan en MinIO con una clave estructurada por año, mes y tipo de parámetro (`{AÑO}/{MES}/{TIPO_PARAMETROS}/{ESTACION}`).

## Referencias Útiles
- [Airflow en Docker](https://airflow.apache.org/docs/apache-airflow/stable/howto/docker-compose/index.html)
- [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/stable/tutorial/taskflow.html)