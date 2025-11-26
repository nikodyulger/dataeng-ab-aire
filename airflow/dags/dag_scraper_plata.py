import os
import json
from datetime import datetime
from airflow.sdk import dag, task, task_group
from airflow.sdk import Asset, Variable
from airflow.providers.docker.operators.docker import DockerOperator

STATION_MAPPING = json.loads(os.getenv("STATION_MAPPING"))

SCRAPER_DOCKER_IMAGE = Variable.get("SCRAPER_DOCKER_IMAGE")
TRANSFORMER_PLATA_DOCKER_IMAGE = Variable.get("TRANSFORMER_PLATA_DOCKER_IMAGE")

ENV_VARS_SCRAPERS = {
    "URL_PORTAL": Variable.get("URL_PORTAL"),
    "HORA_INICIAL": Variable.get("HORA_INICIAL", "00:00"),
    "HORA_FINAL": Variable.get("HORA_FINAL", "23:59"),
    "FECHA_INICIAL": "{{ data_interval_start | ds }}",
    "FECHA_FINAL": "{{ data_interval_start.add(months=1).subtract(days=1) | ds }}",
}

ENV_VARS_MINIO = {
    "MINIO_ENDPOINT": Variable.get("MINIO_ENDPOINT"),
    "MINIO_ACCESS_KEY": Variable.get("MINIO_ACCESS_KEY"),
    "MINIO_SECRET_KEY": Variable.get("MINIO_SECRET_KEY"),
    "MINIO_BUCKET_BRONCE": Variable.get("MINIO_BUCKET_BRONCE"),
    "MINIO_BUCKET_PLATA": Variable.get("MINIO_BUCKET_PLATA"),
}

DOCKER_CONFIG = {
    "docker_url": "unix://var/run/docker.sock",
    "network_mode": "bridge",
    "auto_remove": "success",
    "mount_tmp_dir": False,
    "privileged": True,
}

for slug, estacion in STATION_MAPPING.items():

    @dag(
        dag_id=f"webscraping_{slug}",
        start_date=datetime(2024, 1, 1),
        end_date=datetime(2025, 11, 1),
        schedule="@monthly",
        catchup=True,
        max_active_runs=1,
        tags=[estacion],
    )
    def dag_webscraper_plata():

        @task_group(group_id="etl_meteo")
        def run_etl_meteo():

            scraper_meteo = DockerOperator(
                task_id="scraper_meteo",
                image=SCRAPER_DOCKER_IMAGE,
                environment={
                    **ENV_VARS_SCRAPERS,
                    **ENV_VARS_MINIO,
                    "ESTACION": estacion,
                    "TIPO_PARAMETROS": "METEO",
                },
                **DOCKER_CONFIG,
            )

            transformer_plata_meteo = DockerOperator(
                task_id="transformer_plata_meteo",
                image=TRANSFORMER_PLATA_DOCKER_IMAGE,
                environment={
                    **ENV_VARS_MINIO,
                    "OBJECT_KEY": "{{ task_instance.xcom_pull(task_ids='etl_meteo.scraper_meteo') }}",
                },
                **DOCKER_CONFIG,
            )

            scraper_meteo >> transformer_plata_meteo

        @task_group(group_id="etl_contaminante")
        def run_etl_contaminante():

            scraper_contaminante = DockerOperator(
                task_id=f"scraper_contaminante",
                image=SCRAPER_DOCKER_IMAGE,
                environment={
                    **ENV_VARS_SCRAPERS,
                    **ENV_VARS_MINIO,
                    "ESTACION": estacion,
                    "TIPO_PARAMETROS": "CONTAMINANTE",
                },
                **DOCKER_CONFIG,
            )

            transformer_plata_contam = DockerOperator(
                task_id="transformer_plata_contaminante",
                image=TRANSFORMER_PLATA_DOCKER_IMAGE,
                environment={
                    **ENV_VARS_MINIO,
                    "OBJECT_KEY": "{{ task_instance.xcom_pull(task_ids='etl_contaminante.scraper_contaminante') }}",
                },
                **DOCKER_CONFIG,
            )

            scraper_contaminante >> transformer_plata_contam

        run_etl_meteo() >> run_etl_contaminante()

    dag_webscraper_plata()
