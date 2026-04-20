from datetime import datetime, timedelta

from airflow.sdk import dag, task_group, Variable
from airflow.providers.docker.operators.docker import DockerOperator

STATION_MAPPING = Variable.get(
    "STATION_MAPPING",
    deserialize_json=True,
)
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

SCRAPER_POOL = "scraper_pool"
TRANSFORMER_POOL = "transformer_pool"


@dag(
    dag_id="etl_webscraping_plata",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    max_active_tasks=4,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["plata"],
)
def dag_webscraper_plata():
    for slug, estacion in STATION_MAPPING.items():

        @task_group(group_id=f"station_{slug}")
        def station_group():
            for tipo in ("METEO", "CONTAMINANTE"):
                tipo_key = tipo.lower()
                scraper_task_id = f"scraper_{tipo_key}"
                transformer_task_id = f"transformer_{tipo_key}"

                scraper = DockerOperator(
                    task_id=scraper_task_id,
                    image=SCRAPER_DOCKER_IMAGE,
                    environment={
                        **ENV_VARS_SCRAPERS,
                        **ENV_VARS_MINIO,
                        "SLUG": slug,
                        "ESTACION": estacion,
                        "TIPO_PARAMETROS": tipo,
                        "OUTPUT_DIR": f"/tmp/{slug}/{tipo_key}",
                    },
                    do_xcom_push=True,
                    pool=SCRAPER_POOL,
                    **DOCKER_CONFIG,
                )

                transformer = DockerOperator(
                    task_id=transformer_task_id,
                    image=TRANSFORMER_PLATA_DOCKER_IMAGE,
                    environment={
                        **ENV_VARS_MINIO,
                        "TIPO_PARAMETROS": tipo,
                        "OBJECT_KEY": f"{{{{ task_instance.xcom_pull(task_ids='station_{slug}.scraper_{tipo_key}') }}}}",
                    },
                    pool=TRANSFORMER_POOL,
                    **DOCKER_CONFIG,
                )

                scraper >> transformer

        station_group()


dag_webscraper_plata()
