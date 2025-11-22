from datetime import datetime
from airflow.sdk import dag, task_group
from airflow.sdk import Variable
from airflow.providers.docker.operators.docker import DockerOperator

ESTACION = Variable.get("ESTACION")
URL_PORTAL = Variable.get("URL_PORTAL")
HORA_INICIAL = Variable.get("HORA_INICIAL", "00:00")
HORA_FINAL = Variable.get("HORA_FINAL", "23:59")

MINIO_ENDPOINT = Variable.get("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = Variable.get("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = Variable.get("MINIO_SECRET_KEY")
MINIO_BUCKET = Variable.get("MINIO_BUCKET")
SCRAPER_DOCKER_IMAGE = Variable.get("SCRAPER_DOCKER_IMAGE")


env_vars = {
    "URL_PORTAL": URL_PORTAL,
    "HORA_INICIAL": HORA_INICIAL,
    "HORA_FINAL": HORA_FINAL,
    "FECHA_INICIAL": "{{ data_interval_start | ds }}",
    "FECHA_FINAL": "{{ data_interval_start.add(months=1).subtract(days=1) | ds }}",
    "ESTACION": ESTACION,
    "MINIO_ENDPOINT": MINIO_ENDPOINT,
    "MINIO_ACCESS_KEY": MINIO_ACCESS_KEY,
    "MINIO_SECRET_KEY": MINIO_SECRET_KEY,
    "MINIO_BUCKET": MINIO_BUCKET,
}

docker_config = {
    "image": SCRAPER_DOCKER_IMAGE,
    "docker_url": "unix://var/run/docker.sock",
    "network_mode": "bridge",
    "auto_remove": "success",
    "mount_tmp_dir": False,
    "privileged": True,
}


@dag(
    dag_id="webscraping",
    start_date=datetime(2023, 1, 1),
    end_date=datetime(2025, 11, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    tags=[ESTACION],
)
def dag_webscraper():

    @task_group(group_id="web_scrapers")
    def run_scrapers():

        scraper_meteo = DockerOperator(
            task_id=f"scraper_meteo",
            environment={**env_vars, "TIPO_PARAMETROS": "METEO"},
            **docker_config,
        )

        scraper_contaminante = DockerOperator(
            task_id=f"scraper_contaminante",
            environment={**env_vars, "TIPO_PARAMETROS": "CONTAMINANTE"},
            **docker_config,
        )

        scraper_meteo >> scraper_contaminante

    run_scrapers()


dag_webscraper()
