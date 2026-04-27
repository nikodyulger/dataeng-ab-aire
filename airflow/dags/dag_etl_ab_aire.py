from datetime import datetime, timedelta

from airflow.sdk import dag, task_group, Variable
from airflow.providers.docker.operators.docker import DockerOperator

STATION_MAPPING = Variable.get(
    "STATION_MAPPING",
    deserialize_json=True,
)
SCRAPER_DOCKER_IMAGE = Variable.get("SCRAPER_DOCKER_IMAGE")
TRANSFORMER_PLATA_DOCKER_IMAGE = Variable.get("TRANSFORMER_PLATA_DOCKER_IMAGE")
TRANSFORMER_ORO_DOCKER_IMAGE = Variable.get("TRANSFORMER_ORO_DOCKER_IMAGE")

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
    "MINIO_BUCKET_ORO": Variable.get("MINIO_BUCKET_ORO"),
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
    dag_id="etl_ab_aire",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2026, 3, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=2,
    max_active_tasks=6,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    tags=["bronce", "plata", "oro"],
)
def dag_etl_ab_aire():
    task_groups = []
    for slug, estacion in STATION_MAPPING.items():

        @task_group(group_id=f"station_{slug}")
        def station_group():
            for tipo in ("METEO", "CONTAMINANTE"):
                tipo_key = tipo.lower()
                scraper_task_id = f"scraper_{tipo_key}_bronce"
                transformer_task_id = f"transformer_{tipo_key}_plata"

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
                        "OBJECT_KEY": f"{{{{ task_instance.xcom_pull(task_ids='station_{slug}.scraper_{tipo_key}_bronce') }}}}",
                    },
                    pool=TRANSFORMER_POOL,
                    **DOCKER_CONFIG,
                )

                scraper >> transformer

        tg = station_group()
        task_groups.append(tg)

    # Procesamiento de oro
    process_meteo = DockerOperator(
        task_id="process_meteo_oro",
        image=TRANSFORMER_ORO_DOCKER_IMAGE,
        environment={
            **ENV_VARS_MINIO,
            "TIPO_PARAMETROS": "METEO",
            "YEAR": "{{ data_interval_start.year }}",
            "MES": "{{ data_interval_start.month }}",
        },
        **DOCKER_CONFIG,
    )

    process_contaminante = DockerOperator(
        task_id="process_contaminante_oro",
        image=TRANSFORMER_ORO_DOCKER_IMAGE,
        environment={
            **ENV_VARS_MINIO,
            "TIPO_PARAMETROS": "CONTAMINANTE",
            "YEAR": "{{ data_interval_start.year }}",
            "MES": "{{ data_interval_start.month }}",
        },
        **DOCKER_CONFIG,
    )

    # Los tasks de oro dependen de todos los task groups de plata
    for tg in task_groups:
        tg >> [process_meteo, process_contaminante]


dag_etl_ab_aire()
