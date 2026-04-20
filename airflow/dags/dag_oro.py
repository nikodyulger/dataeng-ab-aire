from datetime import datetime
from airflow.sdk import dag
from airflow.sdk import Variable
from airflow.sensors.external_task import ExternalTaskSensor

from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.standard.operators.empty import EmptyOperator


STATION_MAPPING = Variable.get(
    "STATION_MAPPING",
    deserialize_json=True,
)

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


@dag(
    dag_id="etl_oro",
    start_date=datetime(2024, 1, 1),
    end_date=datetime(2026, 1, 1),
    schedule="@monthly",
    catchup=True,
    max_active_runs=1,
    tags=["oro", "consolidacion"],
)
def dag_etl_oro():
    start = EmptyOperator(task_id="start")

    # Esperar a que dag_scraper_plata termine para este mes
    wait_for_plata = ExternalTaskSensor(
        task_id="wait_for_plata",
        external_dag_id="etl_webscraping_plata",
        mode="reschedule",
        poll_interval=60,
        allowed_states=["success"],
        execution_date_fn=lambda dt: dt,  # Esperar la misma fecha de ejecución
    )

    process_meteo = DockerOperator(
        task_id="process_meteo",
        image=Variable.get("TRANSFORMER_ORO_DOCKER_IMAGE"),
        environment={
            **ENV_VARS_MINIO,
            "TIPO_PARAMETROS": "METEO",
            "YEAR": "{{ data_interval_start.year }}",
            "MES": "{{ data_interval_start.month }}",
        },
        **DOCKER_CONFIG,
    )

    process_contaminante = DockerOperator(
        task_id="process_contaminante",
        image=Variable.get("TRANSFORMER_ORO_DOCKER_IMAGE"),
        environment={
            **ENV_VARS_MINIO,
            "TIPO_PARAMETROS": "CONTAMINANTE",
            "YEAR": "{{ data_interval_start.year }}",
            "MES": "{{ data_interval_start.month }}",
        },
        **DOCKER_CONFIG,
    )

    end = EmptyOperator(task_id="end")

    start >> wait_for_plata >> [process_meteo, process_contaminante] >> end


dag_etl_oro()
