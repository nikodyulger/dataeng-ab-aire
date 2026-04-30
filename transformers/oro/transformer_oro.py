import os
import io
import logging
import polars as pl
from dotenv import load_dotenv
from minio import Minio

if os.path.exists(".env"):
    load_dotenv()

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET_ORIGEN = os.getenv("MINIO_BUCKET_PLATA")
BUCKET_DESTINO = os.getenv("MINIO_BUCKET_ORO")
TIPO_PARAMETROS = os.getenv("TIPO_PARAMETROS")  # "METEO" o "CONTAMINANTE"
TARGET_YEAR = os.getenv("YEAR")
TARGET_MONTH = os.getenv("MES")

logging.basicConfig(level=LOGGING_LEVEL)
logger = logging.getLogger(__name__)


def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_ENDPOINT.startswith("https"),
    )


def list_objects(prefix: str, bucket: str):
    """Devuelve lista de objetos bajo un prefijo."""
    client = get_minio_client()
    return [
        obj.object_name
        for obj in client.list_objects(bucket, prefix=prefix, recursive=True)
    ]


def read_csv_from_minio(path: str, bucket: str) -> pl.DataFrame:
    """Lee CSV desde MinIO → Polars DataFrame."""
    client = get_minio_client()
    response = client.get_object(bucket, path)
    data = response.read()
    return pl.read_csv(io.BytesIO(data))


def write_parquet_to_minio(df: pl.DataFrame, bucket: str, object_name: str):
    client = get_minio_client()
    buffer = io.BytesIO()
    df.write_parquet(buffer)
    buffer.seek(0)
    client.put_object(
        bucket,
        object_name,
        data=buffer,
        length=len(buffer.getvalue()),
        content_type="application/octet-stream",
    )


def calculate_wind_chill(temp_c: pl.Expr, wind_speed_kmh: pl.Expr) -> pl.Expr:
    """
    Sensación térmica con viento (wind chill) estima la temperatura equivalente que percibiría tu piel teniendo en cuenta el efecto del viento
    Fuente: Fórmula de la OMS/Environment Canada.
    **Aplica cuando temperatura ≤ 10°C y velocidad del viento > 4.8 km/h.**
    https://snowy.es/noticias/que-es-la-sensacion-termica-como-se-calcula
    ST = 13.12 + 0.6215 × T − 11.37 × V^0.16 + 0.3965 × T × V^0.16
    """
    return (
        13.12
        + 0.6215 * temp_c
        - 11.37 * (wind_speed_kmh**0.16)
        + 0.3965 * temp_c * (wind_speed_kmh**0.16)
    )


def transform_meteo(df: pl.DataFrame) -> pl.DataFrame:
    logger.info("Aplicando transformaciones METEO en oro")

    # Índice de sensación térmica
    df = df.with_columns(
        pl.when((pl.col("temp_c") <= 10) & (pl.col("vel_viento") > 4.8))
        .then(calculate_wind_chill(pl.col("temp_c"), pl.col("vel_viento")))
        .otherwise(pl.lit(None))
        .alias("wind_chill")
    )

    # Agregados diarios por estación (manteniendo la granularidad horaria original)
    daily_agg = (
        df.with_columns(_join_date=pl.col("fecha").dt.date())
        .group_by(["station_slug", "_join_date"])
        .agg(
            pl.col("temp_c").mean().alias("temp_c_daily_mean"),
            pl.col("temp_c").max().alias("temp_c_daily_max"),
            pl.col("temp_c").min().alias("temp_c_daily_min"),
            pl.col("temp_c").std().alias("temp_c_daily_std"),
            pl.col("humedad").mean().alias("humedad_daily_mean"),
            pl.col("pres").mean().alias("pres_daily_mean"),
            pl.col("vel_viento_ms").mean().alias("vel_viento_ms_daily_mean"),
            pl.col("ruido_db").mean().alias("ruido_db_daily_mean"),
        )
    )

    df = (
        df.with_columns(_join_date=pl.col("fecha").dt.date())
        .join(daily_agg, on=["station_slug", "_join_date"], how="left")
        .drop("_join_date")
    )

    return df


def transform_contaminante(df: pl.DataFrame) -> pl.DataFrame:
    logger.info("Aplicando transformaciones CONTAMINANTE en oro")

    # Conteo de excedencias por umbral
    # Fuentes: https://www.miteco.gob.es/content/dam/miteco/images/es/tabla_objetivos_tcm30-183435.pdf
    thresholds = {
        "pm10": 40,  # µg/m³ (anual)
        "pm25": 25,  # µg/m³ (anual)
        "no2": 40,  # µg/m³ (anual)
        "o3": 120,  # µg/m³ (diario)
        "so2": 125,  # µg/m³ (diario)
        "co": 10,  # mg/m³ (diario)
    }

    for contam, threshold in thresholds.items():
        if contam in df.columns:
            df = df.with_columns(
                (pl.col(contam) > threshold)
                .cast(pl.Int32)
                .alias(f"{contam}_exceeds_threshold")
            )

    # Agregados diarios por estación (manteniendo la granularidad horaria original)
    daily_agg = (
        df.with_columns(_join_date=pl.col("fecha").dt.date())
        .group_by(["station_slug", "_join_date"])
        .agg(
            pl.col("pm10").mean().alias("pm10_daily_mean"),
            pl.col("pm10").max().alias("pm10_daily_max"),
            pl.col("pm25").mean().alias("pm25_daily_mean"),
            pl.col("pm25").max().alias("pm25_daily_max"),
            pl.col("no2").mean().alias("no2_daily_mean"),
            pl.col("o3").mean().alias("o3_daily_mean"),
            pl.col("so2").mean().alias("so2_daily_mean"),
            pl.col("co").mean().alias("co_daily_mean"),
            pl.col("pm10_exceeds_threshold").sum().alias("pm10_hours_exceeding"),
            pl.col("pm25_exceeds_threshold").sum().alias("pm25_hours_exceeding"),
            pl.col("no2_exceeds_threshold").sum().alias("no2_hours_exceeding"),
            pl.col("o3_exceeds_threshold").sum().alias("o3_hours_exceeding"),
            pl.col("so2_exceeds_threshold").sum().alias("so2_hours_exceeding"),
            pl.col("co_exceeds_threshold").sum().alias("co_hours_exceeding"),
        )
    )

    df = (
        df.with_columns(_join_date=pl.col("fecha").dt.date())
        .join(daily_agg, on=["station_slug", "_join_date"], how="left")
        .drop("_join_date")
    )

    return df


def process_tipo_parametros(tipo_parametros: str):
    """
    tipo_parametros = METEO o CONTAMINANTE
    """

    prefix = f"tipo_parametro={tipo_parametros}/year={TARGET_YEAR}/month={TARGET_MONTH}"
    files = list_objects(prefix, BUCKET_ORIGEN)

    logger.info(f"Número de ficheros encontrados en el path {len(files)}")

    if not files:
        raise ValueError(f"No se encontraron ficheros en plata/{prefix}")

    dfs = []
    for f in files:
        df = read_csv_from_minio(f, BUCKET_ORIGEN)
        # Extraer station_slug del path
        parts = f.split("/")
        station_slug = parts[-1].replace(".csv", "")
        df = df.with_columns(
            (pl.lit(station_slug)).alias("station_slug"),
            (pl.lit(tipo_parametros)).alias("tipo_parametros"),
        )
        dfs.append(df)

    df = pl.concat(dfs, how="vertical")

    logger.info(f"DataFrame concatenado: {df.shape}")

    if "fecha" not in df.columns:
        raise ValueError(
            "La columna 'fecha' es obligatoria para el transformador de oro."
        )

    df = df.with_columns(pl.col("fecha").str.to_datetime(strict=False))

    if TIPO_PARAMETROS == "METEO":
        df = transform_meteo(df)
    elif TIPO_PARAMETROS == "CONTAMINANTE":
        df = transform_contaminante(df)

    # Escribir parquet con particionamiento Hive
    output_path = f"tipo_parametros={TIPO_PARAMETROS}/year={TARGET_YEAR}/month={TARGET_MONTH}/consolidated_data.parquet"
    write_parquet_to_minio(df, BUCKET_DESTINO, output_path)

    logger.info(f"[OK] Guardado: {BUCKET_DESTINO}/{output_path}")


def main():
    if not TIPO_PARAMETROS:
        raise ValueError("No se recibió TIPO_PARAMETROS desde el DAG.")

    process_tipo_parametros(TIPO_PARAMETROS)


if __name__ == "__main__":
    main()
