import os
import io
import math
import json
import logging
import polars as pl
import polars.selectors as cs
from dotenv import load_dotenv
from minio import Minio

if os.path.exists(".env"):
    load_dotenv()

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
OBJECT_KEY = os.getenv("OBJECT_KEY")
BUCKET_ORIGEN = os.getenv("MINIO_BUCKET_BRONCE")
BUCKET_DESTINO = os.getenv("MINIO_BUCKET_PLATA")
CONFIG_FILE = "config.json"

logging.basicConfig(level=LOGGING_LEVEL)
logger = logging.getLogger(__name__)



def apply_rename(df: pl.DataFrame, object_key: str) -> pl.DataFrame:
    logger.info("Aplicando normalización de nombres de columna")
    config = load_config()
    target_mapping = config["RENAME_MAPS"]["CONTAMINANTE"] if "CONTAMINANTE" in object_key.upper() else config["RENAME_MAPS"]["METEO"]
    if "CONTAMINANTE" in object_key.upper():
        return df.rename(target_mapping)
    return df.rename(target_mapping)

def cast_numeric_cols(df: pl.DataFrame) -> pl.DataFrame:
    logger.info("Casting parámetros numéricos")
    numeric_cols = df.select(pl.exclude("fecha")).columns
    for col in numeric_cols:
        df = df.with_columns(pl.col(col).cast(pl.Float64, strict=False))
    return df

def round_numeric_cols(df: pl.DataFrame) -> pl.DataFrame:
    return df.with_columns(cs.numeric().round(2))

def impute_nulls(df: pl.DataFrame) -> pl.DataFrame:
    logger.info("Imputación de nulos")
    df = df.filter(~pl.all_horizontal(cs.numeric().is_null())) # filtramos filas con todos los parametros nulos -> suele ser la primera fila
    df = df.fill_null(strategy="forward") # Rellena huecos con el valor anterior (útil en time-series)
    df = df.fill_null(strategy="zero") # Rellena valores restantes con ceros
    return df

def add_time_features(df: pl.DataFrame) -> pl.DataFrame:

    if "fecha" not in df.columns:
        logger.warning("No se encontró columna 'fecha'. Omitiendo Feature Engineering de tiempo.")
        return df
    
    logger.info("Añadiendo características temporales")

    df = df.with_columns(pl.col("fecha").str.to_datetime(strict=False))

    return df.with_columns(
        [
            pl.col("fecha").dt.year().alias("year"),
            pl.col("fecha").dt.month().alias("month"),
            pl.col("fecha").dt.day().alias("day"),
            pl.col("fecha").dt.weekday().alias("weekday"),
            pl.col("fecha").dt.week().alias("week"),
            pl.col("fecha").dt.quarter().alias("quarter"),
        ]
    )


def add_meteo_features(df: pl.DataFrame) -> pl.DataFrame:
    logger.info("Añadiendo características nuevas METEO")

    # Punto de rocio -> Usaremos la fórmula de Magnus
    # gamma = ln(H/100) + (17.27*T)/(237.3+T) donde H es humedad relativa y T es temperatura ambiente
    # pto_rocio = (237.7 * gamma) / (17.27 - gamma)
    # Constante empíricas
    A = 17.27
    B = 237.7

    # Escala de Beaufort según fórmula oficial de la Wikipedia.
    # Se parte de la ecuación V = 0.836 * B^(3/2) (velocidad en m/s).
    # Para obtener B a partir de V, se despeja:
    #     B = (V / 0.836) ** (2/3)
    # Además, la velocidad original está en km/h, por lo que se convierte a m/s:
    #     V_m/s = V_km/h / 3.6

    
    return (
        df
        .with_columns([
            (pl.col("dir_viento") * math.pi / 180).cos().alias("viento_x"),
            (pl.col("dir_viento") * math.pi / 180).sin().alias("viento_y"),
            ((A * pl.col("temp_c") / (B+ pl.col("temp_c"))) + (pl.col("humedad") / 100).log()).alias("magnus_gamma"),
            (pl.col("vel_viento") * (1000.0 / 3600.0)).alias("vel_viento_ms")
        ])
        .with_columns([
            (B * pl.col("magnus_gamma") / (A - pl.col("magnus_gamma"))).alias("punto_rocio"),
            (pl.col("vel_viento_ms") / 0.836).pow(2/3).round(0).clip(0, 12).cast(pl.Int8).alias("beaufort")
        ])
        .drop("magnus_gamma")
    )

def categorize_contaminante(col: pl.Expr, rules: list[dict]) -> pl.Expr:
    """
       Simplificación del Indice de Calidad del Aire 
       Lo hacemos segun el valor de la medida en dicha hora y no segun promedios o ventanas temporales
       Tabla de referencia -> https://troposfera.es/datos/dev-albacete/#/contenidos/indice-calidad-del-aire
    """
    expr = None

    for r in rules:
        cond = (col >= r["min"]) & (col <= r["max"])
        result = pl.lit(r["categoria"])

        if expr is None:
            expr = pl.when(cond).then(result)
        else:
            expr = expr.when(cond).then(result)

    return expr.otherwise(pl.lit(None))

def add_contamintante_features(df: pl.DataFrame) -> pl.DataFrame:
    logger.info("Añadiendo características nuevas CONTAMINANTE")
    config = load_config()
    breakpoints = config['ICA_LIMITS']

    exprs = [
        (pl.col("pm10") + pl.col("pm25")).alias("pm_total")
    ]

    for contam, rules in breakpoints.items():
        if contam in df.columns:
            exprs.append(
                categorize_contaminante(pl.col(contam), rules)
                .alias(f"{contam}_ica_cat")
            )

    return df.with_columns(exprs)

def apply_transformations(df: pl.DataFrame, object_key: str) -> pl.DataFrame:

    df = apply_rename(df, object_key)
    df = cast_numeric_cols(df)
    df = impute_nulls(df)

    if "CONTAMINANTE" in object_key.upper():
        df = add_contamintante_features(df)
    else:
        df = add_meteo_features(df)

    df = round_numeric_cols(df)
    df = add_time_features(df)

    return df


def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_ENDPOINT.startswith("https"),
    )


def get_excel(bucket: str, object_key: str):

    logger.info(f"Estableciendo conexión MinIO y obteniendo objeto {object_key}")
    client = get_minio_client()

    response = client.get_object(bucket, object_key)
    excel_data = response.read()

    response.close()
    response.release_conn()

    file_buffer = io.BytesIO(excel_data)
    df = pl.read_excel(file_buffer)
    logger.info("Fichero cargado en memoria")

    return df


def store_csv(
    df: pl.DataFrame,
    bucket: str,
    object_key: str,
):

    client = get_minio_client()

    year, month, tipo_parametro, filename = object_key.split("/")
    output_key = f"{year}/{month}/{tipo_parametro}/{filename.replace(".xlsx", ".csv")}"

    logger.info(f"Preparando subida a minio://{bucket}/{output_key}")
    csv_bytes = df.write_csv().encode("utf-8")

    client.put_object(
        bucket,
        output_key,
        io.BytesIO(csv_bytes),
        length=len(csv_bytes),
        content_type="text/csv",
    )
    logger.info(f"Objeto subido con éxito")

def load_config():
    """Carga los mappings y límites ICA desde un archivo JSON."""
    try:
        with open(CONFIG_FILE, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        logger.error(f"Archivo de configuración '{CONFIG_FILE}' no encontrado. Asegúrate de crearlo.")
        raise
    except json.JSONDecodeError:
        logger.error(f"Error al decodificar el archivo '{CONFIG_FILE}'. Verifica el formato JSON.")
        raise


def main():

    if not OBJECT_KEY:
        raise ValueError("No se recibió OBJECT_KEY desde el DAG.")

    df = get_excel(BUCKET_ORIGEN, OBJECT_KEY)
    df = apply_transformations(df, OBJECT_KEY)
    store_csv(df, BUCKET_DESTINO, OBJECT_KEY)


if __name__ == "__main__":
    main()
