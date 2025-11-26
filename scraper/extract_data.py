import os
import re
import logging
import unicodedata
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
from minio import Minio

if os.path.exists(".env"):
    load_dotenv()

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")
URL_PORTAL = os.getenv("URL_PORTAL")
FECHA_INICIAL = os.getenv("FECHA_INICIAL")  # YYYY-MM-DD
FECHA_FINAL = os.getenv("FECHA_FINAL")  # YYYY-MM-DD
HORA_INICIAL = os.getenv("HORA_INICIAL")  # HH:MM
HORA_FINAL = os.getenv("HORA_FINAL")  # HH:MM
ESTACION = os.getenv("ESTACION")
TIPO_PARAMETROS = os.getenv("TIPO_PARAMETROS").upper()  # "CONTAMINANTE" or "METEO"
PARAMETROS_CONTAMINANTES = ["PM10", "PM25", "NO2", "O3", "SO2", "CO"]
PARAMETROS_METEO = ["R", "DD", "VV", "TMP", "PRB", "HR"]
PARAMETROS = (
    PARAMETROS_CONTAMINANTES if TIPO_PARAMETROS == "CONTAMINANTE" else PARAMETROS_METEO
)
BUCKET = os.getenv("MINIO_BUCKET")

logging.basicConfig(level=LOGGING_LEVEL)
logger = logging.getLogger(__name__)


def get_filename():
    """Convierte a formato seguro (sin espacios, acentos ni símbolos)"""

    station_name = re.sub(
        r"[^A-Za-z0-9]+",
        "_",
        unicodedata.normalize("NFKD", ESTACION)
        .encode("ascii", "ignore")
        .decode("ascii")
        .replace(".", ""),
    ).strip("_")

    filename = f"{station_name}.xlsx"
    return filename


def upload_to_minio(file_path, file_name, bucket_name):
    """Sube un archivo local a un bucket de MinIO."""
    client = Minio(
        os.getenv("MINIO_ENDPOINT"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=os.getenv("MINIO_ENDPOINT", "").startswith("https"),
    )

    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        logger.info(f"Bucket '{bucket_name}' creado en MinIO.")
    else:
        logger.info(f"Bucket '{bucket_name}' ya existe en MinIO.")

    year = datetime.strptime(FECHA_INICIAL, "%Y-%m-%d").year
    month = datetime.strptime(FECHA_INICIAL, "%Y-%m-%d").month
    object_key = f"{year}/{month}/{TIPO_PARAMETROS}/{file_name}"

    # Subir archivo
    client.fput_object(
        bucket_name=bucket_name, object_name=object_key, file_path=file_path
    )
    logger.info(f"Archivo subido a MinIO: {bucket_name}/{object_key}")

    return object_key


with sync_playwright() as p:

    logger.info(f"Iniciando navegador y accediendo a {URL_PORTAL}")
    browser = p.chromium.launch(
        headless=True, args=["--no-sandbox", "--disable-dev-shm-usage"]
    )

    page = browser.new_page()
    page.goto(URL_PORTAL)

    # Fechas
    logger.info(f"Rellenando fechas: {FECHA_INICIAL} → {FECHA_FINAL}")
    page.fill("input[formcontrolname='fecha_inicial']", FECHA_INICIAL)
    page.fill("input[formcontrolname='fecha_final']", FECHA_FINAL)

    # Horas
    logger.info(f"Rellenando horas: {HORA_INICIAL} → {HORA_FINAL}")
    page.fill("input[formcontrolname='hora_inicial']", HORA_INICIAL)
    page.fill("input[formcontrolname='hora_final']", HORA_FINAL)

    # No hay etiquetas "select" tradicionales
    # Angular carga los elementos dinámicamente al abrir el dropdown
    # Seleccionamos la estación por su nombre
    logger.info(f"Seleccionando estación: {ESTACION}")
    page.locator("ng-select[bindlabel='nombreestacion']").click()
    page.wait_for_selector(".ng-dropdown-panel .ng-option")
    page.locator(".ng-option-label", has_text=ESTACION).click()
    page.wait_for_timeout(1000)

    # Columnas de la tabla: vars meteorológicas o contaminantes
    logger.info(f"Seleccionando parámetros: {PARAMETROS}")
    page.locator("ng-select[bindlabel='name']").click()
    page.wait_for_selector(".ng-dropdown-panel .ng-option")

    for param in PARAMETROS:
        if not page.locator(".ng-dropdown-panel").is_visible():
            page.locator("ng-select[bindlabel='name']").click()
            page.wait_for_selector(".ng-dropdown-panel .ng-option")

        # Hacer clic en la opción
        page.locator(".ng-dropdown-panel-items .ng-option-label").get_by_text(
            param, exact=True
        ).click()
        logger.info(f"Parámetro seleccionado: {param}")
        page.wait_for_timeout(300)

    # Lanzamos la consulta
    page.locator("button:has-text('Consultar')").click()

    # Esperamos que aparezca el botón de descargar
    page.wait_for_selector("a:has-text('Descargar')", timeout=30000)

    # Hacer clic en "Descargar" y esperar el archivo
    logger.info("Descargando archivo Excel…")
    with page.expect_download() as download_info:
        page.locator("a:has-text('Descargar')").click()
    download = download_info.value

    # Guardar el archivo en carpeta local
    file_name = get_filename()
    output_filename_path = os.path.join(os.getcwd(), f"data/{file_name}")
    download_path = download.path()
    download.save_as(output_filename_path)
    logger.info(f"Archivo guardado: {output_filename_path}")

    object_key = upload_to_minio(
        file_path=output_filename_path,
        bucket_name=BUCKET,
        file_name=file_name,
    )

    print(object_key)  # utilizar para el xcom airflow siguiente tarea

    browser.close()
