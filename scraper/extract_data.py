import os
import re
import logging
import unicodedata
from datetime import datetime
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

if os.path.exists(".env"):
    load_dotenv()

LOGGING_LEVEL = os.getenv("LOGGING_LEVEL", "INFO")
URL_PORTAL = os.getenv("URL_PORTAL")
FECHA_INICIAL = os.getenv("FECHA_INICIAL")  # YYYY-MM-DD
FECHA_FINAL = os.getenv("FECHA_FINAL")  # YYYY-MM-DD
HORA_INICIAL = os.getenv("HORA_INICIAL")  # HH:MM
HORA_FINAL = os.getenv("HORA_FINAL")  # HH:MM
ESTACION_METEO = os.getenv("ESTACION_METEO")
TIPO_PARAMETROS = os.getenv("TIPO_PARAMETROS").upper()  # "CONTAMINANTE" or "METEO"
PARAMETROS_CONTAMINANTES = ["PM10", "PM25", "NO2", "O3", "SO2", "CO", "CO2"]
PARAMETROS_METEO = ["R", "DD", "VV", "TMP", "PRB", "HR"]
PARAMETROS = (
    PARAMETROS_CONTAMINANTES if TIPO_PARAMETROS == "CONTAMINANTE" else PARAMETROS_METEO
)

logging.basicConfig(level=LOGGING_LEVEL)
logger = logging.getLogger(__name__)


def get_filename():
    """Convierte a formato seguro (sin espacios, acentos ni símbolos)"""

    station_name = re.sub(
        r"[^A-Za-z0-9]+",
        "_",
        unicodedata.normalize("NFKD", ESTACION_METEO)
        .encode("ascii", "ignore")
        .decode("ascii")
        .replace(".", ""),
    ).strip("_")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{station_name}_{timestamp}.xlsx"
    return filename


with sync_playwright() as p:

    logger.info(f"Iniciando navegador y accediendo a {URL_PORTAL}")
    browser = p.chromium.launch(headless=True)
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
    logger.info(f"Seleccionando estación: {ESTACION_METEO}")
    page.locator("ng-select[bindlabel='nombreestacion']").click()
    page.wait_for_selector(".ng-dropdown-panel .ng-option")
    page.locator(".ng-option-label", has_text=ESTACION_METEO).click()
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
    page.wait_for_selector("a:has-text('Descargar')", timeout=5000)

    # Hacer clic en "Descargar" y esperar el archivo
    logger.info("Descargando archivo Excel…")
    with page.expect_download() as download_info:
        page.locator("a:has-text('Descargar')").click()
    download = download_info.value

    # Guardar el archivo en carpeta local
    filename = get_filename()
    output_filename_path = os.path.join(os.getcwd(), f"data/{filename}")
    download_path = download.path()
    download.save_as(output_filename_path)
    logger.info(f"Archivo guardado: {output_filename_path}")

    browser.close()
