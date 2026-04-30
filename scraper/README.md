
# 🌦️ Scraper Datos Estaciones Meteorológicas de  Albacete

Esta parte del proyecto automatiza la descarga de los datos meteorológicos y de contaminación del portal [Red de vigilancia ambiental del Ayuntamiento de Albacete](https://troposfera.es/datos/dev-albacete/#/dashboard)

El script permite seleccionar una estación meteorológica, rango de fechas, horas y otros parámetros, ejecutar la consulta en el navegador y descargar el resultado en formato **Excel (.xlsx)**.  Además, está preparado para funcionar tanto **en local** como dentro de un **contenedor Docker**.

## Descripción

El script script.py realiza las siguientes tareas:

Carga variables de entorno desde el archivo .env (si existe) o desde las variables del entorno Docker.

Inicia un navegador Chromium mediante Playwright.
El script script.py realiza las siguientes tareas:

Carga variables de entorno desde el archivo .env (si existe) o desde las variables del entorno Docker.

Inicia un navegador Chromium mediante Playwright.

1. Accede a la URL del portal Troposfera.
2. Rellena el formulario de búsqueda:

    - Fecha y hora inicial/final
    - Estación meteorológica
    - Parámetros meteorológicos o contaminantes

3. Lanza la consulta haciendo clic en *“Consultar”*.
4. Espera la tabla de resultados y el botón *“Descargar”*.
5. Descarga el archivo **Excel (.xlsx)** con los resultados.
6. Guarda el archivo localmente con un nombre único basado en la estación y fechas (por ejemplo:
`Avda_Isabel_La_Catolica_2025-09-01_00-00_a_2025-09-30_23-59.xlsx`).

## Poner en marcha

### Desde un contenedor

Crear la imagen

```bash
docker build -t scraper-ab-aire .
```

Ejecutar el contenedor con variables de entorno guardando los ficheros descargados en tu máquina a través de un volumen
```bash
docker run --rm \
  --env-file .env \
  -v $(pwd)/data:/scraper/data \
  scraper-ab-aire
```

**NOTA** En tu máquina aparecerá una carpeta *data* y dentro del contenedor el script está dejando el fichero dentro del WORKDIR `/scraper/data`
### Desde el entorno local

Creamos el entorno virtual

```bash
python -m venv .venv
source .venv/bin/activate      # En Linux/Mac
# o
.venv\Scripts\activate         # En Windows
```

Instalamos las dependencias
```bash
pip install -r requirements.txt
playwright install
```

Ejecutamos
```bash
python extract_data.py
```

## Variables de entorno
Las variables se definen en un archivo `.env` (para desarrollo local) o se pueden pasar directamente a Docker con `--env` o `--env-file`.

| Variable          | Descripción                                                        | Ejemplo                                                        |
| ----------------- | ------------------------------------------------------------------ | -------------------------------------------------------------- |
| `URL_PORTAL`      | URL del portal Troposfera a scrapear                               | `https://troposfera.es/datos/dev-albacete/#/analisis-de-datos` |
| `FECHA_INICIAL`   | Fecha inicial en formato `YYYY-MM-DD`                              | `2025-09-01`                                                   |
| `FECHA_FINAL`     | Fecha final en formato `YYYY-MM-DD`                                | `2025-09-30`                                                   |
| `HORA_INICIAL`    | Hora inicial en formato `HH:MM`                                    | `00:00`                                                        |
| `HORA_FINAL`      | Hora final en formato `HH:MM`                                      | `23:59`                                                        |
| `ESTACION_METEO`  | Nombre exacto de la estación                                       | `Avda. Isabel La Católica (Isleta)`                            |
| `TIPO_PARAMETROS` | Tipo de parámetros a consultar (`CONTAMINANTE` o `METEO`)          | `METEO`                                                        |
| `LOG_LEVEL`       | Nivel de detalle en los logs (`DEBUG`, `INFO`, `WARNING`, `ERROR`) | `INFO`                                                         |

Todas las `ESTACIONES` disponibles:

```json
{
    "almansa_esq_hnos_falco_hospital": "Almansa esq. Hnos. Falcó (Hospital)",
    "arg_vandelvira_el_ensanche": "Arq. Vandelvira (CSC El Ensanche)",
    "avda_espana_esq_tetuan": "Av. España esq. Tetuán",
    "avda_espana_frente_punta" :"Av. España frente Punta",
    "avda_toreros_cp_feria": "Av. Toreros frente C.P. Feria",
    "avda_isabel_la_catolica_isleta": "Avda. Isabel La Católica (Isleta)",
    "calle_caba_villacerrada": "Calle Caba Villacerrada",
    "ctra_madrid_esq_cronista": "Ctra. Madrid esq. Cronista",
    "isleta_paseo_cuba_ranas": "Isleta Paseo Cuba (Ranas)",
    "paseo_cuba_fabrica_harinas": "Paseo Cuba (Fábrica Harinas)",
    "paseo_feria_isleta_molino": "Paseo Feria Isleta Molino",
    "plaza_carretas": "Plaza Carretas",
    "plaza_isabel_ii": "Plaza Isabel II",
    "rosario_esquina_arq_vandelvira": "Rosario esquina Arquitecto Vandelvira",
    "seminario_hospital_perpetuo_socorro": "Seminario (Hospital Perpetuo Socorro)"
  }
```