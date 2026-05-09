# 🌊 Airflow ETL - Calidad del Aire de Albacete

Este README explica cómo configurar y ejecutar el DAG de ETL que orquesta todo el proceso de extracción, transformación y carga de datos meteorológicos y de contaminación.

## Configuración Inicial

### 1. Importar Variables desde `vars.json`

Las variables se encuentran en `airflow/config/vars.json`. Para importarlas:

1. Accede a la **UI de Airflow** en `http://localhost:8080` (usuario: `airflow`, contraseña: `airflow`)
2. Ve a **Admin** → **Variables**
3. Haz clic en el botón **Upload File** (esquina superior derecha)
4. Selecciona el archivo `airflow/config/vars.json`

#### ⚠️ IMPORTANTE: Problema con el Parseador JSON

Cuando importas las variables desde el archivo JSON, Airflow convierte el valor `STATION_MAPPING` a un **string** en lugar de un diccionario JSON. Esto causa errores en el DAG cuando intenta deserializar la variable.

**Solución:**

1. Después de importar el archivo, ve a **Admin** → **Variables** nuevamente
2. Busca la variable `STATION_MAPPING`
3. Haz clic en ella para editarla
4. Copia el valor completo del archivo `vars.json` (la parte del JSON de estaciones):
   ```json
   {
     "avda_isabel_la_catolica_isleta": "Avda. Isabel La Católica (Isleta)",
     "ctra_madrid_esq_cronista": "Ctra. Madrid esq. Cronista",
     "isleta_paseo_cuba_ranas": "Isleta Paseo Cuba (Ranas)",
     "paseo_cuba_fabrica_harinas": "Paseo Cuba (Fábrica Harinas)",
     "plaza_isabel_ii": "Plaza Isabel II"
   }
   ```
5. **Pega el valor completo** en el campo de la variable (reemplazando el string que está ahí)
6. Guarda los cambios

### 2. Configurar Pools

Los pools limitan la concurrencia de tareas para evitar sobrecargar el sistema. Accede a **Admin** → **Pools** y verifica que existan:

- **`scraper_pool`** - Limita la ejecución simultánea de scrapers para que el servidor no se sature con muchas peticiones
- **`transformer_pool`** - Limita la transformación de datos (por defecto: 4 slots)
  - Recomendación: Puede ser más permisivo que scraper_pool porque los archivos ya estan descargados

## El DAG: `etl_ab_aire`

### Descripción General

El DAG `etl_ab_aire` automatiza el flujo completo de datos:

```
Scraper (Bronce) → Transformer Plata → Transformer Oro
```

**Configuración del DAG:**
- **Frecuencia:** Mensual (`@monthly`)
- **Inicio:** 01/01/2024
- **Fin:** 01/03/2026
- **Catchup:** Habilitado (recupera automáticamente datos históricos)
- **Máximo de runs activos:** 2
- **Máximo de tareas activas:** 6
- **Reintentos:** 1 intento automático en caso de fallo (espera 5 minutos)

### Estructura del Flujo

#### 1. **Stage de Bronce: Scraping de Datos**

Para cada estación configurada en `STATION_MAPPING`, se ejecutan 2 scrapers en paralelo:

- **`scraper_meteo_bronce`** - Extrae parámetros meteorológicos (R, DD, VV, TMP, PRB, HR)
- **`scraper_contaminante_bronce`** - Extrae parámetros de contaminación (PM10, PM25, NO2, O3, SO2, CO)

Cada scraper:
- Utiliza **Playwright** en modo headless para automatizar el navegador
- Accede al portal Troposfera con las fechas del mes procesado
- Descarga un archivo Excel con los datos
- Guarda el archivo en **MinIO** (bucket `bronce`) con la estructura: `{AÑO}/{MES}/{TIPO_PARAMETRO}/{ESTACION}/`

#### 2. **Stage de Plata: Transformación**

Los datos se transforman y normalizan:

- **`transformer_meteo_plata`** - Normaliza columnas y formatos de datos meteorológicos
- **`transformer_contaminante_plata`** - Normaliza columnas y formatos de contaminación

Cada transformer:
- Lee los datos del bucket `bronce`
- Aplica renombramientos de columnas según `config.json`
- Normaliza tipos de datos y valores
- Guarda en **MinIO** (bucket `plata`)

#### 3. **Stage de Oro: Agregación Mensual**

Al final, después de procesar todas las estaciones:

- **`process_meteo_oro`** - Agrega todos los datos meteorológicos del mes
- **`process_contaminante_oro`** - Agrega todos los datos de contaminación del mes

Estos transformers:
- Leen todos los archivos de Plata del mes actual
- Consolidan y agregan en un archivo único por tipo
- Guardan en **MinIO** (bucket `oro`)

### Flujo Visual

```
Para cada estación:
  ├─ Scraper Meteo Bronce
  │  └─ Transformer Meteo Plata
  ├─ Scraper Contaminante Bronce
  │  └─ Transformer Contaminante Plata

Después de todas las estaciones:
  ├─ Process Meteo Oro (consolida)
  └─ Process Contaminante Oro (consolida)
```

## Variables Disponibles

| Variable | Descripción | Tipo |
|----------|-------------|------|
| `URL_PORTAL` | URL del portal Troposfera | String |
| `HORA_INICIAL` | Hora de inicio del rango | String (HH:MM) |
| `HORA_FINAL` | Hora de fin del rango | String (HH:MM) |
| `MINIO_ENDPOINT` | Dirección del servidor MinIO | String |
| `MINIO_ACCESS_KEY` | Usuario MinIO | String |
| `MINIO_SECRET_KEY` | Contraseña MinIO | String |
| `MINIO_BUCKET_BRONCE` | Bucket para datos brutos | String |
| `MINIO_BUCKET_PLATA` | Bucket para datos transformados | String |
| `MINIO_BUCKET_ORO` | Bucket para datos consolidados | String |
| `SCRAPER_DOCKER_IMAGE` | Imagen Docker del scraper | String |
| `TRANSFORMER_PLATA_DOCKER_IMAGE` | Imagen Docker del transformer Plata | String |
| `TRANSFORMER_ORO_DOCKER_IMAGE` | Imagen Docker del transformer Oro | String |
| `STATION_MAPPING` | Mapping estaciones (slug → nombre) | JSON Dict |

## Monitoreo y Ejecución

### Ver el DAG en la UI

1. Abre `http://localhost:8080`
2. Busca `etl_ab_aire` en la lista de DAGs
3. Haz clic para ver detalles:
   - **DAG View:** Estructura visual del DAG
   - **Graph View:** Dependencias entre tareas
   - **Tree View:** Historial de ejecuciones
   - **Calendar:** Ejecuciones pasadas y próximas

### Activar el DAG

Por defecto, el DAG viene **pausado**. Para activarlo:

1. En la lista de DAGs, cambia el toggle de `etl_ab_aire` a **On**
2. El DAG comenzará a ejecutar automáticamente según su programación (`@monthly`)
3. Si `catchup=True`, Airflow ejecutará automáticamente los meses faltantes

### Ejecutar una Ejecución Manual

Para probar o ejecutar un período específico:

1. Haz clic en el DAG `etl_ab_aire`
2. Haz clic en el botón **Trigger DAG** (esquina superior derecha)
3. Puedes especificar una fecha de ejecución (data_interval_start)
4. Haz clic en **Trigger**

### Ver Logs

Si una tarea falla:

1. Ve a **Graph View** o **Tree View**
2. Haz clic en la tarea fallida
3. Abre la pestaña **Log**
4. Busca el error en los logs

## Rendimiento y Consideraciones

### Tiempo Estimado

El scraping completo de todas las estaciones y parámetros toma aproximadamente **1.5 horas** en un Mac M1 con 8GB de RAM. Esto depende de:

- Número de estaciones configuradas
- Velocidad de la red y del portal Troposfera
- Capacidad de procesamiento de la máquina

### Optimizaciones

Si necesitas acelerar el proceso:

1. **Aumenta los slots del pool:** En `airflow/config/vars.json` o en la UI, aumenta `scraper_pool`
2. **Aumenta recursos del sistema:** Asigna más RAM y CPU a Docker
3. **Paralelización:** El DAG ya ejecuta múltiples estaciones en paralelo
