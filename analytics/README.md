# 📊 Análisis de Datos - Calidad del Aire de Albacete

Este directorio contiene notebooks de análisis y archivos CSV generados a partir de los datos transformados del ETL.

## Archivos CSV

### `meteo_ab.csv`

**Contenido:** Datos agregados de parámetros meteorológicos de todas las estaciones.

**Columnas principales:**
- `date` - Timestamp del registro
- `station_name` - Nombre de la estación meteorológica
- `R` - Radiación solar (W/m²)
- `DD` - Dirección del viento (grados)
- `VV` - Velocidad del viento (m/s)
- `TMP` - Temperatura (°C)
- `PRB` - Presión barométrica (hPa)
- `HR` - Humedad relativa (%)

**Período:** Enero 2024 - Marzo 2026

**Fuente:** Bucket `oro` de MinIO

---

### `contaminante_ab.csv`

**Contenido:** Datos agregados de parámetros de contaminación de todas las estaciones.

**Columnas principales:**
- `date` - Timestamp del registro
- `station_name` - Nombre de la estación de monitoreo
- `PM10` - Partículas menores a 10 micras (μg/m³)
- `PM25` - Partículas menores a 2.5 micras (μg/m³)
- `NO2` - Dióxido de nitrógeno (μg/m³)
- `O3` - Ozono (μg/m³)
- `SO2` - Dióxido de azufre (μg/m³)
- `CO` - Monóxido de carbono (mg/m³)

**Período:** Enero 2024 - Marzo 2026
**Fuente:** Bucket `oro` de MinIO

## Cómo Usar

### Ejecutar los Notebooks Localmente

1. **Instala las dependencias:**
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # En Mac/Linux
   # .venv\Scripts\activate  # En Windows
   pip install -r requirements.txt
   ```

2. **Abre Jupyter Lab:**
    Por comodidad yo suelo utilizar VsCode para ejecutar los notebooks, pero tambien puedes:
   ```bash
   jupyter lab
   ```

3. **Navega a los notebooks y ejecútalos:**
   - `analysis_meteo.ipynb`
   - `analysis_contaminante.ipynb`