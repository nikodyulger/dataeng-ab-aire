import plotly.express as px
import plotly.graph_objects as go
import pandas as pd


def plot_boxplot_with_selector(df, vars_analisis, unidades, group_col="station_slug"):
    """
    Crea un Box Plot interactivo con unidades de medida dinámicas.
    """

    elements = sorted(df[group_col].unique())
    colors = px.colors.qualitative.Plotly
    fig = go.Figure()

    num_elements = len(elements)
    num_vars = len(vars_analisis)

    # 1. Crear Trazas
    for i, var in enumerate(vars_analisis):
        is_first = i == 0
        for j, item in enumerate(elements):
            fig.add_trace(
                go.Box(
                    y=df[df[group_col] == item][var],
                    name=item,
                    marker_color=colors[j % len(colors)],
                    visible=is_first,
                    boxpoints="outliers",
                    legendgroup=item,
                    showlegend=is_first,
                )
            )

    # 2. Crear Botones con Unidades Dinámicas
    buttons = []
    for i, var in enumerate(vars_analisis):
        visibility = [False] * (num_vars * num_elements)
        visibility[i * num_elements : (i + 1) * num_elements] = [True] * num_elements

        # Obtenemos la unidad o ponemos un string vacío si no existe
        u = unidades.get(var, "")
        label_eje_y = f"{var.replace('_', ' ').upper()} ({u})" if u else var.upper()

        buttons.append(
            dict(
                label=var.replace("_", " ").upper(),
                method="update",
                args=[
                    {"visible": visibility},
                    {
                        "title": f"Análisis de Outliers: {var.upper()}",
                        "yaxis.title.text": label_eje_y,
                    },
                ],
            )
        )

    # 3. Layout
    u_inicial = unidades.get(vars_analisis[0], "")
    label_inicial = (
        f"{vars_analisis[0].upper()} ({u_inicial})"
        if u_inicial
        else vars_analisis[0].upper()
    )

    fig.update_layout(
        updatemenus=[
            {
                "buttons": buttons,
                "direction": "down",
                "showactive": True,
                "x": 0.5,
                "xanchor": "center",
                "y": 1.2,
            }
        ],
        title=f"Análisis de Outliers: {vars_analisis[0].upper()}",
        xaxis_title=group_col.replace("_", " ").title(),
        yaxis_title=label_inicial,
        template="plotly_white",
    )

    fig.update_yaxes(autorange=True, fixedrange=False)

    return fig


def plot_timeseries_with_selector(
    df, vars_analisis, unidades, group_col="station_slug"
):
    """
    Crea un gráfico de líneas temporal interactivo con selector de variables.
    """
    # Preparamos datos: ordenamos por fecha para evitar líneas cruzadas
    df_plot = df.sort_values(["fecha"])
    elements = sorted(df_plot[group_col].unique())
    fig = go.Figure()

    num_elements = len(elements)
    num_vars = len(vars_analisis)

    # 2. Generar Trazas (una por variable y estación)
    for i, var in enumerate(vars_analisis):
        visible = i == 0
        for _, item in enumerate(elements):
            df_sub = df_plot[df_plot[group_col] == item]

            fig.add_trace(
                go.Scatter(
                    x=df_sub["fecha"],
                    y=df_sub[var],
                    name=item,
                    mode="lines",
                    visible=visible,
                    legendgroup=item,
                )
            )

    # 3. Crear Botones
    buttons = []
    for i, var in enumerate(vars_analisis):
        visibility = [False] * (num_vars * num_elements)
        visibility[i * num_elements : (i + 1) * num_elements] = [True] * num_elements

        u = unidades.get(var, "")
        label_y = f"{var.replace('_', ' ').upper()} ({u})" if u else var.upper()

        buttons.append(
            dict(
                label=var.replace("_", " ").replace("daily mean", "DIARIA").upper(),
                method="update",
                args=[
                    {"visible": visibility},
                    {
                        "title": f"Evolución Temporal: {var.replace('_', ' ').upper()}",
                        "yaxis.title.text": label_y,
                    },
                ],
            )
        )

    # 4. Layout y Herramientas Temporales
    u_init = unidades.get(vars_analisis[0], "")

    fig.update_layout(
        updatemenus=[
            {
                "buttons": buttons,
                "direction": "down",
                "showactive": True,
                "x": 0.5,
                "xanchor": "center",
                "y": 1.2,
            }
        ],
        title=f"Evolución Temporal: {vars_analisis[0].replace('_', ' ').upper()}",
        yaxis_title=f"{vars_analisis[0].upper()} ({u_init})",
        hovermode="x unified",
        legend_title=group_col.replace("_", " ").title(),
    )

    # Añadir el slider de tiempo
    fig.update_xaxes(rangeslider_visible=True)

    return fig


def plot_exceedance_days_with_selector(df, cols_exceeds):
    """
    Calcula los días de superación por año y crea un gráfico de barras interactivo.

    Args:
        df: DataFrame original con registros horarios.
        cols_exceeds: Lista de columnas tipo ['pm10_exceeds_threshold', 'no2_exceeds_threshold', ...]
    """
    # 1. Preparar datos: De registros horarios a días únicos de exceso
    df_temp = df.copy()
    df_temp["dia_fecha"] = df_temp["fecha"].dt.date
    stations = sorted(df_temp["station_slug"].unique())
    years = sorted(df_temp["year"].unique().astype(str))
    colors = px.colors.qualitative.Plotly

    fig = go.Figure()

    # Lista para guardar los dataframes procesados por cada contaminante
    # Esto facilita la creación de trazas
    num_vars = len(cols_exceeds)
    num_stations = len(stations)

    for i, col in enumerate(cols_exceeds):
        # Paso A: ¿Hubo exceso ese día? (Any)
        diario = (
            df_temp.groupby(["station_slug", "year", "dia_fecha"])[col]
            .any()
            .reset_index()
        )
        # Paso B: Sumar días por año
        anual = diario.groupby(["station_slug", "year"])[col].sum().reset_index()
        anual["year"] = anual["year"].astype(str)

        visible = i == 0

        for j, st in enumerate(stations):
            st_data = anual[anual["station_slug"] == st]

            # Aseguramos que todos los años estén presentes para que las barras no se desplacen
            # (Si un año no tiene excesos, ponemos 0)
            st_data = (
                st_data.set_index("year").reindex(years, fill_value=0).reset_index()
            )

            fig.add_trace(
                go.Bar(
                    x=st_data["year"],
                    y=st_data[col],
                    name=st,
                    marker_color=colors[j % len(colors)],
                    visible=visible,
                    legendgroup=st,
                    showlegend=(i == 0),
                    text=st_data[col],
                    textposition="auto",
                )
            )

    # 2. Crear botones del selector
    buttons = []
    for i, col in enumerate(cols_exceeds):
        visibility = [False] * (num_vars * num_stations)
        visibility[i * num_stations : (i + 1) * num_stations] = [True] * num_stations

        clean_name = col.replace("_exceeds_threshold", "").upper()

        buttons.append(
            dict(
                label=clean_name,
                method="update",
                args=[
                    {"visible": visibility},
                    {"title": f"Días al Año con Superación de Límites: {clean_name}"},
                ],
            )
        )

    # 3. Layout
    fig.update_layout(
        updatemenus=[
            {
                "buttons": buttons,
                "direction": "down",
                "showactive": True,
                "x": 0.5,
                "xanchor": "center",
                "y": 1.2,
            }
        ],
        title=f"Días al Año con Superación de Límites: {cols_exceeds[0].replace('_exceeds_threshold', '').upper()}",
        xaxis_title="Año",
        yaxis_title="Cantidad de Días",
        barmode="group",
        template="plotly_white",
        legend_title="Estaciones",
    )

    return fig


def plot_pie_with_selector(df, variables_ica):
    """
    Crea un gráfico de tarta (Pie Chart) interactivo para variables categóricas (ICA).
    """
    fig = go.Figure()

    # 1. Añadimos una traza por cada variable
    for i, var in enumerate(variables_ica):
        # Calculamos la distribución (frecuencias)
        ica_dist = df[var].value_counts(normalize=True).reset_index()
        ica_dist.columns = ["categoria", "proporcion"]

        fig.add_trace(
            go.Pie(
                labels=ica_dist["categoria"],
                values=ica_dist["proporcion"],
                name=var,
                visible=(i == 0),
                # Personalización estética
                hole=0.3,
                marker=dict(line=dict(color="#000000", width=1)),
            )
        )

    # 2. Crear los botones
    botones = []
    for i, var in enumerate(variables_ica):
        # Visibilidad simple: 1 traza por variable
        visibilidad = [False] * len(variables_ica)
        visibilidad[i] = True

        # Limpiamos el nombre para el título y la etiqueta
        clean_name = var.replace("_ica_cat", "").replace("_", " ").upper()

        botones.append(
            dict(
                label=clean_name,
                method="update",
                args=[
                    {"visible": visibilidad},
                    {"title": f"Distribución Calidad del Aire: {clean_name}"},
                ],
            )
        )

    # 3. Layout
    first_name = variables_ica[0].replace("_ica_cat", "").replace("_", " ").upper()

    fig.update_layout(
        updatemenus=[
            {
                "buttons": botones,
                "direction": "down",
                "showactive": True,
                "x": 0.5,
                "xanchor": "center",
                "y": 1.15,
            }
        ],
        title=f"Distribución Calidad del Aire: {first_name}",
        template="plotly_white",
        legend_title="Categorías ICA",
    )

    return fig


def plot_temporal_profile_with_selector(
    df_input, pollutants, col_x, unidades, group_col="station_slug"
):
    """
    Crea un gráfico de líneas temporal interactivo con selector de variables.
    Requiere un DataFrame ya pre-procesado (agrupado y ordenado).

    Args:
        df_input: DataFrame ya agrupado (ej: df_hourly o df_daily).
        pollutants: Lista de contaminantes (columnas Y).
        col_x: Nombre de la columna para el eje X ('hour' o 'day_name').
        group_col: Columna de agrupación (estaciones).
        unidades: Diccionario opcional de unidades.
    """
    stations = sorted(df_input[group_col].unique())
    colors = px.colors.qualitative.Plotly
    fig = go.Figure()

    # Determinamos el modo (puntos si es semanal, línea si es horario)
    mode = (
        "lines+markers"
        if df_input[col_x].dtype == "object" or df_input[col_x].dtype.name == "category"
        else "lines"
    )

    num_vars = len(pollutants)
    num_st = len(stations)

    # 2. Generar Trazas
    for i, pol in enumerate(pollutants):
        visible = i == 0
        for j, st in enumerate(stations):
            st_data = df_input[df_input[group_col] == st]

            fig.add_trace(
                go.Scatter(
                    x=st_data[col_x],
                    y=st_data[pol],
                    mode=mode,
                    name=st,
                    line=dict(color=colors[j % len(colors)]),
                    visible=visible,
                    legendgroup=st,
                    showlegend=(i == 0),
                )
            )

    # 3. Crear Botones
    buttons = []
    for i, pol in enumerate(pollutants):
        visibility = [False] * (num_vars * num_st)
        visibility[i * num_st : (i + 1) * num_st] = [True] * num_st

        u = unidades.get(pol, "")
        label_y = f"{pol.upper()} ({u})" if u else pol.upper()

        buttons.append(
            dict(
                label=pol.upper(),
                method="update",
                args=[
                    {"visible": visibility},
                    {
                        "title": f"Perfil Temporal: {pol.upper()}",
                        "yaxis.title": label_y,
                    },
                ],
            )
        )

    # 4. Layout
    fig.update_layout(
        updatemenus=[
            {
                "buttons": buttons,
                "direction": "down",
                "showactive": True,
                "x": 0.5,
                "xanchor": "center",
                "y": 1.15,
            }
        ],
        title=f"Perfil Temporal: {pollutants[0].upper()}",
        xaxis_title=col_x.replace("_", " ").title(),
        yaxis_title=f"{pollutants[0].upper()} ({unidades.get(pollutants[0], '')})",
        template="plotly_white",
        hovermode="x unified",
    )

    return fig
