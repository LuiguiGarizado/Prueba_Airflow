"""
Lectura y union de los archivos CSV.
"""

import logging
import pandas as pd
from .constants import CSV_ENCODING

logger = logging.getLogger("airflow.task")


def extraer_csv(ruta, nombre_fuente):
    """
    Lee un archivo CSV con pandas y agrega columnas para saber
    de cual archivo salio cada fila y su numero de linea original.
    """
    logger.info("Leyendo archivo %s desde %s", nombre_fuente, ruta)

    df = pd.read_csv(ruta, encoding=CSV_ENCODING)

    # Quitamos la columna index si viene en el archivo
    if "index" in df.columns:
        df = df.drop(columns=["index"])

    df["source_file"] = nombre_fuente
    df["source_row_index"] = range(len(df))

    logger.info("Total filas leidas: %d", len(df))
    return df


def combinar_fuentes(df_principal, df_secundario):
    """
    Une los dos DataFrames.
    Ponemos data.csv primero para darle prioridad al quitar duplicados.
    """
    combinado = pd.concat([df_principal, df_secundario], ignore_index=True)
    logger.info("Total combinado: %d filas", len(combinado))
    return combinado
