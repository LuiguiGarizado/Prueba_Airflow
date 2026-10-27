"""
Transformacion y limpieza de los datos de ventas.
"""

import logging
import numpy as np
import pandas as pd
from .constants import DEDUP_KEY, RECORD_TYPE_RETURN, RECORD_TYPE_SALE

logger = logging.getLogger("airflow.task")


def deduplicar(df):
    """
    Elimina filas repetidas entre los dos archivos usando la clave compuesta.
    Conserva la primera aparicion (que viene de data.csv).
    """
    filas_antes = len(df)
    df = df.drop_duplicates(subset=DEDUP_KEY, keep="first")
    filas_despues = len(df)

    logger.info("Deduplicacion: de %d a %d filas (se quitaron %d)", filas_antes, filas_despues, filas_antes - filas_despues)
    return df.reset_index(drop=True)


def normalizar_codigos(df):
    """
    Quita espacios en blanco alrededor de StockCode y lo pasa a mayusculas.
    """
    df["StockCode"] = df["StockCode"].astype(str).str.strip().str.upper()
    return df


def estandarizar_fechas_utc(df):
    """
    Convierte la columna InvoiceDate a fecha con hora en formato UTC.
    Tambien crea una columna con solo la fecha (dia) para agrupar reportes.
    """
    df["transaction_at"] = pd.to_datetime(
        df["InvoiceDate"],
        utc=True,
        dayfirst=False,
        errors="coerce",
    )
    df["transaction_date"] = df["transaction_at"].dt.date

    fechas_malas = df["transaction_at"].isna().sum()
    if fechas_malas:
        logger.warning("Hay %d fechas que no se pudieron leer", fechas_malas)

    return df


def clasificar_registros(df):
    """
    Clasifica cada fila segun la cantidad:
    - Si Quantity > 0 es venta (sale).
    - Si Quantity <= 0 es devolucion o ajuste (return_or_adjustment).
    """
    es_venta = df["Quantity"] > 0
    df["record_type"] = np.where(es_venta, RECORD_TYPE_SALE, RECORD_TYPE_RETURN)

    logger.info("Clasificacion: %d ventas y %d devoluciones", es_venta.sum(), (~es_venta).sum())
    return df


def limpiar_customer_id(df):
    """
    Limpia la columna de cliente:
    - Quita decimales raros (ejemplo: 17850.0 -> '17850').
    - Si no tiene cliente, se deja en None (NULL en la base de datos).
    """
    def _limpiar(val):
        if pd.isna(val):
            return None
        try:
            return str(int(float(val)))
        except (ValueError, TypeError):
            texto = str(val).strip()
            return texto if texto else None

    df["CustomerID"] = df["CustomerID"].apply(_limpiar)
    return df


def construir_mapa_canonico(df):
    """
    Busca la descripcion mas frecuente (la moda) para cada codigo de producto
    y la guarda en mayusculas para usarla como nombre oficial.
    """
    temp = df[["StockCode", "Description"]].copy()
    temp["desc_limpia"] = (
        temp["Description"]
        .fillna("")
        .astype(str)
        .str.strip()
        .str.upper()
    )

    # Ignoramos descripciones vacias o de solo 1 o 2 letras
    temp = temp[temp["desc_limpia"].str.len() > 2]

    # Para cada codigo, tomamos la descripcion que mas se repite
    mapa = (
        temp.groupby("StockCode")["desc_limpia"]
        .agg(lambda x: x.mode().iloc[0] if len(x) > 0 else "UNKNOWN")
        .to_dict()
    )

    logger.info("Catalogo oficial de nombres construido: %d productos", len(mapa))
    return mapa


def aplicar_descripcion_canonica(df, mapa):
    """
    Pone la columna canonical_description usando el nombre oficial del producto.
    """
    df["canonical_description"] = df["StockCode"].map(mapa).fillna("UNKNOWN")
    return df
