"""
Validacion de datos y separacion de errores.
"""

import logging
import pandas as pd
from .constants import RECORD_TYPE_SALE

logger = logging.getLogger("airflow.task")


def validar_y_separar(df):
    """
    Separa los registros validos de los que tienen error:
    - Si es una venta y el precio unitario es <= 0 o nulo, se manda a rechazados.
    - Las devoluciones si se conservan porque se usan para calcular el ingreso neto.
    """
    es_venta = df["record_type"] == RECORD_TYPE_SALE
    precio_malo = df["UnitPrice"].isna() | (df["UnitPrice"] <= 0)

    # Solo rechazamos ventas con precio invalido
    debe_rechazarse = es_venta & precio_malo

    rechazados = df[debe_rechazarse].copy()
    rechazados["rejection_reason"] = "Venta con precio unitario cero, negativo o nulo"

    validos = df[~debe_rechazarse].copy()

    logger.info("Validacion lista: %d registros validos y %d rechazados", len(validos), len(rechazados))
    return validos, rechazados
