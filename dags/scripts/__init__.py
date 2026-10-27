"""
Funciones de extraccion, transformacion y carga para el pipeline de ventas.
"""

from .constants import CSV_ENCODING, DEDUP_KEY, RECORD_TYPE_RETURN, RECORD_TYPE_SALE

from .extract import extraer_csv, combinar_fuentes
from .transform import (
    deduplicar,
    normalizar_codigos,
    estandarizar_fechas_utc,
    clasificar_registros,
    limpiar_customer_id,
    construir_mapa_canonico,
    aplicar_descripcion_canonica,
)
from .validate import validar_y_separar
from .revenue import calcular_revenue_bruto, calcular_resumen_diario
from .load import (
    obtener_engine,
    cargar_transacciones,
    cargar_rechazos,
    cargar_productos,
    cargar_resumen_diario,
)

__all__ = [
    "CSV_ENCODING",
    "DEDUP_KEY",
    "RECORD_TYPE_RETURN",
    "RECORD_TYPE_SALE",
    "extraer_csv",
    "combinar_fuentes",
    "deduplicar",
    "normalizar_codigos",
    "estandarizar_fechas_utc",
    "clasificar_registros",
    "limpiar_customer_id",
    "construir_mapa_canonico",
    "aplicar_descripcion_canonica",
    "validar_y_separar",
    "calcular_revenue_bruto",
    "calcular_resumen_diario",
    "obtener_engine",
    "cargar_transacciones",
    "cargar_rechazos",
    "cargar_productos",
    "cargar_resumen_diario",
]
