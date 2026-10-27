"""
Calculo de ingresos brutos y resumen diario por producto.
"""

import logging
import pandas as pd

logger = logging.getLogger("airflow.task")


def calcular_revenue_bruto(df):
    """
    Calcula el total multiplicando Quantity * UnitPrice.
    En devoluciones la cantidad es negativa, por lo que el monto tambien sale negativo.
    """
    df["gross_revenue"] = (df["Quantity"] * df["UnitPrice"]).round(4)
    return df


def calcular_resumen_diario(df, mapa_canonico):
    """
    Agrupa ventas y devoluciones por dia y por producto, calculando:
    - Cantidad vendida y devuelta
    - Monto bruto y monto de devoluciones
    - Cantidad neta y monto neto (ventas - devoluciones)
    """
    # Sumar solo las ventas
    solo_ventas = df[df["record_type"] == "sale"]
    resumen_ventas = (
        solo_ventas
        .groupby(["transaction_date", "StockCode"])
        .agg(
            sales_quantity=("Quantity", "sum"),
            gross_revenue=("gross_revenue", "sum"),
        )
        .reset_index()
    )

    # Sumar solo las devoluciones
    solo_devoluciones = df[df["record_type"] == "return_or_adjustment"]
    resumen_devoluciones = (
        solo_devoluciones
        .groupby(["transaction_date", "StockCode"])
        .agg(
            returns_quantity=("Quantity", "sum"),
            returns_adjustment=("gross_revenue", "sum"),
        )
        .reset_index()
    )

    # Unir ambas tablas (outer para no perder dias donde solo hubo ventas o devoluciones)
    resumen = pd.merge(
        left=resumen_ventas,
        right=resumen_devoluciones,
        on=["transaction_date", "StockCode"],
        how="outer",
    ).fillna(0)

    # Calcular totales netos
    resumen["net_quantity"] = resumen["sales_quantity"] + resumen["returns_quantity"]
    resumen["net_revenue"] = resumen["gross_revenue"] + resumen["returns_adjustment"]

    # Agregar el nombre oficial del producto
    resumen["canonical_description"] = (
        resumen["StockCode"].map(mapa_canonico).fillna("UNKNOWN")
    )

    resumen = resumen.rename(columns={"StockCode": "stock_code"})
    logger.info("Resumen diario calculado: %d filas", len(resumen))
    return resumen
