"""
Carga de datos procesados hacia las tablas de PostgreSQL.
"""

import json
import logging
import re
import pandas as pd
from sqlalchemy import create_engine, text, types as sa_types

logger = logging.getLogger("airflow.task")


def obtener_engine(cadena_conexion):
    """
    Crea la conexion a la base de datos con SQLAlchemy.
    """
    return create_engine(cadena_conexion)


def cargar_transacciones(df_validos, engine, run_id):
    """
    Guarda las transacciones validas en analytics.fact_transactions.
    Borra primero las filas del mismo archivo para no duplicar si se vuelve a correr.
    """
    if df_validos.empty:
        logger.warning("No hay transacciones validas para guardar")
        return 0

    registros = pd.DataFrame({
        "run_id": run_id,
        "source_file": df_validos["source_file"],
        "source_row_index": df_validos["source_row_index"],
        "invoice_no": df_validos["InvoiceNo"].astype(str),
        "stock_code": df_validos["StockCode"],
        "description": df_validos["Description"],
        "canonical_description": df_validos["canonical_description"],
        "quantity": df_validos["Quantity"],
        "unit_price": df_validos["UnitPrice"],
        "gross_revenue": df_validos["gross_revenue"],
        "record_type": df_validos["record_type"],
        "transaction_at": df_validos["transaction_at"],
        "transaction_date": df_validos["transaction_date"],
        "customer_id": df_validos["CustomerID"],
        "is_guest": df_validos["CustomerID"].isna(),
        "country": df_validos["Country"],
    })

    archivos = registros["source_file"].unique().tolist()

    with engine.begin() as conexion:
        for arch in archivos:
            conexion.execute(
                text("DELETE FROM analytics.fact_transactions WHERE source_file = :sf"),
                {"sf": arch},
            )

        registros.to_sql(
            name="fact_transactions",
            con=conexion,
            schema="analytics",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )

    logger.info("Guardadas %d filas en fact_transactions", len(registros))
    return len(registros)


def cargar_rechazos(df_rechazados, engine, run_id):
    """
    Guarda los registros descartados en audit.rejected_records junto con el motivo.
    """
    if df_rechazados.empty:
        logger.info("No hubo registros rechazados")
        return 0

    def _fila_json(f):
        return json.dumps({
            "InvoiceNo": str(f.get("InvoiceNo", "")),
            "StockCode": str(f.get("StockCode", "")),
            "Description": str(f.get("Description", "")),
            "Quantity": float(f["Quantity"]) if pd.notna(f["Quantity"]) else None,
            "UnitPrice": float(f["UnitPrice"]) if pd.notna(f["UnitPrice"]) else None,
            "InvoiceDate": str(f.get("InvoiceDate", "")),
            "CustomerID": str(f["CustomerID"]) if pd.notna(f.get("CustomerID")) else None,
            "Country": str(f.get("Country", "")),
        }, ensure_ascii=False)

    registros = pd.DataFrame({
        "run_id": run_id,
        "source_file": df_rechazados["source_file"],
        "source_row_index": df_rechazados["source_row_index"],
        "invoice_no": df_rechazados["InvoiceNo"].astype(str),
        "stock_code": df_rechazados["StockCode"],
        "quantity": df_rechazados["Quantity"],
        "unit_price": df_rechazados["UnitPrice"],
        "customer_id": df_rechazados["CustomerID"],
        "rejection_reason": df_rechazados["rejection_reason"],
        "raw_record": df_rechazados.apply(_fila_json, axis=1),
    })

    with engine.begin() as conexion:
        conexion.execute(
            text("DELETE FROM audit.rejected_records WHERE run_id = :rid"),
            {"rid": run_id},
        )

        registros.to_sql(
            name="rejected_records",
            con=conexion,
            schema="audit",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
            dtype={"raw_record": sa_types.JSON()},
        )

    logger.info("Guardados %d rechazos en rejected_records", len(registros))
    return len(registros)


def clasificar_categoria(desc):
    """
    Asigna una categoria al producto buscando palabras clave en su nombre.
    """
    if not desc or desc == "UNKNOWN":
        return "Sin categoría"
    d = str(desc).upper()
    if re.search(r"\b(BAG|LUGGAGE|HANDBAG|TOTE|SHOPPER|BACKPACK)\b", d):
        return "Bolsas y Empaques"
    if re.search(r"\b(HEART|LIGHT|CANDLE|HOLDER|LANTERN|T-LIGHT|LAMP)\b", d):
        return "Iluminación y Velas"
    if re.search(r"\b(MUG|CUP|BOWL|PLATE|TEAPOT|CUTLERY|GLASS|DISH|JUG|BOTTLE|BAKING|CAKE)\b", d):
        return "Cocina y Vajilla"
    if re.search(r"\b(BOX|TIN|STORAGE|DRAWER|JAR|CASE|BASKET|CHEST)\b", d):
        return "Almacenamiento y Cajas"
    if re.search(r"\b(CHRISTMAS|XMAS|TREE|SANTA|NOEL|STAR|SNOW|REINDEER)\b", d):
        return "Navidad y Temporada"
    if re.search(r"\b(CLOCK|MIRROR|FRAME|SIGN|WALL|DECORATION|WREATH|GARLAND|CUSHION|PICTURE)\b", d):
        return "Decoración y Hogar"
    if re.search(r"\b(PEN|PENCIL|NOTEBOOK|CARD|WRAP|TAPE|STICKER|STATIONERY|PAPER)\b", d):
        return "Papelería y Regalos"
    if re.search(r"\b(TOY|GAME|DOLL|PLUSH|CHILD|BABY|KIDS)\b", d):
        return "Juguetes e Infantil"
    return "Hogar y Accesorios"


def cargar_productos(mapa_canonico, engine):
    """
    Carga el catalogo de productos en analytics.dim_products con su categoria.
    """
    if not mapa_canonico:
        return 0

    productos = pd.DataFrame([
        {
            "stock_code": cod,
            "canonical_description": nombre,
            "category": clasificar_categoria(nombre),
        }
        for cod, nombre in mapa_canonico.items()
    ])

    with engine.begin() as conexion:
        conexion.execute(text("TRUNCATE TABLE analytics.dim_products"))

        productos.to_sql(
            name="dim_products",
            con=conexion,
            schema="analytics",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )

    logger.info("Guardados %d productos en dim_products", len(productos))
    return len(productos)


def cargar_resumen_diario(df_resumen, engine):
    """
    Carga el resumen de ingresos diarios en analytics.daily_product_revenue.
    """
    if df_resumen.empty:
        return 0

    with engine.begin() as conexion:
        conexion.execute(text("TRUNCATE TABLE analytics.daily_product_revenue"))

        df_resumen.to_sql(
            name="daily_product_revenue",
            con=conexion,
            schema="analytics",
            if_exists="append",
            index=False,
            method="multi",
            chunksize=5000,
        )

    logger.info("Guardadas %d filas en daily_product_revenue", len(df_resumen))
    return len(df_resumen)
