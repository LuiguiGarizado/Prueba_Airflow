"""
DAG para el proceso de ventas de DataMart.
Horario: todos los dias a las 6:00 PM hora Colombia (23:00 UTC).
Flujo: extract -> transform -> load.
"""

from __future__ import annotations

import json
import os
import urllib.parse
from datetime import datetime, timedelta

from airflow.sdk import dag, task, Variable

# Funciones de extraccion, limpieza y carga
from scripts import (
    extraer_csv,
    combinar_fuentes,
    deduplicar,
    normalizar_codigos,
    estandarizar_fechas_utc,
    clasificar_registros,
    limpiar_customer_id,
    construir_mapa_canonico,
    aplicar_descripcion_canonica,
    validar_y_separar,
    calcular_revenue_bruto,
    calcular_resumen_diario,
    obtener_engine,
    cargar_transacciones,
    cargar_rechazos,
    cargar_productos,
    cargar_resumen_diario as cargar_resumen_db,
)

# Carpetas de datos dentro del contenedor
DATA_DIR = "/opt/airflow/data"
PROCESSED_DIR = "/opt/airflow/data/processed"


# Armamos la conexion a PostgreSQL usando las variables de entorno
def _construir_dw_conn() -> str:
    user = urllib.parse.quote_plus(os.environ.get("DW_USER", "admin_dw"))
    pwd = urllib.parse.quote_plus(os.environ.get("DW_PASSWORD", "analytics_password"))
    host = os.environ.get("DW_HOST", "postgres-analytics")
    port = os.environ.get("DW_PORT", "5432")
    db = os.environ.get("DW_DB", "data_warehouse")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


DW_CONN = _construir_dw_conn()


@dag(
    dag_id="datamart_etl_ventas",
    description="ETL de ventas y devoluciones hacia PostgreSQL analitico",
    schedule="0 23 * * *",
    start_date=datetime(2026, 10, 25),
    catchup=False,
    default_args={
        "owner": "Luigui_Garizado",
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["datamart", "etl", "ventas", "postgres"],
)
def datamart_etl_ventas():
    # Tarea 1: Leer los archivos planos y unirlos
    @task
    def extract():
        """Lee los dos archivos CSV y los guarda unidos en formato parquet."""
        import pandas as pd

        carpeta_datos = Variable.get("RAW_CSV_PATH", DATA_DIR)
        os.makedirs(PROCESSED_DIR, exist_ok=True)

        ruta_data = os.path.join(carpeta_datos, "data.csv")
        ruta_online = os.path.join(carpeta_datos, "online_retail.csv")

        # Leemos cada archivo
        df_principal = extraer_csv(ruta=ruta_data, nombre_fuente="data.csv")
        df_secundario = extraer_csv(ruta=ruta_online, nombre_fuente="online_retail.csv")

        # Unimos ambos
        combinado = combinar_fuentes(
            df_principal=df_principal,
            df_secundario=df_secundario,
        )

        # Guardamos en parquet para que el siguiente paso sea mas rapido
        ruta_salida = os.path.join(PROCESSED_DIR, "combined_raw.parquet")
        combinado.to_parquet(ruta_salida, index=False)
        return ruta_salida

    # Tarea 2: Limpiar, validar y hacer los calculos
    @task
    def transform(ruta_combinado):
        """Aplica la limpieza, separa errores y calcula ventas y devoluciones."""
        import pandas as pd

        df = pd.read_parquet(ruta_combinado)

        # 1. Quitar filas duplicadas
        df = deduplicar(df)

        # 2. Dejar codigos de producto limpios y en mayusculas
        df = normalizar_codigos(df)

        # 3. Pasar fechas a formato UTC
        df = estandarizar_fechas_utc(df)

        # 4. Clasificar si es venta (cantidad > 0) o devolucion (cantidad <= 0)
        df = clasificar_registros(df)

        # 5. Limpiar identificacion de clientes
        df = limpiar_customer_id(df)

        # 6. Definir el nombre oficial de cada producto segun la moda
        mapa_productos = construir_mapa_canonico(df)
        df = aplicar_descripcion_canonica(df=df, mapa=mapa_productos)

        # 7. Separar registros validos de los rechazados por precio
        validos, rechazados = validar_y_separar(df)

        # 8. Calcular el total bruto de cada fila
        validos = calcular_revenue_bruto(validos)

        # 9. Calcular el resumen diario por producto (bruto y neto)
        resumen = calcular_resumen_diario(df=validos, mapa_canonico=mapa_productos)

        # Guardamos los archivos listos para cargar
        ruta_validos = os.path.join(PROCESSED_DIR, "valid.parquet")
        ruta_rechazos = os.path.join(PROCESSED_DIR, "rejected.parquet")
        ruta_resumen = os.path.join(PROCESSED_DIR, "daily_summary.parquet")
        ruta_mapa = os.path.join(PROCESSED_DIR, "canonical_map.json")

        validos.to_parquet(ruta_validos, index=False)
        rechazados.to_parquet(ruta_rechazos, index=False)
        resumen.to_parquet(ruta_resumen, index=False)

        with open(ruta_mapa, "w", encoding="utf-8") as archivo:
            json.dump(mapa_productos, archivo, ensure_ascii=False)

        return {
            "valid_path": ruta_validos,
            "rejected_path": ruta_rechazos,
            "summary_path": ruta_resumen,
            "canonical_map_path": ruta_mapa,
            "valid_count": len(validos),
            "rejected_count": len(rechazados),
            "summary_count": len(resumen),
            "products_count": len(mapa_productos),
        }

    # Tarea 3: Subir a PostgreSQL
    @task
    def load(rutas, **context):
        """Inserta los datos limpios en sus respectivas tablas."""
        import pandas as pd

        id_ejecucion = context["run_id"]
        engine = obtener_engine(cadena_conexion=DW_CONN)

        # Leemos los archivos que dejo transform
        validos = pd.read_parquet(rutas["valid_path"])
        rechazados = pd.read_parquet(rutas["rejected_path"])
        resumen = pd.read_parquet(rutas["summary_path"])

        with open(rutas["canonical_map_path"], "r", encoding="utf-8") as archivo:
            mapa_productos = json.load(archivo)

        # Cargamos cada tabla
        n_productos = cargar_productos(mapa_canonico=mapa_productos, engine=engine)
        n_transacciones = cargar_transacciones(df_validos=validos, engine=engine, run_id=id_ejecucion)
        n_rechazos = cargar_rechazos(df_rechazados=rechazados, engine=engine, run_id=id_ejecucion)
        n_resumen = cargar_resumen_db(df_resumen=resumen, engine=engine)

        engine.dispose()

        return {
            "productos_cargados": n_productos,
            "transacciones_cargadas": n_transacciones,
            "rechazos_cargados": n_rechazos,
            "resumen_diario_cargado": n_resumen,
        }

    # Orden de ejecucion: extract -> transform -> load
    ruta_combinado = extract()
    rutas_procesados = transform(ruta_combinado)
    load(rutas_procesados)


datamart_etl_ventas()
