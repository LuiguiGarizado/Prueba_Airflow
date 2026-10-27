"""
Constantes usadas en el proceso de limpieza y transformacion.
"""

# Codificacion de los archivos CSV originales
CSV_ENCODING = "ISO-8859-1"

# Columnas para revisar si hay registros repetidos entre las dos fuentes
DEDUP_KEY = [
    "InvoiceNo",
    "StockCode",
    "Quantity",
    "UnitPrice",
    "InvoiceDate",
    "CustomerID",
]

# Etiquetas para clasificar el tipo de registro
RECORD_TYPE_SALE = "sale"
RECORD_TYPE_RETURN = "return_or_adjustment"
