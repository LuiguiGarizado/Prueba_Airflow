# Justificacion del Diseno, Casos Ambiguos e Idempotencia

DataMart S.A.S. - Pipeline de ventas  
Autor: Luigui Garizado

---

## 1. Como disene el modelo del repositorio analitico y por que

Cuando empece a planear como estructurar la base de datos en PostgreSQL, mi meta principal fue que fuera practica y rapida para responder preguntas de negocio, sin complicarle la vida a quien vaya a escribir consultas ni sobrecargar el almacenamiento con datos repetidos.

Descarte dos extremos comunes:
- **No usar una sola tabla gigante:** Meter todo en una sola tabla plana hubiera significado repetir nombres largos de productos millones de veces, gastando espacio innecesario y haciendo que cualquier agrupacion por producto fuera lenta.
- **No sobre-normalizar:** Separar todo en 8 o 10 tablas relacionadas al estilo transaccional clasico dificulta los reportes, porque para responder una pregunta basica de ventas habria que hacer cinco JOINs distintos.

Por eso elegi un esquema estrella simplificado, compuesto por dos esquemas (`analytics` y `audit`) y cuatro tablas con responsabilidades bien definidas:

### A. analytics.dim_products (Dimension de productos)
Guarda el catalogo unico de productos con su codigo (`stock_code` como llave primaria), su nombre oficial y una categoria asignada por reglas de negocio. Esto nos da un unico lugar de referencia para los productos y evita guardar descripciones repetidas en las transacciones.

### B. analytics.fact_transactions (Tabla de hechos granular)
Guarda cada linea de detalle de las facturas que pasaron las validaciones. Una duda importante aqui fue: ¿debo separar las ventas de las devoluciones en tablas distintas? Decidi dejarlas en la misma tabla diferenciandolas con la columna `record_type` (`sale` o `return_or_adjustment`).

¿Por que? Porque una venta y una devolucion tienen exactamente los mismos datos (mismo cliente, misma factura, mismo producto, misma fecha y mismo precio). Si las separaba en dos tablas, cada vez que alguien quisiera calcular las ventas netas tendria que hacer un UNION ALL o un JOIN entre dos tablas gigantes. Dejandolas juntas, basta con filtrar por `record_type` o sumar el revenue directamente.

### C. audit.rejected_records (Tabla de auditoria)
Guarda las filas descartadas (por ejemplo ventas con precio menor o igual a cero), junto con el motivo del rechazo y una copia en formato JSON del registro tal como vino en el archivo original. Decidi ponerla en un esquema separado llamado `audit` para que el analista de negocio trabaje tranquilo en `analytics` sabiendo que no hay datos corruptos, mientras que el equipo tecnico puede auditar que filas fallaron y por que.

### D. analytics.daily_product_revenue (Tabla agregada)
Guarda el resumen consolidado por dia y por producto (unidades vendidas, unidades devueltas, monto bruto, monto devuelto y monto neto). En analitica, la mayoria de tableros consultan ventas por dia o por mes. Tener esta tabla precalculada permite que las consultas tarden milisegundos en vez de tener que escanear medio millon de filas cada vez que se abre un reporte.

---

## 2. Como resolvi cada uno de los casos ambiguos

### A. Transacciones sin Customer ID (clientes anonimos o invitados)
En los archivos CSV encontre mas de 130,000 filas (cerca del 25% de los datos) donde el campo CustomerID venia vacio.

- **Que decidi hacer y por que:** No eliminarlas. Si borraba esas filas, la empresa perderia mas de $1.47 millones en ventas netas reales registradas en las facturas. Para el negocio, una venta sin identificacion sigue siendo dinero que entro a caja.
- **Solucion tecnica:** Deje el campo `customer_id` como `NULL` en la base de datos y agregue una columna booleana `is_guest = TRUE` para indicar que fue una compra como invitado (y `FALSE` para clientes identificados). Asi los reportes financieros reflejan la plata real y ademas el equipo de marketing puede analizar por separado el comportamiento de los compradores que no se registran.

### B. Descripciones inconsistentes para un mismo producto
Al revisar el catalogo note que 572 productos tenian dos o mas descripciones distintas para el mismo codigo (por ejemplo: mayusculas vs minusculas, espacios de mas, o palabras cambiadas como `HEART T-LIGHT` y `WHITE HEART T-LIGHT`).

- **Que decidi hacer y por que:** Si dejaba las descripciones tal como venian, al hacer un GROUP BY por producto los reportes salian fragmentados en varias filas para el mismo articulo.
- **Solucion tecnica:** Para cada codigo de producto busque la descripcion mas frecuente (la moda estadistica), la limpie pasandola a mayusculas y la guarde como el nombre oficial (`canonical_description`) en `dim_products`. Al momento de cargar las transacciones, a todas las filas de ese codigo se les asigna su nombre oficial.

### C. Duplicados entre los dos archivos de Kaggle (data.csv y online_retail.csv)
Al analizar las dos fuentes de datos encontre que ambos archivos traian exactamente las mismas transacciones (un 100% de solapamiento). Si simplemente los unia sin filtrar, todas las ventas se duplicarian artificialmente al doble.

- **Que decidi hacer y por que:** Defini una clave compuesta para identificar duplicados reales usando seis campos: `(InvoiceNo, StockCode, Quantity, UnitPrice, InvoiceDate, CustomerID)`. Al concatenar ambos archivos le di prioridad a `data.csv` colocandolo primero, y aplique `drop_duplicates(keep='first')`. De esa forma conserve la version original de la fuente principal y elimine las copias repetidas, dejando exactamente 536,639 filas unicas.

### D. Precios menores o iguales a cero en ventas vs devoluciones
En los archivos aparecian filas con precios en cero o valores negativos.

- **Que decidi hacer y por que:** No todos los casos con precio raro son iguales. Si una fila es una venta normal (`Quantity > 0`) y tiene precio cero o negativo, no tiene sentido contable y representa un error del sistema de facturacion o una prueba. En cambio, si la fila es una devolucion o ajuste (`Quantity <= 0`), el registro es necesario para poder descontar la mercancia del inventario y del ingreso neto.
- **Solucion tecnica:** Aplique una regla selectiva. Solo rechazo filas cuando son ventas con precio menor o igual a cero o vacio, y las envio a `audit.rejected_records` explicando el motivo. Las devoluciones con precio raro se conservan en `fact_transactions` para mantener la trazabilidad de los reembolsos.

---

## 3. Como garantizo la idempotencia del DAG

Para mi, la idempotencia significa que no importa cuantas veces se ejecute el DAG para el mismo dia o con los mismos archivos fuente, el resultado final en la base de datos sea siempre exactamente el mismo: sin duplicar registros, sin inflar numeros y sin dejar tablas a medio camino si algo falla.

Para lograr esto en Airflow y PostgreSQL implemente los siguientes mecanismos en cada fase:

1. **Archivos intermedios limpios:** Las tareas de extraccion y transformacion escriben siempre sobre nombres de archivo fijos en formato parquet (`valid.parquet`, `rejected.parquet`, `daily_summary.parquet`). Si la tarea se vuelve a correr, sobreescribe el archivo anterior de forma limpia sin acumular archivos temporales.
2. **Borrado previo por archivo fuente en `fact_transactions`:** En la tarea de carga, antes de insertar las filas de un archivo fuente (por ejemplo `data.csv`), primero se ejecuta un `DELETE FROM analytics.fact_transactions WHERE source_file = :sf`. Si el DAG se reintenta, primero limpia lo que ese archivo habia cargado antes y luego inserta los datos limpios. Adicionalmente, la tabla tiene una restriccion unica `uq_fact_source (source_file, source_row_index)` que impide que a nivel de base de datos entre una fila repetida.
3. **Limpieza por corrida en `rejected_records`:** Antes de guardar los rechazos, se ejecuta un `DELETE FROM audit.rejected_records WHERE run_id = :rid`. Asi evitamos que al reiniciar una ejecucion se dupliquen las alertas de auditoria con el mismo identificador de corrida.
4. **Recarga limpia en dimensiones y resumenes:** Tanto `dim_products` como `daily_product_revenue` se cargan mediante un refresco completo (`TRUNCATE TABLE` seguido de `to_sql`). Esto asegura que el catalogo de productos y los acumulados diarios siempre tengan el calculo mas actualizado sin dejar filas viejas ni duplicadas. Ademas, `daily_product_revenue` tiene una llave unica `uq_daily_rev` sobre `(transaction_date, stock_code)`.
5. **Transacciones atomicas de base de datos:** Todas las operaciones de borrado e insercion en cada tabla se ejecutan dentro del bloque de contexto `with engine.begin() as conexion:`. Esto significa que si el contenedor se apaga o la conexion se interrumpe a mitad de una carga, PostgreSQL hace un `ROLLBACK` automatico. La base de datos nunca se queda con datos a medias o inconsistentes.
