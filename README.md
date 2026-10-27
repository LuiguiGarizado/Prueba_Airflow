# DataMart S.A.S. - Pipeline de ventas con Airflow

Este proyecto toma los archivos CSV de ventas y devoluciones, limpia la información, separa los registros con errores y guarda los datos procesados en una base de datos PostgreSQL lista para consultas.

Flujo del proceso: extract (extraer) -> transform (transformar) -> load (cargar).

---

## Contenido

1. [Estructura del proyecto](#estructura-del-proyecto)
2. [Requisitos previos](#requisitos-previos)
3. [Instalacion de dependencias con uv (opcional)](#instalacion-de-dependencias-con-uv-opcional)
4. [Como poner a funcionar el proyecto paso a paso](#como-poner-a-funcionar-el-proyecto-paso-a-paso)
5. [Consultas SQL y respuestas de negocio](#consultas-sql-y-respuestas-de-negocio)
6. [Recomendaciones para el equipo de producto](#recomendaciones-para-el-equipo-de-producto)
7. [Reglas aplicadas a los datos](#reglas-aplicadas-a-los-datos)
8. [Casos confusos y decisiones tomadas](#casos-confusos-y-decisiones-tomadas)
9. [Problemas comunes y como resolverlos](#problemas-comunes-y-como-resolverlos)
10. [Como esta organizado el codigo](#como-esta-organizado-el-codigo)

---

## Estructura del proyecto

```
PruebaDesempeño/
├── dags/
│   ├── ETL_dag.py                # Definicion del DAG en Airflow
│   └── scripts/
│       ├── __init__.py           # Paquete con las funciones del proceso
│       ├── constants.py          # Constantes (codificacion, columnas, etc.)
│       ├── extract.py            # Lectura de los CSV
│       ├── transform.py          # Limpieza, fechas, clientes y descripciones
│       ├── validate.py           # Revision de precios y separacion de errores
│       ├── revenue.py            # Calculo de ingresos brutos y netos
│       └── load.py               # Guardado en PostgreSQL
├── data/
│   ├── data.csv                  # Archivo principal de ventas
│   └── online_retail.csv         # Archivo secundario de ventas
├── init-dw/
│   └── tablas.sql                # Creacion de tablas e indices en PostgreSQL
├── config/
│   └── airflow.cfg               # Configuracion interna de Airflow
├── docker-compose.yml            # Levanta Airflow y las dos bases de datos
├── docs/
│   ├── diseno_modelo_y_decisiones.docx # Documento Word con la justificacion de diseno
│   └── diseno_modelo_y_decisiones.md   # Version en texto del documento
├── .env.example                  # Ejemplo de variables de configuracion
├── .env                          # Archivo con tus variables reales
└── README.md                     # Esta guia
```

---

## Requisitos previos

- Docker y Docker Compose instalados.
- Un navegador web para ver la interfaz de Airflow.
- Opcional: DBeaver u otro cliente de bases de datos para revisar PostgreSQL.
- Opcional: uv si quieres correr los scripts en tu computador sin usar Docker.

---

## Instalacion de dependencias con uv (opcional)

Si solo vas a usar Docker, no necesitas instalar nada en tu maquina. Pero si quieres probar los scripts de Python directamente en tu computador:

### 1. Instalar uv (si no lo tienes)

- Windows (PowerShell):
  ```powershell
  powershell -ExecutionPolicy ByPass -c "irm https://astral.sh/uv/install.ps1 | iex"
  ```
- Mac o Linux:
  ```bash
  curl -LsSf https://astral.sh/uv/install.sh | sh
  ```
- O usando pip:
  ```bash
  pip install uv
  ```

### 2. Instalar las librerias

En la carpeta del proyecto ejecuta:

```bash
uv sync
```

Esto crea la carpeta `.venv` e instala las librerias necesarias (pandas, sqlalchemy, psycopg2-binary, pyarrow, openpyxl, numpy).

## Como poner a funcionar el proyecto paso a paso

Sigue estos pasos en orden, desde clonar el repositorio hasta verificar los datos cargados en PostgreSQL:

### Paso 1: Clonar el repositorio y entrar a la carpeta

Abre tu terminal (PowerShell en Windows o Bash en Linux/Mac) y descarga el proyecto:

```bash
git clone <URL_DEL_REPOSITORIO>
cd PruebaDesempeño
```

---

### Paso 2: Crear el archivo .env

Copia el archivo de ejemplo para crear tu archivo `.env`:

```bash
cp .env.example .env
```

*(En Windows PowerShell tambien puedes usar: `Copy-Item .env.example .env`)*.

El archivo `.env.example` ya viene con una llave Fernet valida y lista para usar. No es obligatorio generar una nueva a menos que quieras personalizarla:

- **Si quieres generar tu propia llave Fernet personalizada (opcional):**

  ```bash
  python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
  ```

  *(O con `uv`: `uv run python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"`)*
  Luego pegas el resultado generado en la linea `AIRFLOW_FERNET_KEY=` de tu `.env`.

En este mismo archivo `.env` puedes cambiar los usuarios y contraseñas si lo deseas:

| Variable                       | Para que sirve                                                            | Valor por defecto            |
| :----------------------------- | :------------------------------------------------------------------------ | :--------------------------- |
| `AIRFLOW_FERNET_KEY`         | Llave requerida para encriptar conexiones de Airflow                      | *(Ya viene lista en .env)* |
| `_AIRFLOW_WWW_USER_USERNAME` | Usuario para entrar a la pagina de Airflow (puerto 8080)                  | `admin`                    |
| `_AIRFLOW_WWW_USER_PASSWORD` | Clave para entrar a la pagina de Airflow (puerto 8080)                    | `admin_password`           |
| `AIRFLOW_DB_USER`            | Usuario de la base interna de Airflow (puerto 5434)                       | `airflow`                  |
| `AIRFLOW_DB_PASSWORD`        | Clave de la base interna de Airflow (puerto 5434)                         | `airflow_db_password`      |
| `AIRFLOW_DB_NAME`            | Nombre de la base interna de Airflow                                      | `airflow`                  |
| `DW_USER`                    | Usuario del Data Warehouse (puerto 5433 - **este usas en DBeaver**) | `admin_dw`                 |
| `DW_PASSWORD`                | Clave del Data Warehouse (puerto 5433 - **esta usas en DBeaver**)   | `analytics_password`       |
| `DW_DB`                      | Nombre de la base analitica                                               | `data_warehouse`           |

Nota importante sobre las claves:

- Si clonas el proyecto por primera vez: puedes colocar los usuarios y claves que quieras en tu `.env` antes de levantar Docker, y todo se creara con esos datos.
- Si ya habias levantado Docker antes y decides cambiar las claves en `.env`: PostgreSQL guarda los datos en su disco y no cambia las claves de una base ya creada. Para que tome tus nuevas credenciales limpias, corre en tu terminal:

  ```bash
  docker compose down -v
  docker compose up -d
  ```

  *(La bandera `-v` elimina los discos anteriores y arranca la base desde cero con tus nuevos usuarios y claves).*

---

### Paso 3: Levantar los servker Veras los siguientes contenedores corriendo:

- `postgres`: Base de datos de metadatos de Airflow (puerto 5434).
- `postgres-analytics`: Base de datos analitica donde se crean las tablas de ventas (puerto 5433).
- `airflow-apiserver`: Pagina web de Airflow (puerto 8080).
- `airflow-scheduler`, `airflow-dag-processor`, `airflow-triggerer`: Procesos internos que ejecutan los DAGs.
- `airflow-auto-trigger`: Contenedor que activa y manda a correr el DAG apenas todo este listo.

---

### Paso 4: Validar las Connections y Variables en Airflow

El archivo `docker-compose.yml` registra de forma automatica la conexion al Data Warehouse y la ruta de los archivos planos, para que no tengas que crearlas a mano en la interfaz.

Puedes comprobar que quedaron bien configuradas de dos formas:

#### Opcion A: Desde la pagina web de Airflow (Recomendado)

1. Abre tu navegador y entra a: `http://localhost:8080`
2. Inicia sesion con:
   - Usuario: `admin`
   - Clave: `admin_password` (o lo que pusiste en tu `.env`)
3. **Revisar la Connection (conexion a la base analitica):**
   - En el menu superior, ve a **Admin** -> **Connections**.
   - Busca en la lista la conexion llamada **`analytics_db`**.
   - Veras que apunta al host `postgres-analytics`, puerto `5432` y base `data_warehouse`.
4. **Revisar la Variable (ruta de los CSV):**
   - En el menu superior, ve a **Admin** -> **Variables**.
   - Busca la variable llamada **`RAW_CSV_PATH`**.
   - Su valor debe ser exactamente: `/opt/airflow/data`.

#### Opcion B: Por la terminal con comandos de Airflow

Si quieres verificarlo rapidamente sin abrir el navegador, corre estos dos comandos en tu terminal:

```bash
# Validar la conexion:
docker compose exec airflow-apiserver airflow connections get analytics_db

# Validar la variable:
docker compose exec airflow-apiserver airflow variables get RAW_CSV_PATH
```

---

### Paso 5: Monitorear la ejecucion del DAG

1. En la pagina web de Airflow (`http://localhost:8080`), entra a la pestaña **DAGs**.
2. Veras el DAG llamado **`datamart_etl_ventas`**.
3. Gracias al servicio `airflow-auto-trigger`, el DAG se despausa y se ejecuta automaticamente al levantar Docker.
4. Si haces clic sobre el nombre del DAG y entras a la vista de **Grid** o **Graph**, veras las tres tareas corriendo en orden:
   - `extract`: Lee los dos archivos CSV y los une.
   - `transform`: Limpia clientes, fechas, codigos de producto y separa errores.
   - `load`: Sube los datos limpios a PostgreSQL analitico.
5. Cuando las 3 tareas terminen con un borde verde oscuro (**Success**), los datos estaran cargados.

*(Nota: si alguna vez quieres volverlo a ejecutar manualmente, solo haz clic en el boton arriba a la derecha que dice **Trigger DAG**).*

---

### Paso 6: Verificar que los datos llegaron a PostgreSQL

Para confirmar que los datos se guardaron correctamente en la base analitica, puedes revisarlo con DBeaver o directamente desde tu terminal.

#### Opcion A: En DBeaver

Crea una nueva conexion de tipo **PostgreSQL** con estos datos:

- Host: `localhost`
- Port (Puerto): `5433` *(importante: es 5433, no 5432)*
- Database: `data_warehouse`
- Username: `admin_dw`
- Password: `analytics_password` (o la que pusiste en tu `.env`)

Abre un editor SQL y corre esta consulta para ver el total de filas cargadas:

```sql
SELECT 'fact_transactions' AS tabla, COUNT(*) AS total_filas FROM analytics.fact_transactions
UNION ALL
SELECT 'dim_products', COUNT(*) FROM analytics.dim_products
UNION ALL
SELECT 'daily_product_revenue', COUNT(*) FROM analytics.daily_product_revenue
UNION ALL
SELECT 'rejected_records', COUNT(*) FROM audit.rejected_records;
```

Deberas ver los siguientes numeros:

- `fact_transactions`: **535,463** filas (todas las transacciones de ventas y devoluciones validas).
- `dim_products`: **3,848** productos unicos en el catalogo.
- `daily_product_revenue`: **278,942** registros del resumen consolidado por dia y producto.
- `rejected_records`: **1,176** registros descartados por auditoria por cada ejecucion (ventas con precio <= 0).

#### Opcion B: Por terminal (sin abrir DBeaver)

Corre este comando en tu terminal para obtener el conteo de inmediato:

```bash
docker compose exec postgres-analytics psql -U admin_dw -d data_warehouse -c "
SELECT 'fact_transactions' AS tabla, COUNT(*) AS filas FROM analytics.fact_transactions
UNION ALL
SELECT 'dim_products', COUNT(*) FROM analytics.dim_products
UNION ALL
SELECT 'daily_product_revenue', COUNT(*) FROM analytics.daily_product_revenue
UNION ALL
SELECT 'rejected_records', COUNT(*) FROM audit.rejected_records;
"
```

---

## Consultas SQL y respuestas de negocio

Estas consultas se corren directo en la base de datos analitica (puerto 5433, base `data_warehouse`, usuario `admin_dw`). No necesitan vistas raras, consultan directamente las tablas base cargadas por el pipeline.

---

### 1. Evolucion mensual de ventas netas

Pregunta: ¿Como cambiaron las ventas netas mes a mes teniendo en cuenta las devoluciones?

```sql
SELECT 
    DATE_TRUNC('month', transaction_date)::DATE AS mes,
    TO_CHAR(transaction_date, 'YYYY-MM') AS anio_mes,
    SUM(gross_revenue) AS ventas_brutas,
    SUM(returns_adjustment) AS devoluciones_monto,
    SUM(net_revenue) AS ventas_netas,
    SUM(sales_quantity) AS unidades_vendidas,
    SUM(returns_quantity) AS unidades_devueltas,
    SUM(net_quantity) AS unidades_netas,
    ROUND(
        ABS(SUM(returns_adjustment)) / NULLIF(SUM(gross_revenue), 0) * 100,
        2
    ) AS tasa_devolucion_pct
FROM analytics.daily_product_revenue
GROUP BY 1, 2
ORDER BY mes;
```

Lo que muestran los datos:
Los datos van desde diciembre de 2010 hasta diciembre de 2011.
El mejor mes fue noviembre de 2011, con $1,455,608.80 en ventas netas ($1,503,329.78 en ventas brutas menos $47,720.98 en devoluciones), impulsado por compras previas a Navidad.
Los meses con mayor porcentaje de devoluciones fueron diciembre de 2011 (32.16%) y enero de 2011 (19.04%), debido a cambios y cancelaciones posteriores a las fiestas.

---

### 2. Categorias con mas ventas vs categorias con mas devoluciones

Pregunta: ¿Que categorias vendieron mas dinero y cuales tuvieron el mayor porcentaje de devoluciones?

```sql
SELECT 
    COALESCE(p.category, 'Sin categoría') AS categoria,
    COUNT(DISTINCT f.invoice_no) AS total_pedidos,
    SUM(CASE WHEN f.record_type = 'sale' THEN f.gross_revenue ELSE 0 END) AS gross_revenue_ventas,
    SUM(CASE WHEN f.record_type = 'return_or_adjustment' THEN f.gross_revenue ELSE 0 END) AS total_devoluciones_monto,
    SUM(f.gross_revenue) AS net_revenue,
    SUM(CASE WHEN f.record_type = 'sale' THEN f.quantity ELSE 0 END) AS unidades_vendidas,
    SUM(CASE WHEN f.record_type = 'return_or_adjustment' THEN f.quantity ELSE 0 END) AS unidades_devueltas,
    ROUND(
        ABS(SUM(CASE WHEN f.record_type = 'return_or_adjustment' THEN f.gross_revenue ELSE 0 END))
        / NULLIF(SUM(CASE WHEN f.record_type = 'sale' THEN f.gross_revenue ELSE 0 END), 0) * 100,
        2
    ) AS proporcion_devoluciones_pct
FROM analytics.fact_transactions f
LEFT JOIN analytics.dim_products p ON f.stock_code = p.stock_code
GROUP BY 1
ORDER BY gross_revenue_ventas DESC;
```

Lo que muestran los datos:

- Las que mas dinero vendieron: Hogar y Accesorios ($3,971,260.77 en ventas brutas), Cocina y Vajilla ($1,363,408.36), Iluminacion y Velas ($1,267,593.45) y Bolsas y Empaques ($1,151,190.71).
- Las que mas devoluciones tuvieron: Papeleria y Regalos tuvo una tasa de devolucion del 31.07% ($172,466.64 devueltos sobre $555,072.11 vendidos), seguida por Hogar y Accesorios (12.60%) y Almacenamiento y Cajas (10.68%).
- Las mas estables: Bolsas y Empaques (solo 1.72% de devolucion) y Navidad y Temporada (1.92%).

---

### 3. Top 10 productos con mas ventas netas y Top 10 con mas devoluciones

Pregunta: ¿Cuales son los 10 productos que dejaron mas dinero neto y los 10 que mas se devolvieron?

Top 10 en ventas netas:

```sql
SELECT 
    d.stock_code,
    COALESCE(d.canonical_description, p.canonical_description, 'UNKNOWN') AS descripcion,
    COALESCE(p.category, 'Sin categoría') AS categoria,
    SUM(d.gross_revenue) AS gross_revenue_ventas,
    SUM(d.returns_adjustment) AS devoluciones_monto,
    SUM(d.net_revenue) AS revenue_neto,
    SUM(d.net_quantity) AS unidades_netas
FROM analytics.daily_product_revenue d
LEFT JOIN analytics.dim_products p ON d.stock_code = p.stock_code
GROUP BY d.stock_code, d.canonical_description, p.canonical_description, p.category
ORDER BY revenue_neto DESC
LIMIT 10;
```

Los productos mas vendidos fueron:

- `DOT` (DOTCOM POSTAGE): $206,245.48 netos (705 unidades).
- `22423` (REGENCY CAKESTAND 3 TIER): $164,459.49 netos (12,943 unidades).
- `85123A` (WHITE HANGING HEART T-LIGHT HOLDER): $99,790.93 netos (35,099 unidades).
- `47566` (PARTY BUNTING): $98,243.88 netos (18,006 unidades).
- `85099B` (JUMBO BAG RED RETROSPOT): $92,175.79 netos (47,256 unidades).
- `23084` (RABBIT NIGHT LIGHT): $66,661.63 netos (29,663 unidades).
- `POST` (POSTAGE): $66,230.64 netos (3,003 unidades).
- `22086` (PAPER CHAIN KIT 50'S CHRISTMAS): $63,715.24 netos (18,876 unidades).
- `84879` (ASSORTED COLOUR BIRD ORNAMENT): $58,792.42 netos (36,122 unidades).
- `79321` (CHILLI LIGHTS): $53,746.66 netos (10,222 unidades).

Top 10 en porcentaje de devolucion (productos con mas de $1,000 en ventas):

```sql
SELECT 
    d.stock_code,
    COALESCE(d.canonical_description, p.canonical_description, 'UNKNOWN') AS descripcion,
    COALESCE(p.category, 'Sin categoría') AS categoria,
    SUM(d.sales_quantity) AS unidades_vendidas,
    SUM(d.returns_quantity) AS unidades_devueltas,
    SUM(d.gross_revenue) AS gross_revenue_ventas,
    SUM(d.returns_adjustment) AS devoluciones_monto,
    ROUND(
        ABS(SUM(d.returns_adjustment)) / NULLIF(SUM(d.gross_revenue), 0) * 100,
        2
    ) AS tasa_devolucion_pct
FROM analytics.daily_product_revenue d
LEFT JOIN analytics.dim_products p ON d.stock_code = p.stock_code
GROUP BY d.stock_code, d.canonical_description, p.canonical_description, p.category
HAVING SUM(d.gross_revenue) > 1000
ORDER BY tasa_devolucion_pct DESC
LIMIT 10;
```

Casos notables con devoluciones muy altas:

- `23843` (PAPER CRAFT, LITTLE BIRDIE): 100% de devolucion ($168,469.60 vendidos y los mismos $168,469.60 devueltos por un pedido grande cancelado).
- `23166` (MEDIUM CERAMIC TOP STORAGE JAR): 94.83% de devolucion ($77,479.64 devueltos).
- `85232B` (SET OF 3 BABUSHKA STACKING TINS): 92.76% de devolucion ($1,258.05 devueltos).
- `23113` (PANTRY CHOPPING BOARD): 81.84% de devolucion ($4,803.06 devueltos).

---

### 4. Paises con mas compras y valor promedio por pedido

Pregunta: ¿En que paises se concentran las ventas y cuanto gasta en promedio un cliente en cada pais?

```sql
SELECT 
    country AS pais,
    COUNT(*) AS total_lineas_transaccion,
    COUNT(DISTINCT invoice_no) AS total_pedidos,
    ROUND(
        COUNT(DISTINCT invoice_no)::NUMERIC 
        / (SELECT COUNT(DISTINCT invoice_no) FROM analytics.fact_transactions) * 100, 
        2
    ) AS pct_participacion_pedidos,
    SUM(gross_revenue) AS revenue_neto_total,
    ROUND(SUM(gross_revenue) / COUNT(DISTINCT invoice_no), 2) AS ticket_promedio_neto_pedido,
    ROUND(SUM(quantity) / COUNT(DISTINCT invoice_no), 2) AS unidades_promedio_pedido
FROM analytics.fact_transactions
GROUP BY country
ORDER BY total_pedidos DESC
LIMIT 10;
```

Lo que muestran los datos:

- Reino Unido concentra el 90.43% de todos los pedidos (22,727 facturas) y $8.19 millones en ventas netas ($8,188,700.45).
- Aunque la mayoria compra en Reino Unido (con un gasto promedio de $360.31 por pedido), los clientes internacionales compran pedidos mucho mas grandes:
  - Irlanda (EIRE): gasto promedio de $730.54 por pedido (mas del doble que en UK).
  - Francia: $428.02 por pedido.
  - Alemania: $367.35 por pedido.
  - Australia: $1,985.65 por pedido (compras mayoristas).
  - Paises Bajos (Netherlands): $2,846.62 por pedido (compras mayoristas).

---

### 5. Clientes registrados vs compras sin identificar

Pregunta: ¿Compran diferente los clientes registrados comparados con las compras donde no se pidio identificacion?

```sql
SELECT 
    CASE WHEN is_guest THEN 'Compras sin Customer ID (Anonimas)' ELSE 'Clientes Registrados' END AS tipo_cliente,
    COUNT(DISTINCT invoice_no) AS total_pedidos,
    COUNT(*) AS total_lineas_detalle,
    COUNT(DISTINCT customer_id) AS clientes_unicos_registrados,
    SUM(CASE WHEN record_type = 'sale' THEN gross_revenue ELSE 0 END) AS ventas_brutas,
    SUM(CASE WHEN record_type = 'return_or_adjustment' THEN gross_revenue ELSE 0 END) AS total_devoluciones_monto,
    SUM(gross_revenue) AS revenue_neto,
    ROUND(SUM(gross_revenue) / COUNT(DISTINCT invoice_no), 2) AS ticket_promedio_pedido,
    ROUND(
        ABS(SUM(CASE WHEN record_type = 'return_or_adjustment' THEN gross_revenue ELSE 0 END))
        / NULLIF(SUM(CASE WHEN record_type = 'sale' THEN gross_revenue ELSE 0 END), 0) * 100,
        2
    ) AS tasa_devolucion_pct
FROM analytics.fact_transactions
GROUP BY is_guest;
```

Lo que muestran los datos:

- Las compras sin identificar representan 2,946 pedidos (133,901 lineas de detalle) y sumaron $1,469,611.65 en ventas netas.
- El gasto promedio de quienes compran sin registrarse es de $498.85 por pedido, notablemente mas alto que el de los clientes registrados ($373.12).
- En clientes registrados se identificaron 4,371 clientes unicos con 22,186 pedidos y $8,277,967.57 en ventas netas.

---

### 6. Productos con nombres inconsistentes y total de codigos

Pregunta: ¿Que productos tienen nombres diferentes para el mismo codigo y cuantos codigos existen en total?

Total de codigos unicos:

```sql
SELECT COUNT(DISTINCT stock_code) AS total_codigos_unicos 
FROM analytics.dim_products;
```

Hay 3,848 codigos de producto en total.

Productos con nombres escritos de varias formas:

```sql
SELECT 
    f.stock_code,
    COALESCE(p.canonical_description, 'UNKNOWN') AS descripcion_oficial_canonica,
    COUNT(DISTINCT TRIM(UPPER(f.description))) AS cantidad_descripciones_distintas,
    STRING_AGG(DISTINCT TRIM(UPPER(f.description)), ' | ') AS ejemplos_descripciones
FROM analytics.fact_transactions f
LEFT JOIN analytics.dim_products p ON f.stock_code = p.stock_code
WHERE f.description IS NOT NULL AND TRIM(f.description) <> ''
GROUP BY f.stock_code, p.canonical_description
HAVING COUNT(DISTINCT TRIM(UPPER(f.description))) > 1
ORDER BY cantidad_descripciones_distintas DESC
```

Resultado:
Se encontraron 572 codigos de producto que venian con 2 o mas descripciones distintas en los archivos debido a errores al escribir, mayusculas, minusculas o abreviaciones. El pipeline soluciono esto calculando el nombre mas repetido (la moda) para cada producto y guardandolo como nombre oficial en `dim_products`.

---

## Recomendaciones para el equipo de producto

Viendo los resultados reales de los datos:

1. Control para pedidos mayoristas muy grandes:
   Hubo un pedido del producto `23843` por $168,469.60 que se cancelo por completo (100% de devolucion), y otro del producto `23166` que devolvio $77,479.64. Conviene poner una regla en la tienda para que pedidos mayores a $10,000 pasen por una revision previa o pidan anticipo antes de despachar del almacen.
2. Revisar la categoria Papeleria y Regalos:
   Es la que mas perdidas deja por devolucion (31.07%, mas de $172,400 en reembolsos). Seria bueno revisar la calidad del papel y el empaque para que no se arrugue ni se moje en el envio, ademas de mejorar las fotos en la pagina web para que el cliente sepa bien que esta comprando.
3. Motivar el registro de quienes compran como invitados:
   Hay 2,946 pedidos de gente que compra sin registrarse, dejando $1.47 millones con un ticket promedio de $498.85. Si se les da un incentivo (como un descuento del 5% o 10% en la siguiente compra al registrarse), la empresa podria guardar su correo para mandarles promociones y fidelizarlos.
4. Promocionar mas las ventas en Irlanda y el resto de Europa:
   Los clientes en Irlanda gastan en promedio $730.54 por pedido, el doble que en Reino Unido. Vale la pena hacer mas publicidad y ofrecer buenos costos de envio hacia Irlanda, Francia y Alemania, porque el tamano de sus compras deja buen margen.

---

## Reglas aplicadas a los datos

- Cantidad menor o igual a cero: Se clasifica como devolucion o ajuste (`return_or_adjustment`), nunca como venta regular.
- Precio unitario menor o igual a cero: En ventas normales, el precio debe ser mayor a cero. Si viene en cero o negativo, se separa y se guarda en la tabla `audit.rejected_records` explicando el motivo del descarte.
- Fechas a UTC: Todas las fechas se convierten a UTC para que no haya confusiones por zonas horarias.
- Codigos de producto: Se limpian quitando espacios y pasandolos a mayusculas.
- Ingreso bruto: Se calcula multiplicando `Quantity * UnitPrice`.
- Ingreso neto: Es el ingreso bruto menos las devoluciones del mismo producto en el mismo dia.

---

## Casos confusos y decisiones tomadas

1. Filas sin Customer ID:
   No se eliminaron porque representan cerca del 25% de los registros. Si se borraran, se perderia mas de $1.4 millones en ventas. Se dejaron con `customer_id = NULL` y se les puso una marca `is_guest = TRUE` para saber que fueron compras de invitados.
2. Varias descripciones para un mismo codigo:
   En vez de dejar descripciones repetidas o contradictorias, se calculo la descripcion que mas veces aparecia (la moda) en mayusculas y esa se dejo como nombre oficial en la dimension de productos.
3. Dos archivos CSV con informacion repetida:
   Al revisar `data.csv` y `online_retail.csv`, se encontro que ambos traian los mismos datos. Se le dio prioridad a `data.csv` y al aplicar deduplicacion quedaron 536,639 filas unicas.

---

## Como esta organizado el codigo

El trabajo se divide en tres pasos simples:

1. Extract (`extract.py`): Lee los dos archivos CSV, revisa las columnas y los junta.
2. Transform (`transform.py`, `validate.py`, `revenue.py`): Quita duplicados, limpia fechas, descripciones y clientes, aparta los registros invalidos a la tabla de rechazos y calcula los ingresos brutos y netos.
3. Load (`load.py`): Se conecta a PostgreSQL y sube los datos limpios a las tablas analiticas.

---

## Author

Luigui Garizado - Data Analysis
